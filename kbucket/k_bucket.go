// Package kbucket implements a Kademlia DHT K-Bucket.
// Implementation based on https://github.com/tristanls/k-bucket.
package kbucket

import (
	"crypto/rand"
	"sort"
	"sync"
)

// The default values for the Config struct.
const (
	DefaultBucketSize = 20
	DefaultPingCount  = 3
)

type (
	// FuncPing is called when a new contact will not fit in a bucket. It should ping all of
	// the current contacts, and re-add any that respond to move them to the end of the list.
	// If any do not respond they should be removed, and then the replacement contact should be added.
	FuncPing func(current []Contact, replacement Contact)

	// FuncArbiter resolves conflicts when two contacts with identical IDs are added.
	FuncArbiter func(incumbent, candidate Contact) Contact

	// FuncAddRemove is the signature for event handlers when contacts are added or removed
	FuncAddRemove func(c Contact)
	// FuncUpdate is the signature for event handlers when contacts are updated
	FuncUpdate func(incumbent, replacement Contact)
)

// The Config struct contains all of the configurable parameters of the KBucket.
type Config struct {
	LocalID     []byte
	BucketSize  int
	NodePingCnt int

	OnPing  FuncPing
	Arbiter FuncArbiter

	OnAdd    FuncAddRemove
	OnRemove FuncAddRemove
	OnUpdate FuncUpdate
}

// The KBucket struct implements a Kademlia DHT K-Bucket as a binary tree.
type KBucket struct {
	lock sync.RWMutex

	root       *bucketNode
	localID    []byte
	bucketsize int
	pingCnt    int

	onPing  FuncPing
	arbiter FuncArbiter

	onAdd    FuncAddRemove
	onRemove FuncAddRemove
	onUpdate FuncUpdate
}

// Arbiter sets the arbitration function. If nil is provided it will use the default function.
func (b *KBucket) Arbiter(f FuncArbiter) {
	b.lock.Lock()
	if f == nil {
		b.arbiter = Arbiter
	} else {
		b.arbiter = f
	}
	b.lock.Unlock()
}

// OnPing sets the ping handler. Only one function may the handler at a time. To ignore ping
// events and reject all contacts that would go into full buckets a nil value may be used.
func (b *KBucket) OnPing(f FuncPing) {
	b.lock.Lock()
	b.onPing = f
	b.lock.Unlock()
}

// OnAdd sets the ping handler. Only one function may the handler at a time. To ignore add
// events a nil value may be used.
//
// Because some applications require that the order OnAdd, OnUpdate, and OnRemove functions
// are called in is consistent with how they are added these functions are not called in
// separate go routines and the internal lock is held. As such it is recommended that these
// functions not perform any asynchronous task in their own routine.
func (b *KBucket) OnAdd(f FuncAddRemove) {
	b.lock.Lock()
	b.onAdd = f
	b.lock.Unlock()
}

// OnRemove sets the ping handler. Only one function may the handler at a time. To ignore remove
// events a nil value may be used.
//
// Because some applications require that the order OnAdd, OnUpdate, and OnRemove functions
// are called in is consistent with how they are added these functions are not called in
// separate go routines and the internal lock is held. As such it is recommended that these
// functions not perform any asynchronous task in their own routine.
func (b *KBucket) OnRemove(f FuncAddRemove) {
	b.lock.Lock()
	b.onRemove = f
	b.lock.Unlock()
}

// OnUpdate sets the ping handler. Only one function may the handler at a time. To ignore update
// events a nil value may be used.
//
// Because some applications require that the order OnAdd, OnUpdate, and OnRemove functions
// are called in is consistent with how they are added these functions are not called in
// separate go routines and the internal lock is held. As such it is recommended that these
// functions not perform any asynchronous task in their own routine.
func (b *KBucket) OnUpdate(f FuncUpdate) {
	b.lock.Lock()
	b.onUpdate = f
	b.lock.Unlock()
}

// add is the private version of the Add method, but because it is recursive we split it
// into a separate function to make the locking mechanism easier.
func (b *KBucket) add(c Contact) {
	id := c.ID()
	node := findNode(b.root, id)

	if index := node.indexOf(id); index >= 0 {
		incumbent := node.contacts[index]
		selection := b.arbiter(incumbent, c)

		// If we are to keep the incumbent and the contact we were given wasn't the incumbent
		// needing to be bumped to to newer part of the list then we do nothing
		if selection == incumbent && incumbent != c {
			return
		}

		node.removeIndex(index)
		node.addContact(selection)
		if b.onUpdate != nil {
			b.onUpdate(incumbent, selection)
		}
		return
	}

	// If the bucket has room for new contacts all we need to do is add this one.
	if node.size() < b.bucketsize {
		node.addContact(c)
		if b.onAdd != nil {
			b.onAdd(c)
		}
		return
	}

	// If the bucket is full and we are not allowed to split it we need to ping the
	// first `pingCnt` contacts to determine if they are alive. Only if one of the
	// pinged nodes doesn't repsond can we add the new contact.
	if node.dontSplit {
		if b.onPing != nil {
			cp := make([]Contact, b.pingCnt)
			copy(cp, node.contacts)
			go b.onPing(cp, c)
		}
		return
	}

	splitNode(node, b.localID)
	// Since we don't know the distribution of the now split nodes we need to re-run the
	// add function to make sure the new contact actually goes where it needs to.
	b.add(c)
}

// Add adds a new contact to the k-bucket.
func (b *KBucket) Add(c Contact) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.add(c)
}

// Get retrieves the contact with the matching ID. If no contacts in the tree match the provided
// ID it will return nil.
func (b *KBucket) Get(id []byte) Contact {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return findNode(b.root, id).getContact(id)
}

// Remove removes the contact with the matching ID from the k-bucket and returns it. If no
// contacts match the provided ID it will return nil.
func (b *KBucket) Remove(id []byte) Contact {
	b.lock.Lock()
	defer b.lock.Unlock()

	c := findNode(b.root, id).removeContact(id)
	if c != nil && b.onRemove != nil {
		b.onRemove(c)
	}
	return c
}

// Count returns the total number of contacts in the K-Bucket.
func (b *KBucket) Count() int {
	b.lock.RLock()
	defer b.lock.RUnlock()
	var result int

	nodes := []*bucketNode{b.root}
	for i := 0; i < len(nodes); i++ {
		if node := nodes[i]; node.contacts == nil {
			nodes = append(nodes, node.left, node.right)
		} else {
			result += len(node.contacts)
		}
	}

	return result
}

// Contacts creates and returns a slice of all Contacts stored in the K-Bucket. The order
// will tend to be from farther to closer relative to the local ID, but might not be exact.
func (b *KBucket) Contacts() []Contact {
	b.lock.RLock()
	defer b.lock.RUnlock()
	var result []Contact

	nodes := []*bucketNode{b.root}
	for i := 0; i < len(nodes); i++ {
		if node := nodes[i]; node.contacts == nil {
			nodes = append(nodes, node.left, node.right)
		} else {
			result = append(result, node.contacts...)
		}
	}

	return result
}

// Closest gets the `cnt` closest contacts based on the definition of "closest" provided by the
// distance comparer provided. The default XOR-based distance is implemented by XORDistance.
// If `cnt` is < 0 it will return all contacts sorted based on the distance.
func (b *KBucket) Closest(cmp DistanceCmp, cnt int) []Contact {
	if cmp == nil || cnt == 0 {
		return nil
	}
	list := b.Contacts()
	sort.Sort(distSorter{list: list, cmp: cmp})

	if cnt < 0 || len(list) <= cnt {
		return list
	}
	return list[:cnt]
}

// New creates a new KBucket instance
func New(cfg *Config) *KBucket {
	if cfg == nil {
		cfg = new(Config)
	}
	c := *cfg

	if c.LocalID == nil {
		c.LocalID = make([]byte, 20)
		rand.Read(c.LocalID)
	}
	if c.BucketSize == 0 {
		c.BucketSize = DefaultBucketSize
	}
	if c.NodePingCnt == 0 {
		c.NodePingCnt = DefaultPingCount
	}
	if c.Arbiter == nil {
		c.Arbiter = Arbiter
	}

	result := &KBucket{
		root:       createNode(0),
		localID:    c.LocalID,
		bucketsize: c.BucketSize,
		pingCnt:    c.NodePingCnt,

		onPing:  c.OnPing,
		arbiter: c.Arbiter,

		onAdd:    c.OnAdd,
		onRemove: c.OnRemove,
		onUpdate: c.OnUpdate,
	}
	return result
}

// Arbiter is the default implementation of the FuncArbiter. It simply returns the candidate.
func Arbiter(incumbent, candidate Contact) Contact {
	return candidate
}
