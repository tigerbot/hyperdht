// Package kbucket implements a Kademlia DHT K-Bucket using a binary tree.
// It is intended to be as minimal as possible and makes as few assumptions as possible about
// what is being stored. It also makes no assumptions about how an ID is generated or how long
// it must be, and allows for contacts whose ID length differs from that of the bucket.
//
// Implementation is based on https://github.com/tristanls/k-bucket.
package kbucket

import (
	"crypto/rand"
	"fmt"
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
	FuncPing[C Contact] func(current []C, replacement C)

	// FuncArbiter resolves conflicts when two contacts with identical IDs are added.
	FuncArbiter[C Contact] func(incumbent, candidate C) C

	// FuncAddRemove is the signature for event handlers when contacts are added or removed
	FuncAddRemove[C Contact] func(c C)
	// FuncUpdate is the signature for event handlers when contacts are updated
	FuncUpdate[C Contact] func(incumbent, replacement C)
)

// The Config struct contains all of the configurable parameters of the KBucket.
type Config[C Contact] struct {
	LocalID     []byte
	BucketSize  int
	NodePingCnt int

	OnPing  FuncPing[C]
	Arbiter FuncArbiter[C]

	OnAdd    FuncAddRemove[C]
	OnRemove FuncAddRemove[C]
	OnUpdate FuncUpdate[C]
}

// The KBucket struct implements a Kademlia DHT K-Bucket as a binary tree.
type KBucket[C Contact] struct {
	lock sync.RWMutex

	root       *bucketNode[C]
	localID    []byte
	bucketsize int
	pingCnt    int

	onPing  FuncPing[C]
	arbiter FuncArbiter[C]

	onAdd    FuncAddRemove[C]
	onRemove FuncAddRemove[C]
	onUpdate FuncUpdate[C]
}

// Arbiter sets the arbitration function. If nil is provided it will use the default function.
func (b *KBucket[C]) Arbiter(f FuncArbiter[C]) {
	b.lock.Lock()
	if f == nil {
		b.arbiter = Arbiter[C]
	} else {
		b.arbiter = f
	}
	b.lock.Unlock()
}

// OnPing sets the ping handler. Only one function may the handler at a time. To ignore ping
// events and reject all contacts that would go into full buckets a nil value may be used.
func (b *KBucket[C]) OnPing(f FuncPing[C]) {
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
func (b *KBucket[C]) OnAdd(f FuncAddRemove[C]) {
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
func (b *KBucket[C]) OnRemove(f FuncAddRemove[C]) {
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
func (b *KBucket[C]) OnUpdate(f FuncUpdate[C]) {
	b.lock.Lock()
	b.onUpdate = f
	b.lock.Unlock()
}

// add is the private version of the Add method, but because it is recursive we split it
// into a separate function to make the locking mechanism easier.
func (b *KBucket[C]) add(c C) {
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
	// pinged nodes doesn't respond can we add the new contact.
	if node.dontSplit {
		if b.onPing != nil {
			cp := make([]C, b.pingCnt)
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
func (b *KBucket[C]) Add(c C) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.add(c)
}

// Get retrieves the contact with the matching ID. If no contacts in the tree match the provided
// ID it will return nil.
func (b *KBucket[C]) Get(id []byte) C {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return findNode(b.root, id).getContact(id)
}

// Remove removes the contact with the matching ID from the k-bucket and returns it. If no
// contacts match the provided ID it will return nil.
func (b *KBucket[C]) Remove(id []byte) C {
	b.lock.Lock()
	defer b.lock.Unlock()

	var zero C
	c := findNode[C](b.root, id).removeContact(id)
	if c != zero && b.onRemove != nil {
		b.onRemove(c)
	}
	return c
}

// Count returns the total number of contacts in the K-Bucket.
func (b *KBucket[C]) Count() int {
	b.lock.RLock()
	defer b.lock.RUnlock()
	var result int

	nodes := []*bucketNode[C]{b.root}
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
func (b *KBucket[C]) Contacts() []C {
	b.lock.RLock()
	defer b.lock.RUnlock()

	result := []C{}
	nodes := []*bucketNode[C]{b.root}
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
func (b *KBucket[C]) Closest(cmp DistanceCmp, cnt int) []C {
	if cmp == nil || cnt == 0 {
		return nil
	}
	list := b.Contacts()
	sort.Sort(distSorter[C]{list: list, cmp: cmp})

	if cnt < 0 || len(list) <= cnt {
		return list
	}
	return list[:cnt]
}

// New creates a new KBucket instance
func New[C Contact](cfg *Config[C]) *KBucket[C] {
	if cfg == nil {
		cfg = new(Config[C])
	}
	c := *cfg

	if c.LocalID == nil {
		c.LocalID = make([]byte, 20)
		if n, err := rand.Read(c.LocalID); err != nil || n != 20 {
			panic(fmt.Errorf("read %d/20 expected random bytes for ID: %w", n, err))
		}
	}
	if c.BucketSize == 0 {
		c.BucketSize = DefaultBucketSize
	}
	if c.NodePingCnt == 0 {
		c.NodePingCnt = DefaultPingCount
	}
	if c.Arbiter == nil {
		c.Arbiter = Arbiter[C]
	}

	result := &KBucket[C]{
		root:       createNode[C](0),
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
func Arbiter[C Contact](incumbent, candidate C) C {
	return candidate
}
