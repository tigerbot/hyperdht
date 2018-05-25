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
)

// The Config struct contains all of the configurable parameters of the KBucket.
type Config struct {
	LocalID     []byte
	BucketSize  int
	NodePingCnt int

	OnPing  FuncPing
	Arbiter FuncArbiter
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

// add is the private version of the Add method, but because it is recursive we split it
// into a separate function to make the locking mechanism easier.
func (b *KBucket) add(c Contact) {
	node := findNode(b.root, c.ID())

	if ind := node.indexOf(c.ID()); ind >= 0 {
		if incumbent := node.contacts[ind]; incumbent == c {
			// If the reference is exactly the same (probable in the event that the onPing
			// re-adds contacts that respond) we shouldn't need to call the arbiter, we
			// just need to move the contact to the end of the list.
			node.removeContact(c.ID())
			node.addContact(c)
		} else if selection := b.arbiter(incumbent, c); selection != incumbent {
			// Otherwise if the arbiter returns something not the incumbent then we need to
			// remove the incumbent and add the returned value (might not be `c` either).
			node.removeContact(incumbent.ID())
			node.addContact(selection)
		}
		return
	}

	// If the bucket has room for new contacts all we need to do is add this one.
	if node.size() < b.bucketsize {
		node.addContact(c)
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
	return findNode(b.root, id).removeContact(id)
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
func New(c *Config) *KBucket {
	if c == nil {
		c = new(Config)
	}
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
	}
	return result
}

// Arbiter is the default implementation of the FuncArbiter. It simply returns the candidate.
func Arbiter(incumbent, candidate Contact) Contact {
	return candidate
}
