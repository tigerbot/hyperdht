// Package kbucket implements a Kademlia DHT K-Bucket.
// Implementation based on https://github.com/tristanls/k-bucket.
package kbucket

import (
	"crypto/rand"
	"sort"
	"sync"
)

// The Config struct contains all of the configurable parameters of the KBucket.
type Config struct {
	LocalID     []byte
	BucketSize  int
	NodePingCnt int
}

// The KBucket struct implements a Kademlia DHT K-Bucket as a binary tree.
type KBucket struct {
	lock sync.RWMutex

	root       *bucketNode
	localID    []byte
	bucketsize int
	pingCnt    int
}

// Add adds a new contact to the k-bucket.
func (b *KBucket) Add(c Contact) {
	b.lock.Lock()
	defer b.lock.Unlock()
	node := findNode(b.root, c.ID())

	if ind := node.indexOf(c.ID()); ind >= 0 {
		// TODO: implement update
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
		// TODO: implement ping
		return
	}

	splitNode(node, b.localID)
	findNode(node, c.ID()).addContact(c)
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
func (b *KBucket) Closest(cmp DistanceCmp, cnt int) []Contact {
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
		c.BucketSize = 20
	}
	if c.NodePingCnt == 0 {
		c.NodePingCnt = 3
	}

	result := &KBucket{
		root:       createNode(0),
		localID:    c.LocalID,
		bucketsize: c.BucketSize,
		pingCnt:    c.NodePingCnt,
	}
	return result
}
