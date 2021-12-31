package kbucket

import (
	"bytes"
)

// The Contact interface represents any type that can be stored in the K-Bucket.
type Contact interface {
	comparable
	ID() []byte
}

type bucketNode[C Contact] struct {
	contacts  []C
	dontSplit bool
	bitIndex  int
	left      *bucketNode[C]
	right     *bucketNode[C]
}

func (n *bucketNode[C]) indexOf(id []byte) int {
	for i, c := range n.contacts {
		if bytes.Equal(c.ID(), id) {
			return i
		}
	}

	return -1
}

func (n *bucketNode[C]) size() int {
	return len(n.contacts)
}
func (n *bucketNode[C]) addContact(c C) {
	n.contacts = append(n.contacts, c)
}
func (n *bucketNode[C]) getContact(id []byte) C {
	for _, c := range n.contacts {
		if bytes.Equal(c.ID(), id) {
			return c
		}
	}

	var zero C
	return zero
}
func (n *bucketNode[C]) removeContact(id []byte) C {
	for i, c := range n.contacts {
		if bytes.Equal(c.ID(), id) {
			n.contacts = append(n.contacts[:i], n.contacts[i+1:]...)
			return c
		}
	}

	var zero C
	return zero
}
func (n *bucketNode[C]) removeIndex(i int) C {
	c := n.contacts[i]
	n.contacts = append(n.contacts[:i], n.contacts[i+1:]...)
	return c
}

func createNode[C Contact](bitIndex int) *bucketNode[C] {
	return &bucketNode[C]{contacts: []C{}, bitIndex: bitIndex}
}

func findNode[C Contact](node *bucketNode[C], id []byte) *bucketNode[C] {
	for node.contacts == nil {
		// First determine the relevant byte index and create a mask that will extract the correct
		// bit from the relevant byte. Remember that index 0 refers to the most significant bit
		// So we start with 128 and shift down by the index.
		byteIndex, byteMask := node.bitIndex/8, byte(128>>uint(node.bitIndex%8))

		// ID's that are too short get put in the low bucket (as if we padded the end with zeros).
		if len(id) <= byteIndex || id[byteIndex]&byteMask == 0 {
			node = node.left
		} else {
			node = node.right
		}
	}
	return node
}

func splitNode[C Contact](node *bucketNode[C], localID []byte) {
	// First convert this node from a leave node into an inner tree node, making sure to save
	// the contacts to be redistributed to the new children leaves.
	var contacts []C
	contacts, node.contacts = node.contacts, nil
	node.left, node.right = createNode[C](node.bitIndex+1), createNode[C](node.bitIndex+1)

	for _, c := range contacts {
		findNode[C](node, c.ID()).addContact(c)
	}

	// Determine which node the local ID would be placed in so we can mark the other one
	// to not be split.
	if localNode := findNode[C](node, localID); localNode == node.left {
		node.right.dontSplit = true
	} else {
		node.left.dontSplit = true
	}
}
