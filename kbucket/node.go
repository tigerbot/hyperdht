package kbucket

import (
	"bytes"
)

// The Contact interface represents any type that can be stored in the K-Bucket.
type Contact interface {
	ID() []byte
}

type bucketNode struct {
	contacts  []Contact
	dontSplit bool
	bitIndex  int
	left      *bucketNode
	right     *bucketNode
}

func (n *bucketNode) indexOf(id []byte) int {
	for i, c := range n.contacts {
		if bytes.Equal(c.ID(), id) {
			return i
		}
	}

	return -1
}

func (n *bucketNode) size() int {
	return len(n.contacts)
}
func (n *bucketNode) addContact(c Contact) {
	n.contacts = append(n.contacts, c)
}
func (n *bucketNode) getContact(id []byte) Contact {
	for _, c := range n.contacts {
		if bytes.Equal(c.ID(), id) {
			return c
		}
	}

	return nil
}
func (n *bucketNode) removeContact(id []byte) Contact {
	for i, c := range n.contacts {
		if bytes.Equal(c.ID(), id) {
			n.contacts = append(n.contacts[:i], n.contacts[i+1:]...)
			return c
		}
	}

	return nil
}
func (n *bucketNode) removeIndex(i int) Contact {
	c := n.contacts[i]
	n.contacts = append(n.contacts[:i], n.contacts[i+1:]...)
	return c
}

func createNode(bitIndex int) *bucketNode {
	return &bucketNode{contacts: []Contact{}, bitIndex: bitIndex}
}

func findNode(node *bucketNode, id []byte) *bucketNode {
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

func splitNode(node *bucketNode, localID []byte) {
	// First convert this node from a leave node into an inner tree node, making sure to save
	// the contacts to be redistributed to the new children leaves.
	var contacts []Contact
	contacts, node.contacts = node.contacts, nil
	node.left, node.right = createNode(node.bitIndex+1), createNode(node.bitIndex+1)

	for _, c := range contacts {
		findNode(node, c.ID()).addContact(c)
	}

	// Determine which node the local ID would be placed in so we can mark the other one
	// to not be split.
	if localNode := findNode(node, localID); localNode == node.left {
		node.right.dontSplit = true
	} else {
		node.left.dontSplit = true
	}

}
