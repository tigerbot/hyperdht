package dhtRpc

import (
	"bytes"
	"net"

	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
)

// A Node represents another peer
type Node interface {
	kbucket.Contact
	Addr() net.Addr
}

type basicNode struct {
	id   []byte
	addr net.Addr
}

func (n *basicNode) ID() []byte     { return n.id }
func (n *basicNode) Addr() net.Addr { return n.addr }

type storedNode struct {
	basicNode
	tick uint64

	roundTripToken []byte
	prev, next     *storedNode
}

type queryNode struct {
	basicNode

	queried        bool
	roundTripToken []byte
	referrer       net.Addr
}

type nodeList struct {
	capacity int
	distCmp  kbucket.DistanceCmp
	list     []*queryNode
}

func (l *nodeList) get(id []byte) *queryNode {
	for _, n := range l.list {
		if bytes.Equal(id, n.id[:]) {
			return n
		}
	}
	return nil
}

func (l *nodeList) getUnqueried() *queryNode {
	for _, n := range l.list {
		if !n.queried {
			return n
		}
	}

	return nil
}

func (l *nodeList) insert(n *queryNode) {
	if len(l.list) >= l.capacity && l.distCmp.Closer(l.list[len(l.list)-1].id, n.id) {
		return
	}
	if l.get(n.id) != nil {
		return
	}

	if len(l.list) < l.capacity {
		l.list = append(l.list, n)
	} else {
		l.list[len(l.list)-1] = n
	}

	for pos := len(l.list) - 1; pos > 0 && l.distCmp.Closer(l.list[pos-1].id, l.list[pos].id); pos-- {
		l.list[pos-1], l.list[pos] = l.list[pos], l.list[pos-1]
	}
}
