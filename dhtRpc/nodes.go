package dhtRpc

import (
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
