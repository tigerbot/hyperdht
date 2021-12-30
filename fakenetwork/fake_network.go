// Package fakenetwork provides an emulated UDP-like network for testing purposes.
package fakenetwork

import (
	"context"
	"net"
	"sync"
)

// FakeNetwork emulates a UDP network where some nodes can be behind a NAT and not accessible
// to addresses the node has not previously attempted to contact.
type FakeNetwork struct {
	// Forcefully makes all nodes in the network public. Useful if a single tests needs a
	// public network, but a convenience function used for other tests makes nodes private.
	DisableNAT bool

	ctx   context.Context
	done  context.CancelFunc
	lock  sync.RWMutex
	nodes map[string]*FakeNode
}

// New creates and initializes a new FakeNetwork.
func New() *FakeNetwork {
	result := &FakeNetwork{
		nodes: make(map[string]*FakeNode),
	}
	result.ctx, result.done = context.WithCancel(context.Background())
	return result
}

// NewNode creates a new node on the network with the specified address. If public is true
// then any node on the network will be able to connect to it, otherwise only nodes that this
// node has tried to talk to first will be able to send messages.
func (n *FakeNetwork) NewNode(addr net.Addr, public bool) *FakeNode {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.nodes[addr.String()] != nil {
		panic("network already has a node with address " + addr.String())
	}

	result := &FakeNode{
		network:  n,
		addr:     addr,
		public:   public,
		tunneled: make(map[string]bool),
		packets:  make(chan dataPkts),
	}
	result.ctx, result.done = context.WithCancel(n.ctx)

	n.nodes[addr.String()] = result
	return result
}

func (n *FakeNetwork) getNode(remote, local string) *FakeNode {
	n.lock.RLock()
	defer n.lock.RUnlock()
	node := n.nodes[remote]
	if node == nil {
		return nil
	}

	node.lock.RLock()
	defer node.lock.RUnlock()
	if n.DisableNAT || node.public || node.tunneled[local] {
		return node
	}
	return nil
}
func (n *FakeNetwork) rmNode(node *FakeNode) {
	n.lock.Lock()
	if addr := node.addr.String(); n.nodes[addr] == node {
		delete(n.nodes, addr)
	}
	n.lock.Unlock()
}

// Close disables the entire network and also closes all of the attached nodes.
func (n *FakeNetwork) Close() error {
	n.done()
	n.lock.Lock()
	n.nodes = nil
	n.lock.Unlock()
	return nil
}
