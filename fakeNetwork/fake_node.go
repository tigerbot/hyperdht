package fakeNetwork

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

var _ net.PacketConn = new(FakeNode)

// FakeNode implements net.PacketConn and can send messages to other nodes that it can get from
// the FakeNetwork that created it.
type FakeNode struct {
	ctx  context.Context
	done context.CancelFunc
	lock sync.RWMutex

	network  *FakeNetwork
	addr     net.Addr
	public   bool
	tunneled map[string]bool
	packets  chan dataPkts
}
type dataPkts struct {
	data []byte
	addr net.Addr
}

// ReadFrom implements the net.PacketConn ReadFrom method. I'm not sure if this is how a normal
// UDP PacketConn works, but if more data is received than can fit in the provided buffer it
// is discarded.
func (n *FakeNode) ReadFrom(b []byte) (int, net.Addr, error) {
	if n.network == nil {
		panic("cannot use unattached FakeNode")
	}

	select {
	case <-n.ctx.Done():
		return 0, nil, errors.New("use of closed fake NATed node")
	case pkt := <-n.packets:
		return copy(b, pkt.data), pkt.addr, nil
	}
}

// WriteTo implements the net.PacketConn WriteTo method. Because it is emulating a UDP network
// it will not error if it cannot reach the specified address. However unlike a UDP PacketConn
// it can block until the message is successfully delivered.
//
// TODO? make it possible to set the buffer size of the channel used to allow packets to be
// dropped when the network traffic reaches a certain level. I tried this earlier, but it
// caused more packets to be dropped than actually happens when using UDP.
func (n *FakeNode) WriteTo(b []byte, addr net.Addr) (int, error) {
	if n.network == nil {
		panic("cannot use unattached FakeNode")
	}
	if n.ctx.Err() != nil {
		return 0, errors.New("use of closed fake NATed node")
	}

	n.lock.Lock()
	n.tunneled[addr.String()] = true
	n.lock.Unlock()

	if remote := n.network.getNode(addr.String(), n.addr.String()); remote != nil {
		cp := make([]byte, len(b))
		copy(cp, b)
		select {
		// We could have a default case here to simulate a UDP network being overloaded, but
		// we probably shouldn't do that until we're setting the request timeout in the tests.
		case <-n.ctx.Done():
		case <-remote.ctx.Done():
		case remote.packets <- dataPkts{data: cp, addr: n.addr}:
		}
	}

	// We are simulating a UDP network with no acknowlegement, so never return error
	return len(b), nil
}

// Close implements the net.PacketConn Close method. It will removed the node from the network
// so no other nodes can send messages to it and interrupt any active ReadsFrom calls.
func (n *FakeNode) Close() error {
	if n.network == nil {
		panic("cannot use unattached FakeNode")
	}
	n.network.rmNode(n.addr.String())
	n.done()
	return nil
}

// LocalAddr implements the net.PacketConn LocalAddr method.
func (n *FakeNode) LocalAddr() net.Addr { return n.addr }

// SetDeadline implements the net.PacketConn SetDeadline method. It is currently a no-op.
func (n *FakeNode) SetDeadline(t time.Time) error { return nil }

// SetReadDeadline implements the net.PacketConn SetReadDeadline method. It is currently a no-op.
func (n *FakeNode) SetReadDeadline(t time.Time) error { return nil }

// SetWriteDeadline implements the net.PacketConn SetWriteDeadline method. It is currently a no-op.
func (n *FakeNode) SetWriteDeadline(t time.Time) error { return nil }
