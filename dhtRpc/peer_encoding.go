package dhtRpc

import (
	"net"
	"strconv"

	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
)

type contactWithAddr interface {
	kbucket.Contact
	Addr() net.Addr
}

// A Node represents another peer
type Node struct {
	id   [IDSize]byte
	addr net.Addr
	tick uint64

	roundTripToken []byte
	prev, next     *Node
}

// ID returns the ID of the node on the network.
func (n *Node) ID() []byte {
	cp := n.id
	return cp[:]
}

// Addr returns the network address of the node (as we see it).
func (n *Node) Addr() net.Addr {
	return n.addr
}

func encodeIPv4Peer(peer net.Addr) []byte {
	var host net.IP
	var port int
	if udp, ok := peer.(*net.UDPAddr); ok {
		host, port = udp.IP, udp.Port
	} else if tcp, ok := peer.(*net.TCPAddr); ok {
		host, port = tcp.IP, tcp.Port
	} else {
		if hostStr, portStr, err := net.SplitHostPort(peer.String()); err != nil {
			// TODO? log these kinds of errors
		} else {
			host = net.ParseIP(hostStr)
			port64, _ := strconv.ParseInt(portStr, 10, 32)
			port = int(port64)
		}
	}

	if host == nil || port == 0 || host.To4() == nil {
		return nil
	}
	buf := make([]byte, 6)
	copy(buf, host.To4())
	buf[4], buf[5] = byte((port&0xff00)>>8), byte(port&0x00ff)
	return buf
}
func encodeIPv4Nodes(nodes []kbucket.Contact) []byte {
	const totalSize = IDSize + 6

	buf := make([]byte, 0, len(nodes)*totalSize)
	for _, c := range nodes {
		if node, ok := c.(contactWithAddr); !ok {
			// TODO? log this as some sort of warning
		} else if id := node.ID(); len(id) != IDSize {
			// TODO? log this as some sort of warning
		} else if enc := encodeIPv4Peer(node.Addr()); enc == nil {
			// TODO? log this as some sort of warning
		} else {
			buf = append(buf, id...)
			buf = append(buf, enc...)
		}
	}
	return buf
}

func decodeIPv4Peer(buf []byte) net.Addr {
	if len(buf) != 6 {
		return nil
	}
	return &net.UDPAddr{
		IP:   net.IP(buf[:4]),
		Port: int(buf[4])<<8 | int(buf[5]),
	}
}
