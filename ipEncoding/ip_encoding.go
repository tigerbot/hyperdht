package ipEncoding

import (
	"net"
	"strconv"
)

// A Node represents any value that can be encoded using this package.
type Node interface {
	ID() []byte
	Addr() net.Addr
}

// BasicNode is a simple struct that implements the Node interface. It is also the type
// used in the slice when decoding.
type BasicNode struct {
	MyID   []byte
	MyAddr net.Addr
}

// ID implements the Node interface
func (n BasicNode) ID() []byte { return n.MyID }

// Addr implements the Node interface
func (n BasicNode) Addr() net.Addr { return n.MyAddr }

func encodeIPv4Addr(peer net.Addr) []byte {
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

func decodeIPv4Addr(buf []byte) net.Addr {
	if len(buf) != 6 {
		return nil
	}
	return &net.UDPAddr{
		IP:   net.IP(buf[:4]),
		Port: int(buf[4])<<8 | int(buf[5]),
	}
}

// IPv4Encoder converts back and forth between Nodes and their binary encoding. The IPv4Encoder
// value represents the number of bytes expected as the ID.
type IPv4Encoder int

// Encode converts the list of provided nodes to their binary encoding. Since this is an IPv4
// encoder any nodes that have IPv6 addresses are unsupported and are simply excluded from
// the final encoding. Likewise any nodes whose ID length does not match the value for the
// encoder are excluded.
func (e IPv4Encoder) Encode(nodes []Node) []byte {
	idSize := int(e)
	totalSize := idSize + 6

	buf := make([]byte, 0, len(nodes)*totalSize)
	for _, node := range nodes {
		if id := node.ID(); len(id) != idSize {
			// TODO? log this as some sort of warning
		} else if enc := encodeIPv4Addr(node.Addr()); enc == nil {
			// TODO? log this as some sort of warning
		} else {
			buf = append(buf, id...)
			buf = append(buf, enc...)
		}
	}
	return buf
}

// Decode converts the binary buffer to a list of Nodes. If the length of the buffer is not
// exactly a length that is possible given the ID size it cannot be sure the buffer is actually
// encoded peers and will return nil.
func (e IPv4Encoder) Decode(buf []byte) []Node {
	idSize := int(e)
	totalSize := idSize + 6
	if l := len(buf); l == 0 || l%totalSize != 0 {
		return nil
	}

	result := make([]Node, len(buf)/totalSize)
	for i := range result {
		start := i * totalSize
		node := BasicNode{MyID: make([]byte, idSize)}
		copy(node.MyID, buf[start:start+idSize])
		node.MyAddr = decodeIPv4Addr(buf[start+idSize : start+totalSize])
		result[i] = node
	}

	return result
}
