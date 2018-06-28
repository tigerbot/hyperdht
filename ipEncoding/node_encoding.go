package ipEncoding

import (
	"net"
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

// NodeEncoder converts back and forth between Nodes and their binary encoding.
// The NodeEncoder value represents the number of bytes expected as the ID.
type NodeEncoder struct {
	IPEncoder
	IDSize int
}

// Encode converts the list of provided nodes to their binary encoding. Any nodes whose address
// cannot be encoded using the IPEncoder or whose ID does not match the expected ID size are
// excluded from the final encoding.
func (e NodeEncoder) Encode(nodes []Node) []byte {
	totalSize := e.IDSize + e.EncodedLen()

	buf := make([]byte, 0, len(nodes)*totalSize)
	for _, node := range nodes {
		if id := node.ID(); len(id) != e.IDSize {
			// TODO? log this as some sort of warning
		} else if enc := e.EncodeAddr(node.Addr()); enc == nil {
			// TODO? log this as some sort of warning
		} else {
			buf = append(buf, id...)
			buf = append(buf, enc...)
		}
	}
	return buf
}

// Decode converts the binary buffer to a list of Nodes. If the length of the buffer is not
// exactly a length that is possible given the IP encoder and ID size it cannot be sure the
// buffer is actually encoded peers and will return nil.
func (e NodeEncoder) Decode(buf []byte) []Node {
	totalSize := e.IDSize + e.EncodedLen()
	if l := len(buf); l == 0 || l%totalSize != 0 {
		return nil
	}

	result := make([]Node, len(buf)/totalSize)
	var chunk []byte
	for i := range result {
		chunk, buf = buf[:totalSize], buf[totalSize:]

		idBuf := make([]byte, e.IDSize)
		copy(idBuf, chunk)
		result[i] = BasicNode{
			MyID:   idBuf,
			MyAddr: e.DecodeAddr(chunk[e.IDSize:]),
		}
	}

	return result
}
