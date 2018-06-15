package dhtRpc

import (
	"net"

	"gitlab.daplie.com/core-sdk/hyperdht/ipEncoding"
	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
)

const (
	peerEnc = ipEncoding.IPv4Encoder(0)
	nodeEnc = ipEncoding.IPv4Encoder(IDSize)
)

func encodePeer(addr net.Addr) []byte {
	list := []ipEncoding.Node{basicNode{addr: addr}}
	return peerEnc.Encode(list)
}
func decodePeer(buf []byte) net.Addr {
	list := peerEnc.Decode(buf)
	if len(list) > 0 {
		return list[0].Addr()
	}
	return nil
}

func encodeNodes(nodes []kbucket.Contact) []byte {
	list := make([]ipEncoding.Node, 0, len(nodes))
	for _, c := range nodes {
		if n, ok := c.(ipEncoding.Node); ok {
			list = append(list, n)
		}
	}

	return nodeEnc.Encode(list)
}
func decodeNodes(buf []byte) []ipEncoding.Node {
	return nodeEnc.Decode(buf)
}
