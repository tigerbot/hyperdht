package dhtRpc

import (
	"net"

	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
	"gitlab.daplie.com/core-sdk/hyperdht/peerEncoding"
)

const (
	peerEnc = peerEncoding.IPv4Encoder(0)
	nodeEnc = peerEncoding.IPv4Encoder(IDSize)
)

func encodePeer(addr net.Addr) []byte {
	list := []peerEncoding.Node{basicNode{addr: addr}}
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
	list := make([]peerEncoding.Node, 0, len(nodes))
	for _, c := range nodes {
		if n, ok := c.(peerEncoding.Node); ok {
			list = append(list, n)
		}
	}

	return nodeEnc.Encode(list)
}
func decodeNodes(buf []byte) []peerEncoding.Node {
	return nodeEnc.Decode(buf)
}
