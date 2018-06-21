package dhtRpc

import (
	"net"

	"gitlab.daplie.com/core-sdk/hyperdht/ipEncoding"
	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
)

var nodeEnc = ipEncoding.NodeEncoder{
	IPEncoder: ipEncoding.IPv4Encoder{},
	IDSize:    IDSize,
}

func encodePeer(addr net.Addr) []byte { return nodeEnc.EncodeAddr(addr) }
func decodePeer(buf []byte) net.Addr  { return nodeEnc.DecodeAddr(buf) }

func encodeNodes(nodes []kbucket.Contact) []byte {
	list := make([]ipEncoding.Node, 0, len(nodes))
	for _, c := range nodes {
		if n, ok := c.(ipEncoding.Node); ok {
			list = append(list, n)
		}
	}

	return nodeEnc.Encode(list)
}
func decodeNodes(buf []byte) []ipEncoding.Node { return nodeEnc.Decode(buf) }
