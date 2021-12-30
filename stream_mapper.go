package hyperdht

import (
	"context"
	"net"

	"google.golang.org/protobuf/proto"

	"github.com/tigerbot/hyperdht/dhtrpc"
	"github.com/tigerbot/hyperdht/ipencoding"
)

// QueryOpts contains all of the options that can be included in a Lookup or Announce query.
type QueryOpts struct {
	// LocalAddr is the address the service is listening on in the local network. If non-nil
	// it will assume your CIDR is 16 and include any peers that announced being on a local
	// address in the same subnet in the LocalPeers part of the response.
	// It currently only supports IPv4 addresses, even if the hyperdht node can use IPv6
	LocalAddr net.Addr

	// Specifying a non-zero port will tell the remote peers to store an address using this
	// port instead of the one they see.
	Port int
}

// QueryResponse contains the information about peers on the hyperdht.
type QueryResponse struct {
	// The node that gave this response.
	Node net.Addr
	// The addresses for all peers the node had stored for the key
	Peers []net.Addr
	// The addresses for all peers that are listening on a local address in the same subnet
	// as the address that was provided in the QueryOpts (assuming CIDR of 16).
	LocalPeers []net.Addr
}

type subStream = dhtrpc.QueryStream

// QueryStream parses the raw responses from the DHT RPC and emits them on its response channel.
//
// The dhtrpc.QueryStream is embedded in a way that makes all of its Public methods available
// to be called on this QueryStream even though it can't be directly accessed.
type QueryStream struct {
	*subStream
	localAddr []byte
	encoder   ipencoding.IPEncoder

	ctx      context.Context
	respChan chan QueryResponse
}

// ResponseChan returns the channel that can be used to access all responses from the remote peers
// as they come in. The channel will be closed when the query is finished, so it is safe to range
// over. Also note that the query is back pressured by this channel, so it must be read from to
// continue with the query.
func (s *QueryStream) ResponseChan() <-chan QueryResponse { return s.respChan }

func (s *QueryStream) runMap(selfAddr net.Addr, selfRes *response) {
	defer close(s.respChan)

	handleResponse := func(from net.Addr, res *response) {
		peers := s.decodeAllPeers(res.Peers)
		if peers == nil {
			return
		}

		qRes := QueryResponse{
			Node:       from,
			Peers:      peers,
			LocalPeers: s.decodeLocalPeers(res.LocalPeers),
		}
		select {
		case s.respChan <- qRes:
		case <-s.ctx.Done():
			return
		}
	}

	if selfRes != nil {
		handleResponse(selfAddr, selfRes)
	}
	for rawRes := range s.subStream.ResponseChan() {
		var res response
		if err := proto.Unmarshal(rawRes.Value, &res); err == nil {
			handleResponse(rawRes.Node.Addr(), &res)
		}
	}
}

func (s *QueryStream) decodeAllPeers(buf []byte) []net.Addr {
	encLen := s.encoder.EncodedLen()
	if l := len(buf); l == 0 || l%encLen != 0 {
		return nil
	}

	result := make([]net.Addr, len(buf)/encLen)
	for i := range result {
		result[i], buf = s.encoder.DecodeAddr(buf[:encLen]), buf[encLen:]
	}
	return result
}
func (s *QueryStream) decodeLocalPeers(buf []byte) []net.Addr {
	if len(s.localAddr) != 6 || len(buf) == 0 || len(buf)%4 != 0 {
		return nil
	}

	cp := make([]byte, 6)
	copy(cp, s.localAddr[:2])
	list := make([]net.Addr, len(buf)/4)
	for i := range list {
		cp = append(cp[:2], buf[4*i:4*(i+1)]...)
		list[i] = ipencoding.IPv4Encoder.DecodeAddr(cp)
	}
	return list
}

// CollectStream reads from a QueryStream's channels until the query is complete and returns
// all responses written the the response channel and the final error.
func CollectStream(stream *QueryStream) ([]QueryResponse, error) {
	responses := []QueryResponse{}
	for resp := range stream.ResponseChan() {
		responses = append(responses, resp)
	}
	return responses, <-stream.ErrorChan()
}
