package hyperdht

import (
	"context"
	"net"

	"github.com/golang/protobuf/proto"

	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"
)

// QueryResponse contains the information about peers on the hyperdht.
type QueryResponse struct {
	// The node that gave this response.
	Node net.Addr
	// The addresses for all peers the node had stored for the key
	Peers []net.Addr
}

type subStream = dhtRpc.QueryStream

// QueryStream parses the raw responses from the DHT RPC and emits them on its response channel.
//
// The dhtRpc.QueryStream is embedded in a way that makes all of its Public methods available
// to be called on this QueryStream even though it can't be directly accessed.
type QueryStream struct {
	*subStream

	ctx      context.Context
	respChan chan QueryResponse
}

// ResponseChan returns the channel that can be used to access all responses from the remote peers
// as they come in. The channel will be closed when the query is finished, so it is safe to range
// over. Also note that the query is back pressured by this channel, so it must be read from to
// continue with the query.
func (s *QueryStream) ResponseChan() <-chan QueryResponse { return s.respChan }

func (s *QueryStream) runMap() {
	defer close(s.respChan)

	for rawRes := range s.subStream.ResponseChan() {
		var res Response
		if err := proto.Unmarshal(rawRes.Value, &res); err != nil {
			continue
		}
		peers := decodeAllPeers(res.Peers)
		if peers == nil {
			continue
		}

		qRes := QueryResponse{
			Node:  rawRes.Node.Addr(),
			Peers: peers,
		}
		select {
		case s.respChan <- qRes:
		case <-s.ctx.Done():
			return
		}
	}
}

// CollectStream reads from a QueryStream's channels until the query is complete and returns
// all responses written the the response channel and the final error.
func CollectStream(stream *QueryStream) ([]QueryResponse, error) {
	var responses []QueryResponse
	for resp := range stream.respChan {
		responses = append(responses, resp)
	}
	return responses, <-stream.ErrorChan()
}
