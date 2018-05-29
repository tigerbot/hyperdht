package dhtRpc

import (
	"bytes"
	"context"
	"net"

	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
)

// A Query contains the information that will be sent to other peers.
type Query struct {
	Command string
	Target  []byte
	Value   []byte
}

// A QueryResponse contains a node that responded to a Query and the information they used
// to respond.
type QueryResponse struct {
	Node  Node
	Value []byte
}

// QueryOpts contains the different options that can be used to control query behavior.
type QueryOpts struct {
	Nodes               []Node
	DisableHolepunching bool
}

type queryStream struct {
	ctx   context.Context
	dht   *DHT
	query *Query

	pending    nodeList
	closest    nodeList
	moveCloser bool
	holepunch  bool

	responses chan QueryResponse
}

func (s *queryStream) send(node *queryNode, force, useToken bool) {
	if !force {
		if node.queried {
			return
		}
		node.queried = true
	}

	req := &Request{
		Command: &s.query.Command,
		Id:      s.dht.queryID,
		Target:  s.query.Target,
		Value:   s.query.Value,
	}
	if useToken {
		req.RoundtripToken = node.roundTripToken
	}

	res, err := s.dht.request(s.ctx, node.addr, req)
	if s.holepunch && err != nil && node.referrer != nil {
		if err = s.dht.Holepunch(s.ctx, node.addr, node.referrer); err == nil {
			res, err = s.dht.request(s.ctx, node.addr, req)
		}
	}

	select {
	case <-s.ctx.Done():
		return
	default:
	}

	if err != nil {
		// The node we were trying to reach might not have been in our list, but if it was it's
		// a bad node and we don't want it in our list anymore.
		if node.id != nil {
			s.dht.nodes.Remove(node.id)
		}
		// TODO? other error handling/tracking.
		return
	}

	s.addClosest(node.addr, res)
	if s.moveCloser {
		for _, n := range decodeIPv4Nodes(res.GetNodes()) {
			s.addPending(n, node.addr)
		}
	}

	if len(res.Id) != IDSize {
		return
	}

	queryRes := QueryResponse{
		Node: &basicNode{
			id:   res.Id,
			addr: node.addr,
		},
		Value: res.Value,
	}
	select {
	case s.responses <- queryRes:
	case <-s.ctx.Done():
	}
}

func (s *queryStream) addPending(node Node, ref net.Addr) {
	if node == nil || bytes.Equal(node.ID(), s.dht.id[:]) {
		return
	}

	qNode := &queryNode{
		basicNode: basicNode{
			id:   node.ID(),
			addr: node.Addr(),
		},
		referrer: ref,
	}
	s.pending.insert(qNode)
}
func (s *queryStream) addClosest(peer net.Addr, res *Response) {
	id := res.GetId()
	if id == nil || res.GetRoundtripToken() == nil || bytes.Equal(id, s.dht.id[:]) {
		return
	}

	prev := s.pending.get(id)
	if prev == nil {
		prev = &queryNode{
			basicNode: basicNode{
				id:   id,
				addr: peer,
			},
		}
	}
	prev.roundTripToken = res.RoundtripToken

	s.closest.insert(prev)
}

func newQueryStream(ctx context.Context, dht *DHT, query *Query, opts *QueryOpts) *queryStream {
	const k = 20
	if opts == nil {
		opts = new(QueryOpts)
	}

	result := &queryStream{
		ctx:   ctx,
		dht:   dht,
		query: query,

		pending:    nodeList{k, kbucket.XORDistance(query.Target), nil},
		closest:    nodeList{k, kbucket.XORDistance(query.Target), nil},
		moveCloser: len(opts.Nodes) == 0,
		holepunch:  !opts.DisableHolepunching,

		responses: make(chan QueryResponse),
	}

	if len(opts.Nodes) > 0 {
		for _, n := range opts.Nodes {
			result.addPending(n, nil)
		}
	} else {
		bootstap := dht.nodes.Closest(kbucket.XORDistance(query.Target), k)
		for _, c := range bootstap {
			if n, ok := c.(Node); ok {
				result.addPending(n, nil)
			}
		}

		// TODO: handle initial bootstrapping before nodes is populated.
	}

	return result
}
