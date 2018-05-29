package dhtRpc

import (
	"bytes"
	"context"
	"net"
	"sync"

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
	Concurrency         int
	DisableHolepunching bool
	Verbose             bool

	isUpdate bool
}

type queryStream struct {
	ctx   context.Context
	dht   *DHT
	query *Query

	lock     sync.Mutex
	cond     sync.Cond
	inflight int

	isUpdate   bool
	verbose    bool
	committing bool

	pending    nodeList
	closest    nodeList
	moveCloser bool
	holepunch  bool

	responses chan QueryResponse
}

func (s *queryStream) spawnRoutines(concurrency int) {
	var wait sync.WaitGroup

	wait.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go s.drainLists(&wait)
	}

	wait.Wait()
	close(s.responses)
}

func (s *queryStream) drainLists(wait *sync.WaitGroup) {
	defer wait.Done()
	s.lock.Lock()
	defer s.lock.Unlock()
	// Make sure that if we have a reason to exit that all other routines can wake up to see
	// that reason for themselves and also exit.
	defer s.cond.Broadcast()

	var list *nodeList
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		if s.committing {
			list = &s.closest
		} else {
			list = &s.pending
		}

		if node := list.getUnqueried(); node != nil {
			node.queried = true
			s.inflight++
			s.lock.Unlock()
			s.send(node, s.committing)
			s.lock.Lock()
			s.inflight--
			// We might have just added multiple pending nodes, so wake everyone up to check.
			s.cond.Broadcast()
		} else if s.inflight > 0 {
			s.cond.Wait()
		} else if s.isUpdate && !s.committing {
			s.committing = true
			// We've just switched lists, so we should have a fresh batch of work for all
			// the other routines to help out with.
			s.cond.Broadcast()
		} else {
			return
		}
	}
}

func (s *queryStream) send(node *queryNode, useToken bool) {
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

	s.lock.Lock()
	s.addClosest(node.addr, res)
	if s.moveCloser {
		for _, n := range decodeIPv4Nodes(res.GetNodes()) {
			s.addPending(n, node.addr)
		}
	}
	s.lock.Unlock()

	if len(res.Id) != IDSize || (s.isUpdate && !s.committing && !s.verbose) {
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

	// We always create a new node so we can make sure the queried property is false
	// without messing with that property in the pending list.
	qNode := &queryNode{
		basicNode: basicNode{
			id:   id,
			addr: peer,
		},
		roundTripToken: res.RoundtripToken,
	}
	if prev := s.pending.get(id); prev != nil {
		qNode.referrer = prev.referrer
	}

	s.closest.insert(qNode)
}

func newQueryStream(ctx context.Context, dht *DHT, query *Query, opts *QueryOpts) *queryStream {
	const k = 20
	if opts == nil {
		opts = new(QueryOpts)
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = dht.concurrency
	}

	result := &queryStream{
		ctx:   ctx,
		dht:   dht,
		query: query,

		isUpdate:   opts.isUpdate,
		verbose:    opts.Verbose,
		committing: false,

		pending:    nodeList{k, kbucket.XORDistance(query.Target), nil},
		closest:    nodeList{k, kbucket.XORDistance(query.Target), nil},
		moveCloser: len(opts.Nodes) == 0,
		holepunch:  !opts.DisableHolepunching,

		responses: make(chan QueryResponse),
	}
	result.cond.L = &result.lock

	if len(opts.Nodes) > 0 {
		// Make sure the lists are big enough to fit all the nodes we are told to query.
		result.pending.capacity = len(opts.Nodes)
		result.closest.capacity = len(opts.Nodes)
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
	go result.spawnRoutines(opts.Concurrency)

	return result
}
