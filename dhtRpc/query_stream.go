package dhtRpc

import (
	"bytes"
	"context"
	"net"
	"sync"

	"github.com/pkg/errors"
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

// A QueryStream holds all of the context needed to make queries or updates to remote peers.
type QueryStream struct {
	ctx   context.Context
	dht   *DHT
	query *Query

	lock     sync.Mutex
	cond     sync.Cond
	inflight int

	respCnt   int
	commitCnt int

	isUpdate   bool
	verbose    bool
	committing bool

	pending    nodeList
	closest    nodeList
	moveCloser bool
	holepunch  bool

	respChan chan QueryResponse
	errChan  chan error
	warnChan chan error
}

// ResponseChan returns the channel that can be used to access all responses from the remote peers
// as they come in. The channel will be closed when the query is finished, so it is safe to range
// over.
func (s *QueryStream) ResponseChan() <-chan QueryResponse { return s.respChan }

// ErrorChan returns the channel that can be used to access any error encountered at the end of the
// query. It is buffered and will be written to exactly once immediately before the response
// channel is closed.
func (s *QueryStream) ErrorChan() <-chan error { return s.errChan }

// WarningChan returns the channel that can be used to access any errors connecting to individual
// remote peers.
func (s *QueryStream) WarningChan() <-chan error { return s.warnChan }

// ResponseCnt returns the number of responses received from remote peers. It might be bigger than
// the number of responses sent over the response channel because it will include responses from
// peers with invalid IDs and responses received while getting closer to relevant node for an
// update (which will only be sent over the channel in verbose mode).
func (s *QueryStream) ResponseCnt() int {
	s.lock.Lock()
	result := s.respCnt
	s.lock.Unlock()
	return result
}

// CommitCnt returns the number of responses received from the update requests.
func (s *QueryStream) CommitCnt() int {
	s.lock.Lock()
	result := s.commitCnt
	s.lock.Unlock()
	return result
}

func (s *QueryStream) bootstap(peers []net.Addr) {
	bg := func(addr net.Addr) {
		node := new(queryNode)
		node.addr = addr
		s.send(node, false)

		s.lock.Lock()
		s.inflight--
		s.cond.Broadcast()
		s.lock.Unlock()
	}

	for i := range peers {
		// We need to make sure the inflight value is incremented in this routine and not a
		// sub routine. Otherwise we will have no guarantee it will be updated before the
		// list drainers run and see no nodes pending and no requests in flight.
		s.inflight++
		go bg(peers[i])
	}
}
func (s *QueryStream) spawnRoutines(concurrency int) {
	var wait sync.WaitGroup
	wait.Add(concurrency)

	// We acquire the lock and check the inflight number before spawning the routines to make
	// sure we don't spin up too fast when we need to bootstrap.
	s.lock.Lock()
	for i := 0; i < concurrency; i++ {
		for s.inflight >= concurrency {
			s.cond.Wait()
		}
		go s.drainLists(&wait)
	}
	s.lock.Unlock()

	wait.Wait()
	if err := s.ctx.Err(); err != nil {
		s.errChan <- err
	} else if s.respCnt == 0 {
		s.errChan <- errors.New("no nodes responded")
	} else if s.committing && s.commitCnt == 0 {
		s.errChan <- errors.New("no close node responded to update")
	} else {
		s.errChan <- nil
	}
	close(s.respChan)
}

func (s *QueryStream) drainLists(wait *sync.WaitGroup) {
	defer wait.Done()
	s.lock.Lock()
	defer s.lock.Unlock()
	// Make sure that if we have a reason to exit that all other routines can wake up to see
	// that reason for themselves and also exit.
	defer s.cond.Broadcast()

	var list *nodeList
	for {
		if s.ctx.Err() != nil {
			return
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

func (s *QueryStream) send(node *queryNode, useToken bool) {
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

	if s.ctx.Err() != nil {
		return
	}
	if err != nil {
		// The node we were trying to reach might not have been in our list, but if it was it's
		// a bad node and we don't want it in our list anymore.
		if node.id != nil {
			s.dht.nodes.Remove(node.id)
		}

		select {
		case s.warnChan <- err:
		default:
		}

		return
	}

	s.lock.Lock()
	s.respCnt++
	if s.committing {
		s.commitCnt++
	}

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
	case s.respChan <- queryRes:
	case <-s.ctx.Done():
	}
}

func (s *QueryStream) addPending(node Node, ref net.Addr) {
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
func (s *QueryStream) addClosest(peer net.Addr, res *Response) {
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

func newQueryStream(ctx context.Context, dht *DHT, query *Query, opts *QueryOpts) *QueryStream {
	const k = 20
	if query == nil || query.Target == nil {
		panic("query.Target is required")
	}
	if opts == nil {
		opts = new(QueryOpts)
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = dht.concurrency
	}

	result := &QueryStream{
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

		respChan: make(chan QueryResponse),
		errChan:  make(chan error, 1),
		warnChan: make(chan error),
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
		bootstrap := dht.nodes.Closest(kbucket.XORDistance(query.Target), k)
		for _, c := range bootstrap {
			if n, ok := c.(Node); ok {
				result.addPending(n, nil)
			}
		}

		// We don't have a full list of close nodes, which probably either means we haven't
		// bootstrapped at all yet, or our list has decayed (assuming that's even possible)
		// and we need to bootstrap again.
		if len(bootstrap) < len(dht.bootstrap) && len(bootstrap) < k {
			result.bootstap(dht.bootstrap)
		}
	}
	go result.spawnRoutines(opts.Concurrency)

	return result
}

// CollectStream reads from a QueryStream's channels until the query is complete and returns
// all responses written the the response channel and the final error.
func CollectStream(stream *QueryStream) ([]QueryResponse, error) {
	var responses []QueryResponse
	for resp := range stream.respChan {
		responses = append(responses, resp)
	}
	return responses, <-stream.errChan
}
