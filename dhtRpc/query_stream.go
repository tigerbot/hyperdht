package dhtRpc

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"

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
	lock  sync.Mutex
	dht   *DHT
	query *Query

	concurrency int32
	respCnt     int
	commitCnt   int
	isUpdate    bool
	verbose     bool

	bootstrap  map[string]*queryNode
	pending    queryNodeList
	closest    queryNodeList
	moveCloser bool
	holepunch  bool

	respChan chan QueryResponse
	errChan  chan error
	warnChan chan error
}

type noResponseErr string

func (e noResponseErr) Error() string   { return string(e) }
func (e noResponseErr) Temporary() bool { return true }
func (e noResponseErr) Timeout() bool   { return true }

// ResponseChan returns the channel that can be used to access all responses from the remote peers
// as they come in. The channel will be closed when the query is finished, so it is safe to range
// over. Also note that the query is back pressured by this channel, so it must be read from to
// continue with the query.
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

func (s *QueryStream) runStream() {
	atomic.AddInt32(&s.dht.inflightQueries, 1)
	defer atomic.AddInt32(&s.dht.inflightQueries, -1)

	s.lock.Lock()
	defer s.lock.Unlock()
	cond := sync.Cond{L: &s.lock}

	var committing bool
	defer func() {
		if err := s.ctx.Err(); err != nil {
			s.errChan <- err
		} else if s.respCnt == 0 {
			s.errChan <- noResponseErr("no nodes responded")
		} else if committing && s.commitCnt == 0 {
			s.errChan <- noResponseErr("no close nodes responded to update")
		} else {
			s.errChan <- nil
		}
		close(s.respChan)
	}()

	var inflight int32
	send := func(node *queryNode) {
		s.send(node, committing)
		atomic.AddInt32(&inflight, -1)
		cond.Broadcast()
	}

	list := &s.pending
	for {
		for atomic.LoadInt32(&inflight) >= atomic.LoadInt32(&s.concurrency) {
			cond.Wait()
		}
		if s.ctx.Err() != nil {
			for atomic.LoadInt32(&inflight) > 0 {
				cond.Wait()
			}
			return
		}

		if node := list.getUnqueried(); node != nil {
			node.queried = true
			go send(node)
			atomic.AddInt32(&inflight, 1)
		} else if atomic.LoadInt32(&inflight) > 0 {
			cond.Wait()
		} else if s.isUpdate && !committing {
			committing = true
			list = &s.closest
		} else {
			return
		}
	}
}

func (s *QueryStream) send(node *queryNode, isUpdate bool) {
	req := &request{
		Command: &s.query.Command,
		Id:      s.dht.queryID,
		Target:  s.query.Target,
		Value:   s.query.Value,
	}
	if isUpdate {
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
	if isUpdate {
		s.commitCnt++
	}

	s.addClosest(node.addr, res)
	if s.moveCloser {
		for _, n := range s.dht.encoder.Decode(res.GetNodes()) {
			s.addPending(n, node.addr)
		}
	}
	s.lock.Unlock()

	if len(res.Id) != IDSize || (s.isUpdate && !isUpdate && !s.verbose) {
		return
	}

	queryRes := QueryResponse{
		Node: basicNode{
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

func (s *QueryStream) addBootstrap(addr net.Addr) {
	qNode := new(queryNode)
	qNode.addr = addr
	s.bootstrap[addr.String()] = qNode
	s.pending.insert(qNode)
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

	// All of the bootstrap nodes were added without IDs, but they might not all be ephemeral.
	// To avoid duplicating queries we check to see if any of our bootstrap nodes match the
	// address of this node, and if so we mark it as queried if the bootstrap node was already
	// queried. (And mark the bootstrap node as queried if it wasn't so the new node with the
	// valid ID take priority over it.)
	if b := s.bootstrap[node.Addr().String()]; b != nil {
		qNode.queried, b.queried = b.queried, true
	}

	s.pending.insert(qNode)
}
func (s *QueryStream) addClosest(peer net.Addr, res *response) {
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

		concurrency: int32(opts.Concurrency),
		isUpdate:    opts.isUpdate,
		verbose:     opts.Verbose,

		pending:    queryNodeList{k, kbucket.XORDistance(query.Target), nil},
		closest:    queryNodeList{k, kbucket.XORDistance(query.Target), nil},
		moveCloser: len(opts.Nodes) == 0,
		holepunch:  !opts.DisableHolepunching,

		respChan: make(chan QueryResponse),
		errChan:  make(chan error, 1),
		warnChan: make(chan error),
	}

	if len(opts.Nodes) > 0 {
		// Make sure the lists are big enough to fit all the nodes we are told to query.
		result.pending.capacity = len(opts.Nodes)
		result.closest.capacity = len(opts.Nodes)
		for _, n := range opts.Nodes {
			result.addPending(n, nil)
		}
	} else {
		bootstrap := dht.closest(query.Target, k)
		for _, c := range bootstrap {
			if n, ok := c.(Node); ok {
				result.addPending(n, nil)
			}
		}

		// We don't have a full list of close nodes, which probably either means we haven't
		// bootstrapped at all yet, or our list has decayed (assuming that's even possible)
		// and we need to bootstrap again.
		if len(bootstrap) < len(dht.bootstrap) && len(bootstrap) < k {
			result.bootstrap = make(map[string]*queryNode, len(dht.bootstrap))
			for _, addr := range dht.bootstrap {
				result.addBootstrap(addr)
			}
		}
	}
	go result.runStream()

	return result
}

// CollectStream reads from a QueryStream's channels until the query is complete and returns
// all responses written the the response channel and the final error.
func CollectStream(stream *QueryStream) ([]QueryResponse, error) {
	var responses []QueryResponse
	for resp := range stream.ResponseChan() {
		responses = append(responses, resp)
	}
	return responses, <-stream.ErrorChan()
}

// CollectValues reads from a QueryStream's channels until the query is complete and returns
// all non-nil values from all responses written to the response channel and the final error.
func CollectValues(stream *QueryStream) ([][]byte, error) {
	var result [][]byte
	for resp := range stream.ResponseChan() {
		if resp.Value != nil {
			result = append(result, resp.Value)
		}
	}

	return result, <-stream.ErrorChan()
}

// DiscardStream reads from a QueryStream's response channel to make sure the stream isn't back
// pressured, but it discards all responses, only returning the final error.
func DiscardStream(stream *QueryStream) error {
	for range stream.ResponseChan() {
	}

	return <-stream.ErrorChan()
}
