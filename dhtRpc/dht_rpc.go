// Package dhtRpc makes calls over a Kademlia based DHT.
// It is the go implementation of the `dht-rpc` node library by mafintosh.
package dhtRpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
	"gitlab.daplie.com/core-sdk/hyperdht/udpRequest"
)

const (
	// IDSize is the number of bytes we expect in an ID
	IDSize = 32

	secretCnt      = 2
	secretSize     = 32
	secretLifetime = 5 * time.Minute
	tickInterval   = 5 * time.Second
)

// A QueryHandler handles query events from remote peers.
type QueryHandler func(Node, *Query) ([]byte, error)

func noopHandler(Node, *Query) ([]byte, error) { return nil, nil }

// Config contains all of the options available for a DHT instance
type Config struct {
	ID          []byte
	Ephemeral   bool
	Concurrency int
	BootStrap   []net.Addr

	// Allows for custom socket types or instances to be used. If Socket is nil a new net.UDPConn
	// is created that will listen on the specified port.
	Socket net.PacketConn
	Port   int
}

// A DHT holds everything needed to send RPC calls over a distributed hash table.
type DHT struct {
	id          [IDSize]byte
	queryID     []byte
	concurrency int
	rateLimiter chan bool
	bootstrap   []net.Addr

	// secrets and handlers have nothing in common except that they need to be thread safe and
	// will change very rarely compared to how often they are used, so they share an RWLock
	lock     sync.RWMutex
	secrets  [secretCnt][]byte
	handlers map[string]QueryHandler

	tick   uint64
	top    *storedNode
	bottom *storedNode
	nodes  *kbucket.KBucket

	socket *udpRequest.UDPRequest
	done   chan struct{}
}

// ID returns the ID being used by the DHT.
func (d *DHT) ID() []byte {
	return d.id[:]
}

// Addr returns the network address the socket underlying the DHT is using.
func (d *DHT) Addr() net.Addr {
	return d.socket.Addr()
}

// Nodes returns all of the nodes currently stored in our local part of the table.
func (d *DHT) Nodes() []Node {
	stored := d.nodes.Contacts()
	result := make([]Node, 0, len(stored))
	for _, c := range stored {
		if n, ok := c.(Node); !ok {
			d.nodes.Remove(c.ID())
		} else {
			result = append(result, n)
		}
	}
	return result
}

func (d *DHT) updateTick() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.done:
			return

		case <-ticker.C:
			if tick := atomic.AddUint64(&d.tick, 1); tick&7 == 0 {
				d.pingSome()
			}
		}
	}
}
func (d *DHT) pingSome() {
	curTick := d.tick
	oldest := d.bottom
	for i := 0; i < 3 && oldest != nil; i++ {
		if curTick-oldest.tick >= 3 {
			go d.check(oldest)
		}
		oldest = oldest.next
	}
}
func (d *DHT) check(n *storedNode) bool {
	if _, err := d.Ping(context.TODO(), n.addr); err != nil {
		d.nodes.Remove(n.ID())
		return false
	}
	// We shouldn't need to update the node when the ping succeeds because that should
	// already be happening elsewhere every time any type of request succeeds.
	return true
}

// onNodeAdd, onNodeRemove, and onNodeUpdate all handle changes in the kbucket storage so we can
// maintain a list of all nodes sorted by last contact time (instead of sorted by distance to
// our ID and then last contact time). We use this list to periodically ping the oldest nodes.
func (d *DHT) onNodeAdd(c kbucket.Contact) {
	n, ok := c.(*storedNode)
	if !ok {
		d.nodes.Remove(c.ID())
		return
	}

	if d.top == nil && d.bottom == nil {
		d.top, d.bottom = n, n
		n.prev, n.next = nil, nil
	} else {
		n.prev, n.next = d.top, nil
		d.top.next = n
		d.top = n
	}
}
func (d *DHT) onNodeRemove(c kbucket.Contact) {
	n, ok := c.(*storedNode)
	if !ok {
		return
	}

	if d.bottom != n && d.top != n {
		n.prev.next = n.next
		n.next.prev = n.prev
	} else {
		if d.bottom == n {
			d.bottom = n.next
			if d.bottom != nil {
				d.bottom.prev = nil
			}
		}
		if d.top == n {
			d.top = n.prev
			if d.top != nil {
				d.top.next = nil
			}
		}
	}
	n.next, n.prev = nil, nil
}
func (d *DHT) onNodeUpdate(old, fresh kbucket.Contact) {
	d.onNodeRemove(old)
	d.onNodeAdd(fresh)
}

func (d *DHT) makeToken(peer net.Addr) []byte {
	d.lock.RLock()
	defer d.lock.RUnlock()

	h := sha256.New()
	h.Write(d.secrets[0])
	h.Write([]byte(peer.String()))
	return h.Sum(nil)
}
func (d *DHT) validToken(peer net.Addr, token []byte) bool {
	d.lock.RLock()
	defer d.lock.RUnlock()

	h := sha256.New()
	for _, s := range d.secrets {
		h.Reset()
		h.Write(s)
		h.Write([]byte(peer.String()))
		if bytes.Equal(h.Sum(nil), token) {
			return true
		}
	}

	return false
}
func (d *DHT) rotateSecrets() {
	ticker := time.NewTicker(secretLifetime)
	defer ticker.Stop()

	for {
		select {
		case <-d.done:
			return

		case <-ticker.C:
			d.lock.Lock()
			for i := secretCnt - 1; i > 0; i-- {
				d.secrets[i] = d.secrets[i-1]
			}
			d.secrets[0] = make([]byte, secretSize)
			rand.Read(d.secrets[0])
			d.lock.Unlock()
		}
	}
}

func (d *DHT) addNode(peer net.Addr, id []byte) {
	if len(id) != IDSize || bytes.Equal(d.id[:], id) {
		return
	}

	node := new(storedNode)
	node.id = id
	node.addr = peer
	node.tick = atomic.LoadUint64(&d.tick)

	d.nodes.Add(node)
}

func (d *DHT) forwardRequest(from *udpRequest.PeerRequest, req *Request) {
	if req.GetCommand() != "_ping" {
		return
	}

	to := decodeIPv4Peer(req.ForwardRequest)
	if to == nil {
		return
	}

	req.ForwardRequest = nil
	req.ForwardResponse = encodeIPv4Peer(from.Addr)
	if buf, err := proto.Marshal(req); err == nil {
		d.socket.ForwardRequest(from, to, buf)
	}
}
func (d *DHT) forwardResponse(peer *udpRequest.PeerRequest, req *Request) *udpRequest.PeerRequest {
	if req.GetCommand() != "_ping" {
		return nil
	}

	to := decodeIPv4Peer(req.ForwardResponse)
	if to == nil {
		return nil
	}

	cp := *peer
	cp.Addr = to
	return &cp
}

func (d *DHT) onNodePing(current []kbucket.Contact, replacement kbucket.Contact) {
	if _, ok := replacement.(*storedNode); !ok {
		return
	}
	curTick := atomic.LoadUint64(&d.tick)
	reping := make([]*storedNode, 0, len(current))

	for _, c := range current {
		// The k-bucket shouldn't ever have the wrong type, but handle it just in case
		if node, ok := c.(*storedNode); !ok {
			d.nodes.Remove(c.ID())
			d.nodes.Add(replacement)
			return
		} else if curTick-node.tick >= 3 {
			// More than 10 seconds since we pinged this node, so make sure it's still active
			reping = append(reping, node)
		}
	}

	for _, n := range reping {
		// If check returns false it already removed the stale contact so we can add the new one.
		if !d.check(n) {
			d.nodes.Add(replacement)
			return
		}
	}
}

func (d *DHT) onPing(p *udpRequest.PeerRequest, req *Request) {
	res := &Response{
		Id:             d.queryID,
		Value:          encodeIPv4Peer(p),
		RoundtripToken: d.makeToken(p),
	}

	if buf, err := proto.Marshal(res); err == nil {
		d.socket.Respond(p, buf)
	}
}
func (d *DHT) onFindNode(p *udpRequest.PeerRequest, req *Request) {
	if len(req.Target) != IDSize {
		return
	}

	res := &Response{
		Id:             d.queryID,
		Nodes:          encodeIPv4Nodes(d.nodes.Closest(kbucket.XORDistance(req.Target), 20)),
		RoundtripToken: d.makeToken(p),
	}

	if buf, err := proto.Marshal(res); err == nil {
		d.socket.Respond(p, buf)
	}
}
func (d *DHT) onQuery(p *udpRequest.PeerRequest, req *Request) {
	if len(req.Target) != IDSize {
		return
	}

	node := &basicNode{
		id:   req.Id,
		addr: p.Addr,
	}
	query := &Query{
		Command: req.GetCommand(),
		Target:  req.Target,
		Value:   req.Value,
	}

	var method string
	if req.RoundtripToken != nil {
		method = "update"
	} else {
		method = "query"
	}

	var handler QueryHandler
	d.lock.RLock()
	if h := d.handlers[method+":"+req.GetCommand()]; h != nil {
		handler = h
	} else if h = d.handlers[method]; h != nil {
		handler = h
	} else {
		handler = noopHandler
	}
	d.lock.RUnlock()

	value, err := handler(node, query)
	if err != nil {
		return
	}
	res := &Response{
		Id:             d.queryID,
		Value:          value,
		Nodes:          encodeIPv4Nodes(d.nodes.Closest(kbucket.XORDistance(req.Target), 20)),
		RoundtripToken: d.makeToken(p),
	}

	if buf, err := proto.Marshal(res); err == nil {
		d.socket.Respond(p, buf)
	}
}

// OnQuery registers handlers for incoming queries. If the command string is the zero-value then
// the handler will be used for any incoming queries whose command does not match one with a
// specific handler added. Only one handler may be added for a particular event at a time, so
// adding a nil handler will effectively disable the previous handler.
func (d *DHT) OnQuery(command string, handler QueryHandler) {
	d.lock.Lock()
	defer d.lock.Unlock()

	key := "query"
	if command != "" {
		key += ":" + command
	}
	if handler != nil {
		d.handlers[key] = handler
	} else {
		delete(d.handlers, key)
	}
}

// OnUpdate registers handlers for incoming updates. If the command string is the zero-value then
// the handler will be used for any incoming updates whose command does not match one with a
// specific handler added. Only one handler may be added for a particular event at a time, so
// adding a nil handler will effectively disable the previous handler.
func (d *DHT) OnUpdate(command string, handler QueryHandler) {
	d.lock.Lock()
	defer d.lock.Unlock()

	key := "update"
	if command != "" {
		key += ":" + command
	}
	if handler != nil {
		d.handlers[key] = handler
	} else {
		delete(d.handlers, key)
	}
}

// HandleUDPRequest implements the udpRequest.Handler interface. It is not recommended to
// use this function directly even though it is exported.
func (d *DHT) HandleUDPRequest(p *udpRequest.PeerRequest, reqBuf []byte) {
	req := new(Request)
	if err := proto.Unmarshal(reqBuf, req); err != nil {
		return
	}
	d.addNode(p.Addr, req.Id)

	if req.RoundtripToken != nil && !d.validToken(p, req.RoundtripToken) {
		req.RoundtripToken = nil
	}

	if req.ForwardRequest != nil {
		d.forwardRequest(p, req)
		return
	}
	if req.ForwardResponse != nil {
		p = d.forwardResponse(p, req)
		if p == nil {
			return
		}
	}

	switch req.GetCommand() {
	case "_ping":
		d.onPing(p, req)
	case "_find_node":
		d.onFindNode(p, req)
	default:
		d.onQuery(p, req)
	}
}

func (d *DHT) request(ctx context.Context, peer net.Addr, req *Request) (*Response, error) {
	reqBuf, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithMessage(err, "encoding request")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-d.rateLimiter:
		defer func() { d.rateLimiter <- true }()
	}

	resBuf, err := d.socket.Request(ctx, peer, reqBuf)
	if err != nil {
		return nil, err
	}

	res := new(Response)
	if err := proto.Unmarshal(resBuf, res); err != nil {
		return nil, errors.WithMessage(err, "decoding response")
	}
	d.addNode(peer, res.Id)
	return res, nil
}

// Ping sends a special query that always responds with our address as the peer saw it.
func (d *DHT) Ping(ctx context.Context, peer net.Addr) (net.Addr, error) {
	cmd := "_ping"
	res, err := d.request(ctx, peer, &Request{Command: &cmd, Id: d.queryID})
	if err != nil {
		return nil, err
	}
	if publicAddr := decodeIPv4Peer(res.GetValue()); publicAddr == nil {
		return publicAddr, nil
	}
	return nil, errors.New("response contained invalid address")
}

// Holepunch uses the `referrer` node as a STUN server to UDP hole punch to the `peer`.
func (d *DHT) Holepunch(ctx context.Context, peer, referrer net.Addr) error {
	cmd := "_ping"
	req := &Request{
		Command:        &cmd,
		Id:             d.queryID,
		ForwardRequest: encodeIPv4Peer(peer),
	}
	_, err := d.request(ctx, referrer, req)
	return err
}

// Query creates a new query.
func (d *DHT) Query(ctx context.Context, q *Query, opts *QueryOpts) *QueryStream {
	return newQueryStream(ctx, d, q, opts)
}

// Update is similar to Query except that it will trigger an update query on the 20 closest
// node (distance between node IDs and the target) after the query is finished.
//
// By default the stream will only write to the channel results from the update queries to
// the closest nodes. To include all query responses set the Verbose option to true.
func (d *DHT) Update(ctx context.Context, q *Query, opts *QueryOpts) *QueryStream {
	if opts == nil {
		opts = new(QueryOpts)
	}
	opts.isUpdate = true

	return newQueryStream(ctx, d, q, opts)
}

// Bootstrap finds the closest peers to the DHT's ID. Calling it before calling other queries
// might make the queries faster, but it is not necessary.
//
// Bootstrap should be called at regular intervals if you aren't doing any other queries.
func (d *DHT) Bootstrap(ctx context.Context) error {
	stream := d.Query(ctx, &Query{Command: "_find_node", Target: d.id[:]}, nil)
	// We don't use CollectStream here because we don't want to store all of the reponses in
	// memory when we don't have to.
	for _ = range stream.respChan {
	}
	return <-stream.errChan
}

// Close shuts down the underlying socket and quits all of the background go routines handling
// periodic tasks. The underlying socket is closed even if it was initially provided in the config.
func (d *DHT) Close() error {
	select {
	case <-d.done:
	default:
		close(d.done)
	}

	return d.socket.Close()
}

// New creates a new dht-rpc instance.
func New(cfg *Config) (*DHT, error) {
	if cfg == nil {
		cfg = new(Config)
	}
	c := *cfg

	var err error
	result := new(DHT)
	if c.Concurrency == 0 {
		c.Concurrency = 16
	}
	result.concurrency = c.Concurrency
	result.rateLimiter = make(chan bool, c.Concurrency)
	for i := 0; i < c.Concurrency; i++ {
		result.rateLimiter <- true
	}
	result.bootstrap = c.BootStrap

	if c.ID == nil {
		c.ID = make([]byte, IDSize)
		if _, err = rand.Read(c.ID); err != nil {
			return nil, errors.WithMessage(err, "creating random ID")
		}
	}
	if len(c.ID) != IDSize {
		return nil, errors.Errorf("invalid ID size %d, expected %d bytes", len(c.ID), IDSize)
	}
	copy(result.id[:], c.ID)
	if !c.Ephemeral {
		result.queryID = result.id[:]
	}
	result.nodes = kbucket.New(&kbucket.Config{
		LocalID: result.id[:],
		OnPing:  result.onNodePing,

		OnAdd:    result.onNodeAdd,
		OnRemove: result.onNodeRemove,
		OnUpdate: result.onNodeUpdate,
	})

	for i := range result.secrets {
		result.secrets[i] = make([]byte, secretSize)
		if _, err = rand.Read(result.secrets[i]); err != nil {
			return nil, errors.WithMessage(err, "creating random secret")
		}
	}
	result.handlers = make(map[string]QueryHandler)

	result.socket, err = udpRequest.New(&udpRequest.Config{
		Socket: c.Socket,
		Port:   c.Port,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "creating socket")
	}
	result.socket.SetHandler(result)

	// Don't start any of the background routines until everything that could fail is done.
	result.done = make(chan struct{})
	go result.rotateSecrets()
	go result.updateTick()

	return result, nil
}
