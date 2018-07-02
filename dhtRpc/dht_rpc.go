// Package dhtRpc makes calls over a Kademlia based DHT.
//
// Implementation is based on https://github.com/mafintosh/dht-rpc
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

	"gitlab.daplie.com/core-sdk/hyperdht/internal/protoSchemas"
	"gitlab.daplie.com/core-sdk/hyperdht/ipEncoding"
	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
	"gitlab.daplie.com/core-sdk/hyperdht/udpRequest"
)

type (
	request  = protoSchemas.RpcRequest
	response = protoSchemas.RpcResponse
)

const (
	// IDSize is the number of bytes we expect in an ID
	IDSize = 32

	secretCnt      = 2
	secretSize     = 32
	secretLifetime = 5 * time.Minute
	tickInterval   = 5 * time.Second

	pingCmd     = "_ping"
	findNodeCmd = "_find_node"
)

// A QueryHandler handles query events from remote peers.
type QueryHandler func(Node, *Query) ([]byte, error)

// Config contains all of the options available for a DHT instance
type Config struct {
	ID          []byte     // ID of the node. If nil, one will be randomly generated
	Ephemeral   bool       // Will this node be stored by other nodes?
	Concurrency int        // How many requests can be pending at a time
	BootStrap   []net.Addr // Bootstrap node addresses
	Nodes       []Node     // Initial node list
	IPv6        bool       // Will the node use IPv6 encoding instead of IPv4. Incompatible with nodes using IPv4 encoding.

	// Allows for custom socket types or instances to be used. If Socket is nil a new net.UDPConn
	// is created that will listen on the specified port.
	Socket  net.PacketConn
	Port    int
	timeout time.Duration
}

// A DHT holds everything needed to send RPC calls over a distributed hash table.
type DHT struct {
	id        [IDSize]byte
	queryID   []byte
	bootstrap []net.Addr
	encoder   ipEncoding.NodeEncoder

	concurrency     int
	inflightQueries int32
	rateLimiter     chan bool

	// secrets and handlers have nothing in common except that they need to be thread safe and
	// will change very rarely compared to how often they are used, so they share an RWLock
	lock     sync.RWMutex
	secrets  [secretCnt][]byte
	handlers map[string]QueryHandler

	tick  uint64
	nodes storedNodeList

	socket *udpRequest.UDPRequest
	done   context.CancelFunc
}

// ID returns the ID being used by the DHT.
func (d *DHT) ID() []byte { return d.id[:] }

// Addr returns the network address of the underlying socket the DHT is using.
func (d *DHT) Addr() net.Addr { return d.socket.Addr() }

// Encoder return the NodeEncoder used by this node. Primarily intended so that something
// built on top of it can use the same encoding for IP addresses.
func (d *DHT) Encoder() ipEncoding.IPEncoder { return d.encoder.IPEncoder }

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
func (d *DHT) closest(target []byte, count int) []Node {
	contacts := d.nodes.Closest(kbucket.XORDistance(target), count)
	nodes := make([]Node, 0, len(contacts))
	for _, c := range contacts {
		if n, ok := c.(Node); ok {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (d *DHT) updateTick(ctx context.Context) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if tick := atomic.AddUint64(&d.tick, 1); tick&7 == 0 {
				d.pingSome(tick)
			}
		}
	}
}
func (d *DHT) pingSome(curTick uint64) {
	count := 3
	if atomic.LoadInt32(&d.inflightQueries) > 2 {
		count = 1
	}

	for _, n := range d.nodes.oldest(count) {
		if curTick-atomic.LoadUint64(&n.tick) >= 3 {
			go d.check(n)
		}
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

func (d *DHT) makeToken(peer net.Addr, cmd string) []byte {
	d.lock.RLock()
	defer d.lock.RUnlock()

	h := sha256.New()
	h.Write(d.secrets[0])
	h.Write([]byte(peer.String()))
	h.Write([]byte(cmd))
	return h.Sum(nil)
}
func (d *DHT) validToken(peer net.Addr, cmd string, token []byte) bool {
	d.lock.RLock()
	defer d.lock.RUnlock()

	h := sha256.New()
	if len(token) != h.Size() {
		return false
	}
	for _, s := range d.secrets {
		h.Reset()
		h.Write(s)
		h.Write([]byte(peer.String()))
		h.Write([]byte(cmd))
		if bytes.Equal(h.Sum(nil), token) {
			return true
		}
	}

	return false
}
func (d *DHT) rotateSecrets(ctx context.Context) {
	ticker := time.NewTicker(secretLifetime)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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

func (d *DHT) forwardRequest(from *udpRequest.PeerRequest, req *request) {
	if req.GetCommand() != pingCmd {
		return
	}

	to := d.encoder.DecodeAddr(req.ForwardRequest)
	if to == nil {
		return
	}

	req.ForwardRequest = nil
	req.ForwardResponse = d.encoder.EncodeAddr(from.Addr)
	if buf, err := proto.Marshal(req); err == nil {
		d.socket.ForwardRequest(from, to, buf)
	}
}
func (d *DHT) forwardResponse(peer *udpRequest.PeerRequest, req *request) *udpRequest.PeerRequest {
	if req.GetCommand() != pingCmd {
		return nil
	}

	to := d.encoder.DecodeAddr(req.ForwardResponse)
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

func (d *DHT) getHandler(cmd string, isUpdate bool) QueryHandler {
	var method string
	// I'm not sure what added benefit/security is added by having this token system, but the
	// nodejs implementation uses it and we are trying to stay compatible with it.
	if isUpdate {
		method = "update"
	} else {
		method = "query"
	}

	d.lock.RLock()
	defer d.lock.RUnlock()
	if h := d.handlers[method+":"+cmd]; h != nil {
		return h
	}
	return d.handlers[method]
}
func (d *DHT) createResponse(peer net.Addr, req *request) *response {
	// While I think it would be potentially useful to allow targets of different ID size, there
	// is potential for problems in the ambiguity of what nodes are closest, particularly if the
	// target size is too small. For example if there were hundreds of nodes with the same distance
	// to a target, which ones should be contacted and would a different peer choose differently?
	// TODO? decide a minimal threshold and accept targets any size bigger than that.
	if len(req.Target) != IDSize {
		return nil
	}

	cmd := req.GetCommand()
	res := &response{
		Nodes:          d.encoder.Encode(d.closest(req.Target, 20)),
		RoundtripToken: d.makeToken(peer, cmd),
	}

	// A valid round trip token constitutes a update.
	// I'm not sure what added benefit/security is added by having this token system, but the
	// nodejs implementation uses it and we are trying to stay compatible with it.
	handler := d.getHandler(cmd, d.validToken(peer, cmd, req.RoundtripToken))
	if handler == nil {
		return res
	}

	node := basicNode{id: req.Id, addr: peer}
	query := &Query{
		Command: cmd,
		Target:  req.GetTarget(),
		Value:   req.GetValue(),
	}

	var err error
	if res.Value, err = handler(node, query); err != nil {
		return nil
	}
	return res
}

// HandleUDPRequest implements the udpRequest.Handler interface. It is not recommended to
// use this function directly even though it is exported.
func (d *DHT) HandleUDPRequest(p *udpRequest.PeerRequest, reqBuf []byte) {
	req := new(request)
	if err := proto.Unmarshal(reqBuf, req); err != nil {
		return
	}
	d.addNode(p.Addr, req.Id)

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

	var res *response
	switch req.GetCommand() {
	case "_ping":
		res = &response{Value: d.encoder.EncodeAddr(p.Addr)}
	case "_find_node":
		if len(req.Target) == IDSize {
			res = &response{Nodes: d.encoder.Encode(d.closest(req.Target, 20))}
		}
	default:
		res = d.createResponse(p.Addr, req)
	}

	if res != nil {
		res.Id = d.queryID
		if buf, err := proto.Marshal(res); err == nil {
			d.socket.Respond(p, buf)
		}
	}
}

func (d *DHT) request(ctx context.Context, peer net.Addr, req *request) (*response, error) {
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

	res := new(response)
	if err := proto.Unmarshal(resBuf, res); err != nil {
		return nil, errors.WithMessage(err, "decoding response")
	}
	d.addNode(peer, res.Id)
	return res, nil
}

// Ping sends a special query that always responds with our address as the peer saw it.
func (d *DHT) Ping(ctx context.Context, peer net.Addr) (net.Addr, error) {
	cmd := pingCmd
	res, err := d.request(ctx, peer, &request{Command: &cmd, Id: d.queryID})
	if err != nil {
		return nil, err
	}
	if publicAddr := d.encoder.DecodeAddr(res.GetValue()); publicAddr != nil {
		return publicAddr, nil
	}
	return nil, errors.New("response contained invalid address")
}

// Holepunch uses the `referrer` node as a STUN server to UDP hole punch to the `peer`.
func (d *DHT) Holepunch(ctx context.Context, peer, referrer net.Addr) error {
	cmd := pingCmd
	req := &request{
		Command:        &cmd,
		Id:             d.queryID,
		ForwardRequest: d.encoder.EncodeAddr(peer),
	}
	if req.ForwardRequest == nil {
		return errors.Errorf("invalid peer address %s", peer)
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
// might make the queries faster, but it is not necessary. This is particularly true if the
// DHT is ephemeral and will only be used to make queries.
//
// Bootstrap should be called at regular intervals if you aren't doing any other queries.
func (d *DHT) Bootstrap(ctx context.Context) error {
	fgCon, bgCon := int32(d.concurrency), int32(2)
	if fgCon > 16 {
		bgCon = fgCon / 8
	} else if fgCon < 2 {
		bgCon = fgCon
	}
	var opts *QueryOpts
	if atomic.LoadInt32(&d.inflightQueries) > 0 {
		opts = &QueryOpts{Concurrency: int(bgCon)}
	}

	stream := d.Query(ctx, &Query{Command: "_find_node", Target: d.id[:]}, opts)
	// Bootstrapping is lower priority than most other queries, so if there are other queries
	// active then we lower the number of requests our query can make at a time to avoid hogging
	// the total number of requests that can be pending.
	for _ = range stream.respChan {
		if atomic.LoadInt32(&d.inflightQueries) == 1 {
			atomic.StoreInt32(&stream.concurrency, fgCon)
		} else {
			atomic.StoreInt32(&stream.concurrency, bgCon)
		}
	}
	return <-stream.errChan
}

// Close shuts down the underlying socket and quits all of the background go routines handling
// periodic tasks. The underlying socket is closed even if it was initially provided in the config.
func (d *DHT) Close() error {
	d.done()
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

	if c.IPv6 {
		result.encoder.IPEncoder = ipEncoding.IPv6Encoder{}
	} else {
		result.encoder.IPEncoder = ipEncoding.IPv4Encoder{}
	}
	result.encoder.IDSize = IDSize

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
	result.nodes.init(result.id[:], result.onNodePing)
	for _, node := range c.Nodes {
		result.addNode(node.Addr(), node.ID())
	}

	for i := range result.secrets {
		result.secrets[i] = make([]byte, secretSize)
		if _, err = rand.Read(result.secrets[i]); err != nil {
			return nil, errors.WithMessage(err, "creating random secret")
		}
	}
	result.handlers = make(map[string]QueryHandler)

	// This should be the last step that can fail since it would otherwise have to be closed
	// if there were any other errors.
	result.socket, err = udpRequest.New(&udpRequest.Config{
		Socket:  c.Socket,
		Port:    c.Port,
		Timeout: c.timeout,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "creating socket")
	}
	result.socket.SetHandler(result)

	// Don't start any of the background routines until everything that could fail is done.
	var ctx context.Context
	ctx, result.done = context.WithCancel(context.Background())
	go result.rotateSecrets(ctx)
	go result.updateTick(ctx)

	return result, nil
}
