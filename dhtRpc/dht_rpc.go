// Package dhtRpc makes calls over a Kademlia based DHT.
// It is the go implementation of the `dht-rpc` node library by mafintosh.
package dhtRpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"net"
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

// Config contains all of the options available for a DHT instance
type Config struct {
	ID          []byte
	Ephemeral   bool
	Concurrency int

	BootStrap []string

	// Allows for custom socket types or instances to be used. If Socket is nil a new net.UDPConn
	// is created that will listen on the specified port.
	Socket net.PacketConn
	Port   int
}

// A DHT holds everything needed to send RPC calls over a distributed hash table.
type DHT struct {
	id          [IDSize]byte
	queryID     []byte
	secrets     [secretCnt][]byte
	concurrency int

	tick   uint64
	top    *storedNode
	bottom *storedNode
	nodes  *kbucket.KBucket

	socket *udpRequest.UDPRequest
	done   chan struct{}
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
	h := sha256.New()
	h.Write(d.secrets[0])
	h.Write([]byte(peer.String()))
	return h.Sum(nil)
}
func (d *DHT) validToken(peer net.Addr, token []byte) bool {
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
			for i := range d.secrets[:secretCnt-1] {
				d.secrets[i] = d.secrets[i+1]
			}
			d.secrets[secretCnt-1] = make([]byte, secretSize)
			rand.Read(d.secrets[secretCnt-1])
		}
	}
}

func (d *DHT) addNode(peer net.Addr, id, token []byte) {
	if len(id) != IDSize || bytes.Equal(d.id[:], id) {
		return
	}

	node := new(storedNode)
	node.id = id
	node.addr = peer
	node.roundTripToken = token
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
			d.nodes.Add(n)
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

// HandleUDPRequest implements the udpRequest.Handler interface. It is not recommended to
// use this function directly even though it is exported.
func (d *DHT) HandleUDPRequest(p *udpRequest.PeerRequest, reqBuf []byte) {
	req := new(Request)
	if err := proto.Unmarshal(reqBuf, req); err != nil {
		return
	}
	d.addNode(p.Addr, req.Id, req.RoundtripToken)

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
		// d.onQuery(p, req)
	}
}

func (d *DHT) request(ctx context.Context, peer net.Addr, req *Request) (*Response, error) {
	reqBuf, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithMessage(err, "encoding request")
	}

	// TODO: limit the number of concurrent requests.
	resBuf, err := d.socket.Request(ctx, peer, reqBuf)
	if err != nil {
		return nil, err
	}

	res := new(Response)
	if err := proto.Unmarshal(resBuf, res); err != nil {
		return nil, errors.WithMessage(err, "decoding response")
	}
	d.addNode(peer, res.Id, res.RoundtripToken)
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
func New(c *Config) (*DHT, error) {
	var err error
	result := new(DHT)
	if c == nil {
		c = new(Config)
	}

	if c.Concurrency == 0 {
		c.Concurrency = 16
	}
	result.concurrency = c.Concurrency

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

	result.socket, err = udpRequest.New(&udpRequest.Config{
		Socket:  c.Socket,
		Port:    c.Port,
		Handler: result,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "creating socket")
	}

	// Don't start any of the background routines until everything that could fail is done.
	result.done = make(chan struct{})
	go result.rotateSecrets()
	go result.updateTick()

	return result, nil
}
