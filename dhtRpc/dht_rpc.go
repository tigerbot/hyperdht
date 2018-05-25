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

type DHT struct {
	id      [IDSize]byte
	queryID []byte
	secrets [secretCnt][]byte
	tick    uint64
	nodes   *kbucket.KBucket

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
				// TODO: d.pingSome()
			}
		}
	}
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

	node := new(Node)
	copy(node.id[:], id)
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
	reping := make([]*Node, 0, len(current))

	for _, c := range current {
		// The k-bucket shouldn't ever have anything but *Node, but handle it just in case
		if node, ok := c.(*Node); !ok {
			d.nodes.Remove(c.ID())
			d.nodes.Add(replacement)
			return
		} else if curTick-node.tick >= 3 {
			// More than 10 seconds since we pinged this node, so make sure it's still active
			reping = append(reping, node)
		}
	}

	ctx := context.TODO()
	for _, n := range reping {
		if err := d.Ping(ctx, n.addr); err != nil {
			d.nodes.Remove(n.ID())
			d.nodes.Add(n)
			return
		}
		// We shouldn't need to update the node when the ping succeeds because that should
		// already be happening elsewhere every time any type of request succeeds.
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

func (d *DHT) Ping(ctx context.Context, peer net.Addr) error {
	cmd := "_ping"
	_, err := d.request(ctx, peer, &Request{Command: &cmd, Id: d.queryID})
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

func New(c *Config) (*DHT, error) {
	var err error
	result := new(DHT)
	if c == nil {
		c = new(Config)
	}

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
