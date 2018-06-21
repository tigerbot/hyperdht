// Package hyperdht provided a DHT that supports peer discovery and distibuted hole punching.
package hyperdht

import (
	"bytes"
	"context"
	"encoding/hex"
	"net"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"
	"gitlab.daplie.com/core-sdk/hyperdht/ipEncoding"
)

const (
	lookupType = iota
	announceType
	unannounceType

	queryCmd = "peers"
)

// HyperDHT wraps a DHT RPC instance and handles the particular calls needed for peer
// discovery. All methods that can be called on a dhtRpc.DHT instance can be called
// on a HyperDHT instance even though it's not embedded publicly.
type HyperDHT struct {
	*dht
	store store
}
type dht = dhtRpc.DHT

// OnQuery calls the OnQuery method for the underlying DHT RPC only if the command isn't
// the one the hyperdht needs to function properly.
func (d *HyperDHT) OnQuery(cmd string, handler dhtRpc.QueryHandler) {
	if cmd != queryCmd {
		d.dht.OnQuery(cmd, handler)
	}
}

// OnUpdate calls the OnUpdate method for the underlying DHT RPC only if the command isn't
// the one the hyperdht needs to function properly.
func (d *HyperDHT) OnUpdate(cmd string, handler dhtRpc.QueryHandler) {
	if cmd != queryCmd {
		d.dht.OnUpdate(cmd, handler)
	}
}

func (d *HyperDHT) createStream(ctx context.Context, key []byte, req *PeerRequest) *subStream {
	reqBuf, err := proto.Marshal(req)
	if err != nil {
		// Pretty sure this will never happen, so not worth making people check a return value.
		panic(errors.WithMessage(err, "marshalling initial request buffer"))
	}

	query := &dhtRpc.Query{
		Command: queryCmd,
		Target:  key,
		Value:   reqBuf,
	}
	switch req.GetType() {
	case lookupType:
		return d.dht.Query(ctx, query, nil)
	case announceType:
		return d.dht.Update(ctx, query, &dhtRpc.QueryOpts{Verbose: true})
	case unannounceType:
		return d.dht.Update(ctx, query, nil)
	}
	// Pretty sure this will never happen, so not worth making people check a return value.
	panic(errors.Errorf("invalid stream type %d", req.GetType()))
}
func (d *HyperDHT) createMappedStream(ctx context.Context, key []byte, req *PeerRequest) *QueryStream {
	stream := d.createStream(ctx, key, req)
	result := &QueryStream{stream, req.GetLocalAddress(), ctx, make(chan QueryResponse)}

	localRes := d.processPeers(req, d.Addr(), key, false)
	go result.runMap(d.Addr(), localRes)
	return result
}

// Lookup finds peers that have been added to the DHT using the specified key.
func (d *HyperDHT) Lookup(ctx context.Context, key []byte, opts *QueryOpts) *QueryStream {
	return d.createMappedStream(ctx, key, createRequest(lookupType, opts))
}

// Announce adds this node to the DHT. Note that you should keep announcing yourself at
// regular intervals (fx every 4-5 minutes).
func (d *HyperDHT) Announce(ctx context.Context, key []byte, opts *QueryOpts) *QueryStream {
	return d.createMappedStream(ctx, key, createRequest(announceType, opts))
}

// AnnounceDiscard is similar to Announce, except that it will block until the entire update
// is complete and will discard all responses from the peers instead of processing and converting
// them to the response type of this package.
func (d *HyperDHT) AnnounceDiscard(ctx context.Context, key []byte, opts *QueryOpts) error {
	return dhtRpc.DiscardStream(d.createStream(ctx, key, createRequest(announceType, opts)))
}

// Unannounce removes this node from the DHT.
func (d *HyperDHT) Unannounce(ctx context.Context, key []byte, opts *QueryOpts) error {
	return dhtRpc.DiscardStream(d.createStream(ctx, key, createRequest(unannounceType, opts)))
}

func (d *HyperDHT) onQuery(n dhtRpc.Node, q *dhtRpc.Query) ([]byte, error) {
	return d.onRequest(n, q, false)
}
func (d *HyperDHT) onUpdate(n dhtRpc.Node, q *dhtRpc.Query) ([]byte, error) {
	return d.onRequest(n, q, true)
}
func (d *HyperDHT) onRequest(n dhtRpc.Node, q *dhtRpc.Query, isUpdate bool) ([]byte, error) {
	var req PeerRequest
	if err := proto.Unmarshal(q.Value, &req); err == nil {
		if res := d.processPeers(&req, n.Addr(), q.Target, isUpdate); res != nil {
			if resBuf, err := proto.Marshal(res); err == nil {
				return resBuf, nil
			}
		}
	}

	// Note that in this case returning an error indicates we shouldn't respond at all to
	// the query, but we can still let the DHT RPC tell the remote peer of the closest nodes.
	return nil, nil
}

func (d *HyperDHT) processPeers(req *PeerRequest, from net.Addr, target []byte, isUpdate bool) *PeerResponse {
	if port := req.GetPort(); port != 0 {
		from = overridePort(from, int(port))
	}
	key, id := hex.EncodeToString(target), from.String()
	peer := encodePeer(from)

	if isUpdate && req.GetType() == unannounceType {
		d.store.Del(key, id)
		return nil
	}
	if isUpdate && req.GetType() == announceType {
		info := &peerInfo{encoded: peer}
		if req.LocalAddress != nil && decodePeer(req.LocalAddress) != nil {
			info.localFilter = req.LocalAddress[:2]
			info.localPeer = req.LocalAddress[2:]
		}

		d.store.Put(key, id, info)
	}

	var peersBuf, localBuf []byte
	next := d.store.Iterator(key)
	filter := createLocalFilter(req.LocalAddress)

	for len(peersBuf)+len(localBuf) < 900 {
		info := next()
		if info == nil {
			break
		} else if bytes.Equal(info.encoded, peer) {
			continue
		}

		peersBuf = append(peersBuf, info.encoded...)
		if filter(info) {
			localBuf = append(localBuf, info.localPeer...)
		}
	}

	if peersBuf == nil {
		return nil
	}
	return &PeerResponse{
		Peers:      peersBuf,
		LocalPeers: localBuf,
	}
}

// New creates a new HyperDHT, using the provided config to create a new DHT RPC instance.
func New(cfg *dhtRpc.Config) (*HyperDHT, error) {
	dht, err := dhtRpc.New(cfg)
	if err != nil {
		return nil, err
	}
	return NewWithDHT(dht), nil
}

// NewWithDHT creates a new HyperDHT using the provided DHT RPC instance.
func NewWithDHT(dht *dhtRpc.DHT) *HyperDHT {
	result := &HyperDHT{dht: dht}
	result.store.gc()
	dht.OnQuery(queryCmd, result.onQuery)
	dht.OnUpdate(queryCmd, result.onUpdate)

	return result
}

var peerEnc = ipEncoding.NodeEncoder{IPEncoder: ipEncoding.IPv4Encoder{}}

func encodePeer(addr net.Addr) []byte { return peerEnc.EncodeAddr(addr) }
func decodePeer(buf []byte) net.Addr  { return peerEnc.DecodeAddr(buf) }

func decodeAllPeers(buf []byte) []net.Addr {
	list := peerEnc.Decode(buf)
	if len(list) == 0 {
		return nil
	}

	result := make([]net.Addr, len(list))
	for i := range list {
		result[i] = list[i].Addr()
	}
	return result
}
func decodeLocalPeers(localAddr, buf []byte) []net.Addr {
	if len(localAddr) != 6 || len(buf) == 0 || len(buf)%4 != 0 {
		return nil
	}

	cp := make([]byte, 6)
	copy(cp, localAddr[:2])
	list := make([]net.Addr, len(buf)/4)
	for i := range list {
		cp = append(cp[:2], buf[4*i:4*(i+1)]...)
		list[i] = decodePeer(cp)
	}
	return list
}

func createLocalFilter(localAddr []byte) func(*peerInfo) bool {
	if len(localAddr) != 6 {
		return func(*peerInfo) bool { return false }
	}

	return func(info *peerInfo) bool {
		if info.localPeer == nil || info.localFilter == nil {
			return false
		}
		if info.localFilter[0] != localAddr[0] || info.localFilter[1] != localAddr[1] {
			return false
		}
		if bytes.Equal(localAddr[2:], info.localPeer) {
			return false
		}
		return true
	}
}

func createRequest(kind uint32, opts *QueryOpts) *PeerRequest {
	req := &PeerRequest{
		Type: &kind,
	}
	if opts != nil {
		req.LocalAddress = encodePeer(opts.LocalAddr)
		if port := uint32(opts.Port); port != 0 {
			req.Port = &port
		}
	}
	return req
}

type alteredAddr struct {
	network string
	address string
}

func (a *alteredAddr) Network() string { return a.network }
func (a *alteredAddr) String() string  { return a.address }

func overridePort(addr net.Addr, port int) net.Addr {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr
	}
	return &alteredAddr{
		network: addr.Network(),
		address: net.JoinHostPort(host, strconv.Itoa(port)),
	}
}
