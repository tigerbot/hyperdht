// Package hyperdht provided a DHT that supports peer discovery and distibuted hole punching.
package hyperdht

import (
	"bytes"
	"context"
	"encoding/hex"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"
	"gitlab.daplie.com/core-sdk/hyperdht/peerEncoding"
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

func (d *HyperDHT) createStream(ctx context.Context, kind uint32, key []byte) *subStream {
	req := &Request{
		Type: &kind,
	}
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
	switch kind {
	case lookupType:
		return d.dht.Query(ctx, query, nil)
	case announceType:
		return d.dht.Update(ctx, query, &dhtRpc.QueryOpts{Verbose: true})
	case unannounceType:
		return d.dht.Update(ctx, query, nil)
	}
	// Pretty sure this will never happen, so not worth making people check a return value.
	panic(errors.Errorf("invalid stream type %d", kind))
}

// Lookup finds peers that have been added to the DHT using the specified key.
func (d *HyperDHT) Lookup(ctx context.Context, key []byte) *QueryStream {
	stream := d.createStream(ctx, lookupType, key)
	result := &QueryStream{stream, ctx, make(chan QueryResponse)}
	go result.runMap()
	return result
}

// Announce adds this node to the DHT. Note that you should keep announcing yourself at
// regular intervals (fx every 4-5 minutes).
func (d *HyperDHT) Announce(ctx context.Context, key []byte) *QueryStream {
	stream := d.createStream(ctx, announceType, key)
	result := &QueryStream{stream, ctx, make(chan QueryResponse)}
	go result.runMap()
	return result
}

// Unannounce removes this node from the DHT.
func (d *HyperDHT) Unannounce(ctx context.Context, key []byte) error {
	stream := d.createStream(ctx, unannounceType, key)
	for _ = range stream.ResponseChan() {
	}
	return <-stream.ErrorChan()
}

func (d *HyperDHT) onQuery(n dhtRpc.Node, q *dhtRpc.Query) ([]byte, error) {
	return d.onRequest(n, q, false)
}
func (d *HyperDHT) onUpdate(n dhtRpc.Node, q *dhtRpc.Query) ([]byte, error) {
	return d.onRequest(n, q, true)
}
func (d *HyperDHT) onRequest(n dhtRpc.Node, q *dhtRpc.Query, isUpdate bool) ([]byte, error) {
	var req Request
	if err := proto.Unmarshal(q.Value, &req); err == nil {
		if res := d.processPeers(&req, n.Addr(), q.Target, isUpdate); res != nil {
			if resBuf, err := proto.Marshal(res); err != nil {
				return resBuf, nil
			}
		}
	}

	// Note that in this case returning an error indicates we shouldn't respond at all to
	// the query, but we can still let the DHT RPC tell the remote peer of the closest nodes.
	return nil, nil
}

func (d *HyperDHT) processPeers(req *Request, from net.Addr, target []byte, isUpdate bool) *Response {
	key, id := hex.EncodeToString(target), from.String()
	peer := encodePeer(from)

	if isUpdate && req.GetType() == unannounceType {
		d.store.Del(key, id)
		return nil
	}
	if isUpdate && req.GetType() == announceType {
		info := &peerInfo{encoded: peer}

		d.store.Put(key, id, info)
	}

	var peersBuf []byte
	next := d.store.Iterator(key)

	for len(peersBuf) < 900 {
		info := next()
		if info == nil {
			break
		} else if bytes.Equal(info.encoded, peer) {
			continue
		}

		peersBuf = append(peersBuf, info.encoded...)
	}

	if peersBuf == nil {
		return nil
	}
	return &Response{
		Peers: peersBuf,
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

const peerEnc = peerEncoding.IPv4Encoder(0)

func encodePeer(addr net.Addr) []byte {
	if addr == nil {
		return nil
	}
	list := []peerEncoding.Node{peerEncoding.BasicNode{MyAddr: addr}}
	return peerEnc.Encode(list)
}
func decodePeer(buf []byte) net.Addr {
	if list := peerEnc.Decode(buf); len(list) > 0 {
		return list[0].Addr()
	}
	return nil
}

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
