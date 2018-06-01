// Package hyperdht provided a DHT that supports peer discovery and distibuted hole punching.
package hyperdht

import (
	"bytes"
	"context"
	"encoding/hex"
	"net"

	"github.com/golang/protobuf/proto"

	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"
	"gitlab.daplie.com/core-sdk/hyperdht/peerEncoding"
)

const (
	lookupType = iota
	announceType
	unannounceType

	queryCmd = "peers"
)

type HyperDHT struct {
	dht   *dhtRpc.DHT
	store store
}

// Addr returns the network address the DHT is listening on.
func (d *HyperDHT) Addr() net.Addr {
	return d.dht.Addr()
}

// Bootstrap bootstraps the underlying DHT RPC instance
func (d *HyperDHT) Bootstrap(ctx context.Context) error {
	return d.dht.Bootstrap(ctx)
}

// Ping sends a ping query to the specified remote peer
func (d *HyperDHT) Ping(ctx context.Context, peer net.Addr) (net.Addr, error) {
	return d.dht.Ping(ctx, peer)
}

// Holepunch uses the `referrer` node as a STUN server to UDP hole punch to the `peer`.
func (d *HyperDHT) Holepunch(ctx context.Context, peer, referrer net.Addr) error {
	return d.dht.Holepunch(ctx, peer, referrer)
}

// Close closes the underlying DHT RPC instance
func (d *HyperDHT) Close() error {
	return d.dht.Close()
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
