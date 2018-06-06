package hyperdht

import (
	"context"
	"crypto/sha256"
	"net"
	"strconv"
	"testing"
	"time"

	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"
)

func localizeAddr(addr net.Addr) net.Addr {
	var port int
	if u, ok := addr.(*net.UDPAddr); ok {
		port = u.Port
	} else if t, ok := addr.(*net.TCPAddr); ok {
		port = t.Port
	} else if _, portStr, err := net.SplitHostPort(addr.String()); err != nil {
		panic("invalid network address " + addr.String())
	} else if port, err = strconv.Atoi(portStr); err != nil || port < 0 || port > (1<<16) {
		panic("invalid network address " + addr.String())
	}

	return &net.UDPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: port,
	}
}

type dhtPair struct {
	bootstrap *HyperDHT
	server    *HyperDHT
	client    *HyperDHT
}

func (s *dhtPair) Close() {
	var errs []error

	if err := s.bootstrap.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := s.server.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := s.client.Close(); err != nil {
		errs = append(errs, err)
	}

	if errs != nil {
		panic(errs)
	}
}

func createPair() *dhtPair {
	result := new(dhtPair)
	var err error
	if result.bootstrap, err = New(nil); err != nil {
		panic(err)
	}
	cfg := &dhtRpc.Config{BootStrap: []net.Addr{localizeAddr(result.bootstrap.Addr())}}

	// We have a rather long timeout here for the race condition tests. With how many routines
	// we spawn here it takes a lot of work for the race detector to do whatever it needs to do
	// to detect the races, so we allow it plenty of time. Normal tests shouldn't take that long.
	ctx, done := context.WithTimeout(context.Background(), 20*time.Second)
	defer done()

	if result.server, err = New(cfg); err != nil {
		panic(err)
	} else if err = result.server.Bootstrap(ctx); err != nil {
		panic(err)
	}
	if result.client, err = New(cfg); err != nil {
		panic(err)
	}

	return result
}

func TestHyperDHTBasic(t *testing.T) {
	pair := createPair()
	defer pair.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	sum := sha256.Sum256([]byte("hello"))
	key := sum[:]
	if responses, err := CollectStream(pair.server.Announce(ctx, key, nil)); err != nil {
		t.Fatal("error announcing:", err)
	} else if len(responses) != 0 {
		t.Errorf("announce received %d responses, expected 0\n\t%#v", len(responses), responses)
	}

	if responses, err := CollectStream(pair.client.Lookup(ctx, key, nil)); err != nil {
		t.Error("error looking up:", err)
	} else if len(responses) != 1 {
		t.Errorf("lookup received %d responses, expected 1\n\t%#v", len(responses), responses)
	} else {
		resp := responses[0]
		if len(resp.Peers) != 1 {
			t.Errorf("lookup resulted in %d peers, expected 1\n\t%#v", len(resp.Peers), resp.Peers)
		} else if addr := localizeAddr(pair.server.Addr()); resp.Peers[0].String() != addr.String() {
			t.Errorf("lookup returned peer %s, expected %s", resp.Peers[0], addr)
		}
	}

	if err := pair.server.Unannounce(ctx, key, nil); err != nil {
		t.Error("error unannouncing:", err)
	}

	if responses, err := CollectStream(pair.client.Lookup(ctx, key, nil)); err != nil {
		t.Error("error looking up:", err)
	} else if len(responses) != 0 {
		t.Errorf("second lookup received %d responses, expected 0\n\t%#v", len(responses), responses)
	}
}

func TestHyperDHTLocal(t *testing.T) {
	pair := createPair()
	defer pair.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	sum := sha256.Sum256([]byte("hello"))
	key := sum[:]
	serverAddr := &net.UDPAddr{IP: net.IPv4(192, 168, 1, 123), Port: 1234}
	if responses, err := CollectStream(pair.server.Announce(ctx, key, &QueryOpts{LocalAddr: serverAddr})); err != nil {
		t.Fatal("error announcing:", err)
	} else if len(responses) != 0 {
		t.Errorf("announce received %d responses, expected 0\n\t%#v", len(responses), responses)
	}

	if responses, err := CollectStream(pair.client.Lookup(ctx, key, nil)); err != nil {
		t.Error("error looking up:", err)
	} else if len(responses) != 1 {
		t.Errorf("lookup received %d responses, expected 1\n\t%#v", len(responses), responses)
	} else {
		resp := responses[0]
		if len(resp.Peers) != 1 {
			t.Errorf("lookup resulted in %d peers, expected 1\n\t%#v", len(resp.Peers), resp.Peers)
		} else if addr := localizeAddr(pair.server.Addr()); resp.Peers[0].String() != addr.String() {
			t.Errorf("lookup returned peer %s, expected %s", resp.Peers[0], addr)
		}
		if len(resp.LocalPeers) != 0 {
			t.Errorf("lookup without local addr resulted in %d local peers\n\t%#v", len(resp.LocalPeers), resp.LocalPeers)
		}
	}

	clientAddr := &net.UDPAddr{IP: net.IPv4(192, 168, 1, 137), Port: 4321}
	if responses, err := CollectStream(pair.client.Lookup(ctx, key, &QueryOpts{LocalAddr: clientAddr})); err != nil {
		t.Error("error looking up:", err)
	} else if len(responses) != 1 {
		t.Errorf("lookup received %d responses, expected 1\n\t%#v", len(responses), responses)
	} else {
		resp := responses[0]
		if len(resp.Peers) != 1 {
			t.Errorf("lookup resulted in %d peers, expected 1\n\t%#v", len(resp.Peers), resp.Peers)
		} else if addr := localizeAddr(pair.server.Addr()); resp.Peers[0].String() != addr.String() {
			t.Errorf("lookup returned peer %s, expected %s", resp.Peers[0], addr)
		}
		if len(resp.LocalPeers) != 1 {
			t.Errorf("lookup with contained local addr resulted in %d local peers\n\t%#v", len(resp.LocalPeers), resp.LocalPeers)
		} else if resp.LocalPeers[0].String() != serverAddr.String() {
			t.Errorf("lookup returned local address %s, expected %s", resp.LocalPeers[0], serverAddr)
		}
	}

	clientAddr = &net.UDPAddr{IP: net.IPv4(10, 10, 0, 98), Port: 7531}
	if responses, err := CollectStream(pair.client.Lookup(ctx, key, &QueryOpts{LocalAddr: clientAddr})); err != nil {
		t.Error("error looking up:", err)
	} else if len(responses) != 1 {
		t.Errorf("lookup received %d responses, expected 1\n\t%#v", len(responses), responses)
	} else {
		resp := responses[0]
		if len(resp.Peers) != 1 {
			t.Errorf("lookup resulted in %d peers, expected 1\n\t%#v", len(resp.Peers), resp.Peers)
		} else if addr := localizeAddr(pair.server.Addr()); resp.Peers[0].String() != addr.String() {
			t.Errorf("lookup returned peer %s, expected %s", resp.Peers[0], addr)
		}
		if len(resp.LocalPeers) != 0 {
			t.Errorf("lookup with non-contained local addr resulted in %d local peers\n\t%#v", len(resp.LocalPeers), resp.LocalPeers)
		}
	}
}
