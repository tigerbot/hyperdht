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

func localizeAddr(addr net.Addr, ipv6 bool) net.Addr {
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

	if ipv6 {
		return &net.UDPAddr{IP: net.IPv6loopback, Port: port}
	}
	return &net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: port}
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

func createPair(t *testing.T, ipv6 bool) *dhtPair {
	result := new(dhtPair)
	var err error
	if result.bootstrap, err = New(&dhtRpc.Config{IPv6: ipv6}); err != nil {
		t.Fatal("failed to create bootstrap node:", err)
	}
	cfg := &dhtRpc.Config{
		BootStrap: []net.Addr{localizeAddr(result.bootstrap.Addr(), ipv6)},
		IPv6:      ipv6,
	}
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	if result.server, err = New(cfg); err != nil {
		t.Fatal("failed to create server node:", err)
	} else if err = result.server.Bootstrap(ctx); err != nil {
		t.Fatal("failed to bootstrap server node:", err)
	}

	cfg.Ephemeral = true
	if result.client, err = New(cfg); err != nil {
		t.Fatal("failed to create client node:", err)
	}

	return result
}
func TestHyperDHTBasic(t *testing.T) { hyperDHTBasicTest(t, false) }
func hyperDHTBasicTest(t *testing.T, ipv6 bool) {
	pair := createPair(t, ipv6)
	defer pair.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	sum := sha256.Sum256([]byte("hello"))
	key := sum[:]
	serverAddr := localizeAddr(pair.server.Addr(), ipv6)
	runQuery := func(name string, dht *HyperDHT, expected bool) {
		responses, err := CollectStream(dht.Lookup(ctx, key, nil))
		if err != nil {
			t.Errorf("error looking up from %s: %v", name, err)
		}
		if !expected {
			if len(responses) != 0 {
				t.Errorf("lookup from %s received %d responses, expected 0\n\t%#v", name, len(responses), responses)
			}
			return
		}

		if len(responses) != 1 {
			t.Errorf("lookup from %s received %d responses, expected 1\n\t%#v", name, len(responses), responses)
		} else {
			resp := responses[0]
			if len(resp.Peers) != 1 {
				t.Errorf("lookup from %s resulted in %d peers, expected 1\n\t%#v", name, len(resp.Peers), resp.Peers)
			} else if resp.Peers[0].String() != serverAddr.String() {
				t.Errorf("lookup from %s returned peer %s, expected %s", name, resp.Peers[0], serverAddr)
			}
		}
	}

	runQuery("client pre-announce", pair.client, false)
	runQuery("bootstrap pre-announce", pair.bootstrap, false)
	if responses, err := CollectStream(pair.server.Announce(ctx, key, nil)); err != nil {
		t.Fatal("error announcing:", err)
	} else if len(responses) != 0 {
		t.Errorf("announce received %d responses, expected 0\n\t%#v", len(responses), responses)
	}

	// We lookup from the bootstrap to make sure that a lookup also returns data stored in
	// the node that is doing the lookup, and we lookup from the server to make sure a lookup
	// doesn't return the node that is doing the lookup.
	runQuery("client", pair.client, true)
	runQuery("bootstrap", pair.bootstrap, true)
	runQuery("server", pair.server, false)

	if err := pair.server.Unannounce(ctx, key, nil); err != nil {
		t.Fatal("error unannouncing:", err)
	}
	runQuery("client post-unannounce", pair.client, false)
	runQuery("bootstrap post-unannounce", pair.bootstrap, false)
}

func TestHyperDHTLocal(t *testing.T) { hyperDHTLocalTest(t, false) }
func hyperDHTLocalTest(t *testing.T, ipv6 bool) {
	pair := createPair(t, ipv6)
	defer pair.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	sum := sha256.Sum256([]byte("hello"))
	key := sum[:]
	serverAddr := localizeAddr(pair.server.Addr(), ipv6)
	localAddr := &net.UDPAddr{IP: net.IPv4(192, 168, 1, 123), Port: 1234}
	if responses, err := CollectStream(pair.server.Announce(ctx, key, &QueryOpts{LocalAddr: localAddr})); err != nil {
		t.Fatal("error announcing:", err)
	} else if len(responses) != 0 {
		t.Errorf("announce received %d responses, expected 0\n\t%#v", len(responses), responses)
	}

	runQuery := func(addr net.Addr, expected bool) {
		if responses, err := CollectStream(pair.client.Lookup(ctx, key, &QueryOpts{LocalAddr: addr})); err != nil {
			t.Error("error looking up:", err)
		} else if len(responses) != 1 {
			t.Errorf("lookup received %d responses, expected 1\n\t%#v", len(responses), responses)
		} else {
			resp := responses[0]
			if len(resp.Peers) != 1 {
				t.Errorf("lookup resulted in %d peers, expected 1\n\t%#v", len(resp.Peers), resp.Peers)
			} else if resp.Peers[0].String() != serverAddr.String() {
				t.Errorf("lookup returned peer %s, expected %s", resp.Peers[0], serverAddr)
			}

			if expected {
				if len(resp.LocalPeers) != 1 {
					t.Errorf("lookup with contained local addr resulted in %d local peers\n\t%#v", len(resp.LocalPeers), resp.LocalPeers)
				} else if resp.LocalPeers[0].String() != localAddr.String() {
					t.Errorf("lookup returned local address %s, expected %s", resp.LocalPeers[0], localAddr)
				}
			} else if len(resp.LocalPeers) != 0 {
				t.Errorf("lookup without local addr %s resulted in %d local peers\n\t%#v", addr, len(resp.LocalPeers), resp.LocalPeers)
			}
		}
	}
	runQuery(nil, false)
	runQuery(&net.UDPAddr{IP: net.IP{192, 168, 1, 137}, Port: 4321}, true)
	runQuery(&net.UDPAddr{IP: net.IP{10, 10, 0, 98}, Port: 7531}, false)
}

func TestPortOverride(t *testing.T) {
	pair := createPair(t, false)
	defer pair.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	sum := sha256.Sum256([]byte("hello"))
	key := sum[:]
	if responses, err := CollectStream(pair.server.Announce(ctx, key, &QueryOpts{Port: 4321})); err != nil {
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
		} else if resp.Peers[0].String() != "127.0.0.1:4321" {
			t.Errorf("lookup returned peer %s, expected 127.0.0.1:4321", resp.Peers[0])
		}
	}
}

func TestIPv6(t *testing.T) {
	// It seems it's really difficult to get the docker containers used by our CI test server
	// to support IPv6 for more than a single container, so we make these tests conditional.
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatal("failed to check interfaces for IPv6 support:", err)
	}
	loopbackFlag := net.FlagUp | net.FlagLoopback
	var ipv6Support bool
	for _, iface := range ifaces {
		if iface.Flags&loopbackFlag != loopbackFlag {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			t.Fatalf("failed to get addresses from interface %v: %v", iface.Name, err)
		}
		for _, addr := range addrs {
			if netAddr, ok := addr.(*net.IPNet); !ok {
				t.Errorf("interface returned address of type %T, expected %T", addr, netAddr)
			} else if netAddr.IP.Equal(net.IPv6loopback) {
				ipv6Support = true
			}
		}
	}
	if !ipv6Support {
		t.Skip("device doesn't appear to have an IPv6 loopback addr")
	}

	wrap := func(f func(*testing.T, bool)) func(*testing.T) {
		return func(t *testing.T) {
			f(t, true)
		}
	}
	t.Run("dht-basic", wrap(hyperDHTBasicTest))
	t.Run("dht-local", wrap(hyperDHTLocalTest))
}
