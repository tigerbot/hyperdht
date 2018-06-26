package hyperdht

import (
	"context"
	"crypto/sha256"
	"net"
	"testing"
	"time"

	"gitlab.daplie.com/core-sdk/hyperdht/fakeNetwork"

	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"
)

type dhtPair struct {
	network   *fakeNetwork.FakeNetwork
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
	if err := s.network.Close(); err != nil {
		errs = append(errs, err)
	}

	if errs != nil {
		panic(errs)
	}
}

func createPair(t *testing.T, ipv6 bool) *dhtPair {
	result := &dhtPair{network: fakeNetwork.New()}
	modConfig := func(cfg *dhtRpc.Config) *dhtRpc.Config {
		cp := *cfg
		cp.Socket = result.network.NewNode(fakeNetwork.RandomAddress(ipv6), true)
		return &cp
	}

	var err error
	if result.bootstrap, err = New(modConfig(&dhtRpc.Config{IPv6: ipv6})); err != nil {
		t.Fatal("failed to create bootstrap node:", err)
	}
	cfg := &dhtRpc.Config{
		BootStrap: []net.Addr{result.bootstrap.Addr()},
		IPv6:      ipv6,
	}
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	if result.server, err = New(modConfig(cfg)); err != nil {
		t.Fatal("failed to create server node:", err)
	} else if err = result.server.Bootstrap(ctx); err != nil {
		t.Fatal("failed to bootstrap server node:", err)
	}

	cfg.Ephemeral = true
	if result.client, err = New(modConfig(cfg)); err != nil {
		t.Fatal("failed to create client node:", err)
	}

	return result
}

func dualIPTest(t *testing.T, f func(*testing.T, bool)) {
	wrap := func(ipv6 bool) func(*testing.T) {
		return func(t *testing.T) {
			f(t, ipv6)
		}
	}
	t.Run("ipv4", wrap(false))
	t.Run("ipv6", wrap(true))
}

func TestHyperDHTBasic(t *testing.T) { dualIPTest(t, hyperDHTBasicTest) }
func hyperDHTBasicTest(t *testing.T, ipv6 bool) {
	pair := createPair(t, ipv6)
	defer pair.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	sum := sha256.Sum256([]byte("hello"))
	key := sum[:]
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
			} else if resp.Peers[0].String() != pair.server.Addr().String() {
				t.Errorf("lookup from %s returned peer %s, expected %s", name, resp.Peers[0], pair.server.Addr())
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

func TestHyperDHTLocal(t *testing.T) { dualIPTest(t, hyperDHTLocalTest) }
func hyperDHTLocalTest(t *testing.T, ipv6 bool) {
	pair := createPair(t, ipv6)
	defer pair.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	sum := sha256.Sum256([]byte("hello"))
	key := sum[:]
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
			} else if resp.Peers[0].String() != pair.server.Addr().String() {
				t.Errorf("lookup returned peer %s, expected %s", resp.Peers[0], pair.server.Addr())
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

func TestPortOverride(t *testing.T) { dualIPTest(t, portOverrideTest) }
func portOverrideTest(t *testing.T, ipv6 bool) {
	pair := createPair(t, ipv6)
	defer pair.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var expected string
	if host, _, err := net.SplitHostPort(pair.server.Addr().String()); err != nil {
		t.Fatalf("failed to split address %s: %v", pair.server.Addr(), err)
	} else {
		expected = net.JoinHostPort(host, "4321")
	}

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
		} else if resp.Peers[0].String() != expected {
			t.Errorf("lookup returned peer %s, expected %s", resp.Peers[0], expected)
		}
	}
}
