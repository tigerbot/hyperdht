package dhtRpc

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.daplie.com/core-sdk/hyperdht/fakeNetwork"
)

var raceDetector = false

type dhtSwarm struct {
	network   *fakeNetwork.FakeNetwork
	bootstrap *DHT
	servers   []*DHT
	client    *DHT
}

func (s *dhtSwarm) Close() {
	var errs []error

	if err := s.bootstrap.Close(); err != nil {
		errs = append(errs, err)
	}
	for _, s := range s.servers {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
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

func createSwarm(t *testing.T, size int, ipv6 bool) *dhtSwarm {
	result := &dhtSwarm{network: fakeNetwork.New()}
	modConfig := func(cfg *Config) *Config {
		cp := *cfg
		cp.Socket = result.network.NewNode(fakeNetwork.RandomAddress(ipv6), true)
		return &cp
	}

	var err error
	if result.bootstrap, err = New(modConfig(&Config{Ephemeral: true, IPv6: ipv6})); err != nil {
		t.Fatal("failed to create bootstrap node:", err)
	}

	cfg := &Config{
		BootStrap: []net.Addr{result.bootstrap.Addr()},
		IPv6:      ipv6,
	}

	// The race detector seriously slows things down, especially when we are spawning as many
	// routines as we do with a large swarm. As such we need to have a much longer timeout when
	// the race detector is active.
	timeout := time.Second
	if raceDetector {
		timeout = time.Minute
	}
	ctx, done := context.WithTimeout(context.Background(), timeout)
	defer done()
	var wait sync.WaitGroup
	var failCnt int32
	start := func(ind int, node *DHT) {
		defer wait.Done()

		if err := node.Bootstrap(ctx); err != nil {
			if cnt := atomic.AddInt32(&failCnt, 1); cnt < 9 {
				t.Errorf("failed to bootstrap server node #%d: %v", ind, err)
			} else if cnt == 9 {
				t.Log("too many errors, suppressing logs for the remains errors")
			}
		}
	}

	wait.Add(size)
	for i := 0; i < size; i++ {
		if node, err := New(modConfig(cfg)); err != nil {
			if cnt := atomic.AddInt32(&failCnt, 1); cnt < 9 {
				t.Errorf("failed to create server node #%d: %v", i, err)
			} else if cnt == 9 {
				t.Log("too many errors, suppressing logs for the remains errors")
			}
			wait.Done()
		} else {
			result.servers = append(result.servers, node)
			go start(i, node)
		}
	}
	wait.Wait()
	if failCnt > 0 {
		t.Fatalf("failed to create/bootstrap %d of the server nodes", failCnt)
	}

	if result.client, err = New(modConfig(cfg)); err != nil {
		t.Fatal("failed to create the client node:", err)
	}

	return result
}

func createDHTNode(t *testing.T, network *fakeNetwork.FakeNetwork, ipv6, public bool) *DHT {
	sock := network.NewNode(fakeNetwork.RandomAddress(ipv6), public)
	node, err := New(&Config{Socket: sock, IPv6: ipv6, timeout: 5 * time.Millisecond})
	if err != nil {
		t.Fatal("failed to create new DHT node:", err)
	}
	return node
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
