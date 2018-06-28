package dhtRpc

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"gitlab.daplie.com/core-sdk/hyperdht/fakeNetwork"
)

// These values are all modified in the `race_test.go` file that is only included in the build
// if the tests are being run with the race detector.
var (
	raceDetector          = false
	stdTimeout            = time.Second
	swarmBootstrapTimeout = time.Second
	rateCheckInterval     = time.Millisecond / 4
	rateTestResponsDelay  = time.Millisecond
)

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
		// Only make the bootstrap node public
		cp.Socket = result.network.NewNode(fakeNetwork.RandomAddress(ipv6), cp.BootStrap == nil)
		cp.timeout = 10 * time.Millisecond
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

	ctx, done := context.WithTimeout(context.Background(), swarmBootstrapTimeout)
	defer done()

	var failCnt int32
	logErr := func(action string, ind int, err error) {
		if cnt := atomic.AddInt32(&failCnt, 1); cnt < 9 {
			t.Errorf("failed to %s server node #%d: %v", action, ind, err)
		} else if cnt == 9 {
			t.Log("too many errors, suppressing logs for the remains errors")
		}
	}

	// 16 nodes bootstrapping at a time should be enough for any potential racey conditions to
	// show themselves, and throttling it actually makes the race test run faster. It also helps
	// reduce the timing inconsistencies introduced when we put the bootstrap node under such a
	// heavy load (it's responsible for helping everyone holepunch to almost anyone else).
	throttle := make(chan bool, 16)
	for len(throttle) < cap(throttle) {
		throttle <- true
	}
	var wait sync.WaitGroup
	start := func(ind int, node *DHT) {
		defer wait.Done()

		<-throttle
		defer func() { throttle <- true }()

		err := errors.New("no bootstrap attempted")
		for tries := 0; tries < 4 && err != nil; tries++ {
			// Unless the size of our swarm is one, we want more from our bootstrap than simply not
			// erroring. We also want to have other nodes stored in our list.
			err = node.Bootstrap(ctx)
			if err == nil && size > 1 && len(node.Nodes()) == 0 {
				err = errors.New("no nodes acquired in list")
			}

			if err != nil {
				time.Sleep(time.Millisecond + time.Duration(rand.Int63n(int64(time.Millisecond))))
			}
		}

		if err != nil {
			logErr("bootstrap", ind, err)
		}
	}

	wait.Add(size)
	for i := 0; i < size; i++ {
		if node, err := New(modConfig(cfg)); err != nil {
			logErr("create", i, err)
			wait.Done()
		} else {
			result.servers = append(result.servers, node)
			go start(i, node)
		}
	}
	wait.Wait()
	if failCnt > 0 {
		t.Fatalf("failed to create/bootstrap %d/%d of the server nodes", failCnt, size)
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
