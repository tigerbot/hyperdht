package dhtRpc

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/tigerbot/hyperdht/fakeNetwork"
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

	// 16 nodes bootstrapping at a time should be enough for any potential racy conditions to
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
		if err := bootstrapNode(ctx, node, size > 1); err != nil {
			logErr("bootstrapping", ind, err)
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
func bootstrapNode(ctx context.Context, node *DHT, requireNonempty bool) error {
	var messages []string
	for tries := 0; tries < 8; tries++ {
		// Unless the size of our swarm is one, we want more from our bootstrap than simply not
		// erroring. We also want to have other nodes stored in our list. Otherwise we can't be
		// sure that we're actually able to communicate with any other nodes through the "NAT".
		err := node.Bootstrap(ctx)
		if err == nil && requireNonempty && len(node.Nodes()) == 0 {
			err = errors.New("no nodes acquired in list")
		}

		if err == nil {
			return nil
		}
		messages = append(messages, err.Error())
		time.Sleep(time.Duration(2*tries+1) * swarmBootstrapTimeout / 1000)
	}

	return errors.New(strings.Join(messages, "; "))
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

func TestOpenClose(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Error("providing nil for config or double closing caused panic:", err)
		}
	}()

	dht, err := New(nil)
	if err != nil {
		t.Fatal("providing nil for config errored:", err)
	}

	var wait sync.WaitGroup
	defer wait.Wait()

	wait.Add(1)
	go func() {
		defer wait.Done()
		dht.Close()
	}()
	dht.Close()
}
