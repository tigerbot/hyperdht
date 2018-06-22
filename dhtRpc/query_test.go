package dhtRpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	bootstrap *DHT
	server    *DHT
	client    *DHT
}

func (p *dhtPair) Close() {
	var errs []error

	if err := p.bootstrap.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := p.server.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := p.client.Close(); err != nil {
		errs = append(errs, err)
	}

	if errs != nil {
		panic(errs)
	}
}

func createDHTPair(t *testing.T, ipv6 bool) *dhtPair {
	result := new(dhtPair)
	var err error
	if result.bootstrap, err = New(&Config{Ephemeral: true, IPv6: ipv6}); err != nil {
		t.Fatal("failed to create bootstrap node:", err)
	}

	cfg := &Config{
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
	if result.client, err = New(cfg); err != nil {
		t.Fatal("failed to create client node:", err)
	}

	return result
}

type dhtSwarm struct {
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

	if errs != nil {
		panic(errs)
	}
}

func createSwarm(t *testing.T, size int, ipv6 bool) *dhtSwarm {
	result := new(dhtSwarm)
	var err error
	if result.bootstrap, err = New(&Config{Ephemeral: true, IPv6: ipv6}); err != nil {
		t.Fatal("failed to create bootstrap node:", err)
	}

	cfg := &Config{
		BootStrap: []net.Addr{localizeAddr(result.bootstrap.Addr(), ipv6)},
		IPv6:      ipv6,
	}

	// We have a rather long timeout here for the race condition tests. With how many routines
	// we spawn here it takes a lot of work for the race detector to do whatever it needs to do
	// to detect the races, so we allow it plenty of time. Normal tests Shouldn't take that long.
	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()
	var wait sync.WaitGroup
	var failCnt int32
	start := func(ind int, node *DHT) {
		defer wait.Done()

		if err := node.Bootstrap(ctx); err != nil {
			t.Errorf("failed to bootstrap server node #%d: %v", ind, err)
			atomic.AddInt32(&failCnt, 1)
		}
	}

	wait.Add(size)
	for i := 0; i < size; i++ {
		if node, err := New(cfg); err != nil {
			t.Errorf("failed to create server node #%d: %v", i, err)
			atomic.AddInt32(&failCnt, 1)
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

	if result.client, err = New(cfg); err != nil {
		t.Fatal("failed to create the client node:", err)
	}

	return result
}

func testQuery(t *testing.T, pair *dhtPair, update bool, query *Query, opts *QueryOpts, respValue []byte) {
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var stream *QueryStream
	if update {
		stream = pair.client.Update(ctx, query, opts)
	} else {
		stream = pair.client.Query(ctx, query, opts)
	}
	responses, err := CollectStream(stream)
	if err != nil {
		t.Error("query returned unexpected error:", err)
	} else if len(responses) != 1 {
		t.Errorf("query returned %d response, expected 1", len(responses))
	} else {
		resp := responses[0]
		if !bytes.Equal(resp.Value, respValue) {
			t.Errorf("query response had %q as value expected %q", resp.Value, respValue)
		}
		if !bytes.Equal(resp.Node.ID(), pair.server.ID()) {
			t.Errorf("query response ID is %x, expected %x", resp.Node.ID(), pair.server.ID())
		}
	}
}
func TestSimpleQueryIPv4(t *testing.T) { simpleQueryTest(t, false) }
func TestSimpleQueryIPv6(t *testing.T) { simpleQueryTest(t, true) }
func simpleQueryTest(t *testing.T, ipv6 bool) {
	pair := createDHTPair(t, ipv6)
	defer pair.Close()

	query := Query{
		Command: "hello",
		Target:  pair.server.ID(),
	}
	pair.server.OnQuery("hello", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("query handler received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &query) {
			t.Errorf("query handler query arg is %#v, expected %#v", q, &query)
		}
		return []byte("world"), nil
	})
	pair.server.OnQuery("", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("query handler received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &query) {
			t.Errorf("query handler query arg is %#v, expected %#v", q, &query)
		}
		return []byte("this is not the world"), nil
	})

	testQuery(t, pair, false, &query, nil, []byte("world"))
	pair.server.OnQuery("hello", nil)
	testQuery(t, pair, false, &query, nil, []byte("this is not the world"))
}

func TestSimpleUpdateIPv4(t *testing.T) { simpleUpdateTest(t, false) }
func TestSimpleUpdateIPv6(t *testing.T) { simpleUpdateTest(t, true) }
func simpleUpdateTest(t *testing.T, ipv6 bool) {
	pair := createDHTPair(t, ipv6)
	defer pair.Close()

	update := Query{
		Command: "echo",
		Target:  pair.server.ID(),
		Value:   []byte("Hello World!"),
	}
	pair.server.OnUpdate("echo", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("OnUpdate received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &update) {
			t.Errorf("OnUpdate query arg is %#v, expected %#v", q, &update)
		}
		return q.Value, nil
	})

	testQuery(t, pair, true, &update, nil, update.Value)
}

func TestTargetedQueryIPv4(t *testing.T) { targetedQueryTest(t, false) }
func TestTargetedQueryIPv6(t *testing.T) { targetedQueryTest(t, true) }
func targetedQueryTest(t *testing.T, ipv6 bool) {
	pair := createDHTPair(t, ipv6)
	defer pair.Close()

	serverB, err := New(&Config{BootStrap: pair.server.bootstrap})
	if err != nil {
		t.Fatal("creating second server errored", err)
	}
	defer serverB.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	if err := serverB.Bootstrap(ctx); err != nil {
		t.Fatal("bootstrapping second server errored", err)
	}
	done()
	serverB.OnQuery("", func(Node, *Query) ([]byte, error) {
		t.Error("the second server was accessed")
		return nil, nil
	})
	pair.client.OnQuery("", func(Node, *Query) ([]byte, error) {
		t.Error("the client was accessed")
		return nil, nil
	})

	// Use serverB's ID as target to make sure it's not visited even when it's closer.
	query := Query{
		Command: "hello",
		Target:  serverB.ID(),
	}
	pair.server.OnQuery("hello", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("query handler received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &query) {
			t.Errorf("query handler query arg is %#v, expected %#v", q, &query)
		}
		return []byte("world"), nil
	})

	opts := &QueryOpts{
		Nodes: []Node{basicNode{id: pair.server.ID(), addr: localizeAddr(pair.server.Addr(), ipv6)}},
	}

	testQuery(t, pair, false, &query, opts, []byte("world"))
}

func TestTargetedUpdateIPv4(t *testing.T) { targetedUpdateTest(t, false) }
func TestTargetedUpdateIPv6(t *testing.T) { targetedUpdateTest(t, true) }
func targetedUpdateTest(t *testing.T, ipv6 bool) {
	pair := createDHTPair(t, ipv6)
	defer pair.Close()

	serverB, err := New(&Config{BootStrap: pair.server.bootstrap})
	if err != nil {
		t.Fatal("creating second server errored", err)
	}
	defer serverB.Close()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	if err := serverB.Bootstrap(ctx); err != nil {
		t.Fatal("bootstrapping second server errored", err)
	}
	done()
	serverB.OnQuery("", func(Node, *Query) ([]byte, error) {
		t.Error("the second server's query handler was accessed")
		return nil, nil
	})
	serverB.OnUpdate("", func(Node, *Query) ([]byte, error) {
		t.Error("the second server's update handler was accessed")
		return nil, nil
	})

	pair.client.OnQuery("", func(Node, *Query) ([]byte, error) {
		t.Error("the client's query handler was accessed")
		return nil, nil
	})
	pair.client.OnUpdate("", func(Node, *Query) ([]byte, error) {
		t.Error("the client's update handler was accessed")
		return nil, nil
	})

	// Use serverB's ID as target to make sure it's not visited even when it's closer.
	update := Query{
		Command: "echo",
		Target:  serverB.ID(),
		Value:   []byte("Hello World!"),
	}
	pair.server.OnUpdate("echo", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("OnUpdate received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &update) {
			t.Errorf("OnUpdate query arg is %#v, expected %#v", q, &update)
		}
		return q.Value, nil
	})

	opts := &QueryOpts{
		Nodes: []Node{basicNode{id: pair.server.ID(), addr: localizeAddr(pair.server.Addr(), ipv6)}},
	}

	testQuery(t, pair, true, &update, opts, update.Value)
}

func TestDHTRateLimitIPv4(t *testing.T) { dHTRateLimitTest(t, false) }
func TestDHTRateLimitIPv6(t *testing.T) { dHTRateLimitTest(t, true) }
func dHTRateLimitTest(t *testing.T, ipv6 bool) {
	// We need a swarm so the query stream has more than one peer to query at a time.
	swarm := createSwarm(t, 128, ipv6)
	defer swarm.Close()
	for _, s := range swarm.servers {
		s.OnQuery("", func(Node, *Query) ([]byte, error) {
			time.Sleep(time.Millisecond)
			return []byte("world"), nil
		})
	}

	const parellel = 4
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	streamFinished := make(chan bool)
	for i := 0; i < parellel; i++ {
		query := Query{
			Command: "hello",
			Target:  swarm.servers[i].ID(),
		}
		go func() {
			defer func() { streamFinished <- true }()
			if err := DiscardStream(swarm.client.Query(ctx, &query, nil)); err != nil {
				t.Errorf("backgrounded query errored: %v", err)
			}
		}()
	}

	var finished int
	ticker := time.NewTicker(time.Millisecond / 4)
	defer ticker.Stop()
	var counts []int
	for {
		select {
		case <-streamFinished:
			if finished++; finished >= parellel {
				t.Log(counts)
				return
			}

		case <-ticker.C:
			if cur, limit := swarm.client.socket.Pending(), swarm.client.concurrency; cur > limit {
				t.Fatalf("DHT currently has %d requests pending, expected <= %d", cur, limit)
			} else {
				counts = append(counts, cur)
			}
		}
	}
}

func TestQueryRateLimitIPv4(t *testing.T) { queryRateLimitTest(t, false) }
func TestQueryRateLimitIPv6(t *testing.T) { queryRateLimitTest(t, true) }
func queryRateLimitTest(t *testing.T, ipv6 bool) {
	// We need a swarm so the query stream has more than one peer to query at a time.
	swarm := createSwarm(t, 128, ipv6)
	defer swarm.Close()
	for _, s := range swarm.servers {
		s.OnQuery("", func(Node, *Query) ([]byte, error) {
			time.Sleep(time.Millisecond)
			return []byte("world"), nil
		})
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	streamFinished := make(chan bool)

	const concurrency = 2
	go func() {
		defer func() { streamFinished <- true }()

		query := Query{Command: "hello", Target: swarm.servers[0].ID()}
		opts := QueryOpts{Concurrency: concurrency}
		if err := DiscardStream(swarm.client.Query(ctx, &query, &opts)); err != nil {
			t.Errorf("backgrounded query errored: %v", err)
		}
	}()

	ticker := time.NewTicker(time.Millisecond / 4)
	defer ticker.Stop()
	var counts []int
	for {
		select {
		case <-streamFinished:
			t.Log(counts)
			return

		case <-ticker.C:
			if cur := swarm.client.socket.Pending(); cur > concurrency {
				t.Fatalf("DHT currently has %d requests pending, expected <= %d", cur, concurrency)
			} else {
				counts = append(counts, cur)
			}
		}
	}
}

func TestSwarmQueryIPv4(t *testing.T) { swarmQueryTest(t, false) }
func TestSwarmQueryIPv6(t *testing.T) { swarmQueryTest(t, true) }
func swarmQueryTest(t *testing.T, ipv6 bool) {
	swarm := createSwarm(t, 256, ipv6)
	defer swarm.Close()

	var closest int32
	for _, node := range swarm.servers {
		var value []byte
		node.OnUpdate("kv", func(_ Node, q *Query) ([]byte, error) {
			atomic.AddInt32(&closest, 1)
			value = q.Value
			return nil, nil
		})
		node.OnQuery("kv", func(_ Node, q *Query) ([]byte, error) {
			return value, nil
		})
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	key := sha256.Sum256([]byte("hello"))
	updateVal := []byte("hello")
	update := Query{
		Command: "kv",
		Target:  key[:],
		Value:   updateVal,
	}
	if responses, err := CollectStream(swarm.client.Update(ctx, &update, nil)); err != nil {
		t.Error("update errored:", err)
	} else if len(responses) != 20 {
		t.Errorf("update received %d responses, expected 20", len(responses))
	} else if nodeCnt := atomic.LoadInt32(&closest); nodeCnt != 20 {
		t.Errorf("%d nodes received update, expected 20", nodeCnt)
	}

	query := Query{
		Command: "kv",
		Target:  key[:],
	}
	stream := swarm.client.Query(ctx, &query, nil)
	for resp := range stream.ResponseChan() {
		if resp.Value != nil {
			if !bytes.Equal(resp.Value, updateVal) {
				t.Errorf("queried value %q doesn't match expected %q", resp.Value, updateVal)
			}
		}
	}
}

func TestNonephemeralBootstrapIPv4(t *testing.T) { nonephemeralBootstrapTest(t, false) }
func TestNonephemeralBootstrapIPv6(t *testing.T) { nonephemeralBootstrapTest(t, true) }
func nonephemeralBootstrapTest(t *testing.T, ipv6 bool) {
	swarm := createSwarm(t, 32, ipv6)
	defer swarm.Close()

	if nodes := swarm.client.Nodes(); len(nodes) > 0 {
		t.Fatalf("client has already been bootstrapped:\n\t%#v", nodes)
	}
	swarm.client.bootstrap = make([]net.Addr, 20)
	for i := range swarm.client.bootstrap {
		swarm.client.bootstrap[i] = localizeAddr(swarm.servers[i].Addr(), ipv6)
	}

	for i, node := range swarm.servers {
		num, id := i, node.ID()
		var count int32
		node.OnQuery("query", func(Node, *Query) ([]byte, error) {
			if cnt := atomic.AddInt32(&count, 1); cnt > 1 {
				t.Errorf("server #%d (%x) had query called %d times", num, id, cnt)
			}
			return nil, nil
		})
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	key := sha256.Sum256([]byte("hello"))
	query := Query{
		Command: "query",
		Target:  key[:],
	}
	if _, err := CollectStream(swarm.client.Update(ctx, &query, nil)); err != nil {
		t.Error("query errored:", err)
	}
}
