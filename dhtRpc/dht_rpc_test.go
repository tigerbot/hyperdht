package dhtRpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

func createDHTPair() *dhtPair {
	result := new(dhtPair)
	var err error
	if result.bootstrap, err = New(&Config{Ephemeral: true}); err != nil {
		panic(err)
	}

	addr := result.bootstrap.Addr().(*net.UDPAddr)
	addr.IP = net.IPv4(127, 0, 0, 1)
	if result.server, err = New(&Config{BootStrap: []net.Addr{addr}}); err != nil {
		panic(err)
	}
	if result.client, err = New(&Config{BootStrap: []net.Addr{addr}}); err != nil {
		panic(err)
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err = result.server.Bootstrap(ctx); err != nil {
		panic(err)
	}

	return result
}

func testQuery(t *testing.T, client *DHT, update bool, query *Query, opts *QueryOpts, respValue []byte) {
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var stream *QueryStream
	if update {
		stream = client.Update(ctx, query, opts)
	} else {
		stream = client.Query(ctx, query, opts)
	}
	responses, err := CollectStream(stream)
	if err != nil {
		t.Error("query returned unexpected error:", err)
	} else if len(responses) != 1 {
		t.Errorf("query returned %d response, expected 1\n%v", len(responses), responses)
	} else {
		resp := responses[0]
		if !bytes.Equal(resp.Value, respValue) {
			t.Errorf("query response had %q as value expected %q", resp.Value, respValue)
		}
		if !bytes.Equal(resp.Node.ID(), query.Target) {
			t.Errorf("query response ID is %x, expected %x", resp.Node.ID(), query.Target)
		}
	}
}

func TestSimpleQuery(t *testing.T) {
	pair := createDHTPair()
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

	testQuery(t, pair.client, false, &query, nil, []byte("world"))
	pair.server.OnQuery("hello", nil)
	testQuery(t, pair.client, false, &query, nil, []byte("this is not the world"))
}

func TestSimpleUpdate(t *testing.T) {
	pair := createDHTPair()
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

	testQuery(t, pair.client, true, &update, nil, update.Value)
}

func TestTargetedQuery(t *testing.T) {
	pair := createDHTPair()
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

	addr := pair.server.Addr().(*net.UDPAddr)
	addr.IP = net.IPv4(127, 0, 0, 1)
	opts := &QueryOpts{
		Nodes: []Node{&basicNode{id: pair.server.ID(), addr: addr}},
	}

	testQuery(t, pair.client, false, &query, opts, []byte("world"))
}

func TestTargetedUpdate(t *testing.T) {
	pair := createDHTPair()
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

	addr := pair.server.Addr().(*net.UDPAddr)
	addr.IP = net.IPv4(127, 0, 0, 1)
	opts := &QueryOpts{
		Nodes: []Node{&basicNode{id: pair.server.ID(), addr: addr}},
	}

	testQuery(t, pair.client, true, &update, opts, update.Value)
}

func TestSwarmQuery(t *testing.T) {
	pair := createDHTPair()
	defer pair.Close()

	var wait sync.WaitGroup
	var closest int32
	start := func(ind int, node *DHT) {
		defer wait.Done()

		var value []byte
		node.OnUpdate("kv", func(_ Node, q *Query) ([]byte, error) {
			t.Logf("node update #%d on %x", atomic.AddInt32(&closest, 1), node.ID())
			value = q.Value
			return nil, nil
		})
		node.OnQuery("kv", func(_ Node, q *Query) ([]byte, error) {
			return value, nil
		})

		ctx, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		if err := node.Bootstrap(ctx); err != nil {
			t.Errorf("bootstrapping node #%d errored: %v", ind, err)
		}
	}

	cfg := &Config{BootStrap: pair.server.bootstrap}
	wait.Add(1)
	go start(0, pair.server)
	for i := 1; i < 256; i++ {
		if node, err := New(cfg); err != nil {
			t.Errorf("creating node #%d errored: %v", i, err)
		} else {
			defer node.Close()
			wait.Add(1)
			go start(i, node)
		}
	}
	wait.Wait()

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	key := sha256.Sum256([]byte("hello"))
	updateVal := []byte("hello")
	update := Query{
		Command: "kv",
		Target:  key[:],
		Value:   updateVal,
	}
	if responses, err := CollectStream(pair.client.Update(ctx, &update, nil)); err != nil {
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
	stream := pair.client.Query(ctx, &query, nil)
	for resp := range stream.ResponseChan() {
		if resp.Value != nil {
			if !bytes.Equal(resp.Value, updateVal) {
				t.Errorf("queried value %q doesn't match expected %q", resp.Value, updateVal)
			}
		}
	}
}
