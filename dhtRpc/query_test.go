package dhtRpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.daplie.com/core-sdk/hyperdht/fakeNetwork"
)

func testQuery(t *testing.T, pair *dhtSwarm, update bool, query *Query, opts *QueryOpts, respValue []byte) {
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
		if !bytes.Equal(resp.Node.ID(), pair.servers[0].ID()) {
			t.Errorf("query response ID is %x, expected %x", resp.Node.ID(), pair.servers[0].ID())
		}
	}
}

func TestSimpleQuery(t *testing.T) { dualIPTest(t, simpleQueryTest) }
func simpleQueryTest(t *testing.T, ipv6 bool) {
	pair := createSwarm(t, 1, ipv6)
	defer pair.Close()

	query := Query{
		Command: "hello",
		Target:  pair.servers[0].ID(),
	}
	pair.servers[0].OnQuery("hello", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("query handler received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &query) {
			t.Errorf("query handler query arg is %#v, expected %#v", q, &query)
		}
		return []byte("world"), nil
	})
	pair.servers[0].OnQuery("", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("query handler received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &query) {
			t.Errorf("query handler query arg is %#v, expected %#v", q, &query)
		}
		return []byte("this is not the world"), nil
	})

	testQuery(t, pair, false, &query, nil, []byte("world"))
	pair.servers[0].OnQuery("hello", nil)
	testQuery(t, pair, false, &query, nil, []byte("this is not the world"))
}

func TestSimpleUpdate(t *testing.T) { dualIPTest(t, simpleUpdateTest) }
func simpleUpdateTest(t *testing.T, ipv6 bool) {
	pair := createSwarm(t, 1, ipv6)
	defer pair.Close()

	update := Query{
		Command: "echo",
		Target:  pair.servers[0].ID(),
		Value:   []byte("Hello World!"),
	}
	pair.servers[0].OnUpdate("echo", func(n Node, q *Query) ([]byte, error) {
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

func TestTargetedQuery(t *testing.T) { dualIPTest(t, targetedQueryTest) }
func targetedQueryTest(t *testing.T, ipv6 bool) {
	pair := createSwarm(t, 1, ipv6)
	defer pair.Close()

	serverBSock := pair.network.NewNode(fakeNetwork.RandomAddress(ipv6), true)
	serverB, err := New(&Config{BootStrap: pair.servers[0].bootstrap, Socket: serverBSock})
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
	pair.servers[0].OnQuery("hello", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("query handler received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &query) {
			t.Errorf("query handler query arg is %#v, expected %#v", q, &query)
		}
		return []byte("world"), nil
	})

	opts := &QueryOpts{
		Nodes: []Node{basicNode{id: pair.servers[0].ID(), addr: pair.servers[0].Addr()}},
	}

	testQuery(t, pair, false, &query, opts, []byte("world"))
}

func TestTargetedUpdate(t *testing.T) { dualIPTest(t, targetedUpdateTest) }
func targetedUpdateTest(t *testing.T, ipv6 bool) {
	pair := createSwarm(t, 1, ipv6)
	defer pair.Close()

	serverBSock := pair.network.NewNode(fakeNetwork.RandomAddress(ipv6), true)
	serverB, err := New(&Config{BootStrap: pair.servers[0].bootstrap, Socket: serverBSock})
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
	pair.servers[0].OnUpdate("echo", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("OnUpdate received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &update) {
			t.Errorf("OnUpdate query arg is %#v, expected %#v", q, &update)
		}
		return q.Value, nil
	})

	opts := &QueryOpts{
		Nodes: []Node{basicNode{id: pair.servers[0].ID(), addr: pair.servers[0].Addr()}},
	}

	testQuery(t, pair, true, &update, opts, update.Value)
}

func TestSwarmQuery(t *testing.T) { dualIPTest(t, swarmQueryTest) }
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
	if values, err := CollectValues(swarm.client.Query(ctx, &query, nil)); err != nil {
		t.Error("query errored after update:", err)
	} else {
		if len(values) != 20 {
			t.Errorf("query received %d responses with values, expected 20", len(values))
		}
		for _, val := range values {
			if !bytes.Equal(val, updateVal) {
				t.Errorf("queried value %q doesn't match expected %q", val, updateVal)
			}
		}
	}
}

func TestNonEphemeralBootstrap(t *testing.T) { dualIPTest(t, nonEphemeralBootstrapTest) }
func nonEphemeralBootstrapTest(t *testing.T, ipv6 bool) {
	swarm := createSwarm(t, 32, ipv6)
	defer swarm.Close()

	if nodes := swarm.client.Nodes(); len(nodes) > 0 {
		t.Fatalf("client has already been bootstrapped:\n\t%#v", nodes)
	}
	swarm.client.bootstrap = make([]net.Addr, 20)
	for i := range swarm.client.bootstrap {
		swarm.client.bootstrap[i] = swarm.servers[i].Addr()
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
	if err := DiscardStream(swarm.client.Update(ctx, &query, nil)); err != nil {
		t.Error("query errored:", err)
	}
}
