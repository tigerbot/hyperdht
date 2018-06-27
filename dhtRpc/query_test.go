package dhtRpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"reflect"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
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
	pair := createSwarm(t, 4, ipv6)
	defer pair.Close()

	for i, server := range pair.servers[1:] {
		ind := i
		server.OnQuery("", func(Node, *Query) ([]byte, error) {
			t.Errorf("server #%d was accessed", ind)
			return nil, nil
		})
	}
	pair.client.OnQuery("", func(Node, *Query) ([]byte, error) {
		t.Error("the client was accessed")
		return nil, nil
	})

	// Use a different server's ID as target to make sure it's not visited even when it's closer.
	query := Query{
		Command: "hello",
		Target:  pair.servers[2].ID(),
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

	testQuery(t, pair, false, &query, &QueryOpts{Nodes: []Node{pair.servers[0]}}, []byte("world"))
}

func TestTargetedUpdate(t *testing.T) { dualIPTest(t, targetedUpdateTest) }
func targetedUpdateTest(t *testing.T, ipv6 bool) {
	pair := createSwarm(t, 4, ipv6)
	defer pair.Close()

	for i, server := range pair.servers[1:] {
		ind := i
		server.OnQuery("", func(Node, *Query) ([]byte, error) {
			t.Errorf("server #%d query handler was accessed", ind)
			return nil, nil
		})
		server.OnUpdate("", func(Node, *Query) ([]byte, error) {
			t.Errorf("server #%d update handler was accessed", ind)
			return nil, nil
		})
	}
	pair.client.OnQuery("", func(Node, *Query) ([]byte, error) {
		t.Error("the client's query handler was accessed")
		return nil, nil
	})
	pair.client.OnUpdate("", func(Node, *Query) ([]byte, error) {
		t.Error("the client's update handler was accessed")
		return nil, nil
	})

	// Use a different server's ID as target to make sure it's not visited even when it's closer.
	update := Query{
		Command: "echo",
		Target:  pair.servers[2].ID(),
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

	testQuery(t, pair, true, &update, &QueryOpts{Nodes: []Node{pair.servers[0]}}, update.Value)
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

func TestQueryCancel(t *testing.T) {
	swarm := createSwarm(t, 16, false)
	defer swarm.Close()

	// Make sure there will be a few pending requests when the context expires.
	for _, node := range swarm.servers[:8] {
		node.OnQuery("", func(Node, *Query) ([]byte, error) {
			return nil, errors.New("this node will not respond")
		})
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer done()

	query := &Query{Command: "command", Target: swarm.servers[0].ID()}
	opts := &QueryOpts{Concurrency: 12}
	stream := swarm.client.Query(ctx, query, opts)

	for {
		select {
		case err := <-stream.WarningChan():
			t.Errorf("received unexpected warning when context closed: %v", err)

		case err := <-stream.ErrorChan():
			if err == nil || err.Error() != ctx.Err().Error() {
				t.Errorf("final error was %v, expected %v", err, ctx.Err())
			}
			if dl, ok := ctx.Deadline(); !ok {
				t.Error("context says it doesn't have a deadline. HOW?!?!?")
			} else if dur := time.Now().Sub(dl); dur >= time.Millisecond && !raceDetector {
				t.Errorf("stream took %s to end after context expired, expected < 1ms", dur)
			} else {
				t.Logf("stream exitted within %s of context expiration", dur)
			}
			return
		}
	}
}

func TestQueryError(t *testing.T) {
	swarm := createSwarm(t, 10, false)
	defer swarm.Close()

	swarm.client.Close()
	cfg := &Config{
		BootStrap: []net.Addr{swarm.bootstrap.Addr()},
		Socket:    swarm.network.NewNode(swarm.client.Addr(), true),
		timeout:   5 * time.Millisecond,
	}
	if client, err := New(cfg); err != nil {
		t.Fatal("failed to make new client with timeout:", err)
	} else {
		swarm.client = client
	}

	re := regexp.MustCompile(`no ([\w]+ )?nodes responded`)
	type tmpTimeoutErr interface {
		error
		Temporary() bool
		Timeout() bool
	}
	validateErr := func(stage string, err error) {
		if err == nil {
			t.Errorf("%s did not fail when no nodes should have responded", stage)
			return
		}
		if !re.MatchString(err.Error()) {
			t.Errorf("%s error message %q does not match %v", stage, err, re)
		}
		if tErr, ok := err.(tmpTimeoutErr); !ok {
			t.Errorf("%s error missing Temporary or Timeout methods: %#v", stage, err)
		} else if !tErr.Temporary() || !tErr.Timeout() {
			t.Errorf("%s error did not return true for Temporary and Timeout", stage)
		}
	}

	query := &Query{Command: "command", Target: swarm.servers[0].ID()}
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	// Make it so that none of the server will respond to an update request
	for _, node := range swarm.servers {
		node.OnUpdate("", func(Node, *Query) ([]byte, error) {
			return nil, errors.New("this node will not respond")
		})
	}
	stream := swarm.client.Update(ctx, query, nil)
	resp, err := CollectStream(stream)
	if cnt := stream.CommitCnt(); cnt != 0 {
		t.Errorf("update stream has commit count %d, expected 0", cnt)
	} else if cnt = stream.ResponseCnt(); cnt == 0 {
		t.Error("update stream had no responses, expected responses from query part")
	}
	validateErr("update", err)
	if len(resp) != 0 {
		t.Errorf("received %d responses from non-verbose update", len(resp))
	}

	// Now make it so that none of the server will respond to a query request
	for _, node := range swarm.servers {
		node.OnQuery("", func(Node, *Query) ([]byte, error) {
			return nil, errors.New("this node will not respond")
		})
	}

	// Need to bootstrap so we don't count the response from the bootstrap node. The nodes we
	// had before will have been removed because the update requests failed.
	if err := swarm.client.Bootstrap(ctx); err != nil {
		t.Fatal("failed to bootstrap node before query:", err)
	}

	stream = swarm.client.Query(ctx, query, nil)
	var warnCnt int
	for {
		select {
		case <-stream.WarningChan():
			warnCnt++

		case resp, ok := <-stream.ResponseChan():
			if ok {
				t.Errorf("query stream received unexpected response: %v", resp)
				break
			}
			err := <-stream.ErrorChan()
			validateErr("query", err)
			if cnt := stream.ResponseCnt(); cnt != 0 {
				t.Errorf("query stream has response count %d, expected 0", cnt)
			}
			if warnCnt == 0 {
				t.Error("did not receive any errors on the warning channel")
			}
			return
		}
	}
}
