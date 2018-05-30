package dhtRpc

import (
	"bytes"
	"context"
	"net"
	"reflect"
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

func TestSimpleQuery(t *testing.T) {
	pair := createDHTPair()
	defer pair.Close()

	query := Query{
		Command: "hello",
		Target:  pair.server.ID(),
	}
	pair.server.OnQuery("hello", func(n Node, q *Query) ([]byte, error) {
		if !bytes.Equal(n.ID(), pair.client.ID()) {
			t.Errorf("OnQuery received wrong ID %x, expected %x", n.ID(), pair.client.ID())
		}
		if !reflect.DeepEqual(q, &query) {
			t.Errorf("OnQuery query arg is %#v, expected %#v", q, &query)
		}
		return []byte("world"), nil
	})

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	responses, err := CollectStream(pair.client.Query(ctx, &query, nil))
	if err != nil {
		t.Error("query returned unexpected error:", err)
	} else if len(responses) != 1 {
		t.Errorf("query returned %d response, expected 1\n%v", len(responses), responses)
	} else {
		resp := responses[0]
		if string(resp.Value) != "world" {
			t.Errorf("query response had %q as value expected \"world\"", resp.Value)
		}
		if !bytes.Equal(resp.Node.ID(), pair.server.ID()) {
			t.Errorf("query response ID is %x, expected %x", resp.Node.ID(), pair.server.ID())
		}
	}
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

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	responses, err := CollectStream(pair.client.Update(ctx, &update, nil))
	if err != nil {
		t.Error("update returned unexpected error:", err)
	} else if len(responses) != 1 {
		t.Errorf("update returned %d response, expected 1\n%v", len(responses), responses)
	} else {
		resp := responses[0]
		if !bytes.Equal(resp.Value, update.Value) {
			t.Errorf("update response had %q as value, expected %q", resp.Value, update.Value)
		}
		if !bytes.Equal(resp.Node.ID(), pair.server.ID()) {
			t.Errorf("update response ID is %x, expected %x", resp.Node.ID(), pair.server.ID())
		}
	}
}
