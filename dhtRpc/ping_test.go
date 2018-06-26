package dhtRpc

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"gitlab.daplie.com/core-sdk/hyperdht/fakeNetwork"
)

func TestPing(t *testing.T) { dualIPTest(t, pingTest) }
func pingTest(t *testing.T, ipv6 bool) {
	network := fakeNetwork.New()
	defer network.Close()

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	node1 := createDHTNode(t, network, ipv6, true)
	defer node1.Close()
	node2 := createDHTNode(t, network, ipv6, true)
	defer node2.Close()

	if addr, err := node2.Ping(ctx, node1.Addr()); err != nil {
		t.Error("failed to ping node1 from node2:", err)
	} else if expected := node2.Addr(); addr.String() != expected.String() {
		t.Errorf("node1 told node2 it's address was %s, should have been %s", addr, expected)
	}
}

func TestHolepunch(t *testing.T) { dualIPTest(t, holepunchTest) }
func holepunchTest(t *testing.T, ipv6 bool) {
	network := fakeNetwork.New()
	defer network.Close()

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	middle := createDHTNode(t, network, ipv6, true)
	defer middle.Close()
	node1 := createDHTNode(t, network, ipv6, false)
	defer node1.Close()
	node2 := createDHTNode(t, network, ipv6, false)
	defer node2.Close()

	// We first need to open a path between the private nodes and the public node
	if _, err := node1.Ping(ctx, middle.Addr()); err != nil {
		t.Fatal("first node failed to ping the middle public node:", err)
	}
	if _, err := node2.Ping(ctx, middle.Addr()); err != nil {
		t.Fatal("second node failed to ping the middle public node:", err)
	}

	if _, err := node2.Ping(ctx, node1.Addr()); err == nil {
		t.Error("second node unexpectedly succeeded pinging first node on first try")
	} else if err = node2.Holepunch(ctx, node1.Addr(), middle.Addr()); err != nil {
		t.Error("second node failed to holepunch to first node:", err)
	} else if _, err = node2.Ping(ctx, node1.Addr()); err != nil {
		t.Error("second node failed to ping first node after holepunch:", err)
	}
}

func TestCrossEncodingPing(t *testing.T) {
	network := fakeNetwork.New()
	defer network.Close()

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	middle := createDHTNode(t, network, false, true)
	defer middle.Close()
	node1 := createDHTNode(t, network, false, true)
	defer node1.Close()

	// IPv6 node can't use createDHTNode because we need to make sure we have an IPv4
	// address even though we are using IPv6 encoding.
	sock := network.NewNode(fakeNetwork.RandomAddress(false), true)
	node2, err := New(&Config{Socket: sock, IPv6: true, timeout: 5 * time.Millisecond})
	if err != nil {
		t.Fatal("failed to create new DHT node:", err)
	}
	defer node2.Close()

	// These tests are mostly to increase coverage, but also to make sure things don't
	// crash when we receive requests with incorrectly encoded addresses. The checking
	// of the error message is mostly to make sure we are actually hitting the conditions
	// that we wrote the tests for.
	if _, err := node1.Ping(ctx, node2.Addr()); err == nil {
		t.Error("IPv4 node did not error pinging IPv6 node")
	} else if !strings.Contains(err.Error(), "invalid address") {
		t.Errorf(`IPv4 pinging IPv6 errored with %q, expected it to contain "invalid address"`, err)
	}
	if _, err := node2.Ping(ctx, node1.Addr()); err == nil {
		t.Error("IPv6 node did not error pinging IPv4 node")
	} else if !strings.Contains(err.Error(), "invalid address") {
		t.Errorf(`IPv6 pinging IPv4 errored with %q, expected it to contain "invalid address"`, err)
	}

	// Holepunching to a node using a different encoding or through a node with a different
	// encoding will cause the requests to simply be dropped, so we should have a timeout.
	type timeoutErr interface {
		Timeout() bool
	}
	if err := node1.Holepunch(ctx, node2.Addr(), middle.Addr()); err == nil {
		t.Error("IPv4 node did not error hole punching to IPv6 node")
	} else if !strings.Contains(err.Error(), "timed out") {
		t.Errorf(`IPv4 hole punching to IPv6 errored with %q, expected it to contain "timed out"`, err)
	} else if tErr, ok := err.(timeoutErr); !ok || !tErr.Timeout() {
		t.Errorf("IPv4 hole punching to IPv6 error didn't have Timeout method: %v", err)
	}
	if err := node2.Holepunch(ctx, node1.Addr(), middle.Addr()); err == nil {
		t.Error("IPv6 node did not error hole punching to IPv4 node")
	} else if !strings.Contains(err.Error(), "timed out") {
		t.Errorf(`IPv6 hole punching to IPv4 errored with %q, expected it to contain "timed out"`, err)
	} else if tErr, ok := err.(timeoutErr); !ok || !tErr.Timeout() {
		t.Errorf("IPv6 hole punching to IPv4 error didn't have Timeout method: %v", err)
	}

	// Trying to holepunch to a node whose address is non-encodable by the node should return
	// an error rather than accidentally behaving like a normal ping.
	ipv6Addr := &net.UDPAddr{IP: net.IPv6loopback, Port: 54321}
	if err := node1.Holepunch(ctx, ipv6Addr, middle.Addr()); err == nil {
		t.Error("IPv4 node holepunching to actual IPv6 address did not error")
	} else if !strings.Contains(err.Error(), "invalid peer address") {
		t.Errorf(`IPv4 holepunching to IPv6 address errored with %q, expected it to contain "invalid peer address"`, err)
	}
}
