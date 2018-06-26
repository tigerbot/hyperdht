package dhtRpc

import (
	"context"
	"testing"
	"time"

	"gitlab.daplie.com/core-sdk/hyperdht/fakeNetwork"
)

func createDHTNode(t *testing.T, network *fakeNetwork.FakeNetwork, ipv6, public bool) *DHT {
	sock := network.NewNode(fakeNetwork.RandomAddress(ipv6), public)
	node, err := New(&Config{Socket: sock, IPv6: ipv6, timeout: 5 * time.Millisecond})
	if err != nil {
		t.Fatal("failed to create new DHT node:", err)
	}
	return node
}

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
	node2 := createDHTNode(t, network, true, true)
	defer node2.Close()

	// These tests are mostly to increase coverage, but also to make sure things don't
	// crash when we receive requests with incorrectly encoded addresses.
	if _, err := node1.Ping(ctx, node2.Addr()); err == nil {
		t.Error("IPv4 node did not error pinging IPv6 node")
	}
	if _, err := node2.Ping(ctx, node1.Addr()); err == nil {
		t.Error("IPv6 node did not error pinging IPv4 node")
	}

	if err := node1.Holepunch(ctx, node2.Addr(), middle.Addr()); err == nil {
		t.Error("IPv4 node did not error hole punching to IPv6 node")
	}
	if err := node2.Holepunch(ctx, node1.Addr(), middle.Addr()); err == nil {
		t.Error("IPv6 node did not error hole punching to IPv4 node")
	}
}
