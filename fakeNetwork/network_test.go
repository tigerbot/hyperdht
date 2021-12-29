package fakeNetwork_test

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/tigerbot/hyperdht/fakeNetwork"
)

func TestPanics(t *testing.T) {
	run := func(name string, f func()) {
		defer func() {
			if err := recover(); err == nil {
				t.Errorf("%s did not panic", name)
			}
		}()
		f()
		t.Errorf("%s finished", name)
	}

	network := New()
	addr := RandomAddress(false)
	network.NewNode(addr, true)
	run("network.NewNode with used address", func() {
		network.NewNode(addr, true)
	})

	node := new(FakeNode)
	run("uninitialized node.ReadFrom", func() {
		node.ReadFrom(make([]byte, 100))
	})
	run("uninitialized node.WriteTo", func() {
		node.WriteTo(make([]byte, 100), &net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 654})
	})
	run("uninitialized node.Close", func() {
		node.Close()
	})

	// These aren't really here for any reason but to flush out test coverage
	node.LocalAddr()
	node.SetDeadline(time.Time{})
	node.SetReadDeadline(time.Time{})
	node.SetWriteDeadline(time.Time{})
}

func TestBasic(t *testing.T) {
	network := New()
	defer network.Close()

	var wait sync.WaitGroup
	defer wait.Wait()

	const message = "hello 'across' the world!"
	readAddr, writeAddr := RandomAddress(true), RandomAddress(true)
	reader, writer := network.NewNode(readAddr, true), network.NewNode(writeAddr, true)
	defer reader.Close()
	defer writer.Close()

	wait.Add(1)
	go func() {
		defer wait.Done()
		buf := make([]byte, 128)

		if n, addr, err := reader.ReadFrom(buf); err != nil {
			t.Error("first ReadFrom failed:", err)
		} else {
			if n != len(message) {
				t.Errorf("read returned count %d, expected %d", n, len(message))
			} else if str := string(buf[:n]); str != message {
				t.Errorf("read %q, expected %q", str, message)
			}
			if addr.String() != writeAddr.String() {
				t.Errorf("read claimed its message from %q, but %q sent it", addr, writeAddr)
			}
		}

		if n, addr, err := reader.ReadFrom(buf); err == nil {
			t.Errorf("received unexpected message from %s: %v (%[2]q)", addr, buf[:n])
		} else if !strings.Contains(err.Error(), "use of closed") {
			t.Errorf(`error %q did not contain expected phrase "use of closed"`, err)
		}
	}()

	if n, err := writer.WriteTo([]byte(message), readAddr); err != nil {
		t.Fatal("first WriteTo failed:", err)
	} else if n != len(message) {
		t.Errorf("WriteTo return count %d, expected %d", n, len(message))
	}

	if err := writer.Close(); err != nil {
		t.Fatal("error closing writer:", err)
	}
	if n, err := writer.WriteTo([]byte(message), readAddr); err == nil {
		t.Error("WriteTo succeeded after being closed")
	} else {
		if n != 0 {
			t.Errorf("WriteTo on closed node returned count %d, expected 0", n)
		}
		if !strings.Contains(err.Error(), "use of closed") {
			t.Errorf(`error %q did not contain expected phrase "use of closed"`, err)
		}
	}
}

func TestNonBlocking(t *testing.T) {
	network := New()
	defer network.Close()

	const message = "hello 'across' the world!"
	readAddr, writeAddr := RandomAddress(true), RandomAddress(true)
	reader, writer := network.NewNode(readAddr, true), network.NewNode(writeAddr, true)
	defer reader.Close()
	defer writer.Close()

	var action int32
	time.AfterFunc(10*time.Millisecond, func() {
		atomic.StoreInt32(&action, 1)
		reader.Close()
	})
	if _, err := writer.WriteTo([]byte(message), readAddr); err != nil {
		t.Error("WriteTo errored when remote reader closed:", err)
	}
	if atomic.LoadInt32(&action) != 1 {
		t.Error("WriteTo returned before reader closed, test non-functional")
	}

	reader = network.NewNode(readAddr, true)
	action = 0
	time.AfterFunc(10*time.Millisecond, func() {
		atomic.StoreInt32(&action, 1)
		writer.Close()
	})
	if _, err := writer.WriteTo([]byte(message), readAddr); err != nil {
		t.Error("WriteTo errored when closed after call:", err)
	}
	if atomic.LoadInt32(&action) != 1 {
		t.Error("WriteTo returned before writer closed, test non-functional")
	}
}

func TestAccess(t *testing.T) {
	const badMsg = "NAT should reject this"
	const goodMsg = "hole has been punched"

	network := New()
	defer network.Close()

	var wait sync.WaitGroup
	defer wait.Wait()
	read := func(node *FakeNode) {
		wait.Add(1)
		defer wait.Done()

		var valid int
		buf := make([]byte, 128)
		for {
			n, addr, err := node.ReadFrom(buf)
			if err != nil {
				break
			}
			if str := string(buf[:n]); str != goodMsg {
				t.Errorf("received message %q from %s, expected only %q", str, addr, goodMsg)
			} else {
				valid++
			}
		}
		if valid != 1 {
			t.Errorf("node %s received %d valid messages, expected exactly 1", node.LocalAddr(), valid)
		}
	}

	addr1, addr2 := RandomAddress(true), RandomAddress(true)
	node1, node2 := network.NewNode(addr1, false), network.NewNode(addr2, false)
	defer node1.Close()
	defer node2.Close()
	go read(node1)
	go read(node2)

	if _, err := node1.WriteTo([]byte(badMsg), RandomAddress(false)); err != nil {
		t.Error("WriteTo errored sending to non-existent address:", err)
	}

	if _, err := node1.WriteTo([]byte(badMsg), addr2); err != nil {
		t.Error("node1.WriteTo errored sending to node2:", err)
	}
	if _, err := node2.WriteTo([]byte(goodMsg), addr1); err != nil {
		t.Error("node2.WriteTo errored sending to node1:", err)
	}
	if _, err := node1.WriteTo([]byte(goodMsg), addr2); err != nil {
		t.Error("node1.WriteTo errored sending to node2:", err)
	}
}
