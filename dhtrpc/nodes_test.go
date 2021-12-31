package dhtrpc

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigerbot/hyperdht/kbucket"
)

func TestQueryNodeListOrder(t *testing.T) {
	l := queryNodeList{
		capacity: 5,
		distCmp:  kbucket.XORDistance([]byte{0x55}),
	}

	for i := 0; i < 256; i++ {
		n := &queryNode{
			basicNode: basicNode{id: []byte{byte(i)}},
		}
		l.insert(n)
	}

	expected := []byte{0x55, 0x54, 0x57, 0x56, 0x51}
	for i, b := range expected {
		n := l.getNotQueried()
		if n == nil {
			t.Fatalf("got nil node for query %d", i)
		}
		n.queried = true

		if len(n.id) != 1 || n.id[0] != b {
			t.Errorf("got ID %x for query %d, expected %x", n.id, i, b)
		}
	}
	if n := l.getNotQueried(); n != nil {
		t.Errorf("got unexpected node %#v", n)
	}
}

func TestStoredNodeListOrder(t *testing.T) {
	var l storedNodeList
	l.init([]byte{0x55, 0xaa}, nil)

	// This part only has real meaning if the race detector is running.
	var done int32
	defer atomic.AddInt32(&done, 1)
	go func() {
		for atomic.LoadInt32(&done) == 0 {
			l.oldest(3)
			time.Sleep(10 * time.Microsecond)
		}
	}()

	check := func(expected [][]byte) {
		for i, n := range l.oldest(len(expected)) {
			if !bytes.Equal(expected[i], n.id) {
				t.Errorf("oldest #%d had ID %x, expected %x", i, n.id, expected[i])
			}
		}
	}

	toAdd := [][]byte{{0x5a, 0xa5}, {0xa5, 0x5a}, {0xff, 0xff}, {0xa1, 0xb2}}
	for _, id := range toAdd {
		l.Add(&storedNode{basicNode: basicNode{id: id}})
	}
	check(toAdd[:2])

	// Adding a node already in the list should move it to the newest part of the list
	l.Add(&storedNode{basicNode: basicNode{id: toAdd[1]}})
	check([][]byte{toAdd[0], toAdd[2], toAdd[3], toAdd[1]})
	l.Add(&storedNode{basicNode: basicNode{id: toAdd[0]}})
	check([][]byte{toAdd[2], toAdd[3], toAdd[1], toAdd[0]})

	l.Remove(toAdd[0])
	check([][]byte{toAdd[2], toAdd[3], toAdd[1]})
	if old := l.oldest(10); len(old) != 3 {
		t.Errorf("list after remove had %d items, expected 3\n\t%#v", len(old), old)
	}
}
