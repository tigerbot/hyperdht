package kbucket

import (
	"sync"
	"testing"
)

func TestOnAdd(t *testing.T) {
	bucket := New(&Config{LocalID: []byte{0, 0}})
	var wait sync.Mutex
	wait.Lock()

	added := &testContact{0x80, 0x00}
	bucket.OnAdd(func(c Contact) {
		if c != added {
			t.Errorf("on add received %#v, expected %#v", c, added)
		}
		wait.Unlock()
	})
	bucket.Add(added)
	wait.Lock()

	bucket.OnAdd(nil)
	for i := byte(1); i < DefaultBucketSize; i++ {
		bucket.Add(&testContact{0x00, i})
	}
	if len(bucket.root.contacts) != DefaultBucketSize {
		t.Fatal("root node's contact list is not filled to capacity")
	}

	added = &testContact{0x80, 0x01}
	bucket.OnAdd(func(c Contact) {
		if c != added {
			t.Errorf("on add received %#v, expected %#v", c, added)
		}
		wait.Unlock()
	})
	bucket.Add(added)
	wait.Lock()

	if bucket.root.contacts != nil {
		t.Error("final add did not trigger a split")
	}
}

func TestOnUpdate(t *testing.T) {
	bucket := New(&Config{LocalID: []byte{0, 0}})
	var wait sync.Mutex
	wait.Lock()

	first := &testContact{0x80, 0x01}
	second := &testContact{0x80, 0x01}
	bucket.OnUpdate(func(prev, next Contact) {
		if prev != first {
			t.Errorf("on update received %#v for first argument, expected %#v", prev, first)
		}
		if next != second {
			t.Errorf("on update received %#v for first argument, expected %#v", next, second)
		}
		wait.Unlock()
	})
	bucket.Add(first)
	bucket.Add(second)
	wait.Lock()
}

func TestOnRemove(t *testing.T) {
	bucket := New(&Config{LocalID: []byte{0, 0}})
	var wait sync.Mutex
	wait.Lock()

	added := &testContact{0x80, 0x00}
	bucket.Add(added)

	bucket.OnRemove(func(c Contact) {
		if c != added {
			t.Errorf("on remove received %#v, expected %#v", c, added)
		}
		wait.Unlock()
	})
	bucket.Remove(added.ID())
	wait.Lock()
}

func TestPing(t *testing.T) {
	bucket := New(&Config{LocalID: []byte{0, 0}})
	var wait sync.Mutex
	wait.Lock()

	bucket.OnPing(func(current []Contact, replacement Contact) {
		if len(current) != DefaultPingCount {
			t.Errorf("got %d nodes to ping, expected %d", len(current), DefaultPingCount)
		}
		for i := range current {
			if expect := bucket.root.right.contacts[i]; current[i] != expect {
				t.Errorf("contact %d doesn't match: got %#v, expected %#v", i, current[i], expect)
			}
		}
		if r := replacement.ID(); r[0] != 0x80 || r[1] != DefaultBucketSize {
			t.Errorf("wrong replacement contact ID: got 0x%04x, expected 0x80%02x", r, DefaultBucketSize)
		}
		wait.Unlock()
	})

	for i := byte(0); i <= DefaultBucketSize; i++ {
		bucket.Add(&testContact{0x80, i}) // make sure they all go into "far away" node
	}
	wait.Lock()
}
