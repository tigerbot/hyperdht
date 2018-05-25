package kbucket

import (
	"bytes"
	"reflect"
	"testing"
)

type testContact []byte

// Methods are on the pointers because slices aren't comparable, but pointers to slices are.
func (c *testContact) ID() []byte { return []byte(*c) }
func (c *testContact) copy() *testContact {
	cp := make(testContact, len(*c))
	copy(cp, *c)
	return &cp
}

func TestAdd(t *testing.T) {
	bucket := New(nil)

	a := &testContact{'a'}
	bucket.Add(a)
	if len(bucket.root.contacts) != 1 {
		t.Fatalf("root node doesn't contain added contact: %v", bucket.root)
	} else if !reflect.DeepEqual(bucket.root.contacts, []Contact{a}) {
		t.Fatal("root node contains wrong contact")
	}

	bucket.Add(a.copy())
	if len(bucket.root.contacts) != 1 {
		t.Fatalf("root node duplicated contact with same ID: %v", bucket.root)
	} else if !reflect.DeepEqual(bucket.root.contacts, []Contact{a}) {
		t.Fatal("root node contains wrong contact after re-add")
	}

	b := &testContact{'b'}
	bucket.Add(b)
	if len(bucket.root.contacts) != 2 {
		t.Fatalf("root node doesn't contain second contact: %v", bucket.root)
	} else if !reflect.DeepEqual(bucket.root.contacts, []Contact{a, b}) {
		t.Fatal("root node contains wrong contacts with second contact")
	}

	bucket.Add(a)
	if !reflect.DeepEqual(bucket.root.contacts, []Contact{b, a}) {
		t.Fatal("root node contains wrong contact order after update")
	}
}

func TestGet(t *testing.T) {
	bucket := New(nil)

	if c := bucket.Get([]byte("foo")); c != nil {
		t.Errorf("get on non-existant ID returned non-nil contact %v", c)
	}

	a := &testContact{'a'}
	bucket.Add(a)
	if c := bucket.Get([]byte{'a'}); c != a {
		t.Errorf("Get returned %#v, expected %#v", c, a)
	}
}

func TestRemove(t *testing.T) {
	bucket := New(nil)

	bucket.Add(&testContact{'a'})
	if len(bucket.root.contacts) != 1 {
		t.Fatalf("root node doesn't contain added contact: %v", bucket.root)
	}

	if r := bucket.Remove([]byte{'b'}); r != nil {
		t.Errorf("did not return nil removing non-existant contact")
	} else if len(bucket.root.contacts) != 1 {
		t.Fatalf("removing non-existant contact changed stored contacts")
	}

	bucket.Remove([]byte{'a'})
	if len(bucket.root.contacts) != 0 {
		t.Fatalf("root node still contains removed contact: %v", bucket.root)
	}

	if r := bucket.Remove([]byte{'a'}); r != nil {
		t.Errorf("did not return nil removing contact second time")
	} else if len(bucket.root.contacts) != 0 {
		t.Fatalf("removing contact second time changed stored contacts")
	}
}

func TestClosest(t *testing.T) {
	bucket := New(&Config{LocalID: []byte{0, 0}})
	for i := byte(0); i < 0x12; i++ {
		bucket.Add(&testContact{i})
	}

	if close := bucket.Closest(XORDistance([]byte{0x15}), 0); close != nil {
		t.Errorf("got %#v for 0 closest values, expected nil", close)
	}
	if close := bucket.Closest(nil, 5); close != nil {
		t.Errorf("got %#v for closest values with nil distance comparer, expected nil", close)
	}

	close := bucket.Closest(XORDistance([]byte{0x15}), 3)
	expected := [][]byte{
		{0x11}, // distance: 00000100
		{0x10}, // distance: 00000101
		{0x05}, // distance: 00010000
	}
	if len(close) != 3 {
		t.Errorf("Close returned %d contacts, expected 3", len(close))
	} else {
		for i := range close {
			if !bytes.Equal(close[i].ID(), expected[i]) {
				t.Errorf("wrong ID for contact %d: got %x, expected %x", i, close[i].ID(), expected[i])
			}
		}
	}

	close = bucket.Closest(XORDistance([]byte{0x11}), 3)
	expected = [][]byte{
		{0x11}, // distance: 00000000
		{0x10}, // distance: 00000001
		{0x01}, // distance: 00010000
	}
	if len(close) != 3 {
		t.Errorf("Close returned %d contacts, expected 3", len(close))
	} else {
		for i := range close {
			if !bytes.Equal(close[i].ID(), expected[i]) {
				t.Errorf("wrong ID for contact %d: got %x, expected %x", i, close[i].ID(), expected[i])
			}
		}
	}
}

func TestClosestAll(t *testing.T) {
	bucket := New(&Config{LocalID: []byte{0, 0}})
	for i := 0; i < 1e3; i++ {
		bucket.Add(&testContact{byte(i / 256), byte(i % 256)})
	}

	close := bucket.Closest(XORDistance([]byte{0x80, 0x80}), -1)
	// Remember that not all of the things we added will actually be added to the tree.
	if len(close) < 100 {
		t.Errorf("got closest list length %d, expected at least 100", len(close))
	}

	// Also make sure we don't panic trying to get more than exist.
	close = bucket.Closest(XORDistance([]byte{0x80, 0x80}), 2e3)
	if len(close) < 100 {
		t.Errorf("got closest list length %d, expected at least 100", len(close))
	}
}

func TestCount(t *testing.T) {
	bucket := New(nil)

	if cnt := bucket.Count(); cnt != 0 {
		t.Fatalf("fresh bucket has count %d, expected 0", cnt)
	}

	a := &testContact{'a'}
	bucket.Add(a)
	if cnt := bucket.Count(); cnt != 1 {
		t.Fatalf("bucket after one add has count %d, expected 1", cnt)
	}
	bucket.Add(a)
	if cnt := bucket.Count(); cnt != 1 {
		t.Fatalf("bucket after re-add has count %d, exptected 1", cnt)
	}

	bucket.Add(&testContact{'a'})
	bucket.Add(&testContact{'a'})
	bucket.Add(&testContact{'b'})
	bucket.Add(&testContact{'b'})
	bucket.Add(&testContact{'c'})
	bucket.Add(&testContact{'d'})
	bucket.Add(&testContact{'e'})
	bucket.Add(&testContact{'f'})
	if cnt := bucket.Count(); cnt != 6 {
		t.Fatalf("bucket after multi-add has count %d, exptected 1", cnt)
	}
}

func TestLargeBucket(t *testing.T) {
	bucket := New(&Config{LocalID: []byte{0x55, 0x55}})
	for i := byte(0); i < DefaultBucketSize; i++ {
		bucket.Add(&testContact{0x80, i})
		bucket.Add(&testContact{0x00, i})
		bucket.Add(&testContact{0x60, i})
		bucket.Add(&testContact{0x40, i})
		bucket.Add(&testContact{0x58, i})
		bucket.Add(&testContact{0x50, i})
		bucket.Add(&testContact{0x56, i})
		bucket.Add(&testContact{0x54, i})
	}

	if cnt := bucket.Count(); cnt != 8*DefaultBucketSize {
		t.Fatalf("bucket count %d does not match expected %d", cnt, 8*DefaultBucketSize)
	}

	if c := bucket.Get([]byte{0x5f, 0x00}); c != nil {
		t.Errorf("got contact %#v using non-existant ID, expected nil", c)
	}
	if c := bucket.Get([]byte{0x40, 0x03}); c == nil {
		t.Errorf("got nil contact using ID that should have existed")
	} else if expected := (&testContact{0x40, 0x03}); !reflect.DeepEqual(c, expected) {
		t.Errorf("retrieved contact is %#v, expected %#v", c, expected)
	}

	bucket.Add(&testContact{0x55, 0x00})
	close := bucket.Closest(XORDistance([]byte{0x55, 0x05}), DefaultBucketSize+2)
	if expected := (&testContact{0x55, 0x00}); !reflect.DeepEqual(close[0], expected) {
		t.Errorf("first closest contact is %#v, expected %#v", close[0], expected)
	}
	for i, c := range close[1 : DefaultBucketSize+1] {
		if id := c.ID(); id[0] != 0x54 {
			t.Errorf("first byte of contact #%d is 0x%02x, expected 0x54", i, id[0])
		}
	}
	if expected := (&testContact{0x56, 0x05}); !reflect.DeepEqual(close[DefaultBucketSize+1], expected) {
		t.Errorf("most distant contact is %#v, expected %#v", close[DefaultBucketSize+1], expected)
	}
}
