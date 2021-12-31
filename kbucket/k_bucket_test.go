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

type advancedContact struct {
	id  []byte
	msg string
}

func (c *advancedContact) ID() []byte { return c.id }

func TestAdd(t *testing.T) {
	bucket := New[*testContact](nil)

	a := &testContact{'a'}
	bucket.Add(a)
	if len(bucket.root.contacts) != 1 {
		t.Fatalf("root node doesn't contain added contact: %v", bucket.root)
	} else if !reflect.DeepEqual(bucket.root.contacts, []*testContact{a}) {
		t.Fatal("root node contains wrong contact")
	}

	bucket.Add(a.copy())
	if len(bucket.root.contacts) != 1 {
		t.Fatalf("root node duplicated contact with same ID: %v", bucket.root)
	} else if !reflect.DeepEqual(bucket.root.contacts, []*testContact{a}) {
		t.Fatal("root node contains wrong contact after re-add")
	}

	b := &testContact{'b'}
	bucket.Add(b)
	if len(bucket.root.contacts) != 2 {
		t.Fatalf("root node doesn't contain second contact: %v", bucket.root)
	} else if !reflect.DeepEqual(bucket.root.contacts, []*testContact{a, b}) {
		t.Fatal("root node contains wrong contacts with second contact")
	}

	bucket.Add(a)
	if !reflect.DeepEqual(bucket.root.contacts, []*testContact{b, a}) {
		t.Fatal("root node contains wrong contact order after update")
	}
}

func TestArbiter(t *testing.T) {
	bucket := New[*advancedContact](nil)

	a1 := &advancedContact{id: []byte{'a'}, msg: "this is the first a"}
	a2 := &advancedContact{id: []byte{'a'}, msg: "this is the second a"}
	a3 := &advancedContact{id: []byte{'a'}, msg: "this won't ever be added"}

	bucket.Arbiter(func(a, b *advancedContact) *advancedContact { return a })
	bucket.Add(a1)
	bucket.Add(a2)
	if !reflect.DeepEqual(bucket.root.contacts, []*advancedContact{a1}) {
		t.Fatal("root node contains wrong contact after second add")
	}

	bucket.Arbiter(nil)
	bucket.Add(a2)
	if !reflect.DeepEqual(bucket.root.contacts, []*advancedContact{a2}) {
		t.Fatal("root node contains wrong contact after third add")
	}

	bucket.Arbiter(func(_, _ *advancedContact) *advancedContact { return a3 })
	bucket.Add(a2)
	if !reflect.DeepEqual(bucket.root.contacts, []*advancedContact{a3}) {
		t.Fatal("root node contains wrong contact after fourth add")
	}
}

func TestGet(t *testing.T) {
	bucket := New[*testContact](nil)

	if c := bucket.Get([]byte("foo")); c != nil {
		t.Errorf("get on non-existent ID returned non-nil contact %v", c)
	}

	a := &testContact{'a'}
	bucket.Add(a)
	if c := bucket.Get([]byte{'a'}); c != a {
		t.Errorf("Get returned %#v, expected %#v", c, a)
	}
}

func TestRemove(t *testing.T) {
	bucket := New[*testContact](nil)

	bucket.Add(&testContact{'a'})
	if len(bucket.root.contacts) != 1 {
		t.Fatalf("root node doesn't contain added contact: %v", bucket.root)
	}

	if r := bucket.Remove([]byte{'b'}); r != nil {
		t.Errorf("did not return nil removing non-existent contact")
	} else if len(bucket.root.contacts) != 1 {
		t.Fatalf("removing non-existent contact changed stored contacts")
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
	bucket := New[*testContact](&Config[*testContact]{LocalID: []byte{0, 0}})
	for i := byte(0); i < 0x12; i++ {
		bucket.Add(&testContact{i})
	}

	if closest := bucket.Closest(XORDistance([]byte{0x15}), 0); closest != nil {
		t.Errorf("got %#v for 0 closest values, expected nil", closest)
	}
	if closest := bucket.Closest(nil, 5); closest != nil {
		t.Errorf("got %#v for closest values with nil distance comparer, expected nil", closest)
	}

	closest := bucket.Closest(XORDistance([]byte{0x15}), 3)
	expected := [][]byte{
		{0x11}, // distance: 00000100
		{0x10}, // distance: 00000101
		{0x05}, // distance: 00010000
	}
	if len(closest) != 3 {
		t.Errorf("Close returned %d contacts, expected 3", len(closest))
	} else {
		for i := range closest {
			if !bytes.Equal(closest[i].ID(), expected[i]) {
				t.Errorf("wrong ID for contact %d: got %x, expected %x", i, closest[i].ID(), expected[i])
			}
		}
	}

	closest = bucket.Closest(XORDistance([]byte{0x11}), 3)
	expected = [][]byte{
		{0x11}, // distance: 00000000
		{0x10}, // distance: 00000001
		{0x01}, // distance: 00010000
	}
	if len(closest) != 3 {
		t.Errorf("Close returned %d contacts, expected 3", len(closest))
	} else {
		for i := range closest {
			if !bytes.Equal(closest[i].ID(), expected[i]) {
				t.Errorf("wrong ID for contact %d: got %x, expected %x", i, closest[i].ID(), expected[i])
			}
		}
	}
}

func TestClosestAll(t *testing.T) {
	bucket := New[*testContact](&Config[*testContact]{LocalID: []byte{0, 0}})
	for i := 0; i < 1e3; i++ {
		bucket.Add(&testContact{byte(i / 256), byte(i % 256)})
	}

	closest := bucket.Closest(XORDistance([]byte{0x80, 0x80}), -1)
	// Remember that not all of the things we added will actually be added to the tree.
	if len(closest) < 100 {
		t.Errorf("got closest list length %d, expected at least 100", len(closest))
	}

	// Also make sure we don't panic trying to get more than exist.
	closest = bucket.Closest(XORDistance([]byte{0x80, 0x80}), 2e3)
	if len(closest) < 100 {
		t.Errorf("got closest list length %d, expected at least 100", len(closest))
	}
}

func TestCount(t *testing.T) {
	bucket := New[*testContact](nil)

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
		t.Fatalf("bucket after re-add has count %d, expected 1", cnt)
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
		t.Fatalf("bucket after multi-add has count %d, expected 1", cnt)
	}
}

func TestLargeBucket(t *testing.T) {
	bucket := New[*testContact](&Config[*testContact]{LocalID: []byte{0x55, 0x55}})
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
		t.Errorf("got contact %#v using non-existent ID, expected nil", c)
	}
	if c := bucket.Get([]byte{0x40, 0x03}); c == nil {
		t.Errorf("got nil contact using ID that should have existed")
	} else if expected := (&testContact{0x40, 0x03}); !reflect.DeepEqual(c, expected) {
		t.Errorf("retrieved contact is %#v, expected %#v", c, expected)
	}

	bucket.Add(&testContact{0x55, 0x00})
	closest := bucket.Closest(XORDistance([]byte{0x55, 0x05}), DefaultBucketSize+2)
	if expected := (&testContact{0x55, 0x00}); !reflect.DeepEqual(closest[0], expected) {
		t.Errorf("first closest contact is %#v, expected %#v", closest[0], expected)
	}
	for i, c := range closest[1 : DefaultBucketSize+1] {
		if id := c.ID(); id[0] != 0x54 {
			t.Errorf("first byte of contact #%d is 0x%02x, expected 0x54", i, id[0])
		}
	}
	if expected := (&testContact{0x56, 0x05}); !reflect.DeepEqual(closest[DefaultBucketSize+1], expected) {
		t.Errorf("most distant contact is %#v, expected %#v", closest[DefaultBucketSize+1], expected)
	}
}
