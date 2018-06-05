package hyperdht

import (
	"crypto/rand"
	"fmt"
	"testing"
)

func testStoreIterator(t *testing.T, getNext func() *peerInfo, infos ...*peerInfo) {
	peerMap := make(map[*peerInfo]bool, len(infos))
	for _, peer := range infos {
		peerMap[peer] = false
	}

	for {
		if next := getNext(); next == nil {
			break
		} else if visited, expected := peerMap[next]; !expected {
			t.Errorf("received unexpected peer: %#v", next)
		} else if visited {
			t.Errorf("received peer multiple times: %#v", next)
		} else {
			peerMap[next] = true
		}
	}

	for peer, visited := range peerMap {
		if !visited {
			t.Errorf("did not receive expected peer: %#v", peer)
		}
	}
}

func TestStoreBasics(t *testing.T) {
	var s store
	s.gc()

	info := &peerInfo{[]byte("not actually a peer")}
	s.Put("test-key", "test-id", info)
	if !s.Has("test-key") {
		t.Error("storage doesn't have key immediately after Put")
	}

	testStoreIterator(t, s.Iterator("test-key"), info)

	s.Del("test-key", "test-id")
	if s.Has("test-key") {
		t.Error("storage still has key after Del")
	}
}

func TestStoreMultiple(t *testing.T) {
	var s store
	s.gc()

	infos := make([]*peerInfo, 10)
	update := func(ind int) {
		infos[ind] = &peerInfo{make([]byte, 16)}
		if _, err := rand.Read(infos[ind].encoded); err != nil {
			t.Fatal("errored reading 'randomness' into buffer", err)
		}
		s.Put("test-key", fmt.Sprintf("test-id-%d", ind), infos[ind])
	}
	for i := range infos {
		update(i)
	}
	testStoreIterator(t, s.Iterator("test-key"), infos...)

	s.Del("test-key", "test-id-0")
	s.Del("test-key", "test-id-1")
	testStoreIterator(t, s.Iterator("test-key"), infos[2:]...)

	update(4)
	update(7)
	testStoreIterator(t, s.Iterator("test-key"), infos[2:]...)

	s.gc()
	s.Del("test-key", "test-id-8")
	s.Del("test-key", "test-id-9")
	testStoreIterator(t, s.Iterator("test-key"), infos[2:8]...)

	s.Put("test-key", "test-id-0", infos[0])
	s.Put("test-key", "test-id-1", infos[1])
	testStoreIterator(t, s.Iterator("test-key"), infos[:8]...)

	for i := range infos[:6] {
		update(i)
	}
	testStoreIterator(t, s.Iterator("test-key"), infos[:8]...)
}
