package hyperdht

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"testing"
)

func testStoreIterator(t *testing.T, getNext func() *peerInfo, infos ...*peerInfo) {
	peerMap := make(map[*peerInfo]bool, len(infos))
	for _, peer := range infos {
		peerMap[peer] = false
	}

	var failed bool
	for {
		if next := getNext(); next == nil {
			break
		} else if visited, expected := peerMap[next]; !expected {
			t.Errorf("received unexpected peer: %p (%x)", next, next.encoded)
			failed = true
		} else if visited {
			t.Errorf("received peer multiple times: %p (%x)", next, next.encoded)
			failed = true
		} else {
			peerMap[next] = true
		}
	}

	for peer, visited := range peerMap {
		if !visited {
			t.Errorf("did not receive expected peer: %p (%x)", peer, peer.encoded)
			failed = true
		}
	}

	if failed {
		_, _, line, _ := runtime.Caller(1)
		t.Fatalf("check on line %d failed, all subsequent checks will likely also fail", line)
	}
}

func TestStoreBasics(t *testing.T) {
	var s store
	s.gc()

	info := &peerInfo{encoded: []byte("not actually a peer")}
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
		infos[ind] = &peerInfo{encoded: make([]byte, 16)}
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

	// Delete the nodes that should be left in the old list and make sure the olds lists are empty
	s.Del("test-key", "test-id-6")
	s.Del("test-key", "test-id-7")
	testStoreIterator(t, s.Iterator("test-key"), infos[:6]...)
	if len(s.prevLists) != 0 || len(s.prevValues) != 0 {
		t.Errorf("previous list/values should be empty now but aren't: %#v", &s)
	}

	// garbage collect to put all stored values in old list, then manipulate the size so the
	// next Put calls will trigger the garbage collection again. After that only the most
	// recently stored infos should still be in the list.
	s.gc()
	testStoreIterator(t, s.Iterator("test-key"), infos[:6]...)
	s.size = maxSize - 2
	s.Put("test-key", "test-id-6", infos[6])
	s.Put("test-key", "test-id-7", infos[7])
	testStoreIterator(t, s.Iterator("test-key"), infos[6], infos[7])
}

func TestStoreConsistency(t *testing.T) {
	var s store
	s.gc()

	s.Put("test-key", "test-id-0", &peerInfo{encoded: []byte("not actually a peer")})
	s.Put("test-key", "test-id-1", &peerInfo{encoded: []byte("also not a peer")})

	list := s.lists["test-key"]
	list[0].index = 1
	defer func() {
		if err := recover(); err == nil {
			t.Error("store did not panic when internal state was inconsistent")
		}
	}()
	s.Del("test-key", "test-id-0")
}
