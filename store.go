package hyperdht

import (
	"math/rand"
	"sync"
	"time"
)

const (
	maxSize = 65536
	maxAge  = 15 * time.Minute
)

type store struct {
	lock sync.RWMutex
	size int

	values, prevValues map[string]*storedVal
	lists, prevLists   map[string][]*storedVal
}

type storedVal struct {
	index int

	timestamp time.Time
	value     *peerInfo
}

type peerInfo struct {
	encoded []byte
}

func (s *store) Has(key string) bool {
	_, ok1 := s.lists[key]
	_, ok2 := s.prevLists[key]
	return ok1 || ok2
}

func remove(list []*storedVal, val *storedVal) []*storedVal {
	if list[val.index] != val {
		panic("hyperdht storage got out of sync with itself")
	}

	l := len(list)
	removed := list[l-1]
	if removed != val {
		list[val.index] = removed
		removed.index = val.index
	}
	return list[:l-1]
}

func (s *store) Put(key, id string, val *peerInfo) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fullKey := key + "@" + id

	if prev := s.values[fullKey]; prev != nil {
		prev.timestamp = time.Now()
		prev.value = val
		return
	}
	if prev := s.prevValues[fullKey]; prev != nil {
		s.prevLists[key] = remove(s.prevLists[key], prev)
		delete(s.prevValues, fullKey)
	}

	s.values[fullKey] = &storedVal{
		index:     len(s.lists[key]),
		timestamp: time.Now(),
		value:     val,
	}
	s.size++

	if _, ok := s.lists[key]; !ok {
		s.size++
	}
	s.lists[key] = append(s.lists[key], s.values[fullKey])

	if s.size > maxSize {
		s.gc()
	}
}

func (s *store) Del(key, id string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fullKey := key + "@" + id

	if v := s.values[fullKey]; v != nil {
		if list := remove(s.lists[key], v); len(list) > 0 {
			s.lists[key] = list
		} else {
			delete(s.lists, key)
			s.size--
		}
		delete(s.values, fullKey)
		s.size--
	}

	if v := s.prevValues[fullKey]; v != nil {
		if list := remove(s.prevLists[key], v); len(list) > 0 {
			s.prevLists[key] = list
		} else {
			delete(s.prevLists, key)
		}
		delete(s.prevValues, fullKey)
	}
}

func (s *store) gc() {
	s.prevValues, s.prevLists = s.values, s.lists
	s.values, s.lists = make(map[string]*storedVal), make(map[string][]*storedVal)
	s.size = 0
}

func (s *store) Iterator(key string) func() *peerInfo {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// Technically there is a slightly racey condition here because we will release the lock
	// after making the list, but the values pointed to in the list can change. This isn't
	// a big enough deal for me to worry about until the race detector starts failing and
	// preventing us from detecting more serious race conditions.
	list := make([]*storedVal, 0, len(s.lists[key])+len(s.prevLists[key]))
	list = append(list, s.lists[key]...)
	list = append(list, s.prevLists[key]...)

	now := time.Now()
	return func() *peerInfo {
		var removed *storedVal
		for l := len(list); l > 0; l = len(list) {
			list, removed = list[:l-1], list[l-1]
			if next := rand.Intn(l); next < l-1 {
				removed, list[next] = list[next], removed
			}

			if now.Sub(removed.timestamp) <= maxAge {
				return removed.value
			}
		}

		return nil
	}
}
