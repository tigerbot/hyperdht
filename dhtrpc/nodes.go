package dhtrpc

import (
	"bytes"
	"net"
	"sync"

	"github.com/tigerbot/hyperdht/ipencoding"
	"github.com/tigerbot/hyperdht/kbucket"
)

// A Node represents another peer
type Node = ipencoding.Node

var (
	_, _, _ Node            = new(basicNode), new(storedNode), new(queryNode)
	_       kbucket.Contact = new(storedNode)
)

type basicNode struct {
	id   []byte
	addr net.Addr
}

func (n basicNode) ID() []byte     { return n.id }
func (n basicNode) Addr() net.Addr { return n.addr }

type storedNode struct {
	basicNode

	tick       uint64
	prev, next *storedNode
}

// storedNodeList maintains a linked list and a KBucket in parallel. The KBucket storage is
// to gain the efficiencies of a Kademlia DHT, while the linked list allows us to keep track
// of which nodes are the oldest regardless of their ID.
type storedNodeList struct {
	lock sync.Mutex

	*kbucket.KBucket
	top    *storedNode
	bottom *storedNode
}

func (l *storedNodeList) init(id []byte, onPing kbucket.FuncPing) {
	l.KBucket = kbucket.New(&kbucket.Config{
		LocalID: id,
		OnPing:  onPing,

		OnAdd:    l.onNodeAdd,
		OnRemove: l.onNodeRemove,
		OnUpdate: l.onNodeUpdate,
	})
}

func (l *storedNodeList) oldest(cnt int) []*storedNode {
	l.lock.Lock()
	defer l.lock.Unlock()

	list := make([]*storedNode, 0, cnt)
	for i, oldest := 0, l.bottom; i < cnt && oldest != nil; i, oldest = i+1, oldest.next {
		list = append(list, oldest)
	}
	return list
}

func (l *storedNodeList) addToLinkedList(c kbucket.Contact) {
	n, ok := c.(*storedNode)
	if !ok {
		// This must be in a separate go-routine because we still hold the KBucket lock, and
		// calling the Remove function will try to acquire it again, resulting in deadlock.
		go l.Remove(c.ID())
		return
	}

	if l.top == nil && l.bottom == nil {
		l.top, l.bottom = n, n
		n.prev, n.next = nil, nil
	} else {
		n.prev, n.next = l.top, nil
		l.top.next = n
		l.top = n
	}
}
func (l *storedNodeList) removeFromLinkedList(c kbucket.Contact) {
	n, ok := c.(*storedNode)
	if !ok {
		return
	}

	if l.bottom != n && l.top != n {
		n.prev.next = n.next
		n.next.prev = n.prev
	} else {
		if l.bottom == n {
			l.bottom = n.next
			if l.bottom != nil {
				l.bottom.prev = nil
			}
		}
		if l.top == n {
			l.top = n.prev
			if l.top != nil {
				l.top.next = nil
			}
		}
	}
	n.next, n.prev = nil, nil
}

func (l *storedNodeList) onNodeAdd(c kbucket.Contact) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.addToLinkedList(c)
}
func (l *storedNodeList) onNodeRemove(c kbucket.Contact) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.removeFromLinkedList(c)
}
func (l *storedNodeList) onNodeUpdate(old, fresh kbucket.Contact) {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.removeFromLinkedList(old)
	l.addToLinkedList(fresh)
}

type queryNode struct {
	basicNode

	queried        bool
	roundTripToken []byte
	referrer       net.Addr
}

type queryNodeList struct {
	capacity int
	distCmp  kbucket.DistanceCmp
	list     []*queryNode
}

func (l *queryNodeList) get(id []byte) *queryNode {
	for _, n := range l.list {
		if bytes.Equal(id, n.id) {
			return n
		}
	}
	return nil
}

func (l *queryNodeList) getNotQueried() *queryNode {
	for _, n := range l.list {
		if !n.queried {
			return n
		}
	}

	return nil
}

func (l *queryNodeList) insert(n *queryNode) {
	if len(l.list) >= l.capacity && l.distCmp.Closer(l.list[len(l.list)-1].id, n.id) {
		return
	}
	if n.id != nil && l.get(n.id) != nil {
		return
	}

	if len(l.list) < l.capacity {
		l.list = append(l.list, n)
	} else {
		l.list[len(l.list)-1] = n
	}

	for pos := len(l.list) - 1; pos > 0 && l.distCmp.Closer(l.list[pos].id, l.list[pos-1].id); pos-- {
		l.list[pos-1], l.list[pos] = l.list[pos], l.list[pos-1]
	}
}
