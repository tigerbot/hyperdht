package dhtRpc

import (
	"testing"

	"gitlab.daplie.com/core-sdk/hyperdht/kbucket"
)

func TestNodeListOrder(t *testing.T) {
	l := nodeList{
		capacity: 5,
		distCmp:  kbucket.XORDistance([]byte{0x55}),
	}

	for i := 0; i < 256; i++ {
		n := &queryNode{
			basicNode: basicNode{id: []byte{byte(i)}},
		}
		l.insert(n)
	}

	expected := map[int]bool{
		0x55: true,
		0x54: true,
		0x56: true,
		0x57: true,
		0x51: true,
	}
	for i := 0; i < 256; i++ {
		if n := l.get([]byte{byte(i)}); expected[i] && n == nil {
			t.Errorf("expected to have node for ID %x but didn't", i)
		} else if !expected[i] && n != nil {
			t.Errorf("expected to not have node for ID %x but did", i)
		}
	}
}
