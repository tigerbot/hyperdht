package kbucket

import "testing"

func TestDistance(t *testing.T) {
	type expectation struct {
		a, b   []byte
		closer bool
	}

	distCmp := XORDistance{0x0f, 0xf0}
	expected := []expectation{
		// identical buffers should not be closer than eachother
		{a: []byte{0xff}, b: []byte{0xff}, closer: false},
		{a: []byte{0xff, 0x00}, b: []byte{0xff, 0x00}, closer: false},
		{a: []byte{0xff, 0x00, 0x55}, b: []byte{0xff, 0x00, 0x55}, closer: false},

		// buffers with length closer to target are closer if all else before is same
		{a: []byte{0xff}, b: []byte{0xff, 0x00}, closer: false},
		{a: []byte{0xff, 0x00}, b: []byte{0xff}, closer: true},
		{a: []byte{0xff, 0x00, 0x55}, b: []byte{0xff, 0x00}, closer: false},
		{a: []byte{0xff, 0x00}, b: []byte{0xff, 0x00, 0x55}, closer: true},
	}

	for _, e := range expected {
		if closer := distCmp.Closer(e.a, e.b); closer != e.closer {
			t.Errorf("comparing %x to %x returned %v, expected %v", e.a, e.b, closer, e.closer)
		}
	}
}
