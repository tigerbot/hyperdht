package ipencoding

import (
	"bytes"
	"net"
	"testing"
)

func TestIPv6Encoder(t *testing.T) {
	type expectation struct {
		addr net.Addr
		buf  []byte
	}

	expected := []expectation{
		{&net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 80},
			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1, 0, 80}},
		{&net.UDPAddr{IP: net.IPv6loopback, Port: 8080},
			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x1f, 0x90}},
		{&net.TCPAddr{IP: net.IP{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Port: 443},
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0x01, 0xbb}},
		{customAddr("10.0.0.1:22"),
			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 10, 0, 0, 1, 0x00, 0x16}},
		{customAddr("[afbc:dead::beef]:32754"),
			[]byte{0xaf, 0xbc, 0xde, 0xad, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xbe, 0xef, 0x7f, 0xf2}},
	}
	for _, e := range expected {
		if buf := IPv6Encoder.EncodeAddr(e.addr); !bytes.Equal(buf, e.buf) {
			t.Errorf("%s encoded to %x, expected %x", e.addr, buf, e.buf)
		}
		if addr := IPv6Encoder.DecodeAddr(e.buf); addr.String() != e.addr.String() {
			t.Errorf("%x decoded to %s, expected %s", e.buf, addr, e.addr)
		}
	}

	badAddrs := []net.Addr{
		&net.UDPAddr{IP: net.IPv6zero, Port: 9875},
		&net.TCPAddr{IP: net.IPv6unspecified, Port: 12345},
		customAddr("/tmp/unix-socket-addr"),
		nil,
	}
	for _, e := range badAddrs {
		if buf := IPv6Encoder.EncodeAddr(e); buf != nil {
			t.Errorf("%s encoded to %x, expected nil", e, buf)
		}
	}

	buf := make([]byte, 20)
	for _, l := range []int{0, 1, 3, 6, 7, 9, 12, 15, 19, 20} {
		if out := IPv6Encoder.DecodeAddr(buf[:l]); out != nil {
			t.Errorf("decoding buffer of size %d produced %#v, expected <nil>", l, out)
		}
	}
}

func TestIPv6NodeEncoder(t *testing.T) {
	enc := NodeEncoder{IPEncoder: IPv6Encoder, IDSize: 2}

	input := []Node{
		BasicNode{MyID: []byte{0x00, 0x00}, MyAddr: customAddr("192.168.1.1:4278")},
		BasicNode{MyID: []byte{0x55, 0x55}, MyAddr: customAddr("[::1]:7856")},
		BasicNode{MyID: []byte{0xcc, 0xcc}, MyAddr: customAddr("/this/is/a/unix/address")},
		BasicNode{MyID: []byte{0x00}, MyAddr: customAddr("127.0.0.1:5897")},
	}
	output := enc.Decode(enc.Encode(input))

	expected := []Node{
		BasicNode{MyID: []byte{0x00, 0x00}, MyAddr: customAddr("192.168.1.1:4278")},
		BasicNode{MyID: []byte{0x55, 0x55}, MyAddr: customAddr("[::1]:7856")},
	}
	if len(output) != len(expected) {
		t.Errorf("output has length %d, expected %d", len(output), len(expected))
	} else {
		for i := range expected {
			if id1, id2 := output[i].ID(), expected[i].ID(); !bytes.Equal(id1, id2) {
				t.Errorf("output node %d has ID %x, expected %x", i, id1, id2)
			}
			if a1, a2 := output[i].Addr(), expected[i].Addr(); a1.String() != a2.String() {
				t.Errorf("output node %d has address %q, expected %q", i, a1, a2)
			}
		}
	}

	buf := make([]byte, 42)
	for _, l := range []int{0, 1, 3, 6, 7, 9, 12, 15, 18, 21, 26, 32, 36, 39, 42} {
		if out := enc.Decode(buf[:l]); out != nil {
			t.Errorf("decoding buffer of size %d produced %#v, expected <nil>", l, out)
		}
	}
}
