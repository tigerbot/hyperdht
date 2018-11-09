package ipEncoding

import (
	"bytes"
	"net"
	"testing"
)

type customAddr string

func (a customAddr) Network() string { return "test address" }
func (a customAddr) String() string  { return string(a) }

func TestIPv4Encoder(t *testing.T) {
	type expectation struct {
		addr net.Addr
		buf  []byte
	}

	expected := []expectation{
		{&net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 80}, []byte{127, 0, 0, 1, 0, 80}},
		{&net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 8080}, []byte{127, 0, 0, 1, 0x1f, 0x90}},
		{&net.TCPAddr{IP: net.IP{192, 168, 1, 1}, Port: 443}, []byte{192, 168, 1, 1, 0x01, 0xbb}},
		{&net.TCPAddr{IP: net.IP{169, 254, 10, 1}, Port: 2056}, []byte{169, 254, 10, 1, 0x08, 0x08}},
		{customAddr("10.0.0.1:22"), []byte{10, 0, 0, 1, 0x00, 0x16}},
		{customAddr("45.55.95.192:32754"), []byte{45, 55, 95, 192, 0x7f, 0xf2}},
	}
	for _, e := range expected {
		if buf := IPv4Encoder.EncodeAddr(e.addr); !bytes.Equal(buf, e.buf) {
			t.Errorf("%s encoded to %x, expected %x", e.addr, buf, e.buf)
		}
		if addr := IPv4Encoder.DecodeAddr(e.buf); addr.String() != e.addr.String() {
			t.Errorf("%x decoded to %s, expected %s", e.buf, addr, e.addr)
		}
	}

	// These are IPv6 or bad addresses, so even if the encoding succeeds, strings might not
	// match between original and decoded address, so we don't run that test for these.
	expected = []expectation{
		{&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}, []byte{127, 0, 0, 1, 0x1f, 0x90}},
		{&net.TCPAddr{IP: net.IPv4(192, 168, 1, 1), Port: 443}, []byte{192, 168, 1, 1, 0x01, 0xbb}},
		{customAddr("[::ffff:10.0.0.1]:22"), []byte{10, 0, 0, 1, 0x00, 0x16}},
		{customAddr("[::ffff:2d37:5fc0]:32754"), []byte{45, 55, 95, 192, 0x7f, 0xf2}},

		{customAddr("[::1]:80"), nil},
		{customAddr("/tmp/unix-socket-addr"), nil},
		{nil, nil},
	}
	for _, e := range expected {
		if buf := IPv4Encoder.EncodeAddr(e.addr); !bytes.Equal(buf, e.buf) {
			t.Errorf("%s encoded to %x, expected %x", e.addr, buf, e.buf)
		}
	}

	if addr := IPv4Encoder.DecodeAddr([]byte{192, 168, 254, 254}); addr != nil {
		t.Errorf("decoded bad length buffer produced %q, expected <nil>", addr)
	}
}

func TestIPv4NodeEncoder(t *testing.T) {
	enc := NodeEncoder{IPEncoder: IPv4Encoder, IDSize: 2}

	input := []Node{
		BasicNode{MyID: []byte{0x00, 0x00}, MyAddr: customAddr("192.168.1.1:4278")},
		BasicNode{MyID: []byte{0x55, 0x55}, MyAddr: customAddr("[::1]:7856")},
		BasicNode{MyID: []byte{0xcc, 0xcc}, MyAddr: customAddr("10.10.10.1:31789")},
		BasicNode{MyID: []byte{0x00}, MyAddr: customAddr("127.0.0.1:5897")},
	}
	output := enc.Decode(enc.Encode(input))

	expected := []Node{
		BasicNode{MyID: []byte{0x00, 0x00}, MyAddr: customAddr("192.168.1.1:4278")},
		BasicNode{MyID: []byte{0xcc, 0xcc}, MyAddr: customAddr("10.10.10.1:31789")},
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

	buf := make([]byte, 17)
	for _, l := range []int{0, 1, 3, 6, 7, 9, 12, 15, 17} {
		if out := enc.Decode(buf[:l]); out != nil {
			t.Errorf("decoding buffer of size %d produced %#v, expected <nil>", l, out)
		}
	}
}
