package cmdutils

import (
	"net"
	"reflect"
	"testing"
)

func TestAddrParsing(t *testing.T) {
	bootstrap := []string{
		// "dht1.daplie.com", // Domains should work, but no longer in control of any for reliable tests
		"192.168.25.1:12345",
	}

	parsed := ParseAddrs(bootstrap)
	expected := []net.Addr{
		// &net.UDPAddr{IP: net.IPv4(138, 197, 217, 160), Port: 49737},
		&net.UDPAddr{IP: net.IPv4(192, 168, 25, 1), Port: 12345},
	}
	if !reflect.DeepEqual(parsed, expected) {
		t.Errorf("parsed was %+v, expected %+v", parsed, expected)
	}
}
