package fakeNetwork_test

import (
	"math/rand"
	"net"
	"testing"
	"time"

	. "github.com/tigerbot/hyperdht/fakeNetwork"
)

func TestRandAddrIPv4(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(seed)
	t.Logf("seeded random with %d", seed)

	for i := 0; i < 200 && !t.Failed(); i++ {
		addr := RandomAddress(false).(*net.UDPAddr)
		if addr.IP.To4() == nil {
			t.Errorf("invalid IPv4 address produced: %s", addr.IP)
		}
		if addr.Port < 1024 || addr.Port >= 1<<16 {
			t.Errorf("invalid port produced: %d", addr.Port)
		}
	}
}

func TestRandAddrIPv6(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(seed)
	t.Logf("seeded random with %d", seed)

	var ipv4 int
	for i := 0; i < 200 && !t.Failed(); i++ {
		addr := RandomAddress(true).(*net.UDPAddr)
		if addr.IP.To16() == nil {
			t.Errorf("invalid IPv6 address produced: %s", addr.IP)
		} else if addr.IP.To4() != nil {
			ipv4++
		}
		if addr.Port < 1024 || addr.Port >= 1<<16 {
			t.Errorf("invalid port produced: %d", addr.Port)
		}
	}

	if ipv4 < 40 {
		t.Errorf("IPv6 address creation made %d IPv4 addresses, expected at least 40", ipv4)
	}
}
