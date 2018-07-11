package fakeNetwork

import (
	"math/rand"
	"net"
)

// RandomAddress creates a new *net.UDPAddr with a random IP address and port. The argument
// ipv6 indicates whether IPv6 should be included, and as such this can return IPv4 addresses.
func RandomAddress(ipv6 bool) net.Addr {
	var addr net.UDPAddr
	if ipv6 && rand.Intn(3) > 0 {
		addr.IP = make(net.IP, 16)
	} else {
		addr.IP = make(net.IP, 4)
	}
	// According to https://golang.org/pkg/math/rand/#Read this always returns a nil error.
	// #nosec (this is a test util, so pseudo random is not only fine, it's preferable)
	rand.Read(addr.IP)
	addr.Port = 1024 + rand.Intn(1<<16-1024)

	return &addr
}
