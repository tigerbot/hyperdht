package cmdutils

import (
	"net"

	"github.com/pkg/errors"
)

// ParseAddrs converts the slices of strings expected as input into slices of network
// addresses. If any of the strings in the input is not a valid address
func ParseAddrs(bootstrap []string) []net.Addr {
	result := make([]net.Addr, 0, len(bootstrap))
	for _, input := range bootstrap {
		// The node code defaulted to port 49737 for bootstrap nodes, and so will we. Both
		// SplitHostPort and ResolveUDPAddr will error if there isn't a port specified in the
		// string, so we just assume if the split fails it's because there wasn't a port and
		// we add one. If the assumption is wrong the resolve will still fail anyway.
		if _, _, err := net.SplitHostPort(input); err != nil {
			input = net.JoinHostPort(input, "49737")
		}
		if addr, err := net.ResolveUDPAddr("udp4", input); err != nil {
			panic(errors.Errorf("invalid address %q: %v", input, err))
		} else {
			result = append(result, addr)
		}
	}
	return result
}
