// Package ipEncoding converts between IP addresses (with ports) and binary buffers.
package ipEncoding

import (
	"net"
	"strconv"
)

// A IPEncoder encodes/decodes network address to a specific binary representation.
type IPEncoder interface {
	EncodeAddr(net.Addr) []byte
	DecodeAddr([]byte) net.Addr

	// EncodedLen should return the bytes used to encode a single address.
	EncodedLen() int
}

// IPv4Encoder is an implementation of IPEncoder that encodes the IP address as 4 bytes and the
// port as 2 bytes.
type IPv4Encoder struct{}

// IPv6Encoder is an implementation of IPEncoder that encodes the IP address as 16 bytes and the
// port as 2 bytes.
type IPv6Encoder struct{}

var _, _ IPEncoder = IPv4Encoder{}, IPv6Encoder{}

func extractHostPort(peer net.Addr) (net.IP, int) {
	if peer == nil {
		return nil, 0
	} else if udp, ok := peer.(*net.UDPAddr); ok {
		return udp.IP, udp.Port
	} else if tcp, ok := peer.(*net.TCPAddr); ok {
		return tcp.IP, tcp.Port
	} else if hostStr, portStr, err := net.SplitHostPort(peer.String()); err == nil {
		port64, _ := strconv.ParseInt(portStr, 10, 32)
		return net.ParseIP(hostStr), int(port64)
	}
	return nil, 0
}

func writePort(buf []byte, port int) { buf[0], buf[1] = byte((port&0xff00)>>8), byte(port&0x00ff) }
func readPort(buf []byte) int        { return int(buf[0])<<8 | int(buf[1]) }

// EncodeAddr implements IPEncoder.EncodeAddr. If the address is not an IPv4 address or an IPv6
// encoded version of an IPv4 address it will return nil.
func (e IPv4Encoder) EncodeAddr(peer net.Addr) []byte {
	host, port := extractHostPort(peer)
	host = host.To4() // this is safe to call on nil IP addresses
	if port == 0 || host == nil || host.IsUnspecified() {
		return nil
	}
	buf := make([]byte, e.EncodedLen())
	copy(buf, host)
	writePort(buf[net.IPv4len:], port)
	return buf
}

// DecodeAddr implements IPEncoder.DecodeAddr and reverses EncodeAddr. If the buffer is not
// exactly what's returned by EncodedLen it will return nil.
func (e IPv4Encoder) DecodeAddr(buf []byte) net.Addr {
	if len(buf) != e.EncodedLen() {
		return nil
	}
	return &net.UDPAddr{IP: net.IP(buf[:net.IPv4len]), Port: readPort(buf[net.IPv4len:])}
}

// EncodedLen implements IPEncoder.EncodedLen and returns 6.
func (e IPv4Encoder) EncodedLen() int { return net.IPv4len + 2 }

// EncodeAddr implements IPEncoder.EncodeAddr. If the address is not an IP it will return nil.
func (e IPv6Encoder) EncodeAddr(peer net.Addr) []byte {
	host, port := extractHostPort(peer)
	host = host.To16() // this is safe to call on nil IP addresses
	if port == 0 || host == nil || host.IsUnspecified() {
		return nil
	}

	buf := make([]byte, e.EncodedLen())
	copy(buf, host)
	writePort(buf[net.IPv6len:], port)
	return buf
}

// DecodeAddr implements IPEncoder.DecodeAddr and reverses EncodeAddr. If the buffer is not
// exactly what's returned by EncodedLen it will return nil.
func (e IPv6Encoder) DecodeAddr(buf []byte) net.Addr {
	if len(buf) != e.EncodedLen() {
		return nil
	}
	return &net.UDPAddr{IP: net.IP(buf[:net.IPv6len]), Port: readPort(buf[net.IPv6len:])}
}

// EncodedLen implements IPEncoder.EncodedLen and returns 18.
func (e IPv6Encoder) EncodedLen() int { return net.IPv6len + 2 }
