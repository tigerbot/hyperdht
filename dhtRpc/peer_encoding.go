package dhtRpc

import (
	"net"
	"strconv"
)

func encodeIPv4Peer(peer net.Addr) []byte {
	var host net.IP
	var port int
	if udp, ok := peer.(*net.UDPAddr); ok {
		host, port = udp.IP, udp.Port
	} else if tcp, ok := peer.(*net.TCPAddr); ok {
		host, port = tcp.IP, tcp.Port
	} else {
		if hostStr, portStr, err := net.SplitHostPort(peer.String()); err != nil {
			// TODO? log these kinds of errors
		} else {
			host = net.ParseIP(hostStr)
			port64, _ := strconv.ParseInt(portStr, 10, 32)
			port = int(port64)
		}
	}

	if host == nil || port == 0 || host.To4() == nil {
		return nil
	}
	buf := make([]byte, 6)
	copy(buf, host.To4())
	buf[4], buf[5] = byte((port&0xff00)>>8), byte(port&0x00ff)
	return buf
}

func decodeIPv4Peer(buf []byte) net.Addr {
	if len(buf) != 6 {
		return nil
	}
	return &net.UDPAddr{
		IP:   net.IP(buf[:4]),
		Port: int(buf[4])<<8 | int(buf[5]),
	}
}
