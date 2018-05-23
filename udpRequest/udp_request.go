// Package udpRequest allows for making requests/responses over UDP.
// It is the go implementation of the previous node library udp-request by mafintosh.
package udpRequest

import (
	"context"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const (
	requestMarker = 0x8000
	maxTick       = 0x7fff

	// timeoutDiv is how often during the timeout duration we check
	timeoutDiv = 4
)

var (
	retries = [...]int{timeoutDiv, 2 * timeoutDiv, 3 * timeoutDiv}
)

// A Handler processes incoming UDP requests.
//
// HandleUDPRequest will be called in it's own go routine to prevent a single request from blocking
// all other incoming requests. The buffer should be safe to manipulate if needed.
type Handler interface {
	HandleUDPRequest(*PeerRequest, []byte)
}

// HandlerFunc is an adapter to allow the use of oridnary functions as UDP handlers.
type HandlerFunc func(*PeerRequest, []byte)

// HandleUDPRequest call h
func (f HandlerFunc) HandleUDPRequest(p *PeerRequest, b []byte) { f(p, b) }

// Config contains all of the options available for a UDP instance
type Config struct {
	// Allows for custom socket types or instances to be used. If Socket is nil a new net.UDPConn
	// is created that will listen on the specified port.
	Socket net.PacketConn
	Port   int

	// Timeout and Retry control how to behave while waiting for a response to sent requests.
	// If Retry is true the time before the first retry is the Timeout duration and the time
	// between subsequent retries will increase, so it will take much longer than Timeout before
	// the Request will return with a timeout error.
	Timeout time.Duration
	Retry   bool

	Handler Handler
}

// PeerRequest contains all of the information needed to uniquely reference a request.
type PeerRequest struct {
	net.Addr
	ID int
}

type pendingRequest struct {
	c    chan []byte
	addr net.Addr
	buf  []byte

	timeout  int
	retryCnt int
}

type timeoutErr struct{}

func (e timeoutErr) Error() string   { return "request timed out" }
func (e timeoutErr) Timeout() bool   { return true }
func (e timeoutErr) Temporary() bool { return true }

// The UDPRequest struct manages the coordination between requests and responses sent over the
// opened packet connection.
type UDPRequest struct {
	socket net.PacketConn
	tick   uint32

	lock    sync.RWMutex
	pending map[int]*pendingRequest
	retry   bool

	Handler Handler
}

// Addr returns the local address that the PacketConn is attached to.
func (u *UDPRequest) Addr() net.Addr {
	return u.socket.LocalAddr()
}

func (u *UDPRequest) register(id int, addr net.Addr, buf []byte) <-chan []byte {
	u.lock.Lock()
	defer u.lock.Unlock()
	if u.pending == nil {
		return nil
	}

	p := pendingRequest{
		c:    make(chan []byte, 1),
		addr: addr,
		buf:  buf,

		timeout: timeoutDiv + 1,
	}
	if !u.retry {
		p.retryCnt = len(retries)
	}
	u.pending[id] = &p

	return p.c
}
func (u *UDPRequest) unregister(id int) {
	u.lock.Lock()
	delete(u.pending, id)
	u.lock.Unlock()
}

func (u *UDPRequest) checkTimeouts(timeout time.Duration) {
	ticker := time.NewTicker(timeout / timeoutDiv)
	defer ticker.Stop()

	u.lock.RLock()
	defer u.lock.RUnlock()
	for {
		u.lock.RUnlock()
		<-ticker.C
		u.lock.RLock()

		if u.pending == nil {
			return
		}
		for _, p := range u.pending {
			if p.timeout--; p.timeout > 0 {
				// do nothing for this cycle
			} else if p.retryCnt < len(retries) {
				p.timeout = retries[p.retryCnt]
				p.retryCnt++
				u.socket.WriteTo(p.buf, p.addr)
			} else {
				p.c <- nil
			}
		}
	}
}

func (u *UDPRequest) readMessages() {
	buf := make([]byte, 1<<16)
	errCnt := 0
	for {
		n, addr, err := u.socket.ReadFrom(buf)
		if err != nil {
			if strings.Contains(err.Error(), "use of closed") {
				return
			} else if errCnt++; errCnt > 3 {
				return
			}
			continue
		} else {
			errCnt = 0
		}

		if n < 2 {
			continue
		}
		header := int(buf[0])<<8 | int(buf[1])
		if header&requestMarker != 0 {
			if u.Handler != nil {
				cp := make([]byte, n-2)
				copy(cp, buf[2:])
				go u.Handler.HandleUDPRequest(&PeerRequest{addr, header & maxTick}, cp)
			}
		} else {
			u.lock.RLock()
			if p := u.pending[header]; p != nil {
				cp := make([]byte, n-2)
				copy(cp, buf[2:])
				p.c <- cp
			}
			u.lock.RUnlock()
		}
	}
}

// Request wraps the provided request data in a request frame and sends it to the specified peer
// address. It then waits for a response from the peer and returns its data.
func (u *UDPRequest) Request(ctx context.Context, peer net.Addr, req []byte) ([]byte, error) {
	id := int(atomic.AddUint32(&u.tick, 1) % (maxTick + 1))
	header := id | requestMarker

	buf := make([]byte, len(req)+2)
	buf[0] = byte((header & 0xff00) >> 8)
	buf[1] = byte((header & 0x00ff) >> 0)
	copy(buf[2:], req)

	c := u.register(id, peer, buf)
	if c == nil {
		return nil, timeoutErr{}
	}
	defer u.unregister(id)

	if _, err := u.socket.WriteTo(buf, peer); err != nil {
		return nil, errors.WithMessage(err, "sending request")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-c:
		if res != nil {
			return res, nil
		}
		return nil, timeoutErr{}
	}
}

// Respond sends the provided response data to the peer whose request is referenced by the peer data.
func (u *UDPRequest) Respond(peer *PeerRequest, res []byte) error {
	buf := make([]byte, len(res)+2)
	buf[0] = byte((peer.ID & 0xff00) >> 8)
	buf[1] = byte((peer.ID & 0x00ff) >> 0)
	copy(buf[2:], res)

	_, err := u.socket.WriteTo(buf, peer.Addr)
	return errors.WithMessage(err, "sending response")
}

// Close shuts down the socket and cancels any pending requests. The underlying socket is closed
// even if it was initially provided through the config.
func (u *UDPRequest) Close() error {
	u.lock.Lock()
	defer u.lock.Unlock()
	for _, p := range u.pending {
		p.c <- nil
	}
	u.pending = nil

	return u.socket.Close()
}

// New creates a new UDPRequest instance and starts all of the relevant background goroutines.
func New(c *Config) (*UDPRequest, error) {
	if c == nil {
		c = new(Config)
	}
	if c.Timeout == 0 {
		c.Timeout = time.Second
	}

	result := new(UDPRequest)
	if c.Socket != nil {
		result.socket = c.Socket
	} else {
		sock, err := net.ListenUDP("udp", &net.UDPAddr{Port: c.Port})
		if err != nil {
			return nil, err
		}
		result.socket = sock
	}

	result.Handler = c.Handler
	result.tick = uint32(rand.Intn(maxTick))
	result.pending = make(map[int]*pendingRequest, 4)
	result.retry = c.Retry
	go result.checkTimeouts(c.Timeout)
	go result.readMessages()

	return result, nil
}
