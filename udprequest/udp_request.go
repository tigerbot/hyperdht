// Package udprequest allows requests and responses over a net.PacketConn.
// Each message that is sent is prepended with a short header that indicates whether
// a packet is a request or a response as well as an ID to match responses to their
// corresponding request.
//
// In addition to normal requests and responses, udprequest also allows forwarding
// requests using the same header that was received and forwarding responses to
// hosts other than the one the request was received from. The logic for when this
// would be appropriate/necessary however does not exist in this layer.
//
// Implementation is based on https://github.com/mafintosh/udp-request.
package udprequest

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

// HandlerFunc is an adapter to allow the use of ordinary functions as UDP handlers.
type HandlerFunc func(*PeerRequest, []byte)

// HandleUDPRequest calls f
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
	id int
}

type pendingRequest struct {
	addr     net.Addr
	sendBuf  []byte
	response chan []byte

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
	retry  bool

	lock    sync.RWMutex
	pending map[int]*pendingRequest

	handler Handler
}

// Addr returns the local address of the net.PacketConn being used.
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
		addr:     addr,
		sendBuf:  buf,
		response: make(chan []byte, 1),

		timeout: timeoutDiv + 1,
	}
	if !u.retry {
		p.retryCnt = len(retries)
	}
	u.pending[id] = &p

	return p.response
}
func (u *UDPRequest) unregister(id int) {
	u.lock.Lock()
	delete(u.pending, id)
	u.lock.Unlock()
}

func (u *UDPRequest) checkTimeouts(timeout time.Duration) {
	ticker := time.NewTicker(timeout / timeoutDiv)
	defer ticker.Stop()

	// The lock is primarily for the map, so we can use the RLock even though we do write to some
	// of the values (ie timeout and retryCnt) on each request.
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
				//nolint:errcheck // UDP isn't likely to error and couldn't do anything about it anyway
				u.socket.WriteTo(p.sendBuf, p.addr)
			} else {
				// We don't want to hit this condition again for this request even if the timeout is
				// short enough for us to run this loop again before the request is unregistered, so
				// we set the timeout really high such that it shouldn't be able to get to 0 again.
				p.timeout = 1e6
				// And just as an added precaution we also put the send in a select statement. Since
				// the channel is buffered and only read once, we can be sure that if we hit the
				// default case the value wouldn't be used anyway.
				select {
				case p.response <- nil:
				default:
				}
			}
		}
	}
}

func (u *UDPRequest) readMessages() {
	// If we return it means the underlying socket is not working anymore so we need to make
	// sure that we close properly so any pending and future requests are handled appropriately.
	defer u.Close()

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
		u.lock.RLock()
		h := u.handler
		p := u.pending[header]
		u.lock.RUnlock()
		if header&requestMarker != 0 {
			if h != nil {
				cp := make([]byte, n-2)
				copy(cp, buf[2:])
				go h.HandleUDPRequest(&PeerRequest{addr, header & maxTick}, cp)
			}
		} else {
			if p != nil {
				cp := make([]byte, n-2)
				copy(cp, buf[2:])
				select {
				case p.response <- cp:
				default:
				}
			}
		}
	}
}

// SetHandler sets the provided handler as the one that will be used for incoming requests.
func (u *UDPRequest) SetHandler(h Handler) {
	u.lock.Lock()
	defer u.lock.Unlock()
	u.handler = h
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

	resChan := u.register(id, peer, buf)
	if resChan == nil {
		return nil, timeoutErr{}
	}
	defer u.unregister(id)

	if _, err := u.socket.WriteTo(buf, peer); err != nil {
		return nil, errors.WithMessage(err, "sending request")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-resChan:
		if res != nil {
			return res, nil
		}
		return nil, timeoutErr{}
	}
}

// Pending returns the number of requests that have not yet completed.
func (u *UDPRequest) Pending() int {
	u.lock.RLock()
	defer u.lock.RUnlock()
	return len(u.pending)
}

func (u *UDPRequest) sendMessage(header int, msg []byte, addr net.Addr) error {
	buf := make([]byte, len(msg)+2)
	buf[0] = byte((header & 0xff00) >> 8)
	buf[1] = byte((header & 0x00ff) >> 0)
	copy(buf[2:], msg)

	_, err := u.socket.WriteTo(buf, addr)
	return errors.WithMessage(err, "sending packet")
}

// Respond sends the provided response data to the peer whose request is referenced by the peer data.
func (u *UDPRequest) Respond(peer *PeerRequest, res []byte) error {
	return u.sendMessage(peer.id, res, peer.Addr)
}

// ForwardRequest sends a request to a peer using the same header information that was used in
// the request we received. If the request body informs the new recipient of where the original
// request came from it can then use ForwardResponse to send a response directly to where it
// originated.
func (u *UDPRequest) ForwardRequest(from *PeerRequest, to net.Addr, msg []byte) error {
	return u.sendMessage(from.id|requestMarker, msg, to)
}

// ForwardResponse sends a response to a peer other than the one we received the request from
// using the same header information that we would have otherwise used. It is intended to be
// used to respond to requests that have been forwarded using ForwardRequest.
func (u *UDPRequest) ForwardResponse(from *PeerRequest, to net.Addr, msg []byte) error {
	return u.sendMessage(from.id, msg, to)
}

// Close shuts down the socket and cancels any pending requests. The underlying socket is closed
// even if it was initially provided through the config.
func (u *UDPRequest) Close() error {
	u.lock.Lock()
	defer u.lock.Unlock()
	for _, p := range u.pending {
		select {
		case p.response <- nil:
		default:
		}
	}
	u.pending = nil

	return u.socket.Close()
}

// New creates a new UDPRequest instance and starts all of the relevant background goroutines.
func New(cfg *Config) (*UDPRequest, error) {
	if cfg == nil {
		cfg = new(Config)
	}
	c := *cfg

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

	result.handler = c.Handler
	result.tick = uint32(rand.Intn(maxTick))
	result.pending = make(map[int]*pendingRequest, 4)
	result.retry = c.Retry
	go result.checkTimeouts(c.Timeout)
	go result.readMessages()

	return result, nil
}
