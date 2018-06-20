package udpRequest

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func localizeAddr(addr net.Addr) net.Addr {
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return addr
	}
	newAddr := *udpAddr
	newAddr.IP = net.IPv4(127, 0, 0, 1)
	return &newAddr
}

type Echoer struct {
	sock     *UDPRequest
	reject   int32
	rejected int32
}

func (e *Echoer) HandleUDPRequest(p *PeerRequest, b []byte) {
	if left := atomic.AddInt32(&e.reject, -1); left >= 0 {
		atomic.AddInt32(&e.rejected, 1)
	} else if err := e.sock.Respond(p, b); err != nil {
		fmt.Println(err)
	}
}

func mustCreate(c *Config) *UDPRequest {
	sock, err := New(c)
	if err != nil {
		panic(err)
	}
	return sock
}

func testEcho(t *testing.T, sock *UDPRequest, message string, wait *sync.WaitGroup) {
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	if resp, err := sock.Request(ctx, localizeAddr(sock.Addr()), []byte(message)); err != nil {
		t.Error(err)
	} else if string(resp) != message {
		t.Errorf("got %q, expected %q", resp, message)
	}

	if wait != nil {
		wait.Done()
	}
}
func checkTimeoutErr(t *testing.T, sock *UDPRequest) {
	resp, err := sock.Request(context.Background(), localizeAddr(sock.Addr()), []byte("hello"))
	if resp != nil {
		t.Errorf("got non-nil buffer from timed-out request: %v", resp)
	}
	if err == nil {
		t.Error("got nil error from timed-out request")
	} else {
		if !strings.Contains(err.Error(), "timed out") {
			t.Errorf(`error message %q doesn't contain "timed out"`, err)
		}
		if tErr, ok := err.(timeoutErr); !ok {
			t.Error("got wrong error type for timed out request")
		} else if !tErr.Temporary() || !tErr.Timeout() {
			t.Error("timeout error did not indicate it was temporary and a timeout")
		}
	}
}

func TestRequest(t *testing.T) {
	sock := mustCreate(nil)
	defer sock.Close()
	sock.SetHandler(&Echoer{sock: sock})

	testEcho(t, sock, "hello world!", nil)
}

func TestMultipleRequests(t *testing.T) {
	sock := mustCreate(nil)
	defer sock.Close()
	sock.SetHandler(&Echoer{sock: sock})

	messages := []string{
		"This is the first message.",
		"The second message will be slightly longer",
		"THIRD!!",
		"Not really too much point in actually including the numbers here.",
	}
	var wait sync.WaitGroup

	wait.Add(len(messages))
	for _, msg := range messages {
		go testEcho(t, sock, msg, &wait)
	}
	wait.Wait()

	if len(sock.pending) > 0 {
		t.Errorf("all requests should have been resolved: %v", sock.pending)
	}
}

func TestCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	sock := mustCreate(nil)
	defer sock.Close()

	time.AfterFunc(time.Millisecond, cancel)
	resp, err := sock.Request(ctx, localizeAddr(sock.Addr()), []byte("hello"))
	if resp != nil {
		t.Errorf("got non-nil buffer from canceled request: %v", resp)
	}
	if err == nil {
		t.Error("got nil error from canceled request")
	}
}

func TestTimeout(t *testing.T) {
	sock := mustCreate(&Config{Timeout: 5 * time.Millisecond})
	defer sock.Close()

	if p := sock.Pending(); p != 0 {
		t.Errorf("fresh socket has %d pending requests, expected 0", p)
	} else {
		go func() {
			time.Sleep(time.Millisecond)
			if p := sock.Pending(); p != 1 {
				t.Errorf("socket has %d pending requests, expected 1", p)
			}
		}()
	}

	checkTimeoutErr(t, sock)
}

func TestRetry(t *testing.T) {
	sock := mustCreate(&Config{Timeout: 5 * time.Millisecond, Retry: true})
	defer sock.Close()
	echo := &Echoer{sock: sock, reject: 2}
	sock.SetHandler(echo)

	testEcho(t, sock, "This is a test message", nil)
	if echo.rejected != 2 {
		t.Errorf("rejected %d messages expected to reject 2", echo.rejected)
	}
}

func TestForward(t *testing.T) {
	origin, middle, remote := mustCreate(nil), mustCreate(nil), mustCreate(nil)
	defer origin.Close()
	defer middle.Close()
	defer remote.Close()

	// The layer that can actually handle requests to forward to a particular peer is a layer above
	// us, so we have the "to" value hard coded to what the test needs to pass.
	middle.SetHandler(HandlerFunc(func(p *PeerRequest, msg []byte) {
		if err := middle.ForwardRequest(p, localizeAddr(remote.Addr()), msg); err != nil {
			t.Errorf("middle failed to forward to remote: %v", err)
		}
	}))
	remote.SetHandler(HandlerFunc(func(p *PeerRequest, msg []byte) {
		if err := remote.ForwardResponse(p, localizeAddr(origin.Addr()), msg); err != nil {
			t.Errorf("remote failed to forward to origin: %v", err)
		}
	}))

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	const message = "this will go through the middle socket"
	if resp, err := origin.Request(ctx, localizeAddr(middle.Addr()), []byte(message)); err != nil {
		t.Error(err)
	} else if string(resp) != message {
		t.Errorf("got %q, expected %q", resp, message)
	}
}

func TestCleanup(t *testing.T) {
	sock := mustCreate(nil)

	time.AfterFunc(5*time.Millisecond, func() { sock.Close() })
	checkTimeoutErr(t, sock)
	checkTimeoutErr(t, sock)
}

func TestConflict(t *testing.T) {
	sock := mustCreate(&Config{Port: 7654})
	defer sock.Close()

	if sock, err := New(&Config{Port: 7654}); err == nil {
		sock.Close()
		t.Error("listening on port 7654 did not error the second time")
	}
}

func TestBadPackets(t *testing.T) {
	sock := mustCreate(nil)
	defer sock.Close()
	sock.SetHandler(&Echoer{sock: sock})

	buf := make([]byte, 128)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal("failed to create random buffer:", err)
	}
	t.Logf("random buffer: %x", buf)

	var failCnt int
	for i := range buf {
		if _, err := sock.socket.WriteTo(buf[:i+1], localizeAddr(sock.Addr())); err != nil {
			t.Errorf("error sending bad packet #%d: %v", i+1, err)
			if failCnt++; failCnt >= 4 {
				break
			}
		} else {
			failCnt = 0
		}
	}

	// socket should still work after receiving back packets
	testEcho(t, sock, "hello world!", nil)
}
