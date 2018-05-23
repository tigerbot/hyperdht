package udpRequest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

	if resp, err := sock.Request(ctx, sock.Addr(), []byte(message)); err != nil {
		t.Error(err)
	} else if string(resp) != message {
		t.Errorf("got %q, expected %q", resp, message)
	}

	if wait != nil {
		wait.Done()
	}
}
func checkTimeoutErr(t *testing.T, sock *UDPRequest) {
	resp, err := sock.Request(context.Background(), sock.Addr(), []byte("hello"))
	if resp != nil {
		t.Errorf("got non-nil buffer from timed-out request: %v", resp)
	}
	if err == nil {
		t.Error("got nil error from timed-out request")
	} else if tErr, ok := err.(timeoutErr); !ok {
		t.Error("got wrong error type for timed out request")
	} else if !tErr.Temporary() || !tErr.Timeout() {
		t.Error("timeout error did not indicate it was temporary and a timeout")
	}
}

func TestRequest(t *testing.T) {
	sock := mustCreate(nil)
	defer sock.Close()
	sock.Handler = &Echoer{sock: sock}

	testEcho(t, sock, "hello world!", nil)
}

func TestMultipleRequests(t *testing.T) {
	sock := mustCreate(nil)
	defer sock.Close()
	sock.Handler = &Echoer{sock: sock}

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
	resp, err := sock.Request(ctx, sock.Addr(), []byte("hello"))
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

	checkTimeoutErr(t, sock)
}

func TestRetry(t *testing.T) {
	sock := mustCreate(&Config{Timeout: 5 * time.Millisecond, Retry: true})
	defer sock.Close()
	echo := &Echoer{sock: sock, reject: 2}
	sock.Handler = echo

	testEcho(t, sock, "This is a test message", nil)
	if echo.rejected != 2 {
		t.Errorf("rejected %d messages expected to reject 2", echo.rejected)
	}
}

func TestForward(t *testing.T) {
	// Even though everything in this test happens in a deterministic non-racey order, the
	// nature of the go routines involved makes it so the race detector can't know that. As
	// such we need to utilize a lock in the handlers and assign the handlers through the config.
	var lock sync.Mutex
	lock.Lock()
	var origin, middle, remote *UDPRequest

	// The layer that can actually handle requests to forward to a particular peer is a layer above
	// us, so we have the "to" value hard coded to what the test needs to pass.
	midHandler := HandlerFunc(func(p *PeerRequest, msg []byte) {
		lock.Lock()
		if err := middle.ForwardRequest(p, remote.Addr(), msg); err != nil {
			t.Errorf("middle failed to forward to remote: %v", err)
		}
		lock.Unlock()
	})
	remHandler := HandlerFunc(func(p *PeerRequest, msg []byte) {
		lock.Lock()
		if err := remote.ForwardResponse(p, origin.Addr(), msg); err != nil {
			t.Errorf("remote failed to forward to origin: %v", err)
		}
		lock.Unlock()
	})

	origin = mustCreate(nil)
	defer origin.Close()
	middle = mustCreate(&Config{Handler: midHandler})
	defer middle.Close()
	remote = mustCreate(&Config{Handler: remHandler})
	defer remote.Close()
	lock.Unlock()

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	const message = "this will go through the middle socket"
	if resp, err := origin.Request(ctx, middle.Addr(), []byte(message)); err != nil {
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
