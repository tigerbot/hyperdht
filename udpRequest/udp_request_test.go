package udpRequest

import (
	"context"
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
	} else {
		e.sock.Respond(p, b)
	}
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
	sock, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	sock.Handler = &Echoer{sock: sock}

	testEcho(t, sock, "hello world!", nil)
}

func TestMultipleRequests(t *testing.T) {
	sock, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
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

	sock, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
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
	sock, err := New(&Config{Timeout: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()

	checkTimeoutErr(t, sock)
}

func TestRetry(t *testing.T) {
	sock, err := New(&Config{Timeout: 5 * time.Millisecond, Retry: true})
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	echo := &Echoer{sock: sock, reject: 2}
	sock.Handler = echo

	testEcho(t, sock, "This is a test message", nil)
	if echo.rejected != 2 {
		t.Errorf("rejected %d messages expected to reject 2", echo.rejected)
	}
}

func TestCleanup(t *testing.T) {
	sock, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}

	time.AfterFunc(5*time.Millisecond, func() { sock.Close() })
	checkTimeoutErr(t, sock)
	checkTimeoutErr(t, sock)
}
