package udprequest

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
)

type customSock struct {
	reader *io.PipeReader
	writer *io.PipeWriter
}

type fakeAddr struct{}

func (a fakeAddr) Network() string { return "fake packet conn" }
func (a fakeAddr) String() string  { return "fake packet conn" }

func (s *customSock) ReadFrom(b []byte) (int, net.Addr, error) {
	addr := fakeAddr{}
	n, err := s.reader.Read(b)
	return n, addr, err
}
func (s *customSock) WriteTo(b []byte, addr net.Addr) (int, error) {
	if _, ok := addr.(fakeAddr); !ok {
		return 0, errors.Errorf("cannot reach the address %s", addr)
	}
	return s.writer.Write(b)
}

func (s *customSock) Close() error {
	s.reader.Close()
	return s.writer.Close()
}
func (s *customSock) LocalAddr() net.Addr {
	return fakeAddr{}
}

func (s *customSock) SetDeadline(t time.Time) error      { return nil }
func (s *customSock) SetReadDeadline(t time.Time) error  { return nil }
func (s *customSock) SetWriteDeadline(t time.Time) error { return nil }

func TestCustomSocket(t *testing.T) {
	// This test isn't particularly important, it's mostly to make sure that we can use
	// non-udp servers (a uTP server is more useful, but no need to import one here).
	// It also helps us increase test coverage by allowing us to inject errors.
	pipe := new(customSock)
	pipe.reader, pipe.writer = io.Pipe()

	sock := mustCreate(&Config{Socket: pipe})
	defer sock.Close()
	sock.SetHandler(&Echoer{sock: sock})

	testEcho(t, sock, "hello world!", nil)
}

func TestWriteError(t *testing.T) {
	pipe := new(customSock)
	pipe.reader, pipe.writer = io.Pipe()

	sock := mustCreate(&Config{Socket: pipe})
	defer sock.Close()

	writeErr := errors.New("this should be returned on WriteTo")
	pipe.reader.CloseWithError(writeErr) //nolint:errcheck
	resp, err := sock.Request(context.Background(), sock.Addr(), []byte("hello"))
	if resp != nil {
		t.Errorf("got non-nil buffer from request with dead write: %v", resp)
	}
	if err == nil {
		t.Error("got nil error from request with dead write")
	} else if cause := errors.Cause(err); cause != writeErr {
		t.Errorf("returned error cause was %q, expected to match original error %q", cause, writeErr)
	}
}

func TestReadError(t *testing.T) {
	pipe := new(customSock)
	pipe.reader, pipe.writer = io.Pipe()

	sock := mustCreate(&Config{Socket: pipe})
	defer sock.Close()

	// This is not a "use of closed" error so it should take a different path and close everything.
	readErr := errors.New("this should be returned on ReadFrom")
	time.AfterFunc(time.Millisecond, func() { pipe.writer.CloseWithError(readErr) }) //nolint:errcheck

	// The read errors should trigger a close, so any pending and future requests should
	// "time out"
	checkTimeoutErr(t, sock)
	checkTimeoutErr(t, sock)
}
