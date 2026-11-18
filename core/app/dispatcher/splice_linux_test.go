//go:build linux

package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func tcpPair(t *testing.T) (client, server *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err = net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	server, err = listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return client, server
}

func TestMeteredSpliceReportsBeforeEOFAndCancelsIdleSource(t *testing.T) {
	origin, source := tcpPair(t)
	client, target := tcpPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const timeout = 3 * time.Second
	payload := bytes.Repeat([]byte("meter"), 32<<10)
	progress := make(chan struct{})
	result := make(chan error, 1)
	var accounted atomic.Int64
	go func() {
		handled, err := new(spliceCounter).CopySplice(ctx, source, target, func(n int64) {
			if accounted.Add(n) == int64(len(payload)) {
				close(progress)
			}
		})
		if !handled {
			err = errors.New("kernel splice unavailable for TCP loopback")
		}
		result <- err
	}()

	_ = origin.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := origin.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(timeout))
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(client, received); err != nil {
		t.Fatal(err)
	}
	select {
	case <-progress:
	case err := <-result:
		t.Fatalf("copy returned while origin was still open: %v", err)
	case <-time.After(timeout):
		t.Fatal("bytes were not accounted while the origin was idle")
	}
	if got := accounted.Load(); got != int64(len(received)) || !bytes.Equal(received, payload) {
		t.Fatalf("accounted %d bytes, delivered %d", got, len(received))
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("copy returned %v after cancellation", err)
		}
	case <-time.After(timeout):
		t.Fatal("cancellation did not unblock the idle origin read")
	}
}

func TestMeteredSpliceCancelsBackpressuredTarget(t *testing.T) {
	origin, source := tcpPair(t)
	_, target := tcpPair(t)
	if err := target.SetWriteBuffer(4096); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	progress := make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		handled, err := new(spliceCounter).CopySplice(ctx, source, target, func(int64) {
			select {
			case progress <- struct{}{}:
			default:
			}
		})
		if !handled {
			err = errors.New("kernel splice unavailable for TCP loopback")
		}
		result <- err
	}()
	const timeout = 3 * time.Second
	go func() {
		_ = origin.SetWriteDeadline(time.Now().Add(timeout))
		_, _ = origin.Write(make([]byte, 4<<20))
	}()
	select {
	case <-progress:
	case err := <-result:
		t.Fatalf("copy returned before writing: %v", err)
	case <-time.After(timeout):
		t.Fatal("copy did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("copy returned %v after cancellation", err)
		}
	case <-time.After(timeout):
		t.Fatal("cancellation did not unblock the backpressured target")
	}
}
