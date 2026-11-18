//go:build linux

package dispatcher

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/stats"
	"golang.org/x/sys/unix"
)

func supportsMeteredSplice(writer buf.Writer) bool {
	_, ok := writer.(interface{ SupportsMeteredSplice() })
	return ok
}

func meteredDownlink(counter stats.Counter) stats.Counter {
	return &spliceCounter{Counter: counter}
}

type spliceCounter struct {
	stats.Counter
}

// CopySplice is called by Xray once Vision has switched to raw copying. The
// callback updates Xray's activity timers and counts bytes written to the client.
func (*spliceCounter) CopySplice(ctx context.Context, source net.Conn, target *net.TCPConn, onWrite func(int64)) (bool, error) {
	src, ok := source.(*net.TCPConn)
	if !ok {
		return false, nil
	}
	from, err := src.SyscallConn()
	if err != nil {
		return true, err
	}
	to, err := target.SyscallConn()
	if err != nil {
		return true, err
	}

	var pipe [2]int
	if err := unix.Pipe2(pipe[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		return false, nil
	}
	defer unix.Close(pipe[0])
	defer unix.Close(pipe[1])
	const maxBatch = 1 << 20
	// Larger pipes reduce syscalls. The default capacity also works when the
	// kernel restricts resizing for this user.
	_, _ = unix.FcntlInt(uintptr(pipe[0]), unix.F_SETPIPE_SZ, maxBatch)

	stop := context.AfterFunc(ctx, func() {
		now := time.Now()
		_ = src.SetReadDeadline(now)
		_ = target.SetWriteDeadline(now)
	})
	defer stop()

	flags := unix.SPLICE_F_NONBLOCK | unix.SPLICE_F_MOVE
	transferred := false
	for {
		var inPipe int64
		var spliceErr error
		if err := from.Read(func(fd uintptr) bool {
			inPipe, spliceErr = spliceFD(int(fd), pipe[1], maxBatch, flags)
			return spliceErr != unix.EAGAIN
		}); err != nil {
			return true, copyError(ctx, err)
		}
		if spliceErr != nil {
			if !transferred && spliceUnavailable(spliceErr) {
				return false, nil
			}
			return true, copyError(ctx, spliceErr)
		}
		if inPipe == 0 {
			return true, nil
		}
		transferred = true
		for inPipe > 0 {
			var n int64
			if err := to.Write(func(fd uintptr) bool {
				n, spliceErr = spliceFD(pipe[0], int(fd), int(inPipe), flags)
				return spliceErr != unix.EAGAIN
			}); err != nil {
				return true, copyError(ctx, err)
			}
			if spliceErr != nil {
				return true, copyError(ctx, spliceErr)
			}
			if n == 0 {
				return true, errors.New("splice wrote no bytes from a nonempty pipe")
			}
			onWrite(n)
			inPipe -= n
		}
	}
}

func spliceFD(from, to, size, flags int) (int64, error) {
	for {
		n, err := unix.Splice(from, nil, to, nil, size, flags)
		if err != unix.EINTR {
			return int64(n), err
		}
	}
}

func spliceUnavailable(err error) bool {
	return err == unix.EINVAL || err == unix.ENOSYS || err == unix.EOPNOTSUPP || err == unix.EPERM
}

func copyError(ctx context.Context, err error) error {
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	return err
}
