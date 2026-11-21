//go:build linux
// +build linux

package iouring

import (
	"bytes"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

// 修复前 CleanFlags 用异或，链式请求最后一个 SQE 会被错误地带上 IO_LINK
func TestLinkLastSQEHasNoLinkFlag(t *testing.T) {
	iour := newRing(t)
	set, err := iour.SubmitLinkRequests([]PrepRequest{Nop(), Nop()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-set.Done()

	iour.submitLock.Lock()
	sq := iour.sq
	first := (*[64]byte)(unsafePointer(&sq.sqes[(sq.sqeTail-2)&*sq.mask]))
	last := (*[64]byte)(unsafePointer(&sq.sqes[(sq.sqeTail-1)&*sq.mask]))
	iour.submitLock.Unlock()

	linkMask := iouring_syscall.IOSQE_FLAGS_IO_LINK | iouring_syscall.IOSQE_FLAGS_IO_HARDLINK
	if first[1]&linkMask != iouring_syscall.IOSQE_FLAGS_IO_LINK {
		t.Fatalf("first sqe flags = %#x, want IO_LINK only", first[1])
	}
	if last[1]&linkMask != 0 {
		t.Fatalf("last sqe flags = %#x, want no link flags", last[1])
	}
}

// 直接回调模式：带回调、channel 为 nil 的请求在完成事件 goroutine 中执行回调，
// 回调中再提交请求形成链条
func TestDirectCallbackChain(t *testing.T) {
	iour := newRing(t)
	const total = 5000
	var count int32
	done := make(chan struct{})

	var cb RequestCallback
	cb = func(r Result) error {
		if atomic.AddInt32(&count, 1) == total {
			close(done)
			return nil
		}
		_, err := iour.SubmitRequest(Nop().WithCallback(cb), nil)
		return err
	}
	// 4 条并发链
	for i := 0; i < 4; i++ {
		if _, err := iour.SubmitRequest(Nop().WithCallback(cb), nil); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("direct callback chain stalled at %d", atomic.LoadInt32(&count))
	}
}

func newLoop(t *testing.T, cfg LoopConfig) *Loop {
	t.Helper()
	l, err := NewLoop(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	t.Logf("loop setup flags: %#x", l.SetupFlags)
	return l
}

func TestLoopPostNop(t *testing.T) {
	l := newLoop(t, LoopConfig{Entries: 8})
	const total = 1000 // 远大于 SQ，覆盖 SQ 满时的提交路径
	done := make(chan int32, total)
	if err := l.Post(func() {
		for i := 0; i < total; i++ {
			l.Nop(func(res int32, flags uint32) { done <- res })
		}
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < total; i++ {
		select {
		case res := <-done:
			if res != 0 {
				t.Fatalf("nop res %d", res)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout after %d nops", i)
		}
	}
}

func TestLoopMultishotRecv(t *testing.T) {
	l := newLoop(t, LoopConfig{BufferRings: []BufferRingConfig{{Count: 8, Size: 64}}})
	a, b := socketpair(t)

	type chunk struct {
		data []byte
		more bool
		res  int32
	}
	got := make(chan chunk, 64)
	l.Post(func() {
		br := l.BufferRing(0)
		l.RecvMultishot(b, br, 0, func(res int32, flags uint32) {
			c := chunk{res: res, more: flags&iouring_syscall.IORING_CQE_F_MORE != 0}
			if bid, data, ok := br.Get(res, flags); ok {
				c.data = append([]byte(nil), data...)
				br.Recycle(bid)
			}
			got <- c
		})
	})

	var want []byte
	for i := 0; i < 20; i++ { // 20 次 > 8 个缓冲区，验证 Recycle 生效
		msg := []byte{byte('a' + i), byte('a' + i), byte('a' + i)}
		want = append(want, msg...)
		if _, err := syscall.Write(a, msg); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	syscall.Shutdown(a, syscall.SHUT_WR) // EOF 终止 multishot

	var recv []byte
	for {
		select {
		case c := <-got:
			if c.res < 0 {
				t.Fatalf("recv error %v", syscall.Errno(-c.res))
			}
			recv = append(recv, c.data...)
			if !c.more {
				if c.res != 0 {
					t.Fatalf("multishot ended with res=%d", c.res)
				}
				if !bytes.Equal(recv, want) {
					t.Fatalf("data mismatch:\n got %q\nwant %q", recv, want)
				}
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout, received %q", recv)
		}
	}
}

// 缓冲区耗尽时 multishot 以 -ENOBUFS 终止
func TestLoopMultishotENOBUFS(t *testing.T) {
	l := newLoop(t, LoopConfig{BufferRings: []BufferRingConfig{{Count: 2, Size: 16}}})
	a, b := socketpair(t)
	res := make(chan int32, 8)
	l.Post(func() {
		l.RecvMultishot(b, l.BufferRing(0), 0, func(r int32, flags uint32) {
			if flags&iouring_syscall.IORING_CQE_F_MORE == 0 {
				res <- r
			}
		})
	})
	for i := 0; i < 3; i++ { // 不归还缓冲区
		syscall.Write(a, []byte("x"))
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case r := <-res:
		if syscall.Errno(-r) != syscall.ENOBUFS {
			t.Fatalf("expected ENOBUFS, got %d", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ENOBUFS")
	}
}

func TestLoopAcceptMultishot(t *testing.T) {
	l := newLoop(t, LoopConfig{})
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	lf, _ := ln.(*net.TCPListener).File()
	defer lf.Close()

	fds := make(chan int32, 8)
	l.Post(func() {
		l.AcceptMultishot(int(lf.Fd()), syscall.SOCK_CLOEXEC, func(res int32, flags uint32) { fds <- res })
	})
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp4", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		select {
		case fd := <-fds:
			if fd < 0 {
				t.Fatalf("accept error %v", syscall.Errno(-fd))
			}
			syscall.Close(int(fd))
		case <-time.After(5 * time.Second):
			t.Fatal("accept timeout")
		}
	}
}

func TestLoopCloseWhileWaiting(t *testing.T) {
	l, err := NewLoop(LoopConfig{BufferRings: []BufferRingConfig{{Count: 4, Size: 16}}})
	if err != nil {
		t.Fatal(err)
	}
	_, b := socketpair(t)
	l.Post(func() { l.RecvMultishot(b, l.BufferRing(0), 0, nil) }) // 永远等不到数据
	time.Sleep(20 * time.Millisecond)
	closed := make(chan struct{})
	go func() { l.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked")
	}
	if err := l.Post(func() {}); err != ErrLoopClosed {
		t.Fatalf("post after close: %v", err)
	}
}

func unsafePointer(p *iouring_syscall.SubmissionQueueEntry) unsafe.Pointer { return unsafe.Pointer(p) }

func TestLoopFixedFiles(t *testing.T) {
	l := newLoop(t, LoopConfig{FixedFiles: 16, BufferRings: []BufferRingConfig{{Count: 4, Size: 64}}})
	a, b := socketpair(t)
	got := make(chan string, 1)
	errc := make(chan error, 1)
	l.Post(func() {
		ia, err := l.RegisterFile(a)
		if err != nil {
			errc <- err
			return
		}
		ib, err := l.RegisterFile(b)
		if err != nil {
			errc <- err
			return
		}
		br := l.BufferRing(0)
		l.RecvMultishot(FixedFile(ib), br, 0, func(res int32, flags uint32) {
			if bid, data, ok := br.Get(res, flags); ok {
				got <- string(data)
				br.Recycle(bid)
			} else if res < 0 {
				errc <- syscall.Errno(-res)
			}
		})
		l.Send(FixedFile(ia), []byte("fixed"), 0, func(res int32, flags uint32) {
			if res < 0 {
				errc <- syscall.Errno(-res)
			}
			l.UnregisterFile(ia)
		})
	})
	select {
	case s := <-got:
		if s != "fixed" {
			t.Fatalf("got %q", s)
		}
	case err := <-errc:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}
