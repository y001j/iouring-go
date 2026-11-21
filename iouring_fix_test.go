//go:build linux
// +build linux

package iouring

import (
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func newRing(t *testing.T) *IOURing {
	t.Helper()
	iour, err := New(64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { iour.Close() })
	return iour
}

func socketpair(t *testing.T) (int, int) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Close(fds[0]); syscall.Close(fds[1]) })
	return fds[0], fds[1]
}

func waitResult(t *testing.T, ch <-chan Result) Result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for result")
		return nil
	}
}

// Send/Recv 之前没有设置 resolver：Err() 恒为 nil、ReturnInt() 恒报错
func TestSendRecvResult(t *testing.T) {
	iour := newRing(t)
	a, b := socketpair(t)
	ch := make(chan Result, 2)

	msg := []byte("hello")
	if _, err := iour.SubmitRequest(Send(a, msg, 0), ch); err != nil {
		t.Fatal(err)
	}
	n, err := waitResult(t, ch).ReturnInt()
	if err != nil || n != len(msg) {
		t.Fatalf("send: n=%d err=%v", n, err)
	}

	buf := make([]byte, 16)
	if _, err := iour.SubmitRequest(Recv(b, buf, 0), ch); err != nil {
		t.Fatal(err)
	}
	n, err = waitResult(t, ch).ReturnInt()
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("recv: n=%d err=%v data=%q", n, err, buf[:n])
	}

	// 对非法 fd 发送，错误必须能通过 Err() 拿到
	if _, err := iour.SubmitRequest(Send(1<<20, msg, 0), ch); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, ch).Err(); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("expected EBADF, got %v", err)
	}
}

// Close 时在途请求之前永远不会完成
func TestClosePendingRequests(t *testing.T) {
	iour, err := New(64)
	if err != nil {
		t.Fatal(err)
	}
	_, b := socketpair(t)
	ch := make(chan Result, 1)
	req, err := iour.SubmitRequest(Recv(b, make([]byte, 16), 0), ch) // 无数据，永远挂起
	if err != nil {
		t.Fatal(err)
	}

	if err := iour.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-req.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("pending request not finished after Close")
	}
	if err := waitResult(t, ch).Err(); err != ErrIOURingClosed {
		t.Fatalf("expected ErrIOURingClosed, got %v", err)
	}
	if _, err := iour.SubmitRequest(Nop(), nil); err != ErrIOURingClosed {
		t.Fatalf("submit after close: %v", err)
	}
	if err := iour.Close(); err != nil { // 重复 Close 应安全
		t.Fatal(err)
	}
}

// 一个没人读的 channel 之前会阻塞完成 goroutine，拖住其他所有请求
func TestNoHeadOfLineBlocking(t *testing.T) {
	iour := newRing(t)

	blocked := make(chan Result) // 无缓冲且无人读取
	if _, err := iour.SubmitRequest(Nop(), blocked); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	ch := make(chan Result, 1)
	if _, err := iour.SubmitRequest(Nop(), ch); err != nil {
		t.Fatal(err)
	}
	waitResult(t, ch)
	waitResult(t, blocked) // 稍后读取，结果不丢失
}

// 并发提交：验证合并提交路径下所有请求都能完成（配合 -race）
func TestConcurrentSubmit(t *testing.T) {
	iour, err := New(32) // 故意用小 SQ，覆盖 SQ 满时的 reserve 路径
	if err != nil {
		t.Fatal(err)
	}
	defer iour.Close()

	const goroutines, perG = 16, 500
	ch := make(chan Result, goroutines*perG)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := iour.SubmitRequest(Nop(), ch); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for i := 0; i < goroutines*perG; i++ {
		waitResult(t, ch)
	}

	// 批量 + 链式请求
	set, err := iour.SubmitLinkRequests([]PrepRequest{Nop(), Nop(), Nop()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-set.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("link requests timeout")
	}
}

// Cancel 与完成并发时之前存在数据竞争（req.iour 被置 nil）
func TestCancelRace(t *testing.T) {
	iour := newRing(t)
	for i := 0; i < 200; i++ {
		req, err := iour.SubmitRequest(Nop(), nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Cancel()
		<-req.Done()
	}
}

// 替换 go:linkname 后的地址转换
func TestSockaddrRoundTrip(t *testing.T) {
	cases := []syscall.Sockaddr{
		&syscall.SockaddrInet4{Port: 8080, Addr: [4]byte{127, 0, 0, 1}},
		&syscall.SockaddrInet6{Port: 443, ZoneId: 3, Addr: [16]byte{15: 1}},
		&syscall.SockaddrUnix{Name: "/tmp/iouring.sock"},
		&syscall.SockaddrUnix{Name: "@abstract"},
	}
	for _, sa := range cases {
		ptr, n, err := sockaddr(sa)
		if err != nil {
			t.Fatalf("%#v: %v", sa, err)
		}
		var rsa syscall.RawSockaddrAny
		copy((*[syscall.SizeofSockaddrAny]byte)(ptrOf(&rsa))[:], (*[syscall.SizeofSockaddrAny]byte)(ptr)[:n])
		got, err := anyToSockaddr(&rsa)
		if err != nil {
			t.Fatal(err)
		}
		switch want := sa.(type) {
		case *syscall.SockaddrInet4:
			g := got.(*syscall.SockaddrInet4)
			if g.Port != want.Port || g.Addr != want.Addr {
				t.Fatalf("inet4 mismatch: %#v vs %#v", g, want)
			}
		case *syscall.SockaddrInet6:
			g := got.(*syscall.SockaddrInet6)
			if g.Port != want.Port || g.Addr != want.Addr || g.ZoneId != want.ZoneId {
				t.Fatalf("inet6 mismatch: %#v vs %#v", g, want)
			}
		case *syscall.SockaddrUnix:
			if g := got.(*syscall.SockaddrUnix); g.Name != want.Name {
				t.Fatalf("unix mismatch: %q vs %q", g.Name, want.Name)
			}
		}
	}
}

// 通过 io_uring 完成 Connect + Accept4，验证地址转换在真实系统调用中可用
func TestConnectAccept(t *testing.T) {
	iour := newRing(t)
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	lf, _ := ln.(*net.TCPListener).File()
	defer lf.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	ch := make(chan Result, 2)
	if _, err := iour.SubmitRequest(Accept4(int(lf.Fd()), 0), ch); err != nil {
		t.Fatal(err)
	}

	cfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(cfd)
	prep, err := Connect(cfd, &syscall.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := iour.SubmitRequest(prep, ch); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		r := waitResult(t, ch)
		if err := r.Err(); err != nil {
			t.Fatalf("op %d: %v", r.Opcode(), err)
		}
		if r.Opcode() == OpAccept {
			fd := r.ReturnValue0().(int)
			defer syscall.Close(fd)
			if sa, ok := r.ReturnValue1().(*syscall.SockaddrInet4); !ok || sa.Addr != [4]byte{127, 0, 0, 1} {
				t.Fatalf("unexpected peer addr %#v", r.ReturnValue1())
			}
		}
	}
}

func ptrOf(rsa *syscall.RawSockaddrAny) unsafe.Pointer { return unsafe.Pointer(rsa) }
