//go:build linux
// +build linux

package iouring

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

// tcpPair 返回一对相连的 TCP socket：fd 交给 Loop 使用（阻塞模式），peer 由测试读写。
// SEND_ZC 只支持 TCP/UDP，不能用 unix socketpair。
func tcpPair(t *testing.T) (fd int, peer net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	c, err := net.Dial("tcp4", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	peer = <-accepted
	f, err := c.(*net.TCPConn).File() // dup 出的 fd 为阻塞模式
	c.Close()
	if err != nil {
		t.Fatal(err)
	}
	fd = int(f.Fd())
	t.Cleanup(func() { f.Close(); peer.Close() })
	return fd, peer
}

type zcEvent struct {
	res   int32
	flags uint32
}

// sendAllZC 用 SendZC/SendZCFixed 发送完整的 data（处理短写），返回所有 CQE
func sendAllZC(t *testing.T, l *Loop, fd int, data []byte, fixed bool, idx uint16) []zcEvent {
	t.Helper()
	events := make(chan zcEvent, 1024)
	var off int
	var submit func()
	var h Handler
	h = func(res int32, flags uint32) {
		events <- zcEvent{res, flags}
		if flags&iouring_syscall.IORING_CQE_F_NOTIF != 0 || res <= 0 {
			return
		}
		off += int(res)
		if off < len(data) {
			submit()
		}
	}
	submit = func() {
		if fixed {
			l.SendZCFixed(fd, data[off:], idx, syscall.MSG_NOSIGNAL, iouring_syscall.IORING_SEND_ZC_REPORT_USAGE, h)
		} else {
			l.SendZC(fd, data[off:], syscall.MSG_NOSIGNAL, iouring_syscall.IORING_SEND_ZC_REPORT_USAGE, h)
		}
	}
	l.Post(submit)

	// 等待：发送完毕，且每个带 F_MORE 的结果 CQE 都收到了对应的通知 CQE
	var got []zcEvent
	sent, pendingNotif := 0, 0
	deadline := time.After(10 * time.Second)
	for sent < len(data) || pendingNotif > 0 {
		select {
		case e := <-events:
			got = append(got, e)
			if e.flags&iouring_syscall.IORING_CQE_F_NOTIF != 0 {
				pendingNotif--
				continue
			}
			if e.res < 0 {
				t.Fatalf("send_zc error: %v", syscall.Errno(-e.res))
			}
			sent += int(e.res)
			if e.flags&iouring_syscall.IORING_CQE_F_MORE != 0 {
				pendingNotif++
			}
		case <-deadline:
			t.Fatalf("timeout: sent %d/%d, pending notif %d", sent, len(data), pendingNotif)
		}
	}
	return got
}

func readN(t *testing.T, c net.Conn, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func checkNotif(t *testing.T, events []zcEvent) {
	t.Helper()
	notifs := 0
	for _, e := range events {
		if e.flags&iouring_syscall.IORING_CQE_F_NOTIF != 0 {
			notifs++
			if e.res&iouring_syscall.IORING_NOTIF_USAGE_ZC_COPIED != 0 {
				t.Logf("notif: data was copied (expected on loopback)")
			}
		}
	}
	if notifs == 0 {
		t.Fatal("no IORING_CQE_F_NOTIF completion received")
	}
}

func TestLoopSendZC(t *testing.T) {
	l := newLoop(t, LoopConfig{})
	fd, peer := tcpPair(t)
	data := make([]byte, 1<<20)
	rand.Read(data)

	recvd := make(chan []byte, 1)
	go func() { recvd <- readN(t, peer, len(data)) }()
	events := sendAllZC(t, l, fd, data, false, 0)
	checkNotif(t, events)
	if got := <-recvd; !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}
}

func TestLoopSendZCFixedUserBuffer(t *testing.T) {
	buf := make([]byte, 512<<10)
	rand.Read(buf)
	l := newLoop(t, LoopConfig{FixedBuffers: [][]byte{make([]byte, 4096), buf}})
	fd, peer := tcpPair(t)

	recvd := make(chan []byte, 1)
	part := buf[1000:] // 只要位于注册缓冲区内即可，不必从起始处开始
	go func() { recvd <- readN(t, peer, len(part)) }()
	events := sendAllZC(t, l, fd, part, true, l.FixedBufferIndex(1))
	checkNotif(t, events)
	if got := <-recvd; !bytes.Equal(got, part) {
		t.Fatal("data mismatch")
	}
}

// buffer ring 同时注册为固定缓冲区：multishot recv 收到的数据直接用 SendZCFixed 原地发回，
// 通知 CQE 到达后才归还缓冲区
func TestLoopZeroCopyEchoFromBufferRing(t *testing.T) {
	l := newLoop(t, LoopConfig{BufferRings: []BufferRingConfig{{Count: 16, Size: 4096, Fixed: true}}})
	br := l.BufferRing(0)
	if br.FixedIndex() < 0 {
		t.Fatal("buffer ring not registered as fixed buffer")
	}
	fd, peer := tcpPair(t)

	var copied, notifs int
	stats := make(chan [2]int, 1)
	l.Post(func() {
		var onRecv Handler
		onRecv = func(res int32, flags uint32) {
			bid, data, ok := br.Get(res, flags)
			if ok && res > 0 {
				var off int
				var onSend Handler
				onSend = func(r int32, f uint32) {
					if f&iouring_syscall.IORING_CQE_F_NOTIF != 0 {
						notifs++
						if r&iouring_syscall.IORING_NOTIF_USAGE_ZC_COPIED != 0 {
							copied++
						}
						br.Recycle(bid) // 简化：每个缓冲区只发一次（无短写时成立）
						return
					}
					if r > 0 {
						off += int(r)
						if off < len(data) {
							t.Errorf("unexpected short send %d/%d", off, len(data))
						}
					}
					if f&iouring_syscall.IORING_CQE_F_MORE == 0 {
						br.Recycle(bid)
					}
				}
				l.SendZCFixed(fd, data, uint16(br.FixedIndex()), syscall.MSG_NOSIGNAL, iouring_syscall.IORING_SEND_ZC_REPORT_USAGE, onSend)
			} else if ok {
				br.Recycle(bid)
			}
			if flags&iouring_syscall.IORING_CQE_F_MORE == 0 {
				if res == -int32(syscall.ENOBUFS) {
					l.RecvMultishot(fd, br, 0, onRecv)
					return
				}
				stats <- [2]int{notifs, copied}
			}
		}
		l.RecvMultishot(fd, br, 0, onRecv)
	})

	var want, got []byte
	for i := 0; i < 50; i++ {
		msg := make([]byte, 1000+i*50)
		rand.Read(msg)
		want = append(want, msg...)
		peer.Write(msg)
		got = append(got, readN(t, peer, len(msg))...)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("echo data mismatch")
	}
	peer.(*net.TCPConn).CloseWrite()
	select {
	case s := <-stats:
		t.Logf("notifications=%d copied=%d", s[0], s[1])
		if s[0] == 0 {
			t.Fatal("no zero-copy notifications")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("multishot did not terminate")
	}
}

// SpinTime：轮询期间完成事件与 Post 都能被及时处理，Close 不会卡住
func TestLoopSpinTime(t *testing.T) {
	l, err := NewLoop(LoopConfig{SpinTime: 50 * time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 1)
	for i := 0; i < 2000; i++ {
		l.Post(func() { l.Nop(func(int32, uint32) { done <- struct{}{} }) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout at %d", i)
		}
		if i%500 == 0 {
			time.Sleep(time.Millisecond) // 让 Loop 进入真正的睡眠，验证唤醒路径
		}
	}
	closed := make(chan struct{})
	go func() { l.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked")
	}
}
