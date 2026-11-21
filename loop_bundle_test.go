//go:build linux
// +build linux

package iouring

import (
	"bytes"
	"crypto/rand"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

func requireBundle(t *testing.T, l *Loop) {
	if l.Features()&iouring_syscall.IORING_FEAT_RECVSEND_BUNDLE == 0 {
		t.Skip("kernel does not support recv bundles (needs 6.10+)")
	}
}

// recv bundle：一个 CQE 连续使用多个缓冲区；倒序归还以打乱 ring 中的顺序，验证位置映射
func TestLoopRecvBundle(t *testing.T) {
	const size = 1024
	l := newLoop(t, LoopConfig{BufferRings: []BufferRingConfig{{Count: 32, Size: size}}})
	requireBundle(t, l)
	br := l.BufferRing(0)
	fd, peer := tcpPair(t)

	var want []byte
	write := func(n int) {
		b := make([]byte, n)
		rand.Read(b)
		want = append(want, b...)
		if _, err := peer.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	write(10 * size) // 先积压数据，使第一次接收就能用到多个缓冲区
	time.Sleep(20 * time.Millisecond)

	type result struct {
		data    []byte
		maxBufs int
		err     int32
	}
	done := make(chan result, 1)
	var got []byte
	maxBufs := 0
	var bufs []BundleBuf
	l.Post(func() {
		var h Handler
		h = func(res int32, flags uint32) {
			bufs = br.Bundle(res, flags, bufs[:0])
			if res > 0 && len(bufs) > maxBufs {
				maxBufs = len(bufs)
			}
			for _, b := range bufs {
				got = append(got, b.Data...)
			}
			for i := len(bufs) - 1; i >= 0; i-- { // 倒序归还
				br.Recycle(bufs[i].Bid)
			}
			if flags&iouring_syscall.IORING_CQE_F_MORE == 0 {
				switch {
				case res == -int32(syscall.ENOBUFS) || res > 0:
					l.RecvMultishotBundle(fd, br, 0, h)
				case res == 0:
					done <- result{got, maxBufs, 0}
				default:
					done <- result{err: res}
				}
			}
		}
		l.RecvMultishotBundle(fd, br, 0, h)
	})

	for i := 0; i < 30; i++ {
		write(1 + i*997%7000)
		if i%5 == 0 {
			time.Sleep(2 * time.Millisecond)
		}
	}
	peer.(interface{ CloseWrite() error }).CloseWrite()

	select {
	case r := <-done:
		if r.err != 0 {
			t.Fatalf("recv error %v", syscall.Errno(-r.err))
		}
		t.Logf("max buffers in one CQE: %d", r.maxBufs)
		if r.maxBufs < 2 {
			t.Fatal("no CQE used more than one buffer")
		}
		if !bytes.Equal(r.data, want) {
			t.Fatalf("data mismatch: got %d bytes, want %d", len(r.data), len(want))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

func TestLoopSendmsg(t *testing.T) {
	l := newLoop(t, LoopConfig{})
	fd, peer := tcpPair(t)
	parts := [][]byte{[]byte("hello "), make([]byte, 100<<10), []byte(" world")}
	rand.Read(parts[1])
	var want []byte
	iovs := make([]unix.Iovec, len(parts))
	for i, p := range parts {
		iovs[i].Base = &p[0]
		iovs[i].SetLen(len(p))
		want = append(want, p...)
	}
	msg := &unix.Msghdr{Iov: &iovs[0]}
	msg.SetIovlen(len(iovs))

	res := make(chan int32, 1)
	l.Post(func() { l.Sendmsg(fd, msg, syscall.MSG_NOSIGNAL, func(r int32, _ uint32) { res <- r }) })
	got := readN(t, peer, len(want))
	if r := <-res; int(r) != len(want) {
		t.Fatalf("sendmsg res=%d, want %d", r, len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatal("data mismatch")
	}
}
