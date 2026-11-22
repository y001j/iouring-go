//go:build linux
// +build linux

package iouring

import (
	"bytes"
	"crypto/rand"
	"errors"
	"syscall"
	"testing"
	"time"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

// 增量消费：少量大缓冲区被多次接收连续填充；多个连接共享同一个 ring，
// 数据交错写入同一缓冲区。内核用完（无 BUF_MORE）后才归还。
func TestLoopIncrementalBufferRing(t *testing.T) {
	const size, count, conns = 64 << 10, 4, 8
	l, err := NewLoop(LoopConfig{BufferRings: []BufferRingConfig{{Count: count, Size: size, Incremental: true}}})
	if err != nil {
		if errors.Is(err, syscall.EINVAL) {
			t.Skip("kernel does not support incremental buffer rings (needs 6.12+)")
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	br := l.BufferRing(0)

	type stream struct {
		fd   int
		got  []byte
		want []byte
		done bool
	}
	streams := make([]*stream, conns)
	peers := make([]interface{ Write([]byte) (int, error) }, conns)
	closers := make([]interface{ CloseWrite() error }, conns)
	for i := range streams {
		fd, peer := tcpPair(t)
		streams[i] = &stream{fd: fd}
		peers[i] = peer
		closers[i] = peer.(interface{ CloseWrite() error })
	}

	var cqes, moreCQEs, maxInBuf int
	inBuf := map[uint16]int{} // 每个缓冲区被多少个 CQE 写入过（验证确实被连续填充）
	finished := make(chan struct{})
	ndone := 0
	l.Post(func() {
		for _, st := range streams {
			st := st
			var h Handler
			h = func(res int32, flags uint32) {
				if bid, data, more, ok := br.GetIncremental(res, flags); ok {
					cqes++
					st.got = append(st.got, data...)
					inBuf[bid]++
					if inBuf[bid] > maxInBuf {
						maxInBuf = inBuf[bid]
					}
					if more {
						moreCQEs++
					} else {
						inBuf[bid] = 0
						br.Recycle(bid)
					}
				}
				if flags&iouring_syscall.IORING_CQE_F_MORE == 0 {
					if res > 0 || res == -int32(syscall.ENOBUFS) {
						l.RecvMultishot(st.fd, br, 0, h)
						return
					}
					if res < 0 {
						t.Errorf("recv error %v", syscall.Errno(-res))
					}
					st.done = true
					if ndone++; ndone == conns {
						close(finished)
					}
				}
			}
			l.RecvMultishot(st.fd, br, 0, h)
		}
	})

	for round := 0; round < 40; round++ {
		for i, st := range streams {
			b := make([]byte, 100+(round*7919+i*104729)%20000)
			rand.Read(b)
			st.want = append(st.want, b...)
			peers[i].Write(b)
		}
		if round%8 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	for _, c := range closers {
		c.CloseWrite()
	}
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
	done := make(chan struct{})
	l.Post(func() {
		for i, st := range streams {
			if !bytes.Equal(st.got, st.want) {
				t.Errorf("conn %d: data mismatch (%d vs %d bytes)", i, len(st.got), len(st.want))
			}
		}
		t.Logf("cqes=%d with BUF_MORE=%d, max CQEs into one buffer before release=%d", cqes, moreCQEs, maxInBuf)
		if moreCQEs == 0 {
			t.Error("no CQE had IORING_CQE_F_BUF_MORE: buffers were not consumed incrementally")
		}
		close(done)
	})
	<-done
}
