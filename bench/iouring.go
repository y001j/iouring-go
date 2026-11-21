//go:build linux

package main

import (
	"fmt"
	"sync/atomic"
	"syscall"

	iouring "github.com/y001j/iouring-go"
)

// 基于 iouring-go 的转发代理：
// 每个方向循环执行  Recv(src) -> Send(dst)（短写则续写）-> Recv(src) ...
// 完成事件经由 iouring-go 的 resulter channel 交给 workers 个 goroutine 执行回调。
// 每个方向同一时刻只有一个在途请求，所以多个 worker 并发处理是安全的。

type ipair struct {
	cfd, bfd int
	closed   int32
}

type idir struct {
	ring     *iouring.IOURing
	ch       chan iouring.Result
	src, dst int
	buf      []byte
	pending  []byte
	pair     *ipair
}

func runProxyIOURing(addr, backend string, nrings, workers, bufSize int, coop, direct bool) error {
	// direct 模式：channel 为 nil，回调直接在 iouring-go 的完成事件 goroutine 中执行
	var ch chan iouring.Result
	if !direct {
		ch = make(chan iouring.Result, 1<<16)
	}
	rings := make([]*iouring.IOURing, nrings)
	for i := range rings {
		var opts []iouring.IOURingOption
		if coop {
			opts = append(opts, iouring.WithCoopTaskRun())
		}
		r, err := iouring.New(4096, opts...)
		if err != nil {
			return fmt.Errorf("iouring.New: %w", err)
		}
		rings[i] = r
	}
	for i := 0; ch != nil && i < workers; i++ {
		go func() {
			for res := range ch {
				res.Callback()
			}
		}()
	}

	lfd, err := rawListen(addr, false)
	if err != nil {
		return err
	}
	fmt.Println("proxy-iouring listening on", addr, "rings", nrings, "workers", workers, "coop", coop, "direct", direct)

	for i := 0; ; i++ {
		cfd, _, err := syscall.Accept(lfd)
		if err != nil {
			return err
		}
		bfd, err := rawDial(backend)
		if err != nil {
			syscall.Close(cfd)
			continue
		}
		setNoDelay(cfd)
		r := rings[i%nrings]
		p := &ipair{cfd: cfd, bfd: bfd}
		up := &idir{ring: r, ch: ch, src: cfd, dst: bfd, buf: make([]byte, bufSize), pair: p}
		down := &idir{ring: r, ch: ch, src: bfd, dst: cfd, buf: make([]byte, bufSize), pair: p}
		up.recv()
		down.recv()
	}
}

func (d *idir) recv() {
	prep := iouring.Recv(d.src, d.buf, 0).WithInfo(d).WithCallback(onRecv)
	if _, err := d.ring.SubmitRequest(prep, d.ch); err != nil {
		d.pair.close()
	}
}

func (d *idir) send(data []byte) {
	d.pending = data
	prep := iouring.Send(d.dst, data, 0).WithInfo(d).WithCallback(onSend)
	if _, err := d.ring.SubmitRequest(prep, d.ch); err != nil {
		d.pair.close()
	}
}

// res 返回 Recv/Send 的结果：成功为字节数，失败返回 -1
func res(r iouring.Result) int {
	n, err := r.ReturnInt()
	if err != nil {
		return -1
	}
	return n
}

func onRecv(r iouring.Result) error {
	d := r.GetRequestInfo().(*idir)
	n := res(r)
	if n <= 0 {
		d.pair.close()
		return nil
	}
	d.send(d.buf[:n])
	return nil
}

func onSend(r iouring.Result) error {
	d := r.GetRequestInfo().(*idir)
	n := res(r)
	if n < 0 {
		d.pair.close()
		return nil
	}
	if n < len(d.pending) {
		d.send(d.pending[n:])
		return nil
	}
	d.recv()
	return nil
}

func (p *ipair) close() {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return
	}
	// shutdown 唤醒另一方向上挂起的 Recv
	syscall.Shutdown(p.cfd, syscall.SHUT_RDWR)
	syscall.Shutdown(p.bfd, syscall.SHUT_RDWR)
	syscall.Close(p.cfd)
	syscall.Close(p.bfd)
}
