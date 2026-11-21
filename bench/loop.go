//go:build linux

package main

import (
	"fmt"
	"syscall"

	iouring "github.com/y001j/iouring-go"
	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

// 基于 iouring.Loop 的转发代理：
//   - N 个 Loop，每个独占一个 io_uring 与一个 OS 线程，连接对按轮询分配到 Loop；
//   - 每个方向一个 multishot recv，数据落在 provided buffer ring 的缓冲区中，
//     直接把该缓冲区 Send 给对端（零拷贝转发），发送完成后归还缓冲区；
//   - 同一方向的 Send 串行执行以保证 TCP 字节流顺序；
//   - 缓冲区耗尽（-ENOBUFS）时记录下来，有缓冲区归还时重新提交 multishot recv。

type lstate struct {
	fixed   bool
	loop    *iouring.Loop
	br      *iouring.BufferRing
	starved []*ldir
}

type lchunk struct {
	bid  uint16
	data []byte
}

type lpair struct {
	st     *lstate
	fds    [2]int // 原始 fd（shutdown/close 用）
	idx    [2]int // 注册文件表下标（-1 表示未注册）
	dirs   [2]*ldir
	closed bool
	freed  bool
}

type ldir struct {
	p          *lpair
	st         *lstate
	src, dst   int
	q          []lchunk
	qbuf       []lchunk
	off        int
	sending    bool
	recvActive bool
	onRecvH    iouring.Handler
	onSendH    iouring.Handler
}

func runProxyLoop(addr, backend string, nloops, bufCount, bufSize int, fixed bool) error {
	fixedFiles := 0
	if fixed {
		fixedFiles = 16384
	}
	states := make([]*lstate, nloops)
	for i := range states {
		l, err := iouring.NewLoop(iouring.LoopConfig{
			Entries:     4096,
			BufferRings: []iouring.BufferRingConfig{{Count: bufCount, Size: bufSize}},
			FixedFiles:  fixedFiles,
		})
		if err != nil {
			return fmt.Errorf("NewLoop: %w", err)
		}
		states[i] = &lstate{loop: l, br: l.BufferRing(0), fixed: fixed}
		if i == 0 {
			fmt.Printf("loop setup flags %#x\n", l.SetupFlags)
		}
	}

	lfd, err := rawListen(addr, false)
	if err != nil {
		return err
	}
	fmt.Println("proxy-iouring-loop listening on", addr, "loops", nloops, "bufs", bufCount, "x", bufSize, "fixed", fixed)

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
		st := states[i%nloops]
		st.loop.Post(func() { st.startPair(cfd, bfd) })
	}
}

func (st *lstate) startPair(cfd, bfd int) {
	p := &lpair{st: st, fds: [2]int{cfd, bfd}, idx: [2]int{-1, -1}}
	use := [2]int{cfd, bfd}
	if st.fixed {
		for i, fd := range p.fds {
			if idx, err := st.loop.RegisterFile(fd); err == nil {
				p.idx[i] = idx
				use[i] = iouring.FixedFile(idx)
			}
		}
	}
	p.dirs[0] = st.newDir(p, use[0], use[1])
	p.dirs[1] = st.newDir(p, use[1], use[0])
	p.dirs[0].arm()
	p.dirs[1].arm()
}

func (st *lstate) newDir(p *lpair, src, dst int) *ldir {
	d := &ldir{p: p, st: st, src: src, dst: dst, qbuf: make([]lchunk, 0, 16)}
	d.q = d.qbuf
	// 缓存方法值，避免每次提交都分配闭包
	d.onRecvH = d.onRecv
	d.onSendH = d.onSend
	return d
}

func (d *ldir) arm() {
	d.recvActive = true
	d.st.loop.RecvMultishot(d.src, d.st.br, 0, d.onRecvH)
}

func (d *ldir) onRecv(res int32, flags uint32) {
	st := d.st
	if bid, data, ok := st.br.Get(res, flags); ok {
		if res > 0 && !d.p.closed {
			d.q = append(d.q, lchunk{bid: bid, data: data})
			if !d.sending {
				d.sendNext()
			}
		} else {
			st.br.Recycle(bid)
		}
	}
	if flags&iouring_syscall.IORING_CQE_F_MORE != 0 {
		return
	}

	// multishot 已终止
	d.recvActive = false
	switch {
	case d.p.closed:
		d.p.maybeFree()
	case res == -int32(syscall.ENOBUFS):
		st.starved = append(st.starved, d)
	case res > 0:
		d.arm()
	default: // EOF 或错误
		d.p.close()
	}
}

func (d *ldir) sendNext() {
	d.sending = true
	c := d.q[0]
	d.st.loop.Send(d.dst, c.data[d.off:], syscall.MSG_NOSIGNAL, d.onSendH)
}

func (d *ldir) onSend(res int32, flags uint32) {
	st := d.st
	if res < 0 {
		d.sending = false
		d.p.close()
		return
	}
	d.off += int(res)
	if d.off < len(d.q[0].data) && !d.p.closed {
		d.sendNext() // 短写：续写剩余部分
		return
	}

	st.br.Recycle(d.q[0].bid)
	d.q[0] = lchunk{}
	d.q = d.q[1:]
	if len(d.q) == 0 {
		d.q = d.qbuf[:0] // 复用底层数组，避免反复扩容
	}
	d.off = 0
	st.rearmStarved()

	if len(d.q) > 0 && !d.p.closed {
		d.sendNext()
		return
	}
	d.sending = false
	if d.p.closed {
		d.dropQueue()
		d.p.maybeFree()
	}
}

func (d *ldir) dropQueue() {
	for _, c := range d.q {
		d.st.br.Recycle(c.bid)
	}
	d.q = d.qbuf[:0]
	d.off = 0
}

func (st *lstate) rearmStarved() {
	if len(st.starved) == 0 {
		return
	}
	for i, d := range st.starved {
		st.starved[i] = nil
		if !d.p.closed && !d.recvActive {
			d.arm()
		}
	}
	st.starved = st.starved[:0]
}

// close 关闭连接对：shutdown 使两个方向的 multishot recv 以 EOF 终止，
// 待所有在途请求结束后在 maybeFree 中关闭 fd。
func (p *lpair) close() {
	if p.closed {
		return
	}
	p.closed = true
	syscall.Shutdown(p.fds[0], syscall.SHUT_RDWR)
	syscall.Shutdown(p.fds[1], syscall.SHUT_RDWR)
	for _, d := range p.dirs {
		if !d.sending {
			d.dropQueue()
		}
	}
	// 处于 starved 状态的方向没有在途 recv
	p.maybeFree()
}

func (p *lpair) maybeFree() {
	if p.freed {
		return
	}
	for _, d := range p.dirs {
		if d.recvActive || d.sending {
			return
		}
	}
	p.freed = true
	for _, idx := range p.idx {
		if idx >= 0 {
			p.st.loop.UnregisterFile(idx)
		}
	}
	syscall.Close(p.fds[0])
	syscall.Close(p.fds[1])
}
