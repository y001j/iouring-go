//go:build linux

package main

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// 手写 epoll（水平触发）转发代理：N 个事件循环，每个绑定一个 OS 线程。
// 写不完时把剩余数据挂到对端的 out 缓冲区，注册 EPOLLOUT，并暂停读源端（背压）。

type econn struct {
	fd     int
	peer   *econn
	out    []byte // 待写入本 fd 的数据
	paused bool   // 因对端写阻塞而暂停读
	closed bool
	loop   *eloop
}

type eloop struct {
	epfd int
	buf  []byte
}

var etable [1 << 16]unsafe.Pointer

func tget(fd int32) *econn      { return (*econn)(atomic.LoadPointer(&etable[fd])) }
func tset(fd int, c *econn)     { atomic.StorePointer(&etable[fd], unsafe.Pointer(c)) }

func runProxyEpoll(addr, backend string, nloops, bufSize int) error {
	lfd, err := rawListen(addr, false)
	if err != nil {
		return err
	}
	loops := make([]*eloop, nloops)
	for i := range loops {
		epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
		if err != nil {
			return err
		}
		loops[i] = &eloop{epfd: epfd, buf: make([]byte, bufSize)}
		go loops[i].run()
	}
	fmt.Println("proxy-epoll listening on", addr, "loops", nloops)

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
		syscall.SetNonblock(cfd, true)
		syscall.SetNonblock(bfd, true)

		l := loops[i%nloops]
		c := &econn{fd: cfd, loop: l}
		b := &econn{fd: bfd, loop: l}
		c.peer, b.peer = b, c
		tset(cfd, c)
		tset(bfd, b)
		for _, x := range []*econn{c, b} {
			ev := unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(x.fd)}
			unix.EpollCtl(l.epfd, unix.EPOLL_CTL_ADD, x.fd, &ev)
		}
	}
}

func (l *eloop) update(c *econn) {
	var ev uint32
	if !c.paused {
		ev |= unix.EPOLLIN | unix.EPOLLRDHUP
	}
	if len(c.out) > 0 {
		ev |= unix.EPOLLOUT
	}
	unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, c.fd, &unix.EpollEvent{Events: ev, Fd: int32(c.fd)})
}

func (l *eloop) closePair(c *econn) {
	for _, x := range []*econn{c, c.peer} {
		if x.closed {
			continue
		}
		x.closed = true
		tset(x.fd, nil)
		unix.EpollCtl(l.epfd, unix.EPOLL_CTL_DEL, x.fd, nil)
		syscall.Close(x.fd)
	}
}

func (l *eloop) run() {
	runtime.LockOSThread()
	events := make([]unix.EpollEvent, 256)
	for {
		n, err := unix.EpollWait(l.epfd, events, -1)
		if err != nil {
			continue
		}
		for i := 0; i < n; i++ {
			ev := events[i].Events
			c := tget(events[i].Fd)
			if c == nil || c.closed {
				continue
			}
			if ev&unix.EPOLLOUT != 0 {
				l.flush(c)
			}
			if c.closed {
				continue
			}
			if ev&(unix.EPOLLIN|unix.EPOLLRDHUP|unix.EPOLLHUP|unix.EPOLLERR) != 0 && !c.paused {
				l.forward(c)
			}
		}
	}
}

// forward: 从 c 读，写到 c.peer
func (l *eloop) forward(c *econn) {
	n, err := syscall.Read(c.fd, l.buf)
	if n <= 0 {
		if err == syscall.EAGAIN {
			return
		}
		l.closePair(c)
		return
	}
	p := c.peer
	data := l.buf[:n]
	for len(data) > 0 {
		m, err := syscall.Write(p.fd, data)
		if m > 0 {
			data = data[m:]
			continue
		}
		if err == syscall.EAGAIN {
			break
		}
		l.closePair(c)
		return
	}
	if len(data) > 0 {
		p.out = append(p.out[:0], data...)
		c.paused = true
		l.update(c)
		l.update(p)
	}
}

// flush: 把挂起的数据写入 c，写完后恢复读对端
func (l *eloop) flush(c *econn) {
	for len(c.out) > 0 {
		m, err := syscall.Write(c.fd, c.out)
		if m > 0 {
			c.out = c.out[m:]
			continue
		}
		if err == syscall.EAGAIN {
			return
		}
		l.closePair(c)
		return
	}
	c.out = c.out[:0]
	l.update(c)
	c.peer.paused = false
	l.update(c.peer)
}
