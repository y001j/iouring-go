//go:build linux

package main

import (
	"fmt"
	"io"
	"net"
	"syscall"
)

// runBackend: Go 标准库 echo 服务
func runBackend(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Println("backend listening on", addr)
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func(c net.Conn) {
			defer c.Close()
			c.(*net.TCPConn).SetNoDelay(true)
			buf := make([]byte, 64*1024)
			for {
				n, err := c.Read(buf)
				if err != nil {
					return
				}
				if _, err := c.Write(buf[:n]); err != nil {
					return
				}
			}
		}(c)
	}
}

// runProxyStd: 每个方向一个 goroutine + io.CopyBuffer，底层是 Go runtime 的 epoll netpoller
func runProxyStd(addr, backend string, bufSize int) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Println("proxy-std listening on", addr)
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func(c net.Conn) {
			b, err := net.Dial("tcp", backend)
			if err != nil {
				c.Close()
				return
			}
			pipe := func(dst, src net.Conn) {
				io.CopyBuffer(dst, src, make([]byte, bufSize))
				dst.(*net.TCPConn).CloseWrite()
			}
			go pipe(b, c)
			pipe(c, b)
			c.Close()
			b.Close()
		}(c)
	}
}

// 原始 socket 工具函数（epoll / io_uring 代理共用）

func rawListen(addr string, nonblock bool) (int, error) {
	tcp, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return -1, err
	}
	typ := syscall.SOCK_STREAM | syscall.SOCK_CLOEXEC
	if nonblock {
		typ |= syscall.SOCK_NONBLOCK
	}
	fd, err := syscall.Socket(syscall.AF_INET, typ, 0)
	if err != nil {
		return -1, err
	}
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	sa := &syscall.SockaddrInet4{Port: tcp.Port}
	copy(sa.Addr[:], tcp.IP.To4())
	if err := syscall.Bind(fd, sa); err != nil {
		return -1, err
	}
	return fd, syscall.Listen(fd, 4096)
}

// rawDial: 阻塞 connect（只发生在建连阶段，不在热路径上）
func rawDial(addr string) (int, error) {
	tcp, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return -1, err
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	sa := &syscall.SockaddrInet4{Port: tcp.Port}
	copy(sa.Addr[:], tcp.IP.To4())
	if err := syscall.Connect(fd, sa); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	setNoDelay(fd)
	return fd, nil
}

func setNoDelay(fd int) {
	syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
}
