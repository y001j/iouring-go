//go:build linux
// +build linux

package iouring

import (
	"syscall"
	"unsafe"
)

var zero uintptr

func bytes2iovec(bs [][]byte) []syscall.Iovec {
	iovecs := make([]syscall.Iovec, len(bs))
	for i, b := range bs {
		iovecs[i].SetLen(len(b))
		if len(b) > 0 {
			iovecs[i].Base = &b[0]
		} else {
			iovecs[i].Base = (*byte)(unsafe.Pointer(&zero))
		}
	}
	return iovecs
}

// sockaddr 把 syscall.Sockaddr 转为内核使用的原始地址结构。
// 原实现通过 go:linkname 引用标准库内部符号 syscall.Sockaddr.sockaddr，
// Go 1.23 起链接器禁止此类引用，导致使用本库的程序无法链接，这里改为自行实现。
// 返回的指针指向新分配的内存，调用方需在请求完成前保持其可达（如 userData.hold）。
func sockaddr(addr syscall.Sockaddr) (unsafe.Pointer, uint32, error) {
	switch sa := addr.(type) {
	case *syscall.SockaddrInet4:
		if sa.Port < 0 || sa.Port > 0xFFFF {
			return nil, 0, syscall.EINVAL
		}
		raw := &syscall.RawSockaddrInet4{Family: syscall.AF_INET}
		p := (*[2]byte)(unsafe.Pointer(&raw.Port))
		p[0], p[1] = byte(sa.Port>>8), byte(sa.Port)
		raw.Addr = sa.Addr
		return unsafe.Pointer(raw), syscall.SizeofSockaddrInet4, nil
	case *syscall.SockaddrInet6:
		if sa.Port < 0 || sa.Port > 0xFFFF {
			return nil, 0, syscall.EINVAL
		}
		raw := &syscall.RawSockaddrInet6{Family: syscall.AF_INET6, Scope_id: sa.ZoneId}
		p := (*[2]byte)(unsafe.Pointer(&raw.Port))
		p[0], p[1] = byte(sa.Port>>8), byte(sa.Port)
		raw.Addr = sa.Addr
		return unsafe.Pointer(raw), syscall.SizeofSockaddrInet6, nil
	case *syscall.SockaddrUnix:
		name := sa.Name
		raw := &syscall.RawSockaddrUnix{Family: syscall.AF_UNIX}
		if len(name) >= len(raw.Path) {
			return nil, 0, syscall.EINVAL
		}
		for i := 0; i < len(name); i++ {
			raw.Path[i] = int8(name[i])
		}
		sl := uint32(2) // sizeof(sa_family_t)
		if n := len(name); n > 0 {
			sl += uint32(n) + 1
		}
		if raw.Path[0] == '@' { // 抽象 socket
			raw.Path[0] = 0
			sl--
		}
		return unsafe.Pointer(raw), sl, nil
	}
	return nil, 0, syscall.EAFNOSUPPORT
}

// anyToSockaddr 把内核返回的原始地址转为 syscall.Sockaddr（替代 go:linkname 的 syscall.anyToSockaddr）
func anyToSockaddr(rsa *syscall.RawSockaddrAny) (syscall.Sockaddr, error) {
	switch rsa.Addr.Family {
	case syscall.AF_INET:
		pp := (*syscall.RawSockaddrInet4)(unsafe.Pointer(rsa))
		sa := &syscall.SockaddrInet4{Addr: pp.Addr}
		p := (*[2]byte)(unsafe.Pointer(&pp.Port))
		sa.Port = int(p[0])<<8 + int(p[1])
		return sa, nil
	case syscall.AF_INET6:
		pp := (*syscall.RawSockaddrInet6)(unsafe.Pointer(rsa))
		sa := &syscall.SockaddrInet6{Addr: pp.Addr, ZoneId: pp.Scope_id}
		p := (*[2]byte)(unsafe.Pointer(&pp.Port))
		sa.Port = int(p[0])<<8 + int(p[1])
		return sa, nil
	case syscall.AF_UNIX:
		pp := (*syscall.RawSockaddrUnix)(unsafe.Pointer(rsa))
		sa := &syscall.SockaddrUnix{}
		if pp.Path[0] == 0 {
			// 抽象 socket：首字节 0 替换为 '@'
			pp.Path[0] = '@'
		}
		n := 0
		for n < len(pp.Path) && pp.Path[n] != 0 {
			n++
		}
		b := make([]byte, n)
		for i := 0; i < n; i++ {
			b[i] = byte(pp.Path[i])
		}
		sa.Name = string(b)
		return sa, nil
	}
	return nil, syscall.EAFNOSUPPORT
}
