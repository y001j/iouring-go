//go:build linux
// +build linux

package iouring_syscall

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	IORING_REGISTER_BUFFERS uint8 = iota
	IORING_UNREGISTER_BUFFERS
	IORING_REGISTER_FILES
	IORING_UNREGISTER_FILES
	IORING_REGISTER_EVENTFD
	IORING_UNREGISTER_EVENTFD
	IORING_REGISTER_FILES_UPDATE
	IORING_REGISTER_EVENTFD_ASYNC
	IORING_REGISTER_PROBE
	IORING_REGISTER_PERSONALITY
	IORING_UNREGISTER_PERSONALITY
	IORING_REGISTER_RESTRICTIONS
	IORING_REGISTER_ENABLE_RINGS
)

const (
	IORING_REGISTER_FILES2        uint8 = 13 // 5.13+
	IORING_REGISTER_FILES_UPDATE2 uint8 = 14
	IORING_REGISTER_PBUF_RING   uint8 = 22 // 注册 provided buffer ring（5.19+）
	IORING_UNREGISTER_PBUF_RING uint8 = 23
)

type IOURingFilesUpdate struct {
	Offset uint32
	recv   uint32
	Fds    *int32
}

// IORING_RSRC_REGISTER_SPARSE 注册一个全空（-1）的稀疏文件表（5.19+）
const IORING_RSRC_REGISTER_SPARSE uint32 = 1 << 0

// RsrcRegister 对应内核 struct io_uring_rsrc_register
type RsrcRegister struct {
	Nr    uint32
	Flags uint32
	Resv2 uint64
	Data  uint64
	Tags  uint64
}

func IOURingRegister(fd int, opcode uint8, args unsafe.Pointer, nrArgs uint32) error {
	for {
		_, _, errno := syscall.Syscall6(
			SYS_IO_URING_REGISTER,
			uintptr(fd),
			uintptr(opcode),
			uintptr(args),
			uintptr(nrArgs),
			0,
			0,
		)
		if errno != 0 {
			// EINTR may be returned when blocked
			if errno == syscall.EINTR {
				continue
			}
			return os.NewSyscallError("iouring_register", errno)
		}
		return nil
	}
}
