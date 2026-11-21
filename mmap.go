//go:build linux
// +build linux

package iouring

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

var (
	uint32Size = uint32(unsafe.Sizeof(uint32(0)))
	sqeSize    = uint32(unsafe.Sizeof(iouring_syscall.SubmissionQueueEntry{}))
	cqeSize    = uint32(unsafe.Sizeof(iouring_syscall.CompletionQueueEvent{}))
)

func mmapIOURing(iour *IOURing) (err error) {
	iour.sq, iour.cq, err = mmapRings(iour.fd, iour.params)
	return err
}

func munmapIOURing(iour *IOURing) error {
	return munmapRings(iour.sq, iour.cq)
}

// mmapRings 映射 io_uring 的 SQ/CQ ring 与 SQE 数组
func mmapRings(fd int, params *iouring_syscall.IOURingParams) (sq *SubmissionQueue, cq *CompletionQueue, err error) {
	sq, cq = new(SubmissionQueue), new(CompletionQueue)
	defer func() {
		if err != nil {
			munmapRings(sq, cq)
			sq, cq = nil, nil
		}
	}()

	sqSize := params.SQOffset.Array + params.SQEntries*uint32Size
	cqSize := params.CQOffset.Cqes + params.CQEntries*cqeSize

	singleMmap := params.Features&iouring_syscall.IORING_FEAT_SINGLE_MMAP != 0
	if singleMmap {
		// SQ 与 CQ 共享同一块映射，长度取两者较大值
		if cqSize > sqSize {
			sqSize = cqSize
		}
		cqSize = sqSize
	}

	if sq.mem, err = mmap(fd, sqSize, iouring_syscall.IORING_OFF_SQ_RING); err != nil {
		return sq, cq, fmt.Errorf("mmap sq ring: %w", err)
	}
	if singleMmap {
		cq.mem = sq.mem
	} else if cq.mem, err = mmap(fd, cqSize, iouring_syscall.IORING_OFF_CQ_RING); err != nil {
		return sq, cq, fmt.Errorf("mmap cq ring: %w", err)
	}
	if sq.sqesMem, err = mmap(fd, params.SQEntries*sqeSize, iouring_syscall.IORING_OFF_SQES); err != nil {
		return sq, cq, fmt.Errorf("mmap sqe array: %w", err)
	}

	setupSQ(sq, params)
	setupCQ(cq, params)
	return sq, cq, nil
}

func u32At(mem []byte, off uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&mem[off]))
}

func setupSQ(sq *SubmissionQueue, params *iouring_syscall.IOURingParams) {
	mem, off := sq.mem, params.SQOffset
	sq.head = u32At(mem, off.Head)
	sq.tail = u32At(mem, off.Tail)
	sq.mask = u32At(mem, off.RingMask)
	sq.entries = u32At(mem, off.RingEntries)
	sq.flags = u32At(mem, off.Flags)
	sq.dropped = u32At(mem, off.Dropped)
	sq.array = unsafe.Slice(u32At(mem, off.Array), params.SQEntries)
	sq.sqes = unsafe.Slice((*iouring_syscall.SubmissionQueueEntry)(unsafe.Pointer(&sq.sqesMem[0])), params.SQEntries)
}

func setupCQ(cq *CompletionQueue, params *iouring_syscall.IOURingParams) {
	mem, off := cq.mem, params.CQOffset
	cq.head = u32At(mem, off.Head)
	cq.tail = u32At(mem, off.Tail)
	cq.mask = u32At(mem, off.RingMask)
	cq.entries = u32At(mem, off.RingEntries)
	cq.flags = u32At(mem, off.Flags)
	cq.overflow = u32At(mem, off.Overflow)
	cq.cqes = unsafe.Slice((*iouring_syscall.CompletionQueueEvent)(unsafe.Pointer(&mem[off.Cqes])), params.CQEntries)
}

func munmapRings(sq *SubmissionQueue, cq *CompletionQueue) error {
	if sq != nil {
		if sq.sqesMem != nil {
			if err := unix.Munmap(sq.sqesMem); err != nil {
				return fmt.Errorf("munmap sqe array: %w", err)
			}
			sq.sqesMem, sq.sqes = nil, nil
		}
		if sq.mem != nil {
			shared := cq != nil && cq.mem != nil && &cq.mem[0] == &sq.mem[0]
			if err := unix.Munmap(sq.mem); err != nil {
				return fmt.Errorf("munmap sq: %w", err)
			}
			sq.mem = nil
			if shared {
				cq.mem = nil
			}
		}
	}

	if cq != nil && cq.mem != nil {
		if err := unix.Munmap(cq.mem); err != nil {
			return fmt.Errorf("munmap cq: %w", err)
		}
		cq.mem = nil
	}
	return nil
}

func mmap(fd int, length uint32, offset uint64) ([]byte, error) {
	b, err := unix.Mmap(fd, int64(offset), int(length),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return nil, os.NewSyscallError("mmap", err)
	}
	return b, nil
}
