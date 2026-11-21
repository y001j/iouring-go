//go:build linux
// +build linux

package iouring

import (
	"sync/atomic"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

// iouring operations
const (
	OpNop uint8 = iota
	OpReadv
	OpWritev
	OpFsync
	OpReadFixed
	OpWriteFixed
	OpPollAdd
	OpPollRemove
	OpSyncFileRange
	OpSendmsg
	OpRecvmsg
	OpTimeout
	OpTimeoutRemove
	OpAccept
	OpAsyncCancel
	OpLinkTimeout
	OpConnect
	OpFallocate
	OpOpenat
	OpClose
	OpFilesUpdate
	OpStatx
	OpRead
	OpWrite
	OpFadvise
	OpMadvise
	OpSend
	OpRecv
	OpOpenat2
	OpEpollCtl
	OpSplice
	OpProvideBuffers
	OpRemoveBuffers
	OpTee
	OpShutdown
	OpRenameat
	OpUnlinkat
	OpMkdirat
	OpSymlinkat
	OpLinkat
)

// cancel operation return value
const (
	RequestCanceledSuccessfully = 0
	RequestMaybeCanceled        = 1
)

// timeout operation return value
const (
	TimeoutExpiration = 0
	CountCompletion   = 1
)

var _zero uintptr

type SubmissionQueue struct {
	mem     []byte // mmap 的 SQ ring
	sqesMem []byte // mmap 的 SQE 数组

	head    *uint32
	tail    *uint32
	mask    *uint32
	entries *uint32 // specifies the number of submission queue ring entries
	flags   *uint32 // used by the kernel to communicate stat information to the application
	dropped *uint32 // incrementd for each invalid submission queue entry encountered in the ring buffer

	array []uint32
	sqes  []iouring_syscall.SubmissionQueueEntry // submission queue ring

	sqeHead uint32
	sqeTail uint32
}

func (queue *SubmissionQueue) getSQEntry() *iouring_syscall.SubmissionQueueEntry {
	head := atomic.LoadUint32(queue.head)
	next := queue.sqeTail + 1

	if (next - head) <= *queue.entries {
		sqe := &queue.sqes[queue.sqeTail&*queue.mask]
		queue.sqeTail = next
		sqe.Reset()
		return sqe
	}
	return nil
}

// free 返回 SQ 中尚可填充的 SQE 数量
func (queue *SubmissionQueue) free() uint32 {
	return *queue.entries - (queue.sqeTail - atomic.LoadUint32(queue.head))
}

func atomicLoad(p *uint32) uint32 { return atomic.LoadUint32(p) }

func (queue *SubmissionQueue) fallback(i uint32) {
	queue.sqeTail -= i
}

func (queue *SubmissionQueue) cqOverflow() bool {
	return (atomic.LoadUint32(queue.flags) & iouring_syscall.IORING_SQ_CQ_OVERFLOW) != 0
}

func (queue *SubmissionQueue) needWakeup() bool {
	return (atomic.LoadUint32(queue.flags) & iouring_syscall.IORING_SQ_NEED_WAKEUP) != 0
}

// sync internal status with kernel ring state on the SQ side
// return the number of pending items in the SQ ring, for the shared ring.
func (queue *SubmissionQueue) flush() int {
	if queue.sqeHead == queue.sqeTail {
		return int(*queue.tail - atomic.LoadUint32(queue.head))
	}

	tail := *queue.tail
	for toSubmit := queue.sqeTail - queue.sqeHead; toSubmit > 0; toSubmit-- {
		queue.array[tail&*queue.mask] = queue.sqeHead & *queue.mask
		tail++
		queue.sqeHead++
	}

	atomic.StoreUint32(queue.tail, tail)
	return int(tail - atomic.LoadUint32(queue.head))
}

type CompletionQueue struct {
	mem []byte // mmap 的 CQ ring（SINGLE_MMAP 时与 SQ 共享）

	head     *uint32
	tail     *uint32
	mask     *uint32
	entries  *uint32
	flags    *uint32
	overflow *uint32

	cqes []iouring_syscall.CompletionQueueEvent
}

// setEventfdEnabled 开关内核对 eventfd 的通知（IORING_CQ_EVENTFD_DISABLED，5.8+）
func (queue *CompletionQueue) setEventfdEnabled(enabled bool) {
	for {
		old := atomic.LoadUint32(queue.flags)
		v := old | iouring_syscall.IORING_CQ_EVENTFD_DISABLED
		if enabled {
			v = old &^ iouring_syscall.IORING_CQ_EVENTFD_DISABLED
		}
		if old == v || atomic.CompareAndSwapUint32(queue.flags, old, v) {
			return
		}
	}
}

func (queue *CompletionQueue) peek() (cqe *iouring_syscall.CompletionQueueEvent) {
	head := *queue.head
	if head != atomic.LoadUint32(queue.tail) {
		//	if head < atomic.LoadUint32(queue.tail) {
		cqe = &queue.cqes[head&*queue.mask]
	}
	return
}

func (queue *CompletionQueue) advance(num uint32) {
	if num != 0 {
		atomic.AddUint32(queue.head, num)
	}
}
