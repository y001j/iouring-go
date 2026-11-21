//go:build linux
// +build linux

package iouring

import (
	"time"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

type IOURingOption func(*IOURing)

// WithSQPoll a kernel thread is created to perform submission queue polling
// In Version 5.10 and later, allow using this as non-root,
// if the user has the CAP_SYS_NICE capability
func WithSQPoll() IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_SQPOLL
	}
}

func WithIOPoll() IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_IOPOLL
	}
}

// WithSQPollThreadCPU the poll thread will be bound to the cpu set, only meaningful when WithSQPoll option
func WithSQPollThreadCPU(cpu uint32) IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_SQ_AFF
		iour.params.SQThreadCPU = cpu
	}
}

func WithSQPollThreadIdle(idle time.Duration) IOURingOption {
	return func(iour *IOURing) {
		iour.params.SQThreadIdle = uint32(idle / time.Millisecond)
	}
}

// WithParams use params
func WithParams(params *iouring_syscall.IOURingParams) IOURingOption {
	return func(iour *IOURing) {
		iour.params = params
	}
}

// WithCQSize create the completion queue with size entries
// size must bue greater than entries
func WithCQSize(size uint32) IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_CQSIZE
		iour.params.CQEntries = size
	}
}

// WithAttachWQ new iouring instance being create will share the asynchronous worker thread
// backend of the specified io_uring ring, rather than create a new separate thread pool
func WithAttachWQ(iour *IOURing) IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_ATTACH_WQ
		iour.params.WQFd = uint32(iour.fd)
	}
}

func WithAsync() IOURingOption {
	return func(iour *IOURing) {
		iour.async = true
	}
}

// WithDisableRing the io_uring ring starts in a disabled state
// In this state, restrictions can be registered, but submissions are not allowed
// Available since 5.10
func WithDisableRing() IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_R_DISABLED
	}
}

// WithDrain every SQE will not be started before previously submitted SQEs have completed
func WithDrain() IOURingOption {
	return func(iour *IOURing) {
		iour.drain = true
	}
}

// WithCoopTaskRun 设置 IORING_SETUP_COOP_TASKRUN（需要 Linux 5.19+）。
// 异步完成的请求不再通过 IPI 中断提交线程，而是在下一次进入内核时处理，
// 适合网络 IO 这类大量异步完成的场景。内核不支持时 New 会返回 EINVAL。
func WithCoopTaskRun() IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_COOP_TASKRUN
	}
}
