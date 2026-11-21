// +build linux

package iouring

import (
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

func (iour *IOURing) SubmitLinkRequests(requests []PrepRequest, ch chan<- Result) (RequestSet, error) {
	return iour.submitLinkRequest(requests, ch, false)
}

func (iour *IOURing) SubmitHardLinkRequests(requests []PrepRequest, ch chan<- Result) (RequestSet, error) {
	return iour.submitLinkRequest(requests, ch, true)
}

func (iour *IOURing) submitLinkRequest(requests []PrepRequest, ch chan<- Result, hard bool) (RequestSet, error) {
	flags := iouring_syscall.IOSQE_FLAGS_IO_LINK
	if hard {
		flags = iouring_syscall.IOSQE_FLAGS_IO_HARDLINK
	}
	return iour.submitRequests(requests, ch, flags)
}

func linkTimeout(t time.Duration) PrepRequest {
	timespec := unix.NsecToTimespec(t.Nanoseconds())

	return func(sqe *iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&timespec)
		userData.request.resolver = timeoutResolver

		sqe.PrepOperation(iouring_syscall.IORING_OP_LINK_TIMEOUT, -1, uint64(uintptr(unsafe.Pointer(&timespec))), 1, 0)
	}
}
