//go:build linux
// +build linux

package iouring_syscall

const (
	SYS_IO_URING_SETUP    = 425
	SYS_IO_URING_ENTER    = 426
	SYS_IO_URING_REGISTER = 427
)

// IORING Offset
const (
	IORING_OFF_SQ_RING uint64 = 0
	IORING_OFF_CQ_RING uint64 = 0x8000000
	IORING_OFF_SQES    uint64 = 0x10000000
)

const (
	IORING_OP_NOP uint8 = iota
	IORING_OP_READV
	IORING_OP_WRITEV
	IORING_OP_FSYNC
	IORING_OP_READ_FIXED
	IORING_OP_WRITE_FIXED
	IORING_OP_POLL_ADD
	IORING_OP_POLL_REMOVE
	IORING_OP_SYNC_FILE_RANGE
	IORING_OP_SENDMSG
	IORING_OP_RECVMSG
	IORING_OP_TIMEOUT
	IORING_OP_TIMEOUT_REMOVE
	IORING_OP_ACCEPT
	IORING_OP_ASYNC_CANCEL
	IORING_OP_LINK_TIMEOUT
	IORING_OP_CONNECT
	IORING_OP_FALLOCATE
	IORING_OP_OPENAT
	IORING_OP_CLOSE
	IORING_OP_FILES_UPDATE
	IORING_OP_STATX
	IORING_OP_READ
	IORING_OP_WRITE
	IORING_OP_FADVISE
	IORING_OP_MADVISE
	IORING_OP_SEND
	IORING_OP_RECV
	IORING_OP_OPENAT2
	IORING_OP_EPOLL_CTL
	IORING_OP_SPLICE
	IORING_OP_PROVIDE_BUFFERS
	IORING_OP_REMOVE_BUFFERS
	IORING_OP_TEE
	IORING_OP_SHUTDOWN
	IORING_OP_RENAMEAT
	IORING_OP_UNLINKAT
	IORING_OP_MKDIRAT
	IORING_OP_SYMLINKAT
	IORING_OP_LINKAT
	IORING_OP_MSG_RING   // 5.18
	IORING_OP_FSETXATTR  // 5.19
	IORING_OP_SETXATTR   // 5.19
	IORING_OP_FGETXATTR  // 5.19
	IORING_OP_GETXATTR   // 5.19
	IORING_OP_SOCKET     // 5.19
	IORING_OP_URING_CMD  // 5.19
	IORING_OP_SEND_ZC    // 6.0：零拷贝 send，产生结果 CQE + 通知 CQE（IORING_CQE_F_NOTIF）
	IORING_OP_SENDMSG_ZC // 6.1
)

const (
	IORING_SQ_NEED_WAKEUP uint32 = 1 << iota
	IORING_SQ_CQ_OVERFLOW
	IORING_SQ_TASKRUN
)

const (
	IOSQE_FLAGS_FIXED_FILE uint8 = 1 << iota
	IOSQE_FLAGS_IO_DRAIN
	IOSQE_FLAGS_IO_LINK
	IOSQE_FLAGS_IO_HARDLINK
	IOSQE_FLAGS_ASYNC
	IOSQE_FLAGS_BUFFER_SELECT
)

const IOSQE_SYNC_DATASYNC uint = 1
const IOSQE_TIMEOUT_ABS uint = 1
const IOSQE_SPLICE_F_FD_IN_FIXED = 1 << 31

type SubmissionQueueEntry struct {
	opcode   uint8
	flags    uint8
	ioprio   uint16
	fd       int32
	offset   uint64
	addr     uint64
	len      uint32
	opFlags  uint32
	userdata uint64

	bufIndexOrGroup uint16
	personality     uint16
	spliceFdIn      int32
	extra           [2]uint64
}

func (sqe *SubmissionQueueEntry) Opcode() uint8 {
	return sqe.opcode
}

func (sqe *SubmissionQueueEntry) Reset() {
	*sqe = SubmissionQueueEntry{}
}

func (sqe *SubmissionQueueEntry) PrepOperation(op uint8, fd int32, addrOrSpliceOffIn uint64, len uint32, offset uint64) {
	sqe.opcode = op
	sqe.fd = fd
	sqe.addr = addrOrSpliceOffIn
	sqe.len = len
	sqe.offset = offset
}

func (sqe *SubmissionQueueEntry) Fd() int32 {
	return sqe.fd
}

func (sqe *SubmissionQueueEntry) SetFdIndex(index int32) {
	sqe.fd = index
	sqe.flags |= IOSQE_FLAGS_FIXED_FILE
}

func (sqe *SubmissionQueueEntry) SetOpFlags(opflags uint32) {
	sqe.opFlags = opflags
}

func (sqe *SubmissionQueueEntry) SetUserData(userData uint64) {
	sqe.userdata = userData
}

func (sqe *SubmissionQueueEntry) SetFlags(flags uint8) {
	sqe.flags |= flags
}

// CleanFlags 清除指定标志位。
// 修复：原实现为 sqe.flags ^= flags（异或），对未设置的标志位反而会将其置位，
// 导致链式请求的最后一个 SQE 被错误地带上 IO_LINK/IO_HARDLINK，与后续无关请求串成链。
func (sqe *SubmissionQueueEntry) CleanFlags(flags uint8) {
	sqe.flags &^= flags
}

func (sqe *SubmissionQueueEntry) SetIoprio(ioprio uint16) {
	sqe.ioprio = ioprio
}

func (sqe *SubmissionQueueEntry) SetBufIndex(bufIndex uint16) {
	sqe.bufIndexOrGroup = bufIndex
}

func (sqe *SubmissionQueueEntry) SetBufGroup(bufGroup uint16) {
	sqe.bufIndexOrGroup = bufGroup
}

func (sqe *SubmissionQueueEntry) SetPeronality(personality uint16) {
	sqe.personality = personality
}

func (sqe *SubmissionQueueEntry) SetSpliceFdIn(fdIn int32) {
	sqe.spliceFdIn = fdIn
}

type CompletionQueueEvent struct {
	UserData uint64
	Result   int32
	Flags    uint32
}

const IORING_FSYNC_DATASYNC uint32 = 1
const IORING_TIMEOUT_ABS uint32 = 1

// CQ ring flags
const (
	// IORING_CQ_EVENTFD_DISABLED 置位时内核不再向注册的 eventfd 发送通知（5.8）
	IORING_CQ_EVENTFD_DISABLED uint32 = 1 << iota
)

// CQE flags
const (
	IORING_CQE_F_BUFFER        uint32 = 1 << iota // 使用了 provided buffer，buffer id 位于 flags 高 16 位
	IORING_CQE_F_MORE                             // multishot 请求仍然有效，后续还会产生 CQE
	IORING_CQE_F_SOCK_NONEMPTY                    // socket 中仍有可读数据
	IORING_CQE_F_NOTIF                            // SEND_ZC 的通知 CQE
)

const IORING_CQE_BUFFER_SHIFT = 16

// send/recv 的 ioprio 标志
const (
	IORING_RECVSEND_POLL_FIRST  uint16 = 1 << iota // 先 poll 再发起 IO
	IORING_RECV_MULTISHOT                          // multishot recv（6.0+），需配合 IOSQE_BUFFER_SELECT
	IORING_RECVSEND_FIXED_BUF                      // 使用注册缓冲区，buf_index 为其下标
	IORING_SEND_ZC_REPORT_USAGE                    // SEND_ZC：通知 CQE 的 res 报告数据是否被拷贝
	IORING_RECVSEND_BUNDLE                         // 6.10：一次 recv 连续使用 buffer ring 中的多个缓冲区
)

// IORING_NOTIF_USAGE_ZC_COPIED 在 SEND_ZC 通知 CQE 的 res 中置位，表示数据实际被拷贝（未能零拷贝）
const IORING_NOTIF_USAGE_ZC_COPIED int32 = -1 << 31

// accept 的 ioprio 标志
const IORING_ACCEPT_MULTISHOT uint16 = 1 << 0 // multishot accept（5.19+）

// BufReg 对应内核 struct io_uring_buf_reg，用于 IORING_REGISTER_PBUF_RING
type BufReg struct {
	RingAddr    uint64
	RingEntries uint32
	Bgid        uint16
	Flags       uint16
	Resv        [3]uint64
}

// 内核 struct io_uring_buf 的大小（addr u64, len u32, bid u16, resv u16）
const SizeofIOURingBuf = 16
