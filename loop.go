//go:build linux
// +build linux

package iouring

import (
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

// Loop 是一个单线程 io_uring 事件循环，面向高性能网络 IO。
//
// 与 IOURing（多 goroutine 并发提交、结果经 channel 投递）不同，Loop 的模型是：
//   - 每个 Loop 独占一个 io_uring 实例和一个锁定的 OS 线程（runtime.LockOSThread）；
//   - 所有提交和回调都发生在该线程上，提交路径无锁、无内存分配；
//   - 每轮循环只调用一次 io_uring_enter：提交本轮产生的所有 SQE 并等待下一批完成事件；
//   - 内核支持时启用 IORING_SETUP_SINGLE_ISSUER | DEFER_TASKRUN（6.1+），
//     否则退化为 COOP_TASKRUN（5.19+），再否则不带额外标志；
//   - 支持 multishot recv/accept 与 provided buffer ring（BufferRing）。
//
// 线程模型约束：
//   - Recv/Send/RecvMultishot 等提交方法以及 BufferRing 的方法只能在 Loop 线程中调用，
//     即在 Handler 回调或 Post 投递的函数中调用；
//   - 其他 goroutine 只能调用 Post、Stop、Close；
//   - Handler 不能阻塞。
//   - 传给 Loop 的 fd 应为阻塞模式（未设置 O_NONBLOCK），由 io_uring 负责异步等待。
type Loop struct {
	fd         int
	params     iouring_syscall.IOURingParams
	sq         *SubmissionQueue
	cq         *CompletionQueue
	SetupFlags uint32 // 实际生效的 io_uring setup 标志

	handlers  []Handler // 槽位 -> 回调；user_data = 槽位 + 1
	keeps     [][]byte  // 槽位 -> 请求期间需要保持存活的缓冲区
	freeSlots []uint32

	bufRings []*BufferRing

	fixedFree []int32 // 注册文件表的空闲下标

	postMu   sync.Mutex
	posted   []func()
	running  []func()
	postN    int32 // 待执行的 posted 数量（原子）
	sleeping int32 // Loop 是否（即将）阻塞等待（原子）
	efd      int
	efdBuf   [8]byte
	efdSlot  uint64

	stopping int32
	done     chan struct{}
}

// Handler 处理一个完成事件：res 为 CQE 的结果（负数为 -errno），flags 为 CQE 标志。
// 对 multishot 请求，flags 中带 IORING_CQE_F_MORE 表示后续还会有 CQE。
type Handler func(res int32, flags uint32)

// LoopConfig 配置 Loop
type LoopConfig struct {
	Entries     uint32             // SQ 大小，默认 4096；CQ 为其 4 倍
	BufferRings []BufferRingConfig // 需要注册的 provided buffer ring，组 ID 依次为 0,1,2...
	// DisableSingleIssuer 禁用 SINGLE_ISSUER/DEFER_TASKRUN（用于对比测试）
	DisableSingleIssuer bool
	// FixedFiles 注册的稀疏文件表大小（Linux 5.19+），0 表示不启用。
	// 启用后可用 RegisterFile 把 fd 注册进表，再以 FixedFile(idx) 作为 fd 提交请求，
	// 内核省去每次请求按 fd 查找文件、增减引用计数的开销。
	FixedFiles int
}

// fixedFileBit 标记"fd"参数实际是注册文件表中的下标
const fixedFileBit = 1 << 30

// FixedFile 把注册文件表下标转换为可传给 Loop 提交方法的 fd 参数
func FixedFile(idx int) int { return idx | fixedFileBit }

// BufferRingConfig 配置一个 provided buffer ring
type BufferRingConfig struct {
	Count int // 缓冲区数量，必须为 2 的幂，最大 32768
	Size  int // 每个缓冲区的字节数
}

var ErrLoopClosed = errors.New("iouring loop closed")

// NewLoop 创建并启动一个 Loop。io_uring 实例在 Loop 自己锁定的线程中创建，
// 以满足 SINGLE_ISSUER 的要求。
func NewLoop(cfg LoopConfig) (*Loop, error) {
	if cfg.Entries == 0 {
		cfg.Entries = 4096
	}
	l := &Loop{fd: -1, efd: -1, done: make(chan struct{})}
	ready := make(chan error, 1)
	go func() {
		runtime.LockOSThread() // 不解锁：该线程专属于这个 Loop
		if err := l.setup(cfg); err != nil {
			l.cleanup()
			ready <- err
			close(l.done)
			return
		}
		ready <- nil
		l.run()
	}()
	if err := <-ready; err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Loop) setup(cfg LoopConfig) error {
	attempts := []uint32{
		iouring_syscall.IORING_SETUP_SINGLE_ISSUER | iouring_syscall.IORING_SETUP_DEFER_TASKRUN,
		iouring_syscall.IORING_SETUP_COOP_TASKRUN,
		0,
	}
	if cfg.DisableSingleIssuer {
		attempts = attempts[1:]
	}
	var err error
	for _, flags := range attempts {
		l.params = iouring_syscall.IOURingParams{
			Flags:     flags | iouring_syscall.IORING_SETUP_CQSIZE,
			CQEntries: cfg.Entries * 4,
		}
		l.fd, err = iouring_syscall.IOURingSetup(uint(cfg.Entries), &l.params)
		if err == nil {
			l.SetupFlags = l.params.Flags
			break
		}
		if !errors.Is(err, syscall.EINVAL) {
			return err
		}
	}
	if err != nil {
		return err
	}

	if l.sq, l.cq, err = mmapRings(l.fd, &l.params); err != nil {
		return err
	}

	if cfg.FixedFiles > 0 {
		reg := iouring_syscall.RsrcRegister{Nr: uint32(cfg.FixedFiles), Flags: iouring_syscall.IORING_RSRC_REGISTER_SPARSE}
		if err := iouring_syscall.IOURingRegister(l.fd, iouring_syscall.IORING_REGISTER_FILES2,
			unsafe.Pointer(&reg), uint32(unsafe.Sizeof(reg))); err != nil {
			return err
		}
		l.fixedFree = make([]int32, cfg.FixedFiles)
		for i := range l.fixedFree {
			l.fixedFree[i] = int32(cfg.FixedFiles - 1 - i)
		}
	}

	for i, rc := range cfg.BufferRings {
		br, err := newBufferRing(l, uint16(i), rc.Count, rc.Size)
		if err != nil {
			return err
		}
		l.bufRings = append(l.bufRings, br)
	}

	// eventfd 用于其他 goroutine 唤醒阻塞在 io_uring_enter 中的 Loop（见 Post）。
	// 使用阻塞模式，读取由 io_uring 异步完成。
	if l.efd, err = unix.Eventfd(0, unix.EFD_CLOEXEC); err != nil {
		return os.NewSyscallError("eventfd", err)
	}
	l.armEventfd()
	return nil
}

func (l *Loop) armEventfd() {
	h := func(res int32, flags uint32) {
		if atomic.LoadInt32(&l.stopping) == 0 {
			l.armEventfd()
		}
	}
	l.efdSlot = l.Read(l.efd, l.efdBuf[:], h)
}

// BufferRing 返回第 i 个（组 ID 为 i）provided buffer ring
func (l *Loop) BufferRing(i int) *BufferRing { return l.bufRings[i] }

// Post 把 fn 投递到 Loop 线程执行，可在任意 goroutine 中调用
func (l *Loop) Post(fn func()) error {
	l.postMu.Lock()
	if atomic.LoadInt32(&l.stopping) != 0 {
		l.postMu.Unlock()
		return ErrLoopClosed
	}
	l.posted = append(l.posted, fn)
	atomic.AddInt32(&l.postN, 1)
	l.wakeLocked()
	l.postMu.Unlock()
	return nil
}

// wakeLocked 唤醒阻塞中的 Loop，调用方需持有 postMu。
// 仅在 Loop 已经（或即将）阻塞时写 eventfd，繁忙时不产生额外系统调用。
// 在锁内写 eventfd，保证不会与 cleanup 关闭 eventfd 竞争（否则可能写入被复用的 fd）。
func (l *Loop) wakeLocked() {
	if l.efd >= 0 && atomic.CompareAndSwapInt32(&l.sleeping, 1, 0) {
		var one = [8]byte{1}
		unix.Write(l.efd, one[:])
	}
}

// Stop 通知 Loop 退出，不等待；可在 Loop 线程内调用
func (l *Loop) Stop() {
	l.postMu.Lock()
	atomic.StoreInt32(&l.stopping, 1)
	l.wakeLocked()
	l.postMu.Unlock()
}

// Close 停止 Loop 并等待其退出、释放资源。不能在 Loop 线程内调用（应使用 Stop）。
// 尚未完成的请求会被内核取消，其 Handler 不再被调用。
func (l *Loop) Close() error {
	l.Stop()
	<-l.done
	return nil
}

// Done 在 Loop 退出后关闭
func (l *Loop) Done() <-chan struct{} { return l.done }

func (l *Loop) run() {
	defer close(l.done)
	defer l.cleanup()

	cqes := make([]iouring_syscall.CompletionQueueEvent, 512)
	for {
		l.runPosted()
		if atomic.LoadInt32(&l.stopping) != 0 {
			return
		}

		var wait uint32
		if atomic.LoadInt32(&l.postN) == 0 && !l.cqReady() {
			// 先声明即将睡眠，再检查一次 posted，避免与 Post 竞争导致丢失唤醒
			atomic.StoreInt32(&l.sleeping, 1)
			if atomic.LoadInt32(&l.postN) == 0 && atomic.LoadInt32(&l.stopping) == 0 {
				wait = 1
			} else {
				atomic.StoreInt32(&l.sleeping, 0)
			}
		}
		l.enter(wait)
		atomic.StoreInt32(&l.sleeping, 0)

		for {
			n := l.reap(cqes)
			if n == 0 {
				break
			}
			l.dispatch(cqes[:n])
		}
	}
}

func (l *Loop) runPosted() {
	if atomic.LoadInt32(&l.postN) == 0 {
		return
	}
	l.postMu.Lock()
	l.posted, l.running = l.running[:0], l.posted
	atomic.StoreInt32(&l.postN, 0)
	l.postMu.Unlock()
	for i, fn := range l.running {
		if fn != nil {
			fn()
		}
		l.running[i] = nil
	}
}

func (l *Loop) cqReady() bool {
	return *l.cq.head != atomic.LoadUint32(l.cq.tail)
}

// enter 提交所有待提交的 SQE，并等待至少 wait 个完成事件
func (l *Loop) enter(wait uint32) {
	for _, br := range l.bufRings {
		br.publish()
	}
	toSubmit := uint32(l.sq.flush())
	// DEFER_TASKRUN 模式下，只有带 GETEVENTS 进入内核时才会处理完成事件，因此总是带上
	flags := iouring_syscall.IORING_ENTER_FLAGS_GETEVENTS
	if toSubmit == 0 && wait == 0 && l.SetupFlags&iouring_syscall.IORING_SETUP_DEFER_TASKRUN == 0 {
		if l.SetupFlags&iouring_syscall.IORING_SETUP_COOP_TASKRUN == 0 ||
			atomic.LoadUint32(l.sq.flags)&iouring_syscall.IORING_SQ_TASKRUN == 0 {
			return
		}
	}
	// 错误（EINTR 等）直接忽略：未提交的 SQE 仍在 SQ 中，下一轮会重新提交
	iouring_syscall.IOURingEnter(l.fd, toSubmit, wait, flags, nil)
}

func (l *Loop) reap(buf []iouring_syscall.CompletionQueueEvent) int {
	cq := l.cq
	head := *cq.head
	ready := atomic.LoadUint32(cq.tail) - head
	if ready == 0 {
		return 0
	}
	if ready > uint32(len(buf)) {
		ready = uint32(len(buf))
	}
	mask := *cq.mask
	for i := uint32(0); i < ready; i++ {
		buf[i] = cq.cqes[(head+i)&mask]
	}
	atomic.StoreUint32(cq.head, head+ready)
	return int(ready)
}

func (l *Loop) dispatch(cqes []iouring_syscall.CompletionQueueEvent) {
	for i := range cqes {
		c := &cqes[i]
		if c.UserData == 0 {
			continue
		}
		slot := uint32(c.UserData - 1)
		h := l.handlers[slot]
		if c.Flags&iouring_syscall.IORING_CQE_F_MORE == 0 {
			// 最后一个 CQE：先释放槽位，Handler 中提交的新请求可以复用它
			l.handlers[slot] = nil
			l.keeps[slot] = nil
			l.freeSlots = append(l.freeSlots, slot)
		}
		if h != nil {
			h(c.Result, c.Flags)
		}
	}
}

func (l *Loop) cleanup() {
	if l.fd >= 0 {
		syscall.Close(l.fd) // 内核取消所有在途请求
		l.fd = -1
	}
	munmapRings(l.sq, l.cq)
	for _, br := range l.bufRings {
		br.free()
	}
	l.postMu.Lock()
	if l.efd >= 0 {
		syscall.Close(l.efd)
		l.efd = -1
	}
	l.postMu.Unlock()
}

// ---- 注册文件（仅限 Loop 线程）----

// RegisterFile 把 fd 注册到注册文件表，返回下标；之后以 FixedFile(idx) 作为 fd 提交请求。
// fd 本身仍由调用方持有和关闭（需先 UnregisterFile）。
func (l *Loop) RegisterFile(fd int) (int, error) {
	n := len(l.fixedFree)
	if n == 0 {
		return -1, errors.New("iouring: fixed file table full")
	}
	idx := l.fixedFree[n-1]
	if err := l.updateFile(idx, int32(fd)); err != nil {
		return -1, err
	}
	l.fixedFree = l.fixedFree[:n-1]
	return int(idx), nil
}

// UnregisterFile 从注册文件表中移除下标 idx（不会关闭原 fd）
func (l *Loop) UnregisterFile(idx int) error {
	if err := l.updateFile(int32(idx), -1); err != nil {
		return err
	}
	l.fixedFree = append(l.fixedFree, int32(idx))
	return nil
}

func (l *Loop) updateFile(idx, fd int32) error {
	fds := [1]int32{fd}
	up := iouring_syscall.IOURingFilesUpdate{Offset: uint32(idx), Fds: &fds[0]}
	return iouring_syscall.IOURingRegister(l.fd, iouring_syscall.IORING_REGISTER_FILES_UPDATE, unsafe.Pointer(&up), 1)
}

// setFd 设置 SQE 的 fd；FixedFile 下标则设置 IOSQE_FIXED_FILE
func setFd(sqe *iouring_syscall.SubmissionQueueEntry, fd int) {
	if fd >= 0 && fd&fixedFileBit != 0 {
		sqe.SetFdIndex(int32(fd &^ fixedFileBit))
	}
}

// ---- 提交（仅限 Loop 线程）----

func (l *Loop) alloc(h Handler, keep []byte) uint64 {
	var slot uint32
	if n := len(l.freeSlots); n > 0 {
		slot = l.freeSlots[n-1]
		l.freeSlots = l.freeSlots[:n-1]
		l.handlers[slot] = h
		l.keeps[slot] = keep
	} else {
		slot = uint32(len(l.handlers))
		l.handlers = append(l.handlers, h)
		l.keeps = append(l.keeps, keep)
	}
	return uint64(slot) + 1
}

func (l *Loop) getSQE() *iouring_syscall.SubmissionQueueEntry {
	for {
		if sqe := l.sq.getSQEntry(); sqe != nil {
			return sqe
		}
		l.enter(0) // SQ 已满：立即提交
	}
}

func bufAddr(b []byte) uint64 {
	if len(b) == 0 {
		return uint64(uintptr(unsafe.Pointer(&_zero)))
	}
	return uint64(uintptr(unsafe.Pointer(&b[0])))
}

// Prep 提交一个自定义请求：prep 负责填充 SQE（不要设置 user_data），返回请求的 user_data。
// keep 中的缓冲区在请求完成前保持存活。
func (l *Loop) Prep(prep func(sqe *iouring_syscall.SubmissionQueueEntry), keep []byte, h Handler) uint64 {
	sqe := l.getSQE()
	prep(sqe)
	ud := l.alloc(h, keep)
	sqe.SetUserData(ud)
	return ud
}

// Nop 提交一个 NOP 请求
func (l *Loop) Nop(h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_NOP, -1, 0, 0, 0)
	ud := l.alloc(h, nil)
	sqe.SetUserData(ud)
	return ud
}

// Read 从 fd 读取到 b（偏移量使用文件当前位置）
func (l *Loop) Read(fd int, b []byte, h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_READ, int32(fd), bufAddr(b), uint32(len(b)), ^uint64(0))
	setFd(sqe, fd)
	ud := l.alloc(h, b)
	sqe.SetUserData(ud)
	return ud
}

// Recv 从 socket 接收到 b
func (l *Loop) Recv(fd int, b []byte, flags int, h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_RECV, int32(fd), bufAddr(b), uint32(len(b)), 0)
	setFd(sqe, fd)
	sqe.SetOpFlags(uint32(flags))
	ud := l.alloc(h, b)
	sqe.SetUserData(ud)
	return ud
}

// Send 发送 b；可能发生短写，res 为实际发送的字节数
func (l *Loop) Send(fd int, b []byte, flags int, h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_SEND, int32(fd), bufAddr(b), uint32(len(b)), 0)
	setFd(sqe, fd)
	sqe.SetOpFlags(uint32(flags))
	ud := l.alloc(h, b)
	sqe.SetUserData(ud)
	return ud
}

// RecvMultishot 提交 multishot recv（Linux 6.0+）：一次提交持续接收，
// 每次收到数据产生一个 CQE，数据位于内核从 br 中挑选的缓冲区里：
//
//	bid, data, ok := br.Get(res, flags)  // 处理完 data 后调用 br.Recycle(bid)
//
// CQE 不带 IORING_CQE_F_MORE 表示该 multishot 已终止（EOF、出错或 -ENOBUFS），
// 需要时重新提交。缓冲区耗尽时内核返回 -ENOBUFS。
func (l *Loop) RecvMultishot(fd int, br *BufferRing, flags int, h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_RECV, int32(fd), 0, 0, 0)
	setFd(sqe, fd)
	sqe.SetOpFlags(uint32(flags))
	sqe.SetIoprio(iouring_syscall.IORING_RECV_MULTISHOT)
	sqe.SetFlags(iouring_syscall.IOSQE_FLAGS_BUFFER_SELECT)
	sqe.SetBufGroup(br.bgid)
	ud := l.alloc(h, nil)
	sqe.SetUserData(ud)
	return ud
}

// AcceptMultishot 提交 multishot accept（Linux 5.19+）：每接受一个连接产生一个 CQE，res 为新连接 fd
func (l *Loop) AcceptMultishot(fd int, flags int, h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_ACCEPT, int32(fd), 0, 0, 0)
	sqe.SetOpFlags(uint32(flags))
	sqe.SetIoprio(iouring_syscall.IORING_ACCEPT_MULTISHOT)
	ud := l.alloc(h, nil)
	sqe.SetUserData(ud)
	return ud
}

// CloseFd 通过 io_uring 关闭 fd
func (l *Loop) CloseFd(fd int, h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_CLOSE, int32(fd), 0, 0, 0)
	ud := l.alloc(h, nil)
	sqe.SetUserData(ud)
	return ud
}

// Shutdown 通过 io_uring 执行 shutdown(fd, how)（Linux 5.11+）
func (l *Loop) Shutdown(fd int, how int, h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_SHUTDOWN, int32(fd), 0, uint32(how), 0)
	setFd(sqe, fd)
	ud := l.alloc(h, nil)
	sqe.SetUserData(ud)
	return ud
}

// Cancel 取消 user_data 为 target 的请求（包括 multishot 请求）
func (l *Loop) Cancel(target uint64, h Handler) uint64 {
	sqe := l.getSQE()
	sqe.PrepOperation(iouring_syscall.IORING_OP_ASYNC_CANCEL, -1, target, 0, 0)
	ud := l.alloc(h, nil)
	sqe.SetUserData(ud)
	return ud
}

// ---- provided buffer ring ----

// BufferRing 是注册到内核的一组缓冲区（IORING_REGISTER_PBUF_RING，Linux 5.19+）。
// 内核在接收数据时从中挑选缓冲区，用户处理完后通过 Recycle 归还。
// 所有方法只能在 Loop 线程中调用。
type BufferRing struct {
	l       *Loop
	bgid    uint16
	mask    uint16
	size    int
	ringMem []byte // count 个 struct io_uring_buf，页对齐
	bufMem  []byte // count * size 字节的缓冲区
	tail    uint16
	dirty   bool
}

func newBufferRing(l *Loop, bgid uint16, count, size int) (*BufferRing, error) {
	if count <= 0 || count > 32768 || count&(count-1) != 0 || size <= 0 {
		return nil, errors.New("iouring: invalid buffer ring config")
	}
	br := &BufferRing{l: l, bgid: bgid, mask: uint16(count - 1), size: size}
	var err error
	prot, flags := unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_POPULATE
	if br.ringMem, err = unix.Mmap(-1, 0, count*iouring_syscall.SizeofIOURingBuf, prot, flags); err != nil {
		return nil, os.NewSyscallError("mmap", err)
	}
	if br.bufMem, err = unix.Mmap(-1, 0, count*size, prot, flags); err != nil {
		br.free()
		return nil, os.NewSyscallError("mmap", err)
	}

	reg := iouring_syscall.BufReg{
		RingAddr:    uint64(uintptr(unsafe.Pointer(&br.ringMem[0]))),
		RingEntries: uint32(count),
		Bgid:        bgid,
	}
	if err := iouring_syscall.IOURingRegister(l.fd, iouring_syscall.IORING_REGISTER_PBUF_RING, unsafe.Pointer(&reg), 1); err != nil {
		br.free()
		return nil, err
	}
	for i := 0; i < count; i++ {
		br.put(uint16(i))
	}
	br.publish()
	return br, nil
}

// Size 返回单个缓冲区大小
func (br *BufferRing) Size() int { return br.size }

// Buf 返回 buffer id 对应的完整缓冲区
func (br *BufferRing) Buf(bid uint16) []byte {
	off := int(bid) * br.size
	return br.bufMem[off : off+br.size : off+br.size]
}

// Get 从 multishot recv 的 CQE 中取出数据所在的缓冲区。
// ok 为 false 表示该 CQE 没有占用缓冲区（出错或 EOF）。
func (br *BufferRing) Get(res int32, flags uint32) (bid uint16, data []byte, ok bool) {
	if flags&iouring_syscall.IORING_CQE_F_BUFFER == 0 {
		return 0, nil, false
	}
	bid = uint16(flags >> iouring_syscall.IORING_CQE_BUFFER_SHIFT)
	n := int(res)
	if n < 0 {
		n = 0
	}
	return bid, br.Buf(bid)[:n], true
}

// Recycle 归还缓冲区。归还操作在下一次 io_uring_enter 前批量发布给内核。
func (br *BufferRing) Recycle(bid uint16) {
	br.put(bid)
}

func (br *BufferRing) put(bid uint16) {
	e := br.ringMem[int(br.tail&br.mask)*iouring_syscall.SizeofIOURingBuf:]
	*(*uint64)(unsafe.Pointer(&e[0])) = uint64(uintptr(unsafe.Pointer(&br.bufMem[int(bid)*br.size])))
	*(*uint32)(unsafe.Pointer(&e[8])) = uint32(br.size)
	*(*uint16)(unsafe.Pointer(&e[12])) = bid
	br.tail++
	br.dirty = true
}

// publish 把本地 tail 发布给内核。
// ring 的 tail 是第一个 io_uring_buf 的 resv 字段（偏移 14，u16），与 entry0.bid（偏移 12）
// 同处偏移 12 的 32 位字中（小端）；用 32 位原子写保证 release 语义，同时保留 entry0.bid。
func (br *BufferRing) publish() {
	if !br.dirty {
		return
	}
	br.dirty = false
	w := (*uint32)(unsafe.Pointer(&br.ringMem[12]))
	atomic.StoreUint32(w, atomic.LoadUint32(w)&0xFFFF|uint32(br.tail)<<16)
}

func (br *BufferRing) free() {
	if br.ringMem != nil {
		unix.Munmap(br.ringMem)
		br.ringMem = nil
	}
	if br.bufMem != nil {
		unix.Munmap(br.bufMem)
		br.bufMem = nil
	}
}
