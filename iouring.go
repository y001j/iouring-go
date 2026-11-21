//go:build linux
// +build linux

package iouring

import (
	"errors"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"

	iouring_syscall "github.com/y001j/iouring-go/syscall"
)

// 一次从 CQ 中批量取出的最大 CQE 数
const reapBatch = 256

// IOURing contains iouring_syscall submission and completion queue.
// It's safe for concurrent use by multiple goroutines.
type IOURing struct {
	params *iouring_syscall.IOURingParams
	fd     int

	sq *SubmissionQueue
	cq *CompletionQueue

	async    bool
	drain    bool
	Flags    uint32
	Features uint32

	// submitLock 保护 SQE 的填充、SQ tail 的发布以及 flushing 标志。
	// io_uring_enter 系统调用在锁外执行（见 submitLocked）。
	submitLock sync.Mutex
	// flushing 为 true 表示有一个 goroutine 正在负责调用 io_uring_enter，
	// 其他 goroutine 只需把 SQE 发布到 SQ 即可返回，由它统一批量提交。
	flushing bool
	// deferring 为 true 表示完成事件 goroutine 正在处理一批 CQE（执行回调），
	// 期间新提交的 SQE 只发布不进入内核，批处理结束后与"等待下一批完成事件"
	// 合并为一次 io_uring_enter。
	deferring bool

	userDataLock sync.RWMutex
	userDatas    map[uint64]*UserData

	fileRegister FileRegister

	running    bool // run goroutine 是否已启动
	fdclosed   bool
	closer     chan struct{}
	closed     chan struct{}
}

// New return a IOURing instance by IOURingOptions
func New(entries uint, opts ...IOURingOption) (*IOURing, error) {
	iour := &IOURing{
		params:    &iouring_syscall.IOURingParams{},
		userDatas: make(map[uint64]*UserData),
		closer:    make(chan struct{}),
		closed:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(iour)
	}

	var err error
	iour.fd, err = iouring_syscall.IOURingSetup(entries, iour.params)
	if err != nil {
		return nil, err
	}

	if err := mmapIOURing(iour); err != nil {
		syscall.Close(iour.fd)
		return nil, err
	}

	iour.fileRegister = &fileRegister{
		iouringFd:    iour.fd,
		sparseIndexs: make(map[int]int),
	}
	iour.Flags = iour.params.Flags
	iour.Features = iour.params.Features

	iour.running = true
	go iour.run()
	return iour, nil
}

// Size iouring submission queue size
func (iour *IOURing) Size() int {
	return int(*iour.sq.entries)
}

// Close IOURing
//
// 关闭后，所有尚未完成的请求都会以 ErrIOURingClosed 结束（Done() 被关闭，
// 并照常投递到各自的 resulter channel）。不能在请求回调中调用 Close。
func (iour *IOURing) Close() error {
	iour.submitLock.Lock()

	select {
	case <-iour.closer:
	default:
		close(iour.closer)
	}

	// 等待正在执行 io_uring_enter 的提交者退出，避免其在 munmap 之后访问 SQ
	for iour.flushing {
		iour.submitLock.Unlock()
		runtime.Gosched()
		iour.submitLock.Lock()
	}

	// 完成事件 goroutine 可能阻塞在 io_uring_enter(GETEVENTS) 中，提交一个 NOP 唤醒它
	if iour.running && !iour.fdclosed {
		iour.wakeLocked()
	}
	iour.submitLock.Unlock()

	if iour.running {
		<-iour.closed
	}

	iour.submitLock.Lock()
	defer iour.submitLock.Unlock()

	// 先关闭 ring fd：内核会取消所有在途请求
	if !iour.fdclosed {
		if err := syscall.Close(iour.fd); err != nil {
			return os.NewSyscallError("close", err)
		}
		iour.fdclosed = true
	}

	if err := munmapIOURing(iour); err != nil {
		return err
	}

	iour.failPending(ErrIOURingClosed)
	return nil
}

// wakeLocked 提交一个 user_data 为 0 的 NOP，用于唤醒等待完成事件的 goroutine
func (iour *IOURing) wakeLocked() {
	for iour.sq.free() == 0 {
		if err := iour.submitLocked(true); err != nil {
			return
		}
		if iour.sq.free() > 0 {
			break
		}
		iour.submitLock.Unlock()
		runtime.Gosched()
		iour.submitLock.Lock()
	}
	sqe := iour.sq.getSQEntry()
	sqe.PrepOperation(iouring_syscall.IORING_OP_NOP, -1, 0, 0, 0)
	sqe.SetUserData(0)
	iour.submitLocked(true)
}

// failPending 让所有尚未完成的请求以 err 结束
func (iour *IOURing) failPending(err error) {
	iour.userDataLock.Lock()
	pending := iour.userDatas
	iour.userDatas = make(map[uint64]*UserData)
	iour.userDataLock.Unlock()

	for _, userData := range pending {
		req := userData.request
		req.fail(err)
		if userData.opcode == iouring_syscall.IORING_OP_LINK_TIMEOUT {
			continue
		}
		if userData.resulter == nil && req.callback != nil {
			req.callback(req) // 直接回调模式
			continue
		}
		deliver(userData.resulter, req)
	}
}

// IsClosed IOURing is closed
func (iour *IOURing) IsClosed() (closed bool) {
	select {
	case <-iour.closer:
		closed = true
	default:
	}
	return
}

// reserve 确保 SQ 中至少有 n 个空闲 SQE，调用方需持有 submitLock。
// SQ 已满时驱动一次提交（或等待当前的提交者），而不是持锁自旋导致死锁。
func (iour *IOURing) reserve(n uint32) error {
	for {
		if iour.IsClosed() {
			return ErrIOURingClosed
		}
		if iour.sq.free() >= n {
			return nil
		}
		if !iour.flushing {
			// 强制提交：即使处于 deferring 阶段也必须进入内核腾出 SQ 空间，否则会死锁
			if err := iour.submitLocked(true); err != nil {
				return err
			}
			if iour.sq.free() >= n {
				return nil
			}
		}
		iour.submitLock.Unlock()
		runtime.Gosched()
		iour.submitLock.Lock()
	}
}

func (iour *IOURing) doRequest(sqe *iouring_syscall.SubmissionQueueEntry, request PrepRequest, ch chan<- Result) (*UserData, error) {
	userData := makeUserData(iour, ch)

	request(sqe, userData)
	userData.setOpcode(sqe.Opcode())

	sqe.SetUserData(userData.id)

	userData.request.fd = int(sqe.Fd())
	if sqe.Fd() >= 0 {
		if index, ok := iour.fileRegister.GetFileIndex(int32(sqe.Fd())); ok {
			sqe.SetFdIndex(int32(index))
		} else if iour.Flags&iouring_syscall.IORING_SETUP_SQPOLL != 0 &&
			iour.Features&iouring_syscall.IORING_FEAT_SQPOLL_NONFIXED == 0 {
			/*
				Before version 5.10 of the Linux kernel, to successful use SQPoll, the application
				must register a set of files to be used for IO through iour.RegisterFiles.
				Failure to do so will result in submitted IO being errored with EBADF

				The presence of this feature can be detected by the IORING_FEAT_SQPOLL_NONFIXED
				In Version 5.10 and later, it is no longer necessary to register files to use SQPoll
			*/

			return nil, ErrUnregisteredFile
		}
	}

	if iour.async {
		sqe.SetFlags(iouring_syscall.IOSQE_FLAGS_ASYNC)
	}
	if iour.drain {
		sqe.SetFlags(iouring_syscall.IOSQE_FLAGS_IO_DRAIN)
	}
	return userData, nil
}

// SubmitRequest by Request function and io result is notified via channel
// return request id, can be used to cancel a request
//
// 并发调用时多个请求会被合并到同一次 io_uring_enter 中提交，
// 因此返回时请求可能已由其他 goroutine 代为提交。
func (iour *IOURing) SubmitRequest(request PrepRequest, ch chan<- Result) (Request, error) {
	iour.submitLock.Lock()
	defer iour.submitLock.Unlock()

	if err := iour.reserve(1); err != nil {
		return nil, err
	}

	sqe := iour.sq.getSQEntry()
	userData, err := iour.doRequest(sqe, request, ch)
	if err != nil {
		iour.sq.fallback(1)
		return nil, err
	}

	iour.userDataLock.Lock()
	iour.userDatas[userData.id] = userData
	iour.userDataLock.Unlock()

	if err = iour.submitLocked(false); err != nil {
		iour.userDataLock.Lock()
		delete(iour.userDatas, userData.id)
		iour.userDataLock.Unlock()
		return nil, err
	}

	return userData.request, nil
}

// SubmitRequests by Request functions and io results are notified via channel
func (iour *IOURing) SubmitRequests(requests []PrepRequest, ch chan<- Result) (RequestSet, error) {
	return iour.submitRequests(requests, ch, 0)
}

// submitRequests 提交一组请求；linkFlags 非 0 时将它们串成链（最后一个不带 link 标志）
func (iour *IOURing) submitRequests(requests []PrepRequest, ch chan<- Result, linkFlags uint8) (RequestSet, error) {
	// TODO(iceber): no length limit
	if len(requests) > int(*iour.sq.entries) {
		return nil, errors.New("too many requests")
	}

	iour.submitLock.Lock()
	defer iour.submitLock.Unlock()

	// 一次性预留全部 SQE，保证同一组（尤其是链式）请求不会被拆到两次提交中
	if err := iour.reserve(uint32(len(requests))); err != nil {
		return nil, err
	}

	var sqeN uint32
	userDatas := make([]*UserData, 0, len(requests))
	for i, request := range requests {
		sqe := iour.sq.getSQEntry()
		sqeN++

		userData, err := iour.doRequest(sqe, request, ch)
		if err != nil {
			iour.sq.fallback(sqeN)
			return nil, err
		}
		userDatas = append(userDatas, userData)

		if linkFlags != 0 {
			sqe.CleanFlags(iouring_syscall.IOSQE_FLAGS_IO_HARDLINK | iouring_syscall.IOSQE_FLAGS_IO_LINK)
			if i < len(requests)-1 {
				sqe.SetFlags(linkFlags)
			}
		}
	}

	// must be located before the lock operation to
	// avoid the compiler's adjustment of the code order.
	// issue: https://github.com/Iceber/iouring-go/issues/8
	rset := newRequestSet(userDatas)

	iour.userDataLock.Lock()
	for _, data := range userDatas {
		iour.userDatas[data.id] = data
	}
	iour.userDataLock.Unlock()

	if err := iour.submitLocked(false); err != nil {
		iour.userDataLock.Lock()
		for _, data := range userDatas {
			delete(iour.userDatas, data.id)
		}
		iour.userDataLock.Unlock()

		return nil, err
	}

	return rset, nil
}

func (iour *IOURing) needEnter(flags *uint32) bool {
	if (iour.Flags & iouring_syscall.IORING_SETUP_SQPOLL) == 0 {
		return true
	}

	if iour.sq.needWakeup() {
		// 修复：此处应传入 io_uring_enter 的 IORING_ENTER_SQ_WAKEUP，
		// 原代码误用了 SQ ring 标志 IORING_SQ_NEED_WAKEUP（值为 1，等同于 GETEVENTS）
		*flags |= iouring_syscall.IORING_ENTER_FLAGS_SQ_WAKEUP
		return true
	}
	return false
}

// submitLocked 将已填充的 SQE 发布到 SQ，并（批量）调用 io_uring_enter。调用方需持有 submitLock。
// force 为 true 时忽略 deferring，立即进入内核。
//
// 合并提交（flat combining）：同一时刻只有一个 goroutine（flushing）在锁外执行 io_uring_enter，
// 其间其他 goroutine 可以继续拿锁填充 SQE、发布后直接返回；flushing 者从系统调用返回后
// 发现还有未提交的 SQE 就继续提交，直到 SQ 清空。这样
//   1. 高并发时一次系统调用可以提交多个请求；
//   2. 系统调用不再持有 submitLock，其他 goroutine 不会被串行阻塞。
func (iour *IOURing) submitLocked(force bool) error {
	iour.sq.flush()
	if iour.flushing {
		return nil
	}
	if iour.deferring && !force {
		// 完成事件 goroutine 正在批处理回调，结束后会统一提交
		return nil
	}

	iour.flushing = true
	defer func() { iour.flushing = false }()

	for {
		pending := iour.sq.flush()
		if pending == 0 {
			return nil
		}

		var flags uint32
		if !iour.needEnter(&flags) {
			return nil
		}
		if (iour.Flags & iouring_syscall.IORING_SETUP_IOPOLL) != 0 {
			flags |= iouring_syscall.IORING_ENTER_FLAGS_GETEVENTS
		}

		iour.submitLock.Unlock()
		_, err := iouring_syscall.IOURingEnter(iour.fd, uint32(pending), 0, flags, nil)
		iour.submitLock.Lock()

		if err != nil {
			if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EBUSY) {
				// EBUSY: CQ 溢出积压，等待 run goroutine 收割完成事件后重试
				iour.submitLock.Unlock()
				runtime.Gosched()
				iour.submitLock.Lock()
				continue
			}
			return err
		}
	}
}

// reap 从 CQ 批量拷贝出至多 len(buf) 个 CQE，并一次性推进 CQ head
func (iour *IOURing) reap(buf []iouring_syscall.CompletionQueueEvent) int {
	cq := iour.cq
	head := *cq.head
	ready := atomicLoad(cq.tail) - head
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
	cq.advance(ready)
	return int(ready)
}

func (iour *IOURing) cqReady() bool {
	return *iour.cq.head != atomicLoad(iour.cq.tail)
}

// enterAndWait 提交 deferring 期间积攒的 SQE；wait 为 true 时在同一次
// io_uring_enter 中等待至少一个完成事件（GETEVENTS, min_complete=1）。
//
// 这里不占用 flushing 角色：阻塞等待期间其他 goroutine 仍可自行提交，
// 内核允许多个线程并发调用 io_uring_enter，SQ 的发布始终在 submitLock 下进行。
func (iour *IOURing) enterAndWait(wait bool) {
	iour.submitLock.Lock()
	iour.deferring = false
	var flags uint32
	toSubmit := uint32(iour.sq.flush())
	if toSubmit > 0 && !iour.needEnter(&flags) {
		toSubmit = 0 // SQPOLL：内核线程负责提交
	}
	iour.submitLock.Unlock()

	var minComplete uint32
	if wait {
		minComplete = 1
		flags |= iouring_syscall.IORING_ENTER_FLAGS_GETEVENTS
	}
	if iour.Flags&iouring_syscall.IORING_SETUP_IOPOLL != 0 || iour.sq.cqOverflow() {
		flags |= iouring_syscall.IORING_ENTER_FLAGS_GETEVENTS
	}
	if toSubmit == 0 && flags == 0 {
		return
	}

	if _, err := iouring_syscall.IOURingEnter(iour.fd, toSubmit, minComplete, flags, nil); err != nil {
		// EINTR（Go 运行时的抢占信号）/EAGAIN/EBUSY/ETIME：回到循环重试即可
		if !errors.Is(err, syscall.EINTR) && !errors.Is(err, syscall.EAGAIN) &&
			!errors.Is(err, syscall.EBUSY) && !errors.Is(err, syscall.ETIME) {
			log.Println("iouring: io_uring_enter error: ", err)
			runtime.Gosched()
		}
	}
}

// run 是完成事件循环：直接阻塞在 io_uring_enter(GETEVENTS) 中等待，
// 不再经过 eventfd -> 全局 epoll goroutine -> channel 的多级转发。
func (iour *IOURing) run() {
	defer close(iour.closed)

	cqes := make([]iouring_syscall.CompletionQueueEvent, reapBatch)
	datas := make([]*UserData, reapBatch)
	for {
		if n := iour.reap(cqes); n > 0 {
			iour.submitLock.Lock()
			iour.deferring = true
			iour.submitLock.Unlock()

			iour.complete(cqes, datas, n)

			if iour.cqReady() {
				// CQ 里还有事件：先把本批回调提交的请求送进内核（不等待），继续收割
				iour.enterAndWait(false)
				continue
			}
		}

		if iour.IsClosed() {
			iour.enterAndWait(false)
			// 收割最后一批已完成的事件，剩余的由 Close 中的 failPending 处理
			for iour.complete(cqes, datas, iour.reap(cqes)) > 0 {
			}
			return
		}

		// 提交本批回调产生的请求，并在同一次系统调用中等待下一批完成事件
		iour.enterAndWait(true)
	}
}

// complete 处理一批 CQE：一次加锁完成 user data 的查找与删除，然后逐个通知
func (iour *IOURing) complete(cqes []iouring_syscall.CompletionQueueEvent, datas []*UserData, n int) int {
	if n == 0 {
		return 0
	}

	iour.userDataLock.Lock()
	for i := 0; i < n; i++ {
		id := cqes[i].UserData
		if id == 0 {
			datas[i] = nil // 内部唤醒用的 NOP
			continue
		}
		datas[i] = iour.userDatas[id]
		if datas[i] != nil {
			delete(iour.userDatas, id)
		}
	}
	iour.userDataLock.Unlock()

	for i := 0; i < n; i++ {
		userData := datas[i]
		datas[i] = nil
		if userData == nil {
			if cqes[i].UserData != 0 {
				log.Println("runComplete: notfound user data ", uintptr(cqes[i].UserData))
			}
			continue
		}

		req := userData.request
		req.complate(&cqes[i])

		// ignore link timeout
		if userData.opcode == iouring_syscall.IORING_OP_LINK_TIMEOUT {
			continue
		}

		// 直接回调模式：带回调且未指定 channel 的请求，在完成事件 goroutine 中直接执行回调，
		// 省去 channel 投递和 goroutine 切换。回调中提交的新请求会被合并到下一次 io_uring_enter。
		// 回调不能阻塞，也不能调用 Close。
		if userData.resulter == nil && req.callback != nil {
			req.callback(req)
			continue
		}
		deliver(userData.resulter, req)
	}
	return n
}

// deliver 把结果投递到用户的 channel。
// channel 已满时转交给新的 goroutine 投递，避免一个消费慢的 channel
// 阻塞完成事件 goroutine，进而拖住这个 ring 上所有其他请求（队头阻塞）。
// 代价是 channel 满时同一 channel 内结果的顺序可能与完成顺序不一致。
func deliver(ch chan<- Result, req *request) {
	if ch == nil {
		return
	}
	select {
	case ch <- req:
	default:
		go func() { ch <- req }()
	}
}

// Result submit cancel request
func (iour *IOURing) submitCancel(id uint64) (Request, error) {
	if iour == nil {
		return nil, ErrRequestCompleted
	}

	return iour.SubmitRequest(cancelRequest(id), nil)
}

func cancelRequest(id uint64) PrepRequest {
	return func(sqe *iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = cancelResolver
		sqe.PrepOperation(iouring_syscall.IORING_OP_ASYNC_CANCEL, -1, id, 0, 0)
	}
}
