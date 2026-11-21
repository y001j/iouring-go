# What is io_uring
[io_uring](http://kernel.dk/io_uring.pdf) 

[io_uring-wahtsnew](https://kernel.dk/io_uring-whatsnew.pdf) 

[LWN io_uring](https://lwn.net/Kernel/Index/#io_uring) 

[Lord of the io_uring](https://unixism.net/loti/)

[【译】高性能异步 IO —— io_uring (Effecient IO with io_uring)](http://icebergu.com/archives/linux-iouring)

[Go 与异步 IO - io_uring 的思考](http://icebergu.com/archives/go-iouring)

# Features
- [x] register a file set for io_uring instance
- [x] support file IO
- [x] support socket IO
- [x] support IO timeout
- [x] link request
- [x] set timer
- [x] add request extra info, could get it from the result
- [ ] set logger
- [ ] register buffers and IO with buffers
- [ ] support SQPoll 
- [x] batched submission (concurrent submitters share one `io_uring_enter`)
- [x] direct callback mode (no channel hop)
- [x] high-performance single-thread event loop `Loop`: multishot recv/accept, provided buffer ring, fixed files, `SINGLE_ISSUER`/`DEFER_TASKRUN`

# OS Requirements
* Linux Kernel >= 5.6
* `Loop`: multishot recv needs >= 6.0, provided buffer ring / multishot accept / sparse fixed files need >= 5.19,
  `SINGLE_ISSUER`+`DEFER_TASKRUN` are used automatically on >= 6.1 (falls back to `COOP_TASKRUN`, then no flags)
* Go >= 1.18

# Two programming models

## `IOURing`: goroutine-friendly

Any goroutine can submit; results are delivered through a channel (`SubmitRequest(prep, ch)`).
Concurrent submissions are batched into a single `io_uring_enter`.

**Direct callback mode**: if a request has a callback (`prep.WithCallback(cb)`) and the channel is `nil`,
the callback runs directly in the ring's completion goroutine. Requests submitted from inside callbacks are
batched together with waiting for the next completions in one system call. Callbacks must not block and must
not call `Close`.

## `Loop`: maximum performance for network IO

Each `Loop` owns one io_uring instance and one locked OS thread. All submissions and callbacks happen on that
thread (lock-free, allocation-free submission path) and every loop iteration issues exactly one
`io_uring_enter`. Other goroutines hand work to the loop with `Post`.

```golang
l, _ := iouring.NewLoop(iouring.LoopConfig{
        BufferRings: []iouring.BufferRingConfig{{Count: 1024, Size: 16 << 10}},
})
defer l.Close()

l.Post(func() { // runs on the loop thread
        br := l.BufferRing(0)
        var onRecv iouring.Handler
        onRecv = func(res int32, flags uint32) {
                if bid, data, ok := br.Get(res, flags); ok {
                        // data is valid until br.Recycle(bid)
                        l.Send(fd, data, syscall.MSG_NOSIGNAL, func(int32, uint32) { br.Recycle(bid) })
                }
                if flags&iouring_syscall.IORING_CQE_F_MORE == 0 { /* multishot ended: EOF, error or -ENOBUFS */ }
        }
        l.RecvMultishot(fd, br, 0, onRecv)
})
```

See `bench/loop.go` for a complete TCP forwarding proxy (ordering, short writes, `-ENOBUFS`, connection teardown).

### Busy polling: `LoopConfig.SpinTime`

Before blocking in `io_uring_enter`, the loop can busy-poll for completions for up to `SpinTime`.
Completions that arrive during the spin are handled without a sleep/wake-up cycle, trading CPU for latency.
This pays off where wake-ups are expensive (VMs, low-concurrency request/response); 10–50µs is a good start.

### Zero-copy send: `SendZC`, registered buffers

- `SendZC(fd, b, flags, zcFlags, h)` uses `IORING_OP_SEND_ZC` (6.0+): the kernel transmits directly from the pages of `b`.
  The handler is called **twice**: the result CQE (`res` = bytes sent; `IORING_CQE_F_MORE` means a notification follows),
  then the notification CQE (`IORING_CQE_F_NOTIF`). Only after the notification may `b` be modified or reused.
- `LoopConfig.FixedBuffers` registers user memory with `IORING_REGISTER_BUFFERS` once (pages pinned and mapped up front);
  `SendZCFixed(fd, b, l.FixedBufferIndex(i), ...)` then skips the per-request page pinning.
- `BufferRingConfig.Fixed: true` registers the provided buffer ring's memory as a fixed buffer too. That one region is shared
  by user space and the kernel: the kernel receives into it (provided buffers), and `SendZCFixed(fd, data, uint16(br.FixedIndex()), ...)`
  transmits from it in place. Forwarding or echo then needs no user-space copy, and on a real NIC no kernel copy on transmit either.
- `IORING_SEND_ZC_REPORT_USAGE` makes the notification report whether data was copied (`res & IORING_NOTIF_USAGE_ZC_COPIED`).

Caveats (from the kernel docs): zero copy only pays off above roughly 10KB per send; **traffic looped to a local socket
is always copied** (no gain on loopback or between containers on one host); registered buffers count against
`RLIMIT_MEMLOCK` (`ulimit -l`), and `NewLoop` returns `ENOMEM` when it is too low.
Zero-copy *receive* (`IORING_OP_RECV_ZC`, zcrx) needs NIC support (header split, flow steering, a dedicated RX queue)
and is not implemented here.

# Testing and benchmarks (Docker, works on macOS)

```
docker build -f Dockerfile.test -t iouring-go-test .
docker run --rm --security-opt seccomp=unconfined iouring-go-test                 # tests
docker run --rm --security-opt seccomp=unconfined --ulimit nofile=65536:65536 \
        iouring-go-test bench_ab.sh                                               # forwarding benchmark
```
`seccomp=unconfined` is required because Docker's default seccomp profile blocks io_uring.


# 性能测试（TCP 转发）

## 测试方法

客户端 → 转发代理 → 后端 echo 服务，客户端在 C 个长连接上 ping-pong（写 N 字节、读回 N 字节）。
代理、后端、压测客户端分别绑定 CPU 0-3 / 4-7 / 8-11（各 `GOMAXPROCS=4`），走本机回环。
所有代理同时启动、每个场景下轮流压测，重复 3 轮取中位数（`scripts/bench_ab.sh`），以抵消环境波动。

| 代理 | 实现 |
|---|---|
| std | Go 标准库 `net` + `io.CopyBuffer`（底层 epoll netpoller） |
| epoll | 手写 epoll 事件循环，4 个 loop 各绑定一个 OS 线程 |
| iouring | 本库 `IOURing`，结果经 channel 投递给 4 个 worker goroutine |
| iouring-direct | 本库 `IOURing` 直接回调模式（channel 为 nil） |
| iouring-loop | 本库 `Loop`：4 个 Loop + multishot recv + provided buffer ring |

环境：Docker Desktop（macOS, Apple Silicon）Linux VM，内核 6.10.14，12 核，Go 1.22。原始数据见 `bench/RESULTS.txt`。

## 结果

128 字节（QPS）：

| 连接数 | std | epoll | iouring | iouring-direct | **iouring-loop** |
|---|---|---|---|---|---|
| 1 | 2,613 | 3,555 | 1,802 | 3,516 | **3,846** |
| 64 | 60,959 | 77,145 | 33,693 | 80,552 | **96,002** |
| 512 | 207,750 | 238,182 | 73,735 | 161,638 | **245,488** |

16KB（吞吐）：

| 连接数 | std | epoll | iouring | iouring-direct | iouring-loop |
|---|---|---|---|---|---|
| 1 | 41.7 MB/s | 42.8 MB/s | 16.9 MB/s | 39.2 MB/s | 35.6 MB/s |
| 64 | 768 MB/s | 723 MB/s | 250 MB/s | **868 MB/s** | 763 MB/s |
| 512 | 1,906 MB/s | **2,010 MB/s** | 759 MB/s | 1,711 MB/s | 1,959 MB/s |

p99 延迟：c=64/128B 时 iouring-loop 为 2.1ms（epoll 4.6ms）；c=512/16KB 时为 12.0ms（epoll 18.0ms）。

优化过程（c=512, 128B）：

| 阶段 | QPS | 相对 std |
|---|---|---|
| 优化前 | 54k | 0.29x |
| 批量提交 | 104k | 0.5x |
| 直接回调 + 完成路径改造 | 162k | 0.78x |
| Loop + multishot recv + provided buffer | 245k | 1.18x |

说明：
- 优化后 iouring-loop 的 CPU profile 中约 94% 时间在内核（TCP 收发），用户态开销已很小。
- c=1 场景波动较大，仅供参考；回环网络下的结果不代表真实网卡，部署前建议在目标机器上运行 `bench_ab.sh`。
- `LoopConfig.FixedFiles`（注册文件）在本测试中未带来收益（c=512/128B 时反而下降），默认不启用。

# 更新记录

## 正确性修复
- `Send`/`Recv` 未设置结果解析，`Err()` 恒为 nil、`ReturnInt()` 恒报错
- `New()` 初始化失败时 `Close()` 永久阻塞
- `Close()` 后在途请求永远不完成，现以 `ErrIOURingClosed` 结束
- 单个消费慢的 channel 阻塞完成 goroutine，拖住所有请求（队头阻塞）
- `Cancel()` 与请求完成之间的数据竞争
- `SubmissionQueueEntry.CleanFlags` 使用异或：链式请求最后一个 SQE 被错误地带上 `IO_LINK`，与后续无关请求串成链
- SQPOLL 唤醒误用 `IORING_SQ_NEED_WAKEUP`（应为 `IORING_ENTER_SQ_WAKEUP`）
- `go:linkname` 引用 `syscall` 内部符号，Go 1.23+ 无法链接
- import 路径统一为 `github.com/y001j/iouring-go`；ring 改用 `unix.Mmap` + `unsafe.Slice`（`go vet` 无警告），修正 CQ 大小计算
- 对未完成的请求调用 `Callback()` 返回 `ErrRequestNotCompleted`

## 性能改进
- 合并提交：并发提交的请求合并为一次 `io_uring_enter`，系统调用不再持锁
- 完成路径：去掉 eventfd 与全局 epoll goroutine，完成 goroutine 直接阻塞在 `io_uring_enter(GETEVENTS)` 中；批量收割 CQE
- 直接回调模式：回调中提交的请求与等待下一批完成事件合并为一次系统调用
- 新增 `Loop` API：单线程事件循环、multishot recv/accept、provided buffer ring、`SINGLE_ISSUER`/`DEFER_TASKRUN`、注册文件
- 新增 `WithCoopTaskRun()` 选项

## 新增（零拷贝与轮询）
- `LoopConfig.SpinTime`：阻塞前忙轮询，降低唤醒延迟
- `Loop.SendZC` / `SendZCFixed`：IORING_OP_SEND_ZC 零拷贝发送，处理结果 CQE 与通知 CQE
- `LoopConfig.FixedBuffers`、`BufferRingConfig.Fixed`：注册固定缓冲区；buffer ring 注册后接收与零拷贝发送共用同一块内存
- 补充 opcode 常量（至 `IORING_OP_SENDMSG_ZC`）与 `IORING_SEND_ZC_REPORT_USAGE`、`IORING_NOTIF_USAGE_ZC_COPIED`

## 行为变化
- 最低 Go 版本 1.18
- `SubmitRequest` 返回时，请求可能已由其他 goroutine 代为提交
- channel 已满时结果改由新 goroutine 投递，同一 channel 内的顺序可能与完成顺序不一致
- 带回调且 channel 为 nil 的请求，回调会在完成 goroutine 中直接执行（回调不能阻塞，也不能调用 `Close`）

# Installation
```
go get github.com/y001j/iouring-go
```
[doc](https://pkg.go.dev/github.com/y001j/iouring-go)

# Quickstart
```golang
package main

import (
        "fmt"
        "os"

        "github.com/y001j/iouring-go"
)

var str = "io with iouring"

func main() {
        iour, err := iouring.New(1)
        if err != nil {
                panic(fmt.Sprintf("new IOURing error: %v", err))
        }
        defer iour.Close()

        file, err := os.Create("./tmp.txt")
        if err != nil {
                panic(err)
        }

        ch := make(chan iouring.Result, 1)

        prepRequest := iouring.Write(int(file.Fd()), []byte(str))
        if _, err := iour.SubmitRequest(prepRequest, ch); err != nil {
                panic(err)
        }

        result := <-ch
        i, err := result.ReturnInt()
        if err != nil {
                fmt.Println("write error: ", err)
                return
        }

        fmt.Printf("write byte: %d\n", i)
}
```

# Request With Extra Info
```golang
prepRequest := iouring.Write(int(file.Fd()), []byte(str)).WithInfo(file.Name())

request, err := iour.SubmitRequest(prepRequest, nil)
if err != nil {
    panic(err)
}

<- request.Done()
info, ok := request.GetRequestInfo().(string)
```

# Cancel Request
```golang
prepR := iouring.Timeout(5 * time.Second)
request, err := iour.SubmitRequest(prepR, nil)
if err != nil {
    panic(err)
}

if _, err := request.Cancel(); err != nil{
    fmt.Printf("cancel request error: %v\n", err)
    return
}

<- request.Done()
if err := request.Err(); err != nil{
    if err == iouring.ErrRequestCanceled{
        fmt.Println("request is canceled"0
        return
    }
    fmt.Printf("request error: %v\n", err)
    return
}
```


# Submit multitude request

```golang
var offset uint64
buf1 := make([]byte, 1024)
prep1:= iouring.Pread(fd, buf1, offset)

offset += 1024
buf2 := make([]byte, 1024)
prep2:= iouring.Pread(fd, buf1, offset)

requests, err := iour.SubmitRequests([]iouring.PrepRequest{prep1,prep2}, nil)
if err != nil{
    panic(err)
}
<- requests.Done()
fmt.Println("requests are completed")
```
requests is concurrent execution

# Link request
```golang
var offset uint64
buf := make([]byte, 1024)
prep1 := iouring.Pread(fd, buf1, offset)
prep2 := iouring.Write(int(os.Stdout.Fd()), buf)

iour.SubmitLinkRequests([]iouring.PrepRequest{prep1, prep2}, nil)
```

# Examples
[cat](https://github.com/Iceber/iouring-go/tree/main/examples/cat)

[concurrent-cat](https://github.com/Iceber/iouring-go/tree/main/examples/concurrent-cat)

[cp](https://github.com/Iceber/iouring-go/tree/main/examples/cp)

[request-with-timeout](https://github.com/Iceber/iouring-go/tree/main/examples/timeout/request-with-timeout)

[link-request](https://github.com/Iceber/iouring-go/tree/main/examples/link)

[link-with-timeout](https://github.com/Iceber/iouring-go/tree/main/examples/timeout/link-with-timeout)

[timer](https://github.com/Iceber/iouring-go/tree/main/examples/timeout/timer)

[echo](https://github.com/Iceber/iouring-go/tree/main/examples/echo)

[echo-with-callback](https://github.com/Iceber/iouring-go/tree/main/examples/echo-with-callback)

# TODO
* add tests
* arguments type (eg. int and int32)
* set logger
