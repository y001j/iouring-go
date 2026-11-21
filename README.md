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

# Testing and benchmarks (Docker, works on macOS)

```
docker build -f Dockerfile.test -t iouring-go-test .
docker run --rm --security-opt seccomp=unconfined iouring-go-test                 # tests
docker run --rm --security-opt seccomp=unconfined --ulimit nofile=65536:65536 \
        iouring-go-test bench_ab.sh                                               # forwarding benchmark
```
`seccomp=unconfined` is required because Docker's default seccomp profile blocks io_uring.


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
