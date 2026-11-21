//go:build linux

// iouring-bench: TCP 转发（代理）性能对比
//
//	backend        后端 echo 服务（Go 标准库）
//	proxy-std      转发代理：Go 标准库 net + io.Copy（底层 epoll netpoller）
//	proxy-epoll    转发代理：手写 epoll 事件循环（每个 loop 绑定一个 OS 线程）
//	proxy-iouring  转发代理：基于 iouring-go
//	proxy-iouring-coop  同上，开启 IORING_SETUP_COOP_TASKRUN
//	load           压测客户端：C 个长连接 ping-pong，统计 QPS / 延迟
package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: iouring-bench <backend|proxy-std|proxy-epoll|proxy-iouring|proxy-iouring-coop|load> [flags]")
		os.Exit(2)
	}
	mode, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:9100", "listen address")
	backend := fs.String("backend", "127.0.0.1:9000", "backend address")
	loops := fs.Int("loops", 4, "epoll loops / io_uring rings")
	workers := fs.Int("workers", 4, "iouring: callback worker goroutines")
	bufSize := fs.Int("buf", 64*1024, "per-direction buffer size")
	target := fs.String("target", "127.0.0.1:9100", "load: target address")
	conns := fs.Int("c", 64, "load: concurrent connections")
	size := fs.Int("size", 128, "load: message size")
	dur := fs.Duration("d", 10e9, "load: duration")
	warm := fs.Duration("warmup", 1e9, "load: warmup")
	label := fs.String("label", "", "load: label for output")
	fs.Parse(args)

	// BENCH_PPROF=127.0.0.1:6060 开启 pprof
	if addr := os.Getenv("BENCH_PPROF"); addr != "" {
		go http.ListenAndServe(addr, nil)
	}

	var err error
	switch mode {
	case "backend":
		err = runBackend(*listen)
	case "proxy-std":
		err = runProxyStd(*listen, *backend, *bufSize)
	case "proxy-epoll":
		err = runProxyEpoll(*listen, *backend, *loops, *bufSize)
	case "proxy-iouring":
		err = runProxyIOURing(*listen, *backend, *loops, *workers, *bufSize, false)
	case "proxy-iouring-coop":
		err = runProxyIOURing(*listen, *backend, *loops, *workers, *bufSize, true)
	case "load":
		err = runLoad(*target, *conns, *size, *dur, *warm, *label)
	default:
		err = fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
