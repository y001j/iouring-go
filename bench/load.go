//go:build linux

package main

import (
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// runLoad: C 个长连接，每个连接循环 写 size 字节 -> 读回 size 字节（ping-pong），
// 预热 warmup 后统计 dur 时长内的 QPS 与延迟分布。
func runLoad(target string, conns, size int, dur, warmup time.Duration, label string) error {
	var recording, stop int32
	var wg sync.WaitGroup
	lats := make([][]time.Duration, conns)
	errs := make([]error, conns)

	msg := make([]byte, size)
	for i := range msg {
		msg[i] = byte('a' + i%26)
	}

	for i := 0; i < conns; i++ {
		c, err := net.Dial("tcp", target)
		if err != nil {
			return fmt.Errorf("dial %d: %w", i, err)
		}
		c.(*net.TCPConn).SetNoDelay(true)
		wg.Add(1)
		go func(i int, c net.Conn) {
			defer wg.Done()
			defer c.Close()
			buf := make([]byte, size)
			local := make([]time.Duration, 0, 1<<16)
			for atomic.LoadInt32(&stop) == 0 {
				t0 := time.Now()
				if _, err := c.Write(msg); err != nil {
					errs[i] = err
					break
				}
				if _, err := io.ReadFull(c, buf); err != nil {
					errs[i] = err
					break
				}
				if atomic.LoadInt32(&recording) == 1 {
					local = append(local, time.Since(t0))
				}
			}
			lats[i] = local
		}(i, c)
	}

	time.Sleep(warmup)
	atomic.StoreInt32(&recording, 1)
	time.Sleep(dur)
	atomic.StoreInt32(&recording, 0)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			return fmt.Errorf("conn %d: %w", i, e)
		}
	}

	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	if len(all) == 0 {
		return fmt.Errorf("no samples")
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	var sum time.Duration
	for _, d := range all {
		sum += d
	}
	pct := func(p float64) time.Duration { return all[int(float64(len(all)-1)*p)] }
	qps := float64(len(all)) / dur.Seconds()
	mbps := qps * float64(size) / (1 << 20)
	fmt.Printf("RESULT\t%s\tc=%d\tsize=%d\tqps=%.0f\tMB/s=%.1f\tavg=%s\tp50=%s\tp99=%s\tp999=%s\n",
		label, conns, size, qps, mbps,
		(sum / time.Duration(len(all))).Round(time.Microsecond),
		pct(0.50).Round(time.Microsecond), pct(0.99).Round(time.Microsecond), pct(0.999).Round(time.Microsecond))
	return nil
}
