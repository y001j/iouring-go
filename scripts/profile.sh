#!/bin/bash
# 对某个代理做 10s CPU profile（c=512, size=128）
P=${1:-iouring}
GOMAXPROCS=4 taskset -c 4-7 iouring-bench backend -listen 127.0.0.1:9000 >/dev/null &
sleep 0.3
BENCH_PPROF=127.0.0.1:6060 GOMAXPROCS=4 taskset -c 0-3 iouring-bench proxy-$P -listen 127.0.0.1:9100 >/dev/null &
sleep 0.3
GOMAXPROCS=4 taskset -c 8-11 iouring-bench load -target 127.0.0.1:9100 -c 512 -size 128 -d 12s -warmup 1s -label $P &
sleep 2
curl -s -o /tmp/cpu.pb "http://127.0.0.1:6060/debug/pprof/profile?seconds=8"
wait %3
cd /src && go tool pprof -top -nodecount=30 /usr/local/bin/iouring-bench /tmp/cpu.pb 2>/dev/null | head -40
echo "=== cumulative"
go tool pprof -top -cum -nodecount=30 /usr/local/bin/iouring-bench /tmp/cpu.pb 2>/dev/null | sed -n '6,40p'
