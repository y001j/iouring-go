#!/bin/bash
# TCP 转发性能对比：direct(直连后端基线) / proxy-std / proxy-epoll / proxy-iouring
# CPU 隔离：代理 0-3，后端 4-7，压测客户端 8-11（需要容器至少 12 核）
set -u
DUR=${DUR:-8s}
CONNS=${CONNS:-"1 64 512"}
SIZES=${SIZES:-"128 16384"}
PROXIES=${PROXIES:-"direct std epoll iouring"}
B=iouring-bench

echo "kernel $(uname -r), cpus $(nproc), dur=$DUR"

GOMAXPROCS=4 taskset -c 4-7 $B backend -listen 127.0.0.1:9000 >/dev/null &
sleep 0.5

cpu_ticks() { awk '{print $14+$15}' /proc/$1/stat; }
HZ=$(getconf CLK_TCK)

for p in $PROXIES; do
  port=9000; PID=""
  if [ "$p" != "direct" ]; then
    port=9100
    GOMAXPROCS=4 taskset -c 0-3 $B proxy-$p -listen 127.0.0.1:$port -backend 127.0.0.1:9000 -loops 4 -workers 4 >/dev/null &
    PID=$!
    sleep 0.5
  fi
  for s in $SIZES; do
    for c in $CONNS; do
      t0=$(date +%s.%N); c0=0
      [ -n "$PID" ] && c0=$(cpu_ticks $PID)
      out=$(GOMAXPROCS=4 taskset -c 8-11 $B load -target 127.0.0.1:$port -c $c -size $s -d $DUR -warmup 1s -label $p 2>&1)
      t1=$(date +%s.%N); cpu="-"
      if [ -n "$PID" ]; then
        c1=$(cpu_ticks $PID)
        cpu=$(awk -v a=$c0 -v b=$c1 -v t0=$t0 -v t1=$t1 -v hz=$HZ 'BEGIN{printf "%.0f%%", (b-a)/hz/(t1-t0)*100}')
      fi
      line=$(echo "$out" | grep RESULT || echo "RESULT	$p	c=$c	size=$s	FAILED: $(echo "$out" | tail -1)")
      echo "$line	proxyCPU=$cpu"
    done
  done
  [ -n "$PID" ] && kill $PID && wait $PID 2>/dev/null
  sleep 0.5
done
