#!/bin/bash
# 交错对比压测：所有代理同时启动（各占一个端口、同样绑定 CPU 0-3），
# 每个场景下轮流压测每种代理，重复 ROUNDS 轮，最后输出每个场景的中位数。
# 这样虚拟机/宿主机的性能波动会均匀地落在各个代理上。
set -u
DUR=${DUR:-5s}
ROUNDS=${ROUNDS:-3}
CONNS=${CONNS:-"1 64 512"}
SIZES=${SIZES:-"128 16384"}
PROXIES=${PROXIES:-"std epoll iouring iouring-direct iouring-loop iouring-loopfixed"}
B=iouring-bench
OUT=/tmp/ab_raw.txt
: > $OUT

echo "kernel $(uname -r), cpus $(nproc), dur=$DUR rounds=$ROUNDS"
GOMAXPROCS=4 taskset -c 4-7 $B backend -listen 127.0.0.1:9000 >/dev/null &

declare -A PORT PID
port=9101
for p in $PROXIES; do
  PORT[$p]=$port
  GOMAXPROCS=4 taskset -c 0-3 $B proxy-$p -listen 127.0.0.1:$port -backend 127.0.0.1:9000 -loops 4 -workers 4 >/dev/null &
  PID[$p]=$!
  port=$((port+1))
done
sleep 1

HZ=$(getconf CLK_TCK)
cpu_ticks() { awk '{print $14+$15}' /proc/$1/stat; }

for r in $(seq 1 $ROUNDS); do
  for s in $SIZES; do
    for c in $CONNS; do
      for p in $PROXIES; do
        t0=$(date +%s.%N); c0=$(cpu_ticks ${PID[$p]})
        line=$(GOMAXPROCS=4 taskset -c 8-11 $B load -target 127.0.0.1:${PORT[$p]} -c $c -size $s -d $DUR -warmup 1s -label $p 2>&1 | grep RESULT)
        t1=$(date +%s.%N); c1=$(cpu_ticks ${PID[$p]})
        cpu=$(awk -v a=$c0 -v b=$c1 -v t0=$t0 -v t1=$t1 -v hz=$HZ 'BEGIN{printf "%.0f", (b-a)/hz/(t1-t0)*100}')
        echo "$line	cpu=$cpu	round=$r" | tee -a $OUT
      done
    done
  done
done

echo
echo "===== 中位数（$ROUNDS 轮）====="
printf "%-18s %-6s %-6s %10s %9s %9s %9s %7s\n" proxy conns size qps MB/s avg p99 cpu%
# 每个 (proxy, c, size) 取 qps 中位数所在的那一行
awk -F'\t' '{
  for (i=1;i<=NF;i++){ split($i,kv,"="); v[kv[1]]=kv[2] }
  key=$2"|"v["c"]"|"v["size"]
  n[key]++; rows[key, n[key]]=v["qps"]"|"v["MB/s"]"|"v["avg"]"|"v["p99"]"|"v["cpu"]
  if (!(key in seen)) { seen[key]=1; order[++k]=key }
}
END {
  for (j=1;j<=k;j++) {
    key=order[j]; m=n[key]
    # 按 qps 排序取中位数
    for (a=1;a<=m;a++) { split(rows[key,a],f,"|"); q[a]=f[1]+0; idx[a]=a }
    for (a=1;a<=m;a++) for (b=a+1;b<=m;b++) if (q[idx[b]]<q[idx[a]]) { t=idx[a]; idx[a]=idx[b]; idx[b]=t }
    split(rows[key, idx[int((m+1)/2)]], f, "|"); split(key, kk, "|")
    printf "%-18s %-6s %-6s %10s %9s %9s %9s %7s\n", kk[1], kk[2], kk[3], f[1], f[2], f[3], f[4], f[5]
  }
}' $OUT | sort -t' ' -k3,3n -k2,2n -s
