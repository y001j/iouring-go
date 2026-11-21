#!/bin/sh
# 在容器内运行：单元测试 + echo 服务器网络测试
set -e

echo "== 内核版本: $(uname -r) ($(uname -m))"

echo "== 单元测试"
iouring.test -test.v -test.count=1

echo "== echo 网络测试（基于 io_uring 的 TCP echo 服务器）"
echo-server 127.0.0.1:8000 &
SERVER_PID=$!
sleep 1

fail=0
for i in 1 2 3; do
  msg="hello-iouring-$i"
  out=$(echo-client 127.0.0.1:8000 "$msg")
  echo "$out"
  echo "$out" | grep -q "echo: $msg" || { echo "FAIL: 第 $i 次回显不匹配"; fail=1; }
done

kill $SERVER_PID 2>/dev/null || true

if [ $fail -eq 0 ]; then
  echo "== 全部测试通过"
else
  echo "== 网络测试失败"
  exit 1
fi
