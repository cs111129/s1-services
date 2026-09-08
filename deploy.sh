#!/bin/bash
# deploy.sh — S1 语音交互服务部署（git pull + 语法检查 + 重启 + 验证）
# 用法：本地改完 push 后，SSH 到 S1 执行 ./deploy.sh
set -e
cd "$(dirname "$0")"

echo "① git pull..."
git pull 2>&1 | tail -3

echo "② 语法检查..."
node --check voice-server.js && echo "  voice-server.js ✓"
python3 -m py_compile cam_analyze.py && echo "  cam_analyze.py ✓"

echo "③ 重启 voice-server..."
PID=$(ss -tlnp 2>/dev/null | grep ":18790" | grep -oE "pid=[0-9]+" | head -1 | cut -d= -f2)
[ -n "$PID" ] && kill "$PID" && sleep 1
nohup node voice-server.js > /tmp/voice-server.log 2>&1 < /dev/null &
sleep 2

echo "④ 验证..."
ss -tlnp 2>/dev/null | grep ":18790" >/dev/null && echo "✅ voice-server 已启动（18790）" || { echo "❌ 未监听，看日志 /tmp/voice-server.log"; exit 1; }
