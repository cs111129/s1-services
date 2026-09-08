#!/bin/bash
# restart.sh — 重启 voice-server（本地改完 git pull 后执行）
cd "$(dirname "$0")"
PID=$(ss -tlnp 2>/dev/null | grep ":18790" | grep -oE "pid=[0-9]+" | head -1 | cut -d= -f2)
[ -n "$PID" ] && kill "$PID" && sleep 1
nohup node voice-server.js > /tmp/voice-server.log 2>&1 < /dev/null &
sleep 2
ss -tlnp | grep ":18790" >/dev/null && echo "✅ voice-server 已启动" || echo "❌ 未监听"
