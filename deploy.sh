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
# ★★★ 期0 影子模式开关（CR-20261012-03 §9）—— 想关掉：改成 SHADOW_MODE=off ✓ 重新部署即可 ✓
#   ★★ 为什么写在这里、**不写 systemd unit** ✗✗：
#     本服务的**实际启动方式**是下面那行 `nohup node ...` ✓
#     而 `/etc/systemd/system/voice-server.service` 实测是 **inactive + disabled** ✓（不参与 ✓）
#     ⇒ ★ 写进 unit 的 `Environment=` **根本送不进正在跑的进程** ✗
#       （这是"**改了配置没生效**"的经典形状 ✗ —— 而且看 unit 文件会以为已经配上了 ✓）
#   ★ 另一条本项目踩过的老规矩：**"命令行传参能进进程环境"是会静默失效的机制** ✗
#     ⇒ 配置**落到代码/脚本里** ✓（有版本历史 ✓ 也 review 得了 ✓）
#   ⚠️ 改本文件有**自举陷阱** ✗（配置手册 §105）：deploy.sh 自己会 `git pull` ✓
#     而运行中的 bash 仍持有**旧 inode** ⇒ **这一次执行用的还是旧逻辑** ✓
#     ⇒ 改完本文件必须**先手动 `git pull` 一次**，再跑 `./deploy.sh` ✓
export SHADOW_MODE="${SHADOW_MODE:-on}"
PID=$(ss -tlnp 2>/dev/null | grep ":18790" | grep -oE "pid=[0-9]+" | head -1 | cut -d= -f2)
[ -n "$PID" ] && kill "$PID" && sleep 1
nohup node voice-server.js > /tmp/voice-server.log 2>&1 < /dev/null &
sleep 2

echo "④ 验证..."
ss -tlnp 2>/dev/null | grep ":18790" >/dev/null && echo "✅ voice-server 已启动（18790）" || { echo "❌ 未监听，看日志 /tmp/voice-server.log"; exit 1; }
# ★★ 回读**进程环境**确认开关真进去了 ✗（"部署完成"这四个字证明不了这件事 ✓）
NEWPID=$(ss -tlnp 2>/dev/null | grep ":18790" | grep -oE "pid=[0-9]+" | head -1 | cut -d= -f2)
if [ -n "$NEWPID" ] && tr '\0' '\n' < "/proc/$NEWPID/environ" 2>/dev/null | grep -q '^SHADOW_MODE='; then
  echo "   SHADOW_MODE=$(tr '\0' '\n' < /proc/$NEWPID/environ | grep '^SHADOW_MODE=' | cut -d= -f2) ✓（影子开关已进进程 ✓）"
else
  echo "   ⚠️ 进程环境里没读到 SHADOW_MODE ⇒ 影子模式【没开】✓（新代码仍可用，只是不采样 ✓）"
fi
