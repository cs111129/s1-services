#!/bin/bash
# 快速启动语音服务器

cd "$(dirname "$0")"

echo "检查依赖..."

# 检查 Python 依赖
if ! python3 -c "import dashscope" 2>/dev/null; then
    echo "❌ 缺少 dashscope，请运行: pip3 install dashscope"
    exit 1
fi

# 检查 ffmpeg
if ! command -v ffmpeg &> /dev/null; then
    echo "❌ 缺少 ffmpeg，请安装"
    exit 1
fi

# 检查环境变量
if [ -z "$DASHSCOPE_API_KEY" ]; then
    # 尝试从 bashrc 读取
    source ~/.bashrc 2>/dev/null
    if [ -z "$DASHSCOPE_API_KEY" ]; then
        echo "❌ 未设置 DASHSCOPE_API_KEY"
        exit 1
    fi
fi

echo "✅ 依赖检查通过"
echo ""
echo "启动语音服务器（端口 18790）..."
node voice-server.js
