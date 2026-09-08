# WebChat 语音交互 V1

语音输入输出模块，支持语音识别（ASR）和语音合成（TTS）。

## 架构

```
前端录音 → 语音服务器 ASR → 返回文字
前端把文字 POST 到 WebChat /api/messages → AI 通过 SSE 回复
前端收到 SSE assistant.done 事件 → 把 AI 回复 POST 到语音服务器 TTS
语音服务器返回 MP3 → 前端自动播放
```

## 文件说明

- **voice-server.js** - 语音服务器（Node.js），提供 ASR 和 TTS 两个端点
- **voice-input.js** - 前端语音模块，集成到 WebChat
- **asr_recognition.py** - ASR 识别脚本（调用阿里云 DashScope）
- **start.sh** - 启动脚本

## API 端点

### POST /api/voice-input
接收音频文件，返回识别文字。

**请求：** multipart/form-data，字段名 `audio`

**返回：**
```json
{"success": true, "text": "识别结果"}
```

### POST /api/tts
接收文字，返回语音 URL。

**请求：**
```json
{"text": "要转语音的文字"}
```

**返回：**
```json
{"success": true, "audio": "/api/voice-audio/xxx.mp3"}
```

### GET /api/voice-audio/:filename
获取生成的音频文件。

## 启动

```bash
cd /root/.openclaw/陈盛工作区/项目/语音交互
./start.sh
```

服务器将在 `0.0.0.0:18790` 启动。

## 集成到 WebChat

在 WebChat 的 HTML 中引入：

```html
<script src="http://your-server:18790/voice-input.js"></script>
```

或者直接将 `voice-input.js` 复制到 WebChat 的 `public/` 目录。

## 测试

```bash
# 测试 TTS
curl -X POST http://localhost:18790/api/tts \
  -H 'Content-Type: application/json' \
  -d '{"text":"测试"}'

# 应返回
# {"success":true,"audio":"/api/voice-audio/tts-xxx.mp3"}
```

## 依赖

- Node.js
- Python 3
- ffmpeg
- 阿里云 DashScope API Key（环境变量 `DASHSCOPE_API_KEY`）

## 前端功能

- 按住麦克风按钮录音
- 松开自动识别并发送到 AI
- AI 回复完成后自动播放语音
- 状态提示（录音中、识别中等）
