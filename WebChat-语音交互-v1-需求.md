# WebChat 语音交互 V1 — 需求文档

## 目标
在 WebChat 中实现"按住说话"语音输入，经 ASR 转文字→AI 回复→TTS 语音播报。

不是实时流式对话，是「推了一发，打完收工」模式。桌面兄弟直接复用后端。

## 数据流
```
[用户按住麦克风按钮说话]
       ↓
MediaRecorder 录音 → Blob(WAV/PCM)
       ↓
HTTP POST → [Node.js 服务端]
       ↓
保存临时 PCM 文件
       ↓
Python ASR 脚本 → DashScope Recognition.call(文件)
       ↓
文字结果 → OpenClaw Agent 回复
       ↓
回复文字 → Python TTS 脚本 → DashScope SpeechSynthesizer
       ↓
TTS 音频文件 MP3
       ↓
返回前端 → 自动播放
```

## 前端改动（WebChat index.html）

### 1. 麦克风按钮
- 在输入框旁边加一个麦克风图标按钮（🎤）
- 按下：调用 `navigator.mediaDevices.getUserMedia()` 获取音频流
- 使用 `MediaRecorder` 录音，`audio/webm` 格式（Chrome）或 fallback `audio/wav`
- 松开/点击停止：停止录制，发送音频

### 2. 发送音频
- HTTP POST 到 `/api/voice-input`，`Content-Type: multipart/form-data`
- body: `{ audio: Blob }`
- 显示"语音识别中..."状态
- 收到响应后：显示识别文字，自动播放返回的音频

### 3. 音频播放
- 创建 `<audio>` 元素播放返回的 MP3
- 自动播放（需先处理 autoplay policy → 用户手势触发录音时已有 active context）

### 4. UI 状态
- 录音中：按钮变红，显示"录音中..."
- ASR 中：显示"语音识别中..."
- AI 回复中：显示"处理中..."
- 完成：显示文字回复，自动播放音频

## 后端改动（Node.js Gateway 扩展）

### 1. 新路由 `/api/voice-input`
- POST 方法接收音频 multipart/form-data
- 保存音频到临时文件
- 调用 Python ASR 脚本获取文字
- 发送文字到 Agent 获取回复
- 调用 Python TTS 脚本生成音频
- 返回 JSON: `{ text, audio_url }`

### 2. Python ASR 脚本
```python
# asr_recognition.py
# Usage: python3 asr_recognition.py /path/to/audio.pcm
# Output: JSON to stdout: {"text": "识别结果"}

from dashscope.audio.asr import Recognition, RecognitionCallback, RecognitionResult

class ASRCallback(RecognitionCallback):
    def __init__(self):
        self.result_text = ""
    def on_event(self, result: RecognitionResult):
        sentence = result.get_sentence()
        if sentence and isinstance(sentence, dict):
            self.result_text += sentence.get("text", "")
    def on_complete(self):
        pass
    def on_error(self, result):
        print(json.dumps({"error": str(result)}), file=sys.stderr)
    def on_close(self):
        pass

def recognize(audio_path: str) -> str:
    cb = ASRCallback()
    rec = Recognition(
        model="paraformer-realtime-v2",
        callback=cb,
        format="pcm",  # or "wav" / "opus" / "webm"
        sample_rate=16000
    )
    result = rec.call(audio_path)  # one-shot
    return result.get_sentence()["text"]
```

### 3. Python TTS 脚本（已有）
- 复用 `tts.py`，传递文本参数，输出 MP3 文件

### 4. 音频格式转换（如果需要）
- 浏览器录制的 webm/opus 可能需要转成 PCM 16kHz 16bit mono
- 用 ffmpeg 转换
- 或者 ASR 支持 opus 格式（待测试）

## 需要确认的事项

### 阿里云侧
- [ ] `paraformer-realtime-v2` 模型名是否正确（待测试）
- [ ] ASR 支持的音频格式（pcm / wav / opus / mp3）
- [ ] 免费额度（通常首月 2h 免费）

### 浏览器兼容性
- [ ] `MediaRecorder` 支持情况（Chrome/Edge/Safari ✅）
- [ ] 音频格式（Chrome 默认 webm，Safari 可能只有 wav）
- [ ] 自动播放策略（用户手势触发后 3 秒内可 autoplay）

### 后端
- [ ] Python ASR 脚本与 Node.js 通信方式（subprocess vs HTTP）
- [ ] 临时文件清理策略
- [ ] 并发处理（多个用户同时语音输入）

## 后续 V2
- 流式识别（`start()` / `send_audio_frame()` / `stop()`）
- 边录边显示识别文字
- 打断功能（用户说话时可打断 AI 回复）
- 桌面兄弟硬件原生集成
