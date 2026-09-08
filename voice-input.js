// WebChat 语音交互模块 V1
(function() {
  const VOICE_SERVER = window.location.origin.replace(':18789', ':18790');
  let mediaRecorder = null;
  let chunks = [];
  let isRecording = false;
  let currentAIResponse = '';

  function createMicButton() {
    const btn = document.createElement('button');
    btn.id = 'mic-btn';
    btn.innerHTML = '🎤';
    btn.style.cssText = `
      padding: 8px 12px;
      margin-left: 8px;
      border: none;
      border-radius: 4px;
      background: #4CAF50;
      color: white;
      cursor: pointer;
      font-size: 18px;
      transition: background 0.3s;
    `;
    btn.title = '按住说话';
    
    btn.addEventListener('mousedown', startRecording);
    btn.addEventListener('mouseup', stopRecording);
    btn.addEventListener('touchstart', startRecording);
    btn.addEventListener('touchend', stopRecording);
    
    return btn;
  }

  function showStatus(msg) {
    let status = document.getElementById('voice-status');
    if (!status) {
      status = document.createElement('div');
      status.id = 'voice-status';
      status.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 12px 20px;
        background: rgba(0,0,0,0.8);
        color: white;
        border-radius: 8px;
        z-index: 9999;
        font-size: 14px;
      `;
      document.body.appendChild(status);
    }
    status.textContent = msg;
    status.style.display = 'block';
  }

  function hideStatus() {
    const status = document.getElementById('voice-status');
    if (status) status.style.display = 'none';
  }

  async function startRecording(e) {
    e.preventDefault();
    if (isRecording) return;
    
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
      chunks = [];
      
      mediaRecorder.ondataavailable = e => chunks.push(e.data);
      mediaRecorder.onstop = sendAudio;
      mediaRecorder.start();
      
      isRecording = true;
      document.getElementById('mic-btn').style.background = '#f44336';
      showStatus('🔴 录音中...');
    } catch (err) {
      showStatus('❌ 录音失败: ' + err.message);
      setTimeout(hideStatus, 2000);
    }
  }

  function stopRecording(e) {
    e.preventDefault();
    if (!isRecording || !mediaRecorder) return;
    
    mediaRecorder.stop();
    mediaRecorder.stream.getTracks().forEach(t => t.stop());
    isRecording = false;
    document.getElementById('mic-btn').style.background = '#4CAF50';
  }

  async function sendAudio() {
    showStatus('⏳ 识别中...');
    
    const blob = new Blob(chunks, { type: 'audio/webm' });
    const form = new FormData();
    form.append('audio', blob, 'voice.webm');
    
    try {
      const res = await fetch(VOICE_SERVER + '/api/voice-input', { method: 'POST', body: form });
      const data = await res.json();
      
      if (!data.success || !data.text) {
        showStatus('❌ 识别失败');
        setTimeout(hideStatus, 2000);
        return;
      }
      
      hideStatus();
      
      // 显示识别文字并发送到 WebChat
      appendUserMessage('🎤 ' + data.text);
      dispatchMessage(data.text);
      
    } catch (err) {
      showStatus('❌ 处理失败: ' + err.message);
      setTimeout(hideStatus, 2000);
    }
  }

  function appendUserMessage(text) {
    const messages = document.querySelector('.messages') || document.querySelector('#messages');
    if (!messages) return;
    
    const msg = document.createElement('div');
    msg.className = 'message user-message';
    msg.textContent = text;
    messages.appendChild(msg);
    messages.scrollTop = messages.scrollHeight;
  }

  function dispatchMessage(text) {
    // 直接调用 WebChat 的 sendMessage 函数
    if (typeof sendMessage === 'function') {
      sendMessage(text, []);
    } else {
      // 兜底：找输入框手动触发表单
      const input = document.querySelector('input[type="text"]') || document.querySelector('textarea');
      const form = document.querySelector('form');
      if (input && form) {
        input.value = text;
        form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
      }
    }
  }

  // 监听 SSE assistant.done 事件 → 自动 TTS
  function setupSSEListener() {
    // 拦截 EventSource 或监听自定义事件
    const originalEventSource = window.EventSource;
    
    if (originalEventSource) {
      window.EventSource = function(...args) {
        const es = new originalEventSource(...args);
        
        es.addEventListener('assistant.delta', (e) => {
          try {
            const payload = JSON.parse(e.data);
            if (payload.text) {
              currentAIResponse += payload.text;
            }
          } catch {}
        });
        
        es.addEventListener('assistant.done', async () => {
          if (currentAIResponse.trim()) {
            await playTTS(currentAIResponse);
            currentAIResponse = '';
          }
        });
        
        es.addEventListener('assistant.start', () => {
          currentAIResponse = '';
        });
        
        return es;
      };
    }
  }

  async function playTTS(text) {
    try {
      const res = await fetch(VOICE_SERVER + '/api/tts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text })
      });
      
      const data = await res.json();
      if (data.success && data.audio) {
        const audio = new Audio(VOICE_SERVER + data.audio);
        audio.play();
      }
    } catch (err) {
      console.error('TTS 播放失败:', err);
    }
  }

  function init() {
    const composer = document.querySelector('.composer') || document.querySelector('form');
    if (!composer) {
      console.error('未找到输入框容器');
      return;
    }
    
    composer.appendChild(createMicButton());
    setupSSEListener();
    
    console.log('语音交互模块已加载');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
