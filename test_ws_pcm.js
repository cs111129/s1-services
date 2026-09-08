// 自测：读 /tmp/test.pcm(真实TTS语音) → 走 WS 全链路
const WebSocket = require('ws');
const fs = require('fs');
const ws = new WebSocket('ws://127.0.0.1:18790/voice');

const pcmPath = process.argv[2] || '/tmp/test.pcm';
const pcm = fs.readFileSync(pcmPath);
let session = [];

ws.on('open', () => {
  console.log(`[open] 已连接, 音频 ${pcm.length}B`);
  ws.send(JSON.stringify({ type: 'hello', device: 'box3-test' }));
  setTimeout(() => {
    for (let i = 0; i < pcm.length; i += 320) ws.send(pcm.slice(i, i + 320));
    ws.send(JSON.stringify({ type: 'stop' }));
    console.log('[send] 音频+stop');
  }, 200);
});

ws.on('message', (data, isBinary) => {
  if (isBinary) session.push(`[audio帧] ${data.length}B`);
  else {
    const t = data.toString();
    session.push('[json] ' + t);
    if (t.includes('bye')) {
      console.log('=== 全链路结果 ===');
      session.forEach(s => console.log(s));
      ws.close(); process.exit(0);
    }
  }
});
ws.on('error', e => { console.error('ERR', e.message); process.exit(1); });
setTimeout(() => { console.log('超时'); console.log(session.join('\n')); process.exit(1); }, 25000);
