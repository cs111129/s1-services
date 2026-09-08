// 测试 BOX-3 云语音 WebSocket /voice 协议
const WebSocket = require('ws');
const ws = new WebSocket('ws://127.0.0.1:18790/voice');

let session = [];
ws.on('open', () => {
  console.log('[open] 已连接');
  ws.send(JSON.stringify({ type: 'hello', device: 'box3-test' }));
  // 发 0.3s 16k 单声道 16bit 静音 PCM(9600 字节) 后 stop
  const silentPcm = Buffer.alloc(9600, 0);
  setTimeout(() => {
    for (let i = 0; i < silentPcm.length; i += 320) ws.send(silentPcm.slice(i, i + 320));
    ws.send(JSON.stringify({ type: 'stop' }));
    console.log('[send] hello + 静音PCM + stop');
  }, 300);
});

ws.on('message', (data, isBinary) => {
  if (isBinary) {
    session.push(`[audio帧] ${data.length}B`);
  } else {
    const t = data.toString();
    session.push('[json] ' + t);
    if (t.includes('bye')) {
      console.log('=== 收到 ===');
      session.forEach(s => console.log(s));
      ws.close(); process.exit(0);
    }
  }
});
ws.on('error', e => { console.error('ERR', e.message); process.exit(1); });
setTimeout(() => { console.log('超时'); console.log(session.join('\n')); process.exit(1); }, 20000);
