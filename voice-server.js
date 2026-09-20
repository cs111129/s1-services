const http = require('http');
const fs = require('fs');
const path = require('path');
const { exec, execFile, execSync } = require('child_process');
const { promisify } = require('util');
const WebSocket = require('ws');   // 新增：WebSocket（BOX-3 云语音）

const execAsync = promisify(exec);
const execFileAsync = promisify(execFile);
const PORT = 18790;
const AUDIO_DIR = path.join(__dirname, 'audio-files');
const CAM_DIR = path.join(__dirname, 'cam-uploads');   // 视觉采集板整组上传存储目录
const TTS_SCRIPT = '/root/.openclaw/陈盛工作区/脚本/语音/tts.py';
const TTS_VOICE = 'longzhe';  // CosyVoice 音色名(可选几十种): longzhe/longhua/xiaoyi/aiyue/...
const ASR_SCRIPT = path.join(__dirname, 'asr_recognition.py');

// 确保 DASHSCOPE_API_KEY 环境变量可用
const DASHSCOPE_API_KEY = process.env.DASHSCOPE_API_KEY ||
  fs.readFileSync('/root/.bashrc', 'utf-8')
    .split('\n')
    .find(l => l.includes('DASHSCOPE_API_KEY'))
    ?.match(/"([^"]+)"/)?.[1] ||
  '';

// DeepSeek（回复生成；BOX-3 语音问答）
const DEEPSEEK_KEY = process.env.DEEPSEEK_API_KEY ||
  fs.readFileSync('/root/.bashrc', 'utf-8')
    .split('\n')
    .find(l => l.includes('DEEPSEEK_API_KEY') && l.includes('='))
    ?.replace(/^.*=\s*"?([^"\s]+)"?\s*$/, '$1') ||
  '';

const execEnv = Object.assign({}, process.env, {
  DASHSCOPE_API_KEY: DASHSCOPE_API_KEY
});

if (!fs.existsSync(AUDIO_DIR)) {
  fs.mkdirSync(AUDIO_DIR, { recursive: true });
}

/* ============ 视觉采集板 远程控制（下行指令 + 状态） ============ */
// 设备密钥（A 验证期公共密钥，与固件 downlink.c 的 DEVICE_SECRET 一致）
const CAM_DEVICE_SECRET = 'lingmu-cam-2026-secret';
// S2 db-api 地址 + ingest token（S1 转发拉 SOP 列表用，与 cam_analyze.py 一致）
const CAM_S2_DBAPI = 'http://8.136.146.89:18903';
const CAM_INGEST_TOKEN = 'cam-ingest-token-2026';
// 设备离线判定间隔：超过此毫秒无状态上报视为离线（前端判在线/离线）
const CAM_OFFLINE_MS = 15000;
// 待下发指令缓存：device_id -> { cmd, ts }
const camCmdQueue = {};
// 设备状态缓存：device_id -> { status, last_seen, vbat?, batt_pct?, batt_at? }
// ★ 2026-09-19 起新增电量：vbat(电池电压 V) / batt_pct(电量 0~100)，随 /api/cam/status 上报
//   （固件 v0.5+ 已实测会带：`上报状态 idle -> ok（电量 84% / 4.05V）`；空闲时每 5 分钟自动报一次）
//   老固件不带这两个字段 → 保持 null，前端显示"未上报"，不会显示成 0%
const camDeviceState = {};

// 把设备上报的电量写进状态缓存。
// ★ 只在**确实带了合法值**时才覆盖 —— 老固件/心跳包不带这两个字段，
//   不能把缓存里已有的电量冲成 undefined（否则一次 cmd 轮询就把电量弄没了）。
function setCamBattery(dev, o) {
  const st = camDeviceState[dev];
  if (!st || !o) return;
  const v = Number(o.vbat), p = Number(o.batt_pct);
  if (isFinite(v) && v > 0) st.vbat = Math.round(v * 100) / 100;   // 保留 2 位，如 4.05
  if (isFinite(p) && p >= 0 && p <= 100) st.batt_pct = Math.round(p);
  if (st.vbat !== undefined || st.batt_pct !== undefined) st.batt_at = Date.now();
}

/* ============ 设备鉴权（docs/44 通信安全升级）============ */
// 阶段2：**优先读请求头 X-Device-Secret / X-Device-Id**，回退 URL query 或 JSON body。
//        过渡期三处都认，老固件行为完全不变；观察一周后再删 URL 参数。
// 阶段3：一机一密 —— 密钥表 /root/cam-device-secrets.json，
//        表里没有的 device_id 用公共兜底密钥（兼容未出厂烧写的设备）。
// 为什么密钥不能留在 URL：URL 会被 nginx/反代/主机商 access log 原样记录，
//        上了 HTTPS 也照样泄漏（TLS 在反代处就终结了）。
//
// ⚠️ 下面这些状态必须放**模块作用域**。第一版我写在请求回调里（因为 getQuery 在那儿），
//    结果每个请求都重新初始化：
//      · 密钥表缓存永远失效 → 每次请求都读盘 + 打一行日志
//      · "只记一次"的日志变成刷屏 —— 设备每 3 秒轮询，200 台就是每秒 60 多行
//    "看起来日志在打"不等于"逻辑在跑"，尤其当状态被意外重置时。
const CAM_SECRET_FILE = process.env.CAM_SECRET_FILE || '/root/cam-device-secrets.json';
let _secCache = null, _secMtime = -1;
function camSecrets() {
  try {
    const st = fs.statSync(CAM_SECRET_FILE);
    if (_secCache && st.mtimeMs === _secMtime) return _secCache;   // 热加载：文件没变就用缓存
    _secCache = JSON.parse(fs.readFileSync(CAM_SECRET_FILE, 'utf8'));
    _secMtime = st.mtimeMs;
    log(`cam 密钥表已加载: ${Object.keys(_secCache.devices || {}).length} 台设备, `
      + `requireSecretOnUpload=${!!_secCache.requireSecretOnUpload}`);
  } catch (e) {
    if (!_secCache) {
      // 文件不存在/坏了 → 退回旧行为（公共密钥、上传不强制），不能因此让设备全断
      _secCache = { devices: {}, fallbackSecret: CAM_DEVICE_SECRET, requireSecretOnUpload: false };
      log(`cam 密钥表读取失败(${e.message})，用公共密钥兜底`);
    }
  }
  return _secCache;
}
// 某台设备应该用的密钥：表里有就用表里的，没有就用公共兜底
function deviceSecretOf(deviceId) {
  const c = camSecrets();
  return ((c.devices || {})[deviceId]) || c.fallbackSecret || CAM_DEVICE_SECRET;
}
function checkDeviceSecret(secret, deviceId) {
  if (!secret) return false;
  return secret === deviceSecretOf(deviceId);
}
// 从 请求头 / query / body 三处取凭据（阶段2 过渡期三处都认）
function pickSecret(req, q, o) {
  return String(req.headers['x-device-secret'] || (q && q.secret) || (o && o.secret) || '').trim();
}
function pickDeviceId(req, q, o) {
  return String(req.headers['x-device-id'] || (q && q.device_id) || (o && o.device_id) || '').trim();
}
// 鉴权来源按【端点 + 来源】各记一次。
// ★ 只看"有没有出现过 header"是不够的：设备可能只在 /api/cam/cmd 上加了头、
//   而 /api/cam-upload 还没加 —— 那种情况下把 requireSecretOnUpload 置 true 会把上传全打断。
//   所以日志必须能区分端点。
const _authSrcSeen = {};
function logAuthSource(req, deviceId, endpoint) {
  const src = req.headers['x-device-secret'] ? 'header' : 'url/body';
  const key = endpoint + '|' + src;
  if (!_authSrcSeen[key]) {
    _authSrcSeen[key] = 1;
    log(`cam 鉴权来源首次出现: [${endpoint}] ${src}（设备 ${deviceId || '?'}）`);
  }
}
// cam-upload 缺凭据的告警只打一次，别刷屏
let _uploadNoSecretWarned = false;
// cam-upload 首次带上凭据时高亮提示一次 —— 这是"可以置 requireSecretOnUpload=true"的唯一可靠信号
let _uploadSecretOkLogged = false;

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function parseMultipart(buffer, boundary) {
  const parts = [];
  const boundaryBuffer = Buffer.from(`--${boundary}`);
  let start = 0;

  while (true) {
    const idx = buffer.indexOf(boundaryBuffer, start);
    if (idx === -1) break;

    const end = buffer.indexOf(boundaryBuffer, idx + boundaryBuffer.length);
    if (end === -1) break;

    const part = buffer.slice(idx + boundaryBuffer.length, end);
    const headerEnd = part.indexOf(Buffer.from('\r\n\r\n'));

    if (headerEnd !== -1) {
      const headers = part.slice(0, headerEnd).toString();
      const body = part.slice(headerEnd + 4, part.length - 2);

      const nameMatch = headers.match(/name="([^"]+)"/);
      if (nameMatch) {
        parts.push({ name: nameMatch[1], data: body });
      }
    }

    start = end;
  }

  return parts;
}

// ASR: 音频 → 文字（浏览器 webm 音频）
async function handleASR(audioData) {
  const timestamp = Date.now();
  const webmPath = path.join(AUDIO_DIR, `input-${timestamp}.webm`);
  const pcmPath = path.join(AUDIO_DIR, `input-${timestamp}.pcm`);

  try {
    fs.writeFileSync(webmPath, audioData);
    await execAsync(`ffmpeg -i "${webmPath}" -acodec pcm_s16le -ar 16000 -ac 1 -f wav "${pcmPath}" -y`);

    const { stdout } = await execAsync(`python3 "${ASR_SCRIPT}" "${pcmPath}"`, { env: execEnv });
    const result = JSON.parse(stdout);

    fs.unlinkSync(webmPath);
    fs.unlinkSync(pcmPath);

    return { success: true, text: result.text || '' };
  } catch (error) {
    [webmPath, pcmPath].forEach(f => fs.existsSync(f) && fs.unlinkSync(f));
    throw error;
  }
}

// TTS: 文字 → 音频
async function handleTTS(text) {
  const timestamp = Date.now();
  const ttsMp3 = `tts-${timestamp}.mp3`;
  const ttsWav = `tts-${timestamp}.wav`;
  const mp3Target = path.join(AUDIO_DIR, ttsMp3);
  const wavTarget = path.join(AUDIO_DIR, ttsWav);

  try {
    await execFileAsync('python3', [TTS_SCRIPT, text, '--voice', TTS_VOICE], { env: execEnv });

    const ttsSource = '/tmp/tts-chunk-1.mp3';
    if (!fs.existsSync(ttsSource)) {
      throw new Error('TTS 输出文件不存在');
    }

    fs.copyFileSync(ttsSource, mp3Target);

    // 同时生成 WAV 版本（SoX 无 libmad 无法播 MP3）
    try {
      execSync(`ffmpeg -y -i "${mp3Target}" -acodec pcm_s16le -ar 16000 -ac 1 "${wavTarget}"`, { stdio: 'pipe', timeout: 15000 });
    } catch(e) {
      log(`WAV 转码失败(不影响 MP3): ${e.message}`);
    }

    return { success: true, audio: `/api/voice-audio/${ttsMp3}` };
  } catch (error) {
    throw error;
  }
}

/* ============ BOX-3 云语音（WebSocket 流式） ============ */

// 给原始 16k 单声道 PCM 加 WAV 头，供 asr_recognition.py(格式wav) 识别
function pcmToWav(pcm) {
  const sampleRate = 16000, ch = 1, bits = 16;
  const dataSize = pcm.length;
  const buf = Buffer.alloc(44 + dataSize);
  buf.write('RIFF', 0);
  buf.writeUInt32LE(36 + dataSize, 4);
  buf.write('WAVE', 8);
  buf.write('fmt ', 12);
  buf.writeUInt32LE(16, 16);
  buf.writeUInt16LE(1, 20);        // PCM
  buf.writeUInt16LE(ch, 22);
  buf.writeUInt32LE(sampleRate, 24);
  buf.writeUInt32LE(sampleRate * ch * bits / 8, 28);
  buf.writeUInt16LE(ch * bits / 8, 32);
  buf.writeUInt16LE(bits, 34);
  buf.write('data', 36);
  buf.writeUInt32LE(dataSize, 40);
  pcm.copy(buf, 44);
  return buf;
}

// BOX-3 原始 PCM → 文字
async function handleASRFromPCM(pcmBuf) {
  if (pcmBuf.length < 3200) throw new Error('音频太短');
  const ts = Date.now();
  const wavPath = path.join(AUDIO_DIR, `box3-${ts}.wav`);
  fs.writeFileSync(wavPath, pcmToWav(pcmBuf));
  const { stdout } = await execAsync(`python3 "${ASR_SCRIPT}" "${wavPath}"`, { env: execEnv });
  fs.unlinkSync(wavPath);
  return JSON.parse(stdout).text || '';
}

// DeepSeek 生成口语化回复
async function deepseekReply(text) {
  if (!DEEPSEEK_KEY) return text;   // 无 key 则回显识别文字
  const prompt = '你是灵眸浴场/宾馆环境语音助手。用一句口语化中文回答用户，不要啰嗦。用户说：' + text;
  try {
    const r = await fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${DEEPSEEK_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'deepseek-chat', messages: [{ role: 'user', content: prompt }], temperature: 0.3, max_tokens: 120 })
    });
    const j = await r.json();
    return (j.choices && j.choices[0].message.content || text).trim();
  } catch (e) {
    log('DeepSeek 失败: ' + e.message);
    return text;
  }
}

// tts.py 合成 → MP3 → 16k PCM 文件，返回文件路径
async function ttsToPcmFile(text) {
  const mp3 = '/tmp/tts-chunk-1.mp3';
  if (fs.existsSync(mp3)) fs.unlinkSync(mp3);
  await execFileAsync('python3', [TTS_SCRIPT, text, '--voice', TTS_VOICE], { env: execEnv });
  if (!fs.existsSync(mp3)) throw new Error('TTS 无输出');
  const pcmOut = path.join(AUDIO_DIR, `box3-tts-${Date.now()}.pcm`);
  execSync(`ffmpeg -y -i "${mp3}" -acodec pcm_s16le -ar 16000 -ac 1 -f s16le "${pcmOut}"`, { stdio: 'pipe', timeout: 15000 });
  return pcmOut;
}

function handleVoiceWS(ws) {
  let pcm = Buffer.alloc(0);
  ws.on('message', async (data, isBinary) => {
    if (isBinary) { pcm = Buffer.concat([pcm, data]); return; }
    let obj;
    try { obj = JSON.parse(data.toString()); } catch (e) { return; }

    if (obj.type === 'hello') {
      ws.send(JSON.stringify({ type: 'ready' }));
      log(`BOX-3 voice connected (${obj.device || 'unknown'})`);
    } else if (obj.type === 'stop') {
      try {
        const heard = await handleASRFromPCM(pcm);
        log(`ASR: ${heard}`);
        ws.send(JSON.stringify({ type: 'text', content: heard }));

        const reply = await deepseekReply(heard);
        log(`Reply: ${reply}`);
        ws.send(JSON.stringify({ type: 'status', content: 'talking' }));

        const pcmFile = await ttsToPcmFile(reply);
        const audio = fs.readFileSync(pcmFile);
        fs.unlinkSync(pcmFile);
        for (let i = 0; i < audio.length; i += 320) ws.send(audio.slice(i, i + 320));
        ws.send(JSON.stringify({ type: 'bye', code: 0 }));
        pcm = Buffer.alloc(0);
      } catch (e) {
        log(`voice WS error: ${e.message}`);
        try { ws.send(JSON.stringify({ type: 'bye', code: 1 })); } catch (e2) {}
      }
    }
  });
  ws.on('close', () => log('BOX-3 voice closed'));
  ws.on('error', (e) => log('voice WS error ' + e.message));
}

const server = http.createServer(async (req, res) => {
  // CORS（docs/44 阶段2：允许设备用 X-Device-Secret / X-Device-Id 请求头）
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Device-Secret, X-Device-Id');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  // ============ 视觉采集板 设备控制 ============

  // 读取 URL query 工具
  function getQuery(url) {
    const q = {};
    const i = url.indexOf('?');
    if (i === -1) return q;
    url.slice(i + 1).split('&').forEach(kv => {
      const [k, v] = kv.split('=');
      if (k) q[decodeURIComponent(k)] = decodeURIComponent(v || '');
    });
    return q;
  }

  // 设备鉴权相关函数与状态已挪到**模块作用域**（见文件上方 CAM_SECRET_FILE 那一段）。
  // 原先写在这里会被每个请求重新初始化 → 缓存失效 + 日志刷屏。

  // POST /api/cam/register - 设备联网注册（A 验证期：登记状态，不强制持久化）
  if (req.method === 'POST' && req.url === '/api/cam/register') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const o = JSON.parse(body || '{}');
        const dev = pickDeviceId(req, null, o);
        if (!checkDeviceSecret(pickSecret(req, null, o), dev)) {
          res.writeHead(401, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '设备密钥错误' }));
          return;
        }
        if (!dev) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '缺少 device_id' }));
          return;
        }
        logAuthSource(req, dev, 'register');
        // ★ 合并写入，不能整个替换 —— 否则注册会把已缓存的电量抹掉
        if (!camDeviceState[dev]) camDeviceState[dev] = {};
        camDeviceState[dev].status = o.status || 'idle';
        camDeviceState[dev].last_seen = Date.now();
        setCamBattery(dev, o);
        log(`cam 设备注册: ${dev} (mac=${o.mac || '?'})`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true }));
      } catch (e) {
        log(`cam register 错误: ${e.message}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // GET /api/cam-sop - 设备端拉 SOP 列表（40号文档，S1 转发 S2 db-api）
  if (req.method === 'GET' && req.url.startsWith('/api/cam-sop')) {
    const q = getQuery(req.url);
    const dev = pickDeviceId(req, q, null);
    if (!checkDeviceSecret(pickSecret(req, q, null), dev)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '设备密钥错误' }));
      return;
    }
    logAuthSource(req, dev, 'sop');
    fetch(CAM_S2_DBAPI + '/api/cam-sop', {
      headers: { 'Authorization': 'Bearer ' + CAM_INGEST_TOKEN }
    }).then(function(r) { return r.json(); }).then(function(data) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
    }).catch(function(e) {
      log(`cam-sop 转发失败: ${e.message}`);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ sops: [] }));
    });
    return;
  }

  // GET /api/cam/cmd - 设备轮询待执行指令（设备每 3 秒调用）
  if (req.method === 'GET' && req.url.startsWith('/api/cam/cmd')) {
    const q = getQuery(req.url);
    const dev = pickDeviceId(req, q, null);
    if (!checkDeviceSecret(pickSecret(req, q, null), dev)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '设备密钥错误' }));
      return;
    }
    if (!dev) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '缺少 device_id' }));
      return;
    }
    logAuthSource(req, dev, 'cmd');
    // 设备轮询 = 心跳，更新 last_seen（保持 status 不变，只刷新心跳）
    if (!camDeviceState[dev]) {
      camDeviceState[dev] = { status: 'idle', last_seen: Date.now() };
    } else {
      camDeviceState[dev].last_seen = Date.now();
    }
    // 有未消费指令则返回并清除
    const pending = camCmdQueue[dev];
    let cmd = null;
    if (pending) {
      cmd = pending.cmd;
      delete camCmdQueue[dev];
      log(`cam 下发指令 ${cmd} -> ${dev}`);
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ cmd, interval_ms: pending ? (Number(pending.interval_ms) || 0) : 0, max_seconds: pending ? (Number(pending.max_seconds) || 0) : 0, sop_id: pending ? (pending.sop_id || "") : "", enable_audio: pending ? (pending.enable_audio !== false) : true, mode: pending ? (pending.mode || "") : "" }));
    return;
  }

  // POST /api/cam/status - 设备上报状态（前端据此判在线/离线 + 显示状态）
  if (req.method === 'POST' && req.url === '/api/cam/status') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const o = JSON.parse(body || '{}');
        const dev = pickDeviceId(req, null, o);
        if (!checkDeviceSecret(pickSecret(req, null, o), dev)) {
          res.writeHead(401, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '设备密钥错误' }));
          return;
        }
        if (!dev) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '缺少 device_id' }));
          return;
        }
        logAuthSource(req, dev, 'status');
        // ★ 合并写入（原来整个替换），并接收设备上报的电量
        if (!camDeviceState[dev]) camDeviceState[dev] = {};
        camDeviceState[dev].status = o.status || 'idle';
        camDeviceState[dev].last_seen = Date.now();
        setCamBattery(dev, o);
        log(`cam 状态上报: ${dev} = ${o.status}`
          + (camDeviceState[dev].batt_pct !== undefined
              ? `（电量 ${camDeviceState[dev].batt_pct}%${camDeviceState[dev].vbat !== undefined ? ' / ' + camDeviceState[dev].vbat + 'V' : ''}）`
              : '（本次未带电量）'));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true }));
      } catch (e) {
        log(`cam status 错误: ${e.message}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // POST /api/cam/cmd/send - S2 前端下发指令给设备（经 S2 后代转发到 S1）
  if (req.method === 'POST' && req.url === '/api/cam/cmd/send') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const o = JSON.parse(body || '{}');
        const dev = o.device_id;
        const action = o.action;   // start / stop
        if (!dev || !action) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '缺少 device_id 或 action' }));
          return;
        }
        const cmd = action === 'start' ? 'start_record' : 'stop_record';
        // 34号：透传 mode（full/video/audio）；同时保留 enable_audio 旧字段（由 mode 映射，固件优先认 mode）
        camCmdQueue[dev] = { cmd, ts: Date.now(), interval_ms: Number(o.interval_ms) || 0, max_seconds: Number(o.max_seconds) || 0, sop_id: o.sop_id || "", enable_audio: o.enable_audio !== false, mode: o.mode || "" };
        log(`cam 指令入队: ${cmd} -> ${dev}`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, msg: '已下发指令' }));
      } catch (e) {
        log(`cam cmd/send 错误: ${e.message}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // GET /api/cam/device/list - 设备列表 + 在线状态（前端管理后台用，A 验证期在 S1 直接提供）
  if (req.method === 'GET' && req.url === '/api/cam/device/list') {
    const devices = Object.entries(camDeviceState).map(([id, st]) => {
      const online = (Date.now() - st.last_seen) < CAM_OFFLINE_MS;
      // ★ 电量一并暴露给前端（docs/26 第 35 行："存这两列 + 让手机端能读到"）
      //   没上报过就是 null，前端显示"未上报"，不要当 0%
      return {
        device_id: id, status: st.status, online, last_seen: st.last_seen,
        vbat: st.vbat !== undefined ? st.vbat : null,
        batt_pct: st.batt_pct !== undefined ? st.batt_pct : null,
        batt_at: st.batt_at || null
      };
    });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ devices }));
    return;
  }

  // POST /api/voice-input - ASR
  if (req.method === 'POST' && req.url === '/api/voice-input') {
    const contentType = req.headers['content-type'] || '';
    const boundaryMatch = contentType.match(/boundary=(.+)/);

    if (!boundaryMatch) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '缺少 boundary' }));
      return;
    }

    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', async () => {
      try {
        const buffer = Buffer.concat(chunks);
        const parts = parseMultipart(buffer, boundaryMatch[1]);
        const audioPart = parts.find(p => p.name === 'audio');

        if (!audioPart) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '未找到音频数据' }));
          return;
        }

        const result = await handleASR(audioPart.data);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (error) {
        log(`ASR 错误: ${error.message}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: error.message }));
      }
    });
    return;
  }

  // POST /api/tts - TTS
  if (req.method === 'POST' && req.url === '/api/tts') {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', async () => {
      try {
        const body = JSON.parse(Buffer.concat(chunks).toString());
        if (!body.text) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '缺少 text 参数' }));
          return;
        }

        const result = await handleTTS(body.text);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (error) {
        log(`TTS 错误: ${error.message}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: error.message }));
      }
    });
    return;
  }

  // POST /api/cam-upload - 视觉采集板整组上传（multipart：batch + 多张 jpg + rec.wav）
  if (req.method === 'POST' && req.url === '/api/cam-upload') {
    // ★ docs/44：这个端点原来是**完全无鉴权**的 —— 谁能连上 18790 就能塞文件、
    //   并触发一次付费的 AI 分析。这里补上，但要兼容现状：
    //   当前固件上传时**不发任何凭据**（upload.c 里没有 secret 字段），直接强制会让设备全断。
    //   所以分两步：① 带了凭据就校验，错的直接拒 ② 没带凭据时由密钥表的
    //   requireSecretOnUpload 决定（默认 false=放行+告警）。设备侧加上
    //   X-Device-Secret 或 multipart 里的 secret 字段后，把那个开关置 true 收紧。
    const hdrSecret = String(req.headers['x-device-secret'] || '').trim();
    const hdrDev = String(req.headers['x-device-id'] || '').trim();
    const contentType = req.headers['content-type'] || '';
    const boundaryMatch = contentType.match(/boundary=(.+)/);

    if (!boundaryMatch) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '缺少 boundary' }));
      return;
    }
    if (hdrSecret && !checkDeviceSecret(hdrSecret, hdrDev)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '设备密钥错误' }));
      return;
    }
    // ⚠️ 这里**不能**因为"没有请求头"就直接拒 —— 凭据还可以放在 multipart 的 secret 字段里
    //    （docs/45 明确承诺了两种方式都支持）。第一版就是这么写的，结果
    //    requireSecretOnUpload=true 之后 multipart 方式**必然 401** —— 文档说支持、实际不支持。
    //    所以这道拦截挪到**解析完 multipart 之后**（见下面 effSecret 那里）。
    //    代价：没带头的请求会先读进内存再拒 —— 和改造前的行为完全一样，没有变差。

    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', async () => {
      try {
        const buffer = Buffer.concat(chunks);
        const parts = parseMultipart(buffer, boundaryMatch[1]);

        // 先取 device_id（拼进 batch 前缀，保证跨设备 batch 号不撞）
        const devPart = parts.find(p => p.name === 'device_id');
        const device_id = devPart ? devPart.data.toString().trim() : '';
        // 设备凭据：请求头优先，其次 multipart 里的 secret 字段（两种都支持，便于设备侧二选一）
        const secPart = parts.find(p => p.name === 'secret');
        const effSecret = hdrSecret || (secPart ? secPart.data.toString().trim() : '');
        const effDev = hdrDev || device_id;
        if (effSecret && !checkDeviceSecret(effSecret, effDev)) {
          res.writeHead(401, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '设备密钥错误' }));
          return;
        }
        // ★ requireSecretOnUpload 的拦截放在这里（解析之后），这样两种凭据方式都能被认到。
        //   放前面会让 multipart 的 secret 字段永远走过不 —— 那是个假承诺。
        if (!effSecret && camSecrets().requireSecretOnUpload) {
          res.writeHead(401, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '设备密钥错误（未携带凭据）' }));
          return;
        }
        if (effSecret) {
          logAuthSource(req, effDev, 'upload');
          // ★ 这是"可以把 requireSecretOnUpload 置 true"的唯一可靠信号：
          //   必须看到 **upload 这个端点** 已经带上凭据，而不是别的端点带了头。
          if (!_uploadSecretOkLogged) {
            _uploadSecretOkLogged = true;
            log('✅ cam-upload 已开始携带设备凭据 —— 此时才可以把 requireSecretOnUpload 置 true '
              + '（关掉"无凭据也能上传"的口子）。改 /root/cam-device-secrets.json 即刻生效，无需重启。');
          }
        } else if (!_uploadNoSecretWarned) {
          _uploadNoSecretWarned = true;
          log('⚠️ cam-upload 未携带任何设备凭据（当前固件如此）—— 过渡期放行。'
            + '设备侧给上传加上 X-Device-Secret（或 multipart 的 secret 字段）后，'
            + '看到上面那条 ✅ 日志，再把 requireSecretOnUpload 置 true。');
        }
        // 取出 batch（无则用时间戳）；拼 device_id 前缀使其全局唯一
        const batchPart = parts.find(p => p.name === 'batch');
        const rawBatch = batchPart ? batchPart.data.toString().trim() : '';
        const batch = (device_id ? device_id + '_' : '') + (rawBatch || ('auto-' + Date.now()));
        const sopPart = parts.find(p => p.name === 'sop_id');
        const sop_id = sopPart ? sopPart.data.toString().trim() : '';
        // 拍照间隔（41号文档，设备端传实际间隔，缺省兜底 5000ms）
        const intervalPart = parts.find(p => p.name === 'interval_ms');
        const interval_ms = intervalPart ? intervalPart.data.toString().trim() : '';

        // 建目录 CAM_DIR/<batch>/（先清空旧内容，防 batch 复用残留旧帧）
        const batchDir = path.join(CAM_DIR, batch);
        if (fs.existsSync(batchDir)) fs.rmSync(batchDir, { recursive: true, force: true });
        fs.mkdirSync(batchDir, { recursive: true });

        let fileCount = 0;
        for (const p of parts) {
          // 普通字段（batch/device_id 已处理）跳过；其余按 name 作为文件名保存
          if (p.name === 'batch' || p.name === 'device_id' || p.name === 'sop_id' || p.name === 'interval_ms' || p.name === 'secret') continue;
          // upload_log 是纯文本元信息（37号文档），写到 _upload_log.txt 供 cam_analyze 解析入库，不计入文件数
          if (p.name === 'upload_log') {
            fs.writeFileSync(path.join(batchDir, '_upload_log.txt'), p.data);
            continue;
          }
          const safeName = path.basename(p.name);   // 防止路径穿越
          if (!safeName) continue;
          fs.writeFileSync(path.join(batchDir, safeName), p.data);
          fileCount++;
        }

        log(`cam-upload 成功: batch=${batch}, device_id=${device_id || '(无)'}, 收到 ${fileCount} 个文件`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, batch, device_id, files: fileCount }));

        // 落盘后异步触发云端分析（图片视觉时序 + 录音 ASR + 综合分析 → 推送 S2 ingest）
        // 把 device_id 传给分析脚本，供它查 S2 current-binding 拿快照 + 推 ingest。
        const analyzer = path.join(__dirname, 'cam_analyze.py');
        exec(`python3 "${analyzer}" "${batchDir}" "${device_id}" "${sop_id}" "${interval_ms}" >> /tmp/cam-analyze.log 2>&1`, (err, stdout, stderr) => {
          if (err) log(`cam 分析启动失败: ${err.message}`);
          else log(`cam 分析已启动: batch=${batch}, device_id=${device_id || '(无)'}`);
        });
      } catch (error) {
        log(`cam-upload 错误: ${error.message}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: error.message }));
      }
    });
    return;
  }

  // GET /api/cam/file/<batch>/<filename> - 视觉板 图片/音频文件访问（S2 鉴权代理转发到 S1）
  // 读 cam-uploads/<batch>/<filename>，按扩展名返回 Content-Type；path.basename 防路径穿越
  if (req.method === 'GET' && req.url.startsWith('/api/cam/file/')) {
    const rest = req.url.replace('/api/cam/file/', '');   // "<batch>/<filename>"
    const parts = rest.split('/');
    const batch = parts[0] || '';
    const filename = path.basename(parts[1] || '');       // 防穿越：只取 basename

    if (!batch || !filename) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '路径错误' }));
      return;
    }

    const dir = path.join(CAM_DIR, batch);
    if (!fs.existsSync(dir)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'batch 目录不存在' }));
      return;
    }
    const filePath = path.join(dir, filename);
    if (!fs.existsSync(filePath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '文件不存在' }));
      return;
    }

    const ext = path.extname(filename).toLowerCase();
    let mime = 'application/octet-stream';
    if (ext === '.jpg' || ext === '.jpeg') mime = 'image/jpeg';
    else if (ext === '.png') mime = 'image/png';
    else if (ext === '.wav') mime = 'audio/wav';
    res.writeHead(200, { 'Content-Type': mime });
    fs.createReadStream(filePath).pipe(res);
    return;
  }

  // GET /api/voice-audio/:filename
  if (req.method === 'GET' && req.url.startsWith('/api/voice-audio/')) {
    let filename = req.url.replace('/api/voice-audio/', '');
    let filePath = path.join(AUDIO_DIR, filename);

    // 如果有 WAV 版本则优先使用（SoX 无 libmad 不能播 MP3）
    const wavFile = filename.replace('.mp3', '.wav');
    const wavPath = path.join(AUDIO_DIR, wavFile);
    if (filename.endsWith('.mp3') && fs.existsSync(wavPath)) {
      filePath = wavPath;
    }

    if (!fs.existsSync(filePath)) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const mime = ext === '.wav' ? 'audio/wav' : 'audio/mpeg';
    res.writeHead(200, { 'Content-Type': mime });
    fs.createReadStream(filePath).pipe(res);
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

server.listen(PORT, '0.0.0.0', () => {
  log(`语音服务器启动在 0.0.0.0:${PORT}`);
});

// WebSocket /voice（BOX-3 云语音）
const wss = new WebSocket.Server({ server, path: '/voice' });
wss.on('connection', (ws, req) => { const ip = (req && req.headers && (req.headers['x-forwarded-for'] || req.socket.remoteAddress)) || '?'; log('WS raw connection from ' + ip); handleVoiceWS(ws); });
log('WebSocket /voice 就绪 (BOX-3 云语音)');
