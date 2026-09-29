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

// 当前绑定学员缓存（CR-20260922-01）：device_id -> { id, name }
// ★ 三态语义（规格书 §4.3，别记错 ✗）：
//   · 键【不存在】      = 服务器还没推过 / 没实现 ⇒ 设备显示「未知」（灰）
//   · 值为 null        = 明确【没绑定】        ⇒ 设备显示「未绑定」（金）
//   · 值为 {id,name}   = 已绑定                ⇒ 设备显示姓名（白）
// ⚠️ 绝不能把「键不存在」当成「未绑定」✗✗ —— 员工会以为没绑（实际绑了）→ 去重复绑定甚至绑到别人名下
// ⚠️ 纯内存：S1 一重启就退回「未知」⇒ 由 S2 每 5 分钟幂等重推兜底（规格书 §6）
const camBindInfo = {};

// 给 /api/cam/cmd 的响应挂上 student 字段（规格书 §4.2 参考实现）
// 返回【新的】对象，不改动传进来的那个 —— 否则 cmd/send 里那份 pending 会被污染
function withStudent(dev, resp) {
  // ★ 判据必须是「键存不存在」而不是「值真不真」✗✗：
  //   camBindInfo[dev] 有 3 种状态 —— 键不存在 / 值 null / 值 {id,name}。
  //   原写法 `if (!b) return resp` 把 null 和 undefined 一起判掉了 ⇒
  //   **「明确解绑」被当成「没推过」**，响应里键被去掉，设备显示「未知」而不是「未绑定」。
  //   这正是规格书 §4.3 警告的语义陷阱（"字段不存在" ≠ "值为空"）。
  if (!(dev in camBindInfo)) return resp;                     // 没推过 → 不加这个键（= 未知）
  const b = camBindInfo[dev];
  const out = Object.assign({}, resp);
  // 值为 null（明确解绑）或 name 为空 → 都给 null（设备显示「未绑定」，比显示空名字好）
  out.student = (b && b.name ? { id: b.id || '', name: b.name } : null);
  return out;
}

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

/* ============ CR-20260924-01：开公网路由前的加固（限大小 + 限频）============ */
// ★ 为什么【不】用设备密钥 X-Device-Secret 来护这两个端点 —— 这是 grep 过消费者的结论：
//   · voice-input.js（WebChat 前端语音模块）：**浏览器直调**，放不了密钥 ✗
//   · openclaw-webchat-channel 的 dist/web/voice-input.js：同上 ✗
//   · combined-proxy.js / https-proxy.js：也在转发这三条路径 ✓
//   ⇒ 加设备密钥会把 WebChat 那条【在用的】链路掐断 ⇒
//     改用单子给的第②个选项：**限 body 大小 + 限频** ——
//     既护住 DashScope 额度与进程内存，又对任何现有消费者零影响 ✓
//   ★ 判据：「我找到了一个使用者」≠「我找全了使用者」——
//     加鉴权之前必须先把消费者 grep 全，否则就是"为了保护一个端点，打断另一个功能"。
const VOICE_MAX_BODY = parseInt(process.env.VOICE_MAX_BODY || '', 10) || 2 * 1024 * 1024;  // 2MB（8 秒 μ-law ≈128KB，余量足够）
const VOICE_RATE_WINDOW_MS = 60 * 1000;
// ★ 每 IP 每分钟。默认 120 而不是 40 —— 一次语音交互 = 1 ASR + 1 TTS（2 次请求），
//   120 ⇒ 每分钟 60 轮对话，远高于真实用量；但门店 NAT 后面可能同时有
//   设备 + 几台手机（共用一个公网 IP）⇒ 40 会偏紧，容易打出假 429 ✗
const VOICE_RATE_MAX = parseInt(process.env.VOICE_RATE_MAX || '', 10) || 120;
const _voiceRate = {};   // ip -> { n, t }
// ★★ 取客户端 IP：**必须优先 X-Real-IP，不能用 X-Forwarded-For 的第一个** ✗
//   原因：nginx 那边两个头来源不同 ——
//     · `X-Real-IP $remote_addr`        = nginx 拿到的真实对端地址，**客户端伪造不了** ✓
//     · `X-Forwarded-For $proxy_add_x_forwarded_for` = **在客户端传来的 XFF 后面追加** ⇒
//       客户端只要先发一个 `X-Forwarded-For: 1.2.3.4`，它就会排在**最前面**，
//       而我原来取 `split(',')[0]` ⇒ **每次换一个假 IP 就能绕过限频** ✗✗（等于没限）
//   ⇒ 改：X-Real-IP 优先；没有它才退回 XFF；再没有才用 socket 地址
function voiceClientIp(req) {
  const real = String(req.headers['x-real-ip'] || '').trim();
  if (real) return real;
  const xff = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  if (xff) return xff;
  return String(req.socket.remoteAddress || '?').trim();
}
// 返回 true = 该拒绝（已限频）
function voiceRateLimited(req) {
  const ip = voiceClientIp(req);
  const now = Date.now();
  let b = _voiceRate[ip];
  if (!b || now - b.t > VOICE_RATE_WINDOW_MS) { b = _voiceRate[ip] = { n: 0, t: now }; }
  b.n++;
  // 顺手清理过期桶，避免长期运行内存无界增长（每 IP 一个对象）
  const keys = Object.keys(_voiceRate);
  if (keys.length > 500) {
    for (const k of keys) { if (now - _voiceRate[k].t > VOICE_RATE_WINDOW_MS) delete _voiceRate[k]; }
  }
  if (b.n > VOICE_RATE_MAX) {
    // ★ 只在本轮第一次超限时打日志，否则刷屏（设备若进了这个状态会一直打）
    if (b.n === VOICE_RATE_MAX + 1) log(`⚠️ voice 限频触发: ${ip} 超过 ${VOICE_RATE_MAX} 次/分钟`);
    return true;
  }
  return false;
}
// 按 Content-Length 提前拒（拿不到 CL 时靠 on('data') 里边读边数兜底）
function voiceTooLargeByHeader(req) {
  const cl = parseInt(req.headers['content-length'] || '0', 10);
  return cl > VOICE_MAX_BODY;
}
function voiceTooLargeReply(res) {
  res.writeHead(413, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: `请求体过大（上限 ${Math.round(VOICE_MAX_BODY / 1024)}KB）` }));
}
function voiceRateReply(res) {
  res.writeHead(429, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: '请求过于频繁，请稍后再试' }));
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
    // ★ CR-20260921-04：set_config 的三个设置字段【只带存在的那几项】✗
    //   不塞 undefined、也不补 0 —— 设备端靠"字段缺省 = 保持原值"来工作（规格书 §4.3）
    const cfgExtra = {};
    if (pending) {
      cmd = pending.cmd;
      if (pending.cmd === 'set_config') {
        if (pending.vol !== undefined) cfgExtra.vol = pending.vol;
        if (pending.bri_idx !== undefined) cfgExtra.bri_idx = pending.bri_idx;
        if (pending.screen_off_idx !== undefined) cfgExtra.screen_off_idx = pending.screen_off_idx;
      }
      // ★ CR-20260921-02：take_photo 要把 shot_id 带给设备（设备可据此命名 batch）
      if (pending.cmd === 'take_photo' && pending.shot_id) cfgExtra.shot_id = pending.shot_id;
      delete camCmdQueue[dev];
      log(`cam 下发指令 ${cmd} -> ${dev}${Object.keys(cfgExtra).length ? ' ' + JSON.stringify(cfgExtra) : ''}`);
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    // ★ CR-20260922-01：搭车在设备已在轮询的这条接口上加 student 字段（不新增请求 ✗ —— 一次 TLS 握手吃 45KB RAM）
    // ★ CR-20260921-04：set_config 时把 cfgExtra 里的设置字段一并带出
    res.end(JSON.stringify(withStudent(dev, Object.assign({ cmd, interval_ms: pending ? (Number(pending.interval_ms) || 0) : 0, max_seconds: pending ? (Number(pending.max_seconds) || 0) : 0, sop_id: pending ? (pending.sop_id || "") : "", enable_audio: pending ? (pending.enable_audio !== false) : true, mode: pending ? (pending.mode || "") : "" }, cfgExtra))));
    return;
  }

  // POST /api/cam/device/bind - 服务器端(S2) 推送「当前绑定学员」（CR-20260922-01 §4.1）
  //   与 /api/cam/cmd/send 同构：POST + JSON body + 成功回 { success: true }
  //   ★ 幂等：重复推同一个学员 = 覆盖同一个值，无副作用 ✓
  //   student_name：UTF-8 中文【原文】（不要 \uXXXX 转义 ✗ —— 设备端是 strstr，不做转义还原）
  //                 null / 空 = 明确解绑（⇒ cmd 响应里 student: null，设备显示「未绑定」）
  if (req.method === 'POST' && req.url === '/api/cam/device/bind') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const o = JSON.parse(body || '{}');
        const dev = o.device_id;
        if (!dev) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: '缺少 device_id' }));
          return;
        }
        if (o.student_name === null || o.student_name === undefined || String(o.student_name).trim() === '') {
          camBindInfo[dev] = null;                              // ★ 明确解绑（不是"没推过"✗）
          log(`cam 学员解绑 -> ${dev}`);
        } else {
          camBindInfo[dev] = { id: String(o.student_id || ''), name: String(o.student_name).trim() };
          log(`cam 学员绑定 -> ${dev}: ${camBindInfo[dev].name}`);
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true }));
      } catch (e) {
        log(`cam device/bind 错误: ${e.message}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
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
        // ★ CR-20260921-02：设备抓拍上传成功后【立即】上报一次，带 last_shot_batch
        //   ⇒ 前端靠它判断"图好了没"（设备空闲时 status 是 5 分钟一次，等周期上报必挂）
        //   ⚠️ 合并写入：不带该字段的包**不许清掉已有值**（与 vbat 同一套语义）
        if (o.last_shot_batch !== undefined && o.last_shot_batch !== null && String(o.last_shot_batch).trim() !== '') {
          camDeviceState[dev].last_shot_batch = String(o.last_shot_batch).trim();
          camDeviceState[dev].last_shot_at = Date.now();
          log(`cam 抓拍回带 -> ${dev}: ${camDeviceState[dev].last_shot_batch}`);
        }
        // ★ CR-20260921-02 §15.2b：设备【一识别到 take_photo 就立即回】shot_ack
        //   ⇒ 这是本次缺失的那个"可观测性"：前端超时时能区分
        //      「有 ack = 设备收到了但没出图」vs「无 ack = 设备压根没收到」
        //   ⚠️ 同一套合并写入（不带该字段的包不冲旧值）
        if (o.shot_ack !== undefined && o.shot_ack !== null && String(o.shot_ack).trim() !== '') {
          camDeviceState[dev].shot_ack = String(o.shot_ack).trim();
          camDeviceState[dev].shot_ack_at = Date.now();
          log(`cam 抓拍确认(ack) -> ${dev}: ${camDeviceState[dev].shot_ack}`);
        }
        // ★ 抓拍失败原因（设备端一直在发，服务器端此前直接忽略了 ✗）
        //   取值如「相机出图失败」「设备忙（录制/上传中）」「上一张还在处理」
        if (o.shot_err !== undefined && o.shot_err !== null && String(o.shot_err).trim() !== '') {
          camDeviceState[dev].shot_err = String(o.shot_err).trim();
          camDeviceState[dev].shot_err_at = Date.now();
          log(`cam 抓拍失败 -> ${dev}: ${camDeviceState[dev].shot_err}`);
        }
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

  // ★ CR-20260921-04：远程设备设置的档位表 + 校验（模块级纯函数 ⇒ 可单元测试）
//   档位抄自 funeng2-manifest.md §3.9（★ 单一真源，别在这里另发明档位 ✗）
//   · vol            = 【实际音量值】20/40/65/80/100
//   · bri_idx        = 亮度【下标 0~4】 → 40/80/128/180/255
//   · screen_off_idx = 息屏【下标 0~3】 → 0(常亮)/30/60/120 秒
//   ★ 三字段都可缺省 ⇒ 只下发要改的那几项，缺的项设备【保持原值】（规格书 §4.3）
//   ⚠️ 越界【拒绝】而不是 clamp —— "想设 999 被设成 255" 比"没改"更糟（规格书 §4.3）
const CFG_VOL_STEPS = [20, 40, 65, 80, 100];
const CFG_BRI_STEPS = [40, 80, 128, 180, 255];
const CFG_SCREEN_OFF_STEPS = [0, 30, 60, 120];

// ★ 严格类型前置判断（单元测试抓到的真坑）：
//   `Number([]) === 0`、`Number(false) === 0`、`Number('  ') === 0`、`Number(null) === 0`
//   ⇒ 只用 Number() 强转的话，**空数组/布尔/空白串会被当成合法的第 0 档**（静默设成常亮/最低亮度）
//   ⇒ 必须**先限定类型**：只接受 number，或"内容全是数字的字符串"。
function isNumericLike(v) {
  if (typeof v === 'number') return isFinite(v);
  if (typeof v === 'string') {
    const s = v.trim();
    if (s === '') return false;
    return /^-?\d+(\.\d+)?$/.test(s);
  }
  return false;   // 数组 / 对象 / 布尔 / null / undefined 一律不认
}

function validateSetConfig(o) {
  const out = {};
  const bad = [];
  if (o.vol !== undefined && o.vol !== null) {
    const v = Number(o.vol);
    if (isNumericLike(o.vol) && CFG_VOL_STEPS.indexOf(v) >= 0) out.vol = v; else bad.push('vol=' + JSON.stringify(o.vol));
  }
  if (o.bri_idx !== undefined && o.bri_idx !== null) {
    const b = Number(o.bri_idx);
    if (isNumericLike(o.bri_idx) && Number.isInteger(b) && b >= 0 && b < CFG_BRI_STEPS.length) out.bri_idx = b; else bad.push('bri_idx=' + JSON.stringify(o.bri_idx));
  }
  if (o.screen_off_idx !== undefined && o.screen_off_idx !== null) {
    const s = Number(o.screen_off_idx);
    if (isNumericLike(o.screen_off_idx) && Number.isInteger(s) && s >= 0 && s < CFG_SCREEN_OFF_STEPS.length) out.screen_off_idx = s; else bad.push('screen_off_idx=' + JSON.stringify(o.screen_off_idx));
  }
  if (bad.length) {
    return { ok: false, error: '档位不合法（越界/类型错一律拒绝，不 clamp）: ' + bad.join(', '),
             accept: { vol: CFG_VOL_STEPS, bri_idx: '0~' + (CFG_BRI_STEPS.length - 1), screen_off_idx: '0~' + (CFG_SCREEN_OFF_STEPS.length - 1) } };
  }
  if (Object.keys(out).length === 0) {
    return { ok: false, error: '至少要带一项（vol / bri_idx / screen_off_idx）' };
  }
  return { ok: true, out };
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

        // ★ CR-20260921-02：远程抓拍一张（录制前预览确认用）
        //   设备端收到后：拍 1 张 → 单独上传（batch = <dev>_shot_<时间戳>）
        //   → **上传成功后立即上报一次 status**（带 last_shot_batch）⇒ 前端才知道图好了
        //   ⚠️ 录制中设备会忽略（不抢相机）；一次只认一张
        if (action === 'take_photo') {
          const shotId = String(o.shot_id || ('shot_' + Date.now())).trim();
          camCmdQueue[dev] = { cmd: 'take_photo', ts: Date.now(), shot_id: shotId };
          log(`cam 抓拍入队 -> ${dev} (shot_id=${shotId})`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, shot_id: shotId }));
          return;
        }

        // ★ CR-20260921-04：远程下发设备设置（音量/亮度/息屏）—— 校验见模块级 validateSetConfig()
        if (action === 'set_config') {
          const v = validateSetConfig(o);
          if (!v.ok) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(v));
            return;
          }
          camCmdQueue[dev] = Object.assign({ cmd: 'set_config', ts: Date.now() }, v.out);
          log(`cam 设置入队 -> ${dev}: ${JSON.stringify(v.out)}`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, queued: v.out }));
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
        batt_at: st.batt_at || null,
        // ★ CR-20260921-02：最近一次抓拍的 batch（前端据此判断"预览图好了没"）
        last_shot_batch: st.last_shot_batch || null,
        last_shot_at: st.last_shot_at || null,
        // ★ CR-20260921-02 §15.2b：抓拍确认与失败原因 —— 前端超时提示靠它们区分原因
        shot_ack: st.shot_ack || null,
        shot_ack_at: st.shot_ack_at || null,
        shot_err: st.shot_err || null,
        shot_err_at: st.shot_err_at || null,
        // ★★ 2026-09-23 新增：把【绑定状态】也报出来 —— 供 S2 判断"该清理谁"
        //   起因（S2 侧 CR-20260922-01 缺陷）：S2 清理 S1 上的陈旧绑定原来只靠自己的内存缓存，
        //   重启即失效 ⇒ S1 会一直留着旧绑定。S2 改用「向 S1 要设备清单」后，
        //   又发现光有 device_id **分不清**「从没绑过」和「绑过但现在该解绑」✗
        //   ⇒ 把绑定状态一起报出来，S2 才能精确地、且【只清一次】地推 null ✓
        //   ⚠️ 判据用 `!== undefined` 而不是真值判断：camBindInfo[dev] 可能是
        //      `null`（= 明确解绑过）—— 那是有效信息，不能当成"不知道"✗
        bound_student: (camBindInfo[id] && camBindInfo[id].name) ? camBindInfo[id].name : null,
        bind_known: camBindInfo[id] !== undefined
      };
    });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ devices }));
    return;
  }

  // POST /api/voice-input - ASR
  if (req.method === 'POST' && req.url === '/api/voice-input') {
    // ★ CR-20260924-01 加固：限频 + 限大小（开公网路由前必备，见文件上方那段注释）
    if (voiceRateLimited(req)) { voiceRateReply(res); return; }
    if (voiceTooLargeByHeader(req)) { voiceTooLargeReply(res); return; }

    const contentType = req.headers['content-type'] || '';
    const boundaryMatch = contentType.match(/boundary=(.+)/);

    if (!boundaryMatch) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: '缺少 boundary' }));
      return;
    }

    const chunks = [];
    let got = 0, tooBig = false;
    req.on('data', chunk => {
      if (tooBig) return;
      got += chunk.length;
      // ★ 兜底：拿不到 Content-Length（chunked）时边读边数，超限立刻断，别把整包读进内存
      if (got > VOICE_MAX_BODY) {
        tooBig = true;
        log(`⚠️ voice-input 请求体超限（>${Math.round(VOICE_MAX_BODY / 1024)}KB）已断开: ${voiceClientIp(req)}`);
        voiceTooLargeReply(res);
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', async () => {
      if (tooBig) return;
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
    // ★ CR-20260924-01 加固：限频 + 限大小（同上）
    if (voiceRateLimited(req)) { voiceRateReply(res); return; }
    if (voiceTooLargeByHeader(req)) { voiceTooLargeReply(res); return; }

    const chunks = [];
    let got = 0, tooBig = false;
    req.on('data', chunk => {
      if (tooBig) return;
      got += chunk.length;
      if (got > VOICE_MAX_BODY) {
        tooBig = true;
        log(`⚠️ tts 请求体超限（>${Math.round(VOICE_MAX_BODY / 1024)}KB）已断开: ${voiceClientIp(req)}`);
        voiceTooLargeReply(res);
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', async () => {
      if (tooBig) return;
      try {
        const body = JSON.parse(Buffer.concat(chunks).toString());
        // ★ 文本也要限长：TTS 按字数计费，且超长文本会让 DashScope 调用很久/失败
        const VOICE_MAX_TEXT = 1000;
        if (body.text && String(body.text).length > VOICE_MAX_TEXT) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: `文本过长（上限 ${VOICE_MAX_TEXT} 字，当前 ${String(body.text).length} 字）` }));
          return;
        }
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
        // ★ CR-20260921-02：**预览抓拍批次（*_shot_*）不做分析** ✗✗
        //   预览图是临时产物 ⇒ 若也跑分析会：① 白花 AI 费用 ② 作为一条"考核记录"污染学员记录列表
        //   判据：批次名含 `_shot_`（设备端按 <dev>_shot_<时间戳> 命名 ✓ 规格书 §4.4 ✓）
        if (/_shot_/.test(batch)) {
          log(`cam 预览抓拍批次，跳过分析: batch=${batch}`);
        } else {
          const analyzer = path.join(__dirname, 'cam_analyze.py');
          exec(`python3 "${analyzer}" "${batchDir}" "${device_id}" "${sop_id}" "${interval_ms}" >> /tmp/cam-analyze.log 2>&1`, (err, stdout, stderr) => {
            if (err) log(`cam 分析启动失败: ${err.message}`);
            else log(`cam 分析已启动: batch=${batch}, device_id=${device_id || '(无)'}`);
          });
        }
      } catch (error) {
        log(`cam-upload 错误: ${error.message}`);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: error.message }));
      }
    });
    return;
  }

  // GET /api/cam/file-list/<batch> - 列出某批次目录里的文件名（CR-20260921-02）
  //   ★ 用途：前端**不写死文件名**（设备端实测是 snap_001.jpg 三位，但别 trust 位数 ✗）
  //     取图流程：列目录 → 取第一个 .jpg → 再走 /api/cam/file/<batch>/<filename> ✓
  //   ★ 放在 /api/cam/file/ 之前匹配（否则会被下面 startsWith('/api/cam/file/') 抢走 ✗）
  if (req.method === 'GET' && req.url.startsWith('/api/cam/file-list/')) {
    try {
      const batch = path.basename(decodeURIComponent(req.url.replace('/api/cam/file-list/', '').split('?')[0]));
      if (!batch) { res.writeHead(400, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: '缺少 batch' })); return; }
      const dir = path.join(CAM_DIR, batch);
      if (!fs.existsSync(dir) || !fs.statSync(dir).isDirectory()) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: '批次不存在', batch }));
        return;
      }
      const files = fs.readdirSync(dir).filter(function (f) { return !f.startsWith('_'); });   // 隐藏 _upload_log.txt 等
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, batch, files, jpgs: files.filter(function (f) { return /\.jpe?g$/i.test(f); }) }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
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

  /* ============ CR-20260929-01：设备问 → 知识库检索 → AI 答 → TTS ============ */
  // 设备端硬约束（#42 内存）：一轮只许【2 次 TLS 握手】⇒ ASR+检索+LLM+TTS 必须在
  //   同一个请求里串完，一次返回 {text, reply, audio}（设备再 GET 音频）。
  //   ✗ 不能设计成"设备再来问一次答案"—— 那要多一次握手。
  //
  // ★★ 与 CR 规格书的差别（★ 重要）：规格书按"直接调 DeepSeek 答"写的，
  //    但用户要的是【先检索知识库、再回答】⇒ 这里多了一步 S2 的知识库检索。
  //    没有这一步，模型会用它自己的知识编答案 —— 给学员用，编一个错流程比说"不知道"坏得多。
  //
  // ★ 为什么检索走 S2：知识库与 kb-search 服务都在 S2（18906），
  //   公网路由 https://ai.hiiin.com/api/kb-search 现成，实测 S1→S2 0.12~0.27s。
  const KB_SEARCH_URL = process.env.KB_SEARCH_URL || 'https://ai.hiiin.com/api/kb-search';
  const KB_TIMEOUT_MS = 8000;
  const LLM_TIMEOUT_MS = 12000;
  const VOICE_CHAT_ROUNDS = 3;          // 保留最近 3 轮上下文（内存；重启即丢，丢了当新会话）
  const VOICE_REPLY_MAX = 60;           // reply 硬上限（设备端要求 ≤60 汉字；屏上只放得下 24 字）
  const _chatSessions = new Map();      // device -> [{role,content}...]

  // 设备专用提示词 —— ★ 独立一套，**不动**学员端那套（那套要"详细完整/8000 tokens"，正好相反）
  const VOICE_CHAT_SYSTEM = [
    '你是「赋能02」培训录制设备的语音助手，服务酒店门店的一线员工。',
    '',
    '【最重要的规则】',
    '1. 只依据下面【知识库参考内容】回答。参考内容里没有答案时，直接说「这个我查不到，你问下店长吧」。',
    '2. 绝对不许用你自己的知识补充、推测或编造步骤 —— 宁可说不知道。（员工会照着做，编错了会出事）',
    '3. 不要提「知识库」「参考内容」「资料」这些词，就像同事随口告诉你一样。',
    '',
    '【怎么说话】',
    '· 最多两句话，总共不超过 40 个字，越短越好。',
    '· 口语化，像老员工教新人。',
    '· 不要 markdown、不要分点、不要星号/括号/井号、不要 emoji —— 这些会被逐字念出来。',
    '· 直接说做法，不要复述问题、不要寒暄。'
  ].join('\n');

  // 取该设备的上下文数组（没有 device 就返回 null = 无会话）
  function chatHistoryOf(device) {
    if (!device) return null;
    let s = _chatSessions.get(device);
    if (!s) { s = []; _chatSessions.set(device, s); }
    if (_chatSessions.size > 500) {          // 防无界增长
      const first = _chatSessions.keys().next().value;
      if (first !== device) _chatSessions.delete(first);
    }
    return s;
  }

  // 查知识库。★ 返回 {ok:true, results} / {ok:false} —— 必须区分
  //   「检索成功但没这条知识」和「检索服务挂了」（"未知 ≠ 零"，两者对用户说的话不一样）
  async function kbSearchForVoice(query) {
    try {
      const ac = new AbortController();
      const tid = setTimeout(() => ac.abort(), KB_TIMEOUT_MS);
      try {
        const r = await fetch(KB_SEARCH_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: String(query).slice(0, 200) }),
          signal: ac.signal
        });
        if (!r.ok) { log(`⚠️ 知识库检索返回 HTTP ${r.status}`); return { ok: false, results: [] }; }
        const j = await r.json();
        return { ok: true, results: (j.results || []).slice(0, 3) };
      } finally { clearTimeout(tid); }
    } catch (e) {
      log(`⚠️ 知识库检索失败: ${e.message}`);
      return { ok: false, results: [] };
    }
  }

  // 兜底话术（检索不到 / 检索服务不可用）
  const NO_KB_REPLY = '这个我查不到，你问下店长吧。';
  const KB_DOWN_REPLY = '我这边知识库暂时连不上，你稍后再问一次吧。';

  function cleanReply(s) {
    return String(s || '')
      .replace(/```[\s\S]*?```/g, ' ')
      .replace(/[*#`>_~|]/g, '')          // markdown 记号：TTS 会逐字念
      .replace(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\u{FE0F}]/gu, '')  // emoji
      .replace(/\[[^\]]*\]|（[^）]*注[^）]*）|\([^)]*注[^)]*\)/g, '')      // 括号注释
      .replace(/\s+/g, ' ')
      .trim();
  }

  async function voiceChatAnswer(text, kbResults, history) {
    let kbBlock = '';
    if (kbResults.length) {
      kbBlock = '【知识库参考内容】\n' + kbResults.map((r, i) =>
        `[${i + 1}] ${r.file}\n${r.snippet}`).join('\n\n');
    } else {
      kbBlock = '【知识库参考内容】\n（没有检索到任何相关内容）';
    }
    const msgs = [{ role: 'system', content: VOICE_CHAT_SYSTEM + '\n\n' + kbBlock }];
    if (history) for (const h of history) msgs.push(h);
    msgs.push({ role: 'user', content: text });

    const ac = new AbortController();
    const tid = setTimeout(() => ac.abort(), LLM_TIMEOUT_MS);
    try {
      const r = await fetch('https://api.deepseek.com/chat/completions', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${DEEPSEEK_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ model: 'deepseek-chat', messages: msgs, temperature: 0.3, max_tokens: 160 }),
        signal: ac.signal
      });
      const j = await r.json();
      if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
      const content = j.choices && j.choices[0] && j.choices[0].message.content;
      return { reply: cleanReply(content), usage: j.usage || null };
    } finally { clearTimeout(tid); }
  }

  if (req.method === 'POST' && req.url === '/api/voice-chat') {
    if (voiceRateLimited(req)) { voiceRateReply(res); return; }
    if (voiceTooLargeByHeader(req)) { voiceTooLargeReply(res); return; }

    const contentType = req.headers['content-type'] || '';
    const boundaryMatch = contentType.match(/boundary=(.+)/);
    if (!boundaryMatch) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: 'no_boundary' }));
      return;
    }

    const chunks = [];
    let got = 0, tooBig = false;
    req.on('data', chunk => {
      if (tooBig) return;
      got += chunk.length;
      if (got > VOICE_MAX_BODY) {
        tooBig = true;
        log(`⚠️ voice-chat 请求体超限（>${Math.round(VOICE_MAX_BODY / 1024)}KB）已断开: ${voiceClientIp(req)}`);
        voiceTooLargeReply(res);
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', async () => {
      if (tooBig) return;
      const t0 = Date.now();
      try {
        const parts = parseMultipart(Buffer.concat(chunks), boundaryMatch[1]);
        const audioPart = parts.find(p => p.name === 'audio');
        const devPart = parts.find(p => p.name === 'device');
        const resetPart = parts.find(p => p.name === 'reset');
        // ★ "字段不存在" ≠ "值为空"（规格书 §4.4）：老固件不带 device ⇒ 按【无会话】处理，
        //   不许 400。reset 同理，不带 = "0"（续上下文）。
        const device = devPart ? String(devPart.data.toString('utf8')).trim() : '';
        const reset = resetPart ? String(resetPart.data.toString('utf8')).trim() : '0';

        if (!audioPart || !audioPart.data || !audioPart.data.length) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'no_audio' }));
          return;
        }

        // ① ASR
        const asr = await handleASR(audioPart.data);
        const heard = (asr.text || '').trim();
        if (!heard) {
          log(`voice-chat: 没听清（device=${device || '-'}）`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'asr_empty' }));
          return;
        }

        // ② 知识库检索（★ 这一步是"先检索再回答"的核心）
        const kb = await kbSearchForVoice(heard);

        // ③ 决定回答：检索服务挂了 / 没这条知识 ⇒ 直接用兜底话术，**不叫模型**
        //    （省一次调用，更重要的是【保证不会张口编】）
        let reply, usage = null;
        if (!kb.ok) {
          reply = KB_DOWN_REPLY;
        } else if (!kb.results.length) {
          reply = NO_KB_REPLY;
        } else {
          let history = null;
          if (device) {
            const s = chatHistoryOf(device);
            if (reset === '1' && s) s.length = 0;
            history = s ? s.slice(-VOICE_CHAT_ROUNDS * 2) : null;
          }
          try {
            const ans = await voiceChatAnswer(heard, kb.results, history);
            reply = ans.reply;
            usage = ans.usage;
          } catch (e) {
            log(`⚠️ voice-chat LLM 失败: ${e.message}`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: 'llm_failed' }));
            return;
          }
          // 写回上下文（只存真的答过的轮次）
          if (device) {
            const s = chatHistoryOf(device);
            s.push({ role: 'user', content: heard });
            s.push({ role: 'assistant', content: reply });
            while (s.length > VOICE_CHAT_ROUNDS * 2) s.shift();
          }
        }

        // ④ ★ 服务端自己截断（别指望设备端，规格书 §4.4 写死了）
        if (reply.length > VOICE_REPLY_MAX) reply = reply.slice(0, VOICE_REPLY_MAX) + '…';
        if (!reply) reply = NO_KB_REPLY;

        // ⑤ TTS
        let audioPath = '';
        try {
          const t = await handleTTS(reply);
          audioPath = t.audio || '';
        } catch (e) {
          log(`⚠️ voice-chat TTS 失败: ${e.message}`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'tts_failed' }));
          return;
        }

        // ★ 全过程都要留证据：否则出事故时只看到"回复很短"，分不清是
        //   "知识库没命中"（正常）还是"模型没答"（异常）
        log(`voice-chat ✓ device=${device || '-'} kb=${kb.ok ? kb.results.length : 'DOWN'} ` +
            `heard="${heard.slice(0, 20)}" reply=${reply.length}字 ` +
            `tokens=${usage ? (usage.prompt_tokens + '/' + usage.completion_tokens) : '-'} ` +
            `耗时=${((Date.now() - t0) / 1000).toFixed(1)}s`);

        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          success: true,
          text: heard,
          reply: reply,
          audio: audioPath,
          session: device ? (device + '#' + Math.ceil((chatHistoryOf(device) || []).length / 2)) : ''
        }, null, 0));
      } catch (e) {
        log(`⚠️ voice-chat 异常: ${e.message}`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'asr_failed' }));
      }
    });
    return;
  }

  // GET /api/voice-audio/:filename
  if (req.method === 'GET' && req.url.startsWith('/api/voice-audio/')) {
    // ★★ CR-20260924-01：补 path.basename() 防【目录穿越】✗✗
    //   原文：let filename = req.url.replace('/api/voice-audio/', '');
    //         let filePath = path.join(AUDIO_DIR, filename);
    //   ⇒ '../../../../../../etc/passwd' 会被 path.join 规范化 ⇒ **爬出 AUDIO_DIR** ✗✗
    //   ★ 同一文件里 cam-upload(L880) 与 cam-file(L943) 都写了 path.basename 并注明"防穿越" ⇒
    //     "别处都防了、就这一处没防" = **漏**，不是设计 ✓
    //   实测（S1 本机直连 18790，curl --path-as-is，6 层 ..）：
    //     /etc/hostname 200 · /etc/passwd 200 · **/etc/shadow 200(1024B)** · ~/.ssh/authorized_keys 200 ✗✗
    //   ⚠️ 顺手多写两步（不是多余，是必需）：
    //     ① 先切掉 '?' 查询串 —— 否则 'x.mp3?token=abc' 整个当文件名 ⇒ 正常取音频会 404
    //     ② decodeURIComponent —— 否则 %E4%B8%AD 这类中文/编码文件名取不到
    //   注意顺序：**先切查询串、再 decode、最后 basename** ——
    //     decode 之后再 basename 才安全（%2e%2e%2f 解出来是 ../，basename 一并吃掉 ✓）
    const _rawName = String(req.url).replace('/api/voice-audio/', '').split('?')[0];
    let _decoded;
    try { _decoded = decodeURIComponent(_rawName); } catch (e) { _decoded = _rawName; }  // 非法 %XX 不炸，按原样
    const filename = path.basename(_decoded);
    if (!filename) { res.writeHead(404); res.end('Not Found'); return; }
    let filePath = path.join(AUDIO_DIR, filename);
    // ★ 双保险：即使将来有人改了上面那行，这一步也能拦住逃逸（belongs-to 校验）
    if (path.resolve(filePath).indexOf(path.resolve(AUDIO_DIR) + path.sep) !== 0) {
      log(`⚠️ voice-audio 拦截越界访问: ${req.url}`);
      res.writeHead(404); res.end('Not Found'); return;
    }

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
