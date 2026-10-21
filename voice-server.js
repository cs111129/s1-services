const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');   // ★ CR-20261009-01：设备密钥定长比较（timingSafeEqual）
const { exec, execFile, execSync } = require('child_process');
const { promisify } = require('util');
const WebSocket = require('ws');   // 新增：WebSocket（BOX-3 云语音）

const execAsync = promisify(exec);
const execFileAsync = promisify(execFile);
const PORT = 18790;
const AUDIO_DIR = path.join(__dirname, 'audio-files');
const CAM_DIR = path.join(__dirname, 'cam-uploads');   // 视觉采集板整组上传存储目录
const TTS_SCRIPT = '/root/.openclaw/陈盛工作区/脚本/语音/tts.py';
// ★ 音色可用环境变量覆盖（2026-10-07）—— 换音色不用改代码、不用重新 review
//   实测 cosyvoice-v1 **19 个音色全部可用**（清单 + 试听样本见 02-运维/scripts/voice-probe*.py）
//   男：longzhe longcheng longshu longshuo longfei longxiang longxiaocheng longlaotie longjielidou
//   女：longwan longhua longxiaochun longxiaoxia longxiaobai longjing longmiao longyue longyuan
//       儿童：longtong ｜ ★ `cosyvoice-v2` 本账号**未开通**（报 418 InvalidParameter）；`qwen-tts` 走兼容模式 404
//   ★★ 2026-10-07 用户试听后拍板：**改用 `longyuan`（龙媛，女声 9 号）**（原 longzhe 龙哲，男）
//   ★★★ 2026-10-12 **服务商把 CosyVoice v1 下线了**（用户当场报"问完不念、说语音合成没成功"✗）
//     实测（同一密钥，9 个组合矩阵）：
//       · `cosyvoice-v1` + longyuan/longzhe/longxiaochun ⇒ websocket `task-failed`
//         `{"error_code":"InvalidParameter","error_message":"Model not exist."}` ✗✗
//       · `cosyvoice-v2` + `longyuan_v2` ⇒ ✅ **11357 B 正常产出** ✓（`longxiaochun_v2` 也可以 ✓）
//       · `cosyvoice-v2` + 旧名字 `longyuan` ⇒ `empty_response` ✗（★ 音色名也要带 `_v2` ✗）
//     ★ 与本文件上面那条 10-07 的记载**正好相反**（当时 v2 报 418 未开通 ✓）——
//       即：**服务商既下线了 v1、又把 v2 开通了** ✓ ⇒ 两条一起改，缺一个都不出声 ✗
//     ★ 判据：**外部服务的"模型/音色名"是契约，会变** ✗ —— 它一变，症状是"合成失败"，
//       而旧代码里 `tts.py` **全失败仍退出 0** ✗ ⇒ 上层只看到"输出文件不存在"（误导 ✓）
//       ⇒ 所以下面顺手把**脚本原话**带进错误里 ✓（下次一眼看得出是服务商的问题 ✓）
//   ★★★ 2026-10-12 用户点单：**换成 `longwan`（龙婉）** ✓
//     实测（同一把钥匙，v2 模型 ✓）：`longwan` **无产出** ✗ ／ **`longwan_v2` ✅ 52317 B · 3.24 s** ✓
//     ⇒ ★ 规律再确认一次：**v2 模型下音色名要带 `_v2`** ✗（不带就是"无产出/empty_response"✓）
//     ★ 判据：**改音色名 = 改契约** ✗ ⇒ **必须先实测再改** ✓，别照文档抄（文档里通常不带后缀 ✓）
const TTS_MODEL = process.env.TTS_MODEL || 'cosyvoice-v2';   // ★ v1 已下线 ✓ 别改回 v1 ✗
const TTS_VOICE = process.env.TTS_VOICE || 'longwan_v2';     // ★ 龙婉（2026-10-12 用户点单 ✓）·带 _v2 ✓
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
// ★★ 2026-10-12：ingest token 从**文件**读（`/root/.cam-ingest-token`，0600 ✓ **不进 git** ✓）
//   优先级：**环境变量 > 文件**；★ **都没有 ⇒ 返回空串 + 大声报错** ✗（不再有硬编码兜底 ✓）
//   ★ 为什么这里**不是** fail-closed 直接退出（与 db-api 不同 ✗）：
//     db-api 是"校验方"⇒ 没 token 就没法鉴权 ⇒ **它必须拒绝启动** ✓
//     而 S1 是**设备入口**（voice/cam 全在这一个进程里 ✓）⇒ 为一个"转发凭据"整个起不来，
//     会把**本来不依赖 S2 的功能**（状态上报 / 上传 / 指令下发）一起打死 ✗✗
//     ⇒ 取不到就**只让依赖 S2 的那几条路失败并留痕** ✓（日志写清"为什么失败"✓ 而不是静默用默认值 ✓）
//   ★ 判据：**"取不到密钥"该 fail-closed 还是降级，取决于"它保护的是什么"** ✗
//     —— 保护"谁能进来"的（鉴权）必须 fail-closed；只是"我要出去"的凭据，降级 + 明确报错即可 ✓
function loadIngestToken() {
  const env = String(process.env.CAM_INGEST_TOKEN || '').trim();
  if (env) return env;
  try {
    const v = String(require('fs').readFileSync('/root/.cam-ingest-token', 'utf8') || '').trim();
    if (v) return v;
  } catch (e) {}
  console.error('[ingest] ✗ 取不到 CAM_INGEST_TOKEN（环境变量与 /root/.cam-ingest-token 都没有）'
    + ' ⇒ 依赖 S2 的接口（SOP 列表 / OTA 台账 / 密钥同步）会 401；'
    + '设备本地上报与指令下发不受影响。修法：把 token 写进 /root/.cam-ingest-token（0600）后重启本服务');
  return '';
}
const CAM_INGEST_TOKEN = loadIngestToken();

// ★★★ 2026-10-21 P0 兜底 ✗✗
//   ★ 今天真实事故：★ 我在模块级引用了 handler 内的 `jsonOut` ✗ ⇒ ReferenceError
//     ⇒ **进程直接退出** ⇒ ★ 整个语音服务（对练／小星／TTS）全挂 2~3 分钟 ✗✗
//   ★ 判据：**一个端点的缺陷，不该等于全站语音不可用** ✗
//   ★★ 这里只**记日志、不退出** ✓ —— 注意这只兜"没人接的异常" ✓
//      真正的修法是每个 handler 自己 try/catch ✓（那个还得逐个补 ✓）
process.on('uncaughtException', (e) => {
  try { log(`💥 uncaughtException（★ 已兜住，进程继续 ✓）: ${e && e.stack ? String(e.stack).split('\n').slice(0, 4).join(' | ') : e}`); } catch (_) {}
});
process.on('unhandledRejection', (e) => {
  try { log(`💥 unhandledRejection（★ 已兜住 ✓）: ${e && e.message ? e.message : e}`); } catch (_) {}
});
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
// ★ CR-20261009-01：密钥 → 设备号 的**反向索引**（一机一密落地后，固件不发 X-Device-Id
//   也能认出"是谁在请求" ✓ 顺带把「/api/cam-sop 不带 device_id ⇒ 岗位范围过滤一直失效」修掉 ✓）
//   ⚠️ 只对**一机一密**的设备有效：还共用公共兜底密钥的设备，反向查必然查不到（那是正常的 ✓）
let _secReverse = new Map();
function camSecrets() {
  try {
    const st = fs.statSync(CAM_SECRET_FILE);
    if (_secCache && st.mtimeMs === _secMtime) return _secCache;   // 热加载：文件没变就用缓存
    _secCache = JSON.parse(fs.readFileSync(CAM_SECRET_FILE, 'utf8'));
    _secMtime = st.mtimeMs;
    _secReverse = new Map();
    Object.entries(_secCache.devices || {}).forEach(([id, s]) => { if (s) _secReverse.set(s, id); });
    log(`cam 密钥表已加载: ${_secReverse.size} 台一机一密设备, `
      + `已吊销 ${(_secCache.revoked || []).length} 台, `
      + `requireSecretOnUpload=${!!_secCache.requireSecretOnUpload}`);
  } catch (e) {
    if (!_secCache) {
      _secCache = { devices: {}, fallbackSecret: CAM_DEVICE_SECRET, requireSecretOnUpload: false };
      log(`cam 密钥表读取失败(${e.message})，用公共密钥兜底`);
    }
  }
  return _secCache;
}
// 某台设备应该用的密钥：表里有就用表里的，没有就用公共兜底
// ★★ 已吊销的设备返回 ''（= 永不匹配）—— **绝不回落公共兜底** ✗✗
//   否则"吊销"会变成"降级成公共密钥"，等于吊销失效（同族：删掉 ≠ 停用）
function deviceSecretOf(deviceId) {
  const c = camSecrets();
  if (deviceId && (c.revoked || []).indexOf(deviceId) >= 0) return '';
  return ((c.devices || {})[deviceId]) || c.fallbackSecret || CAM_DEVICE_SECRET;
}
// 定长比较（避免按字符提前返回；密钥是 32 字节随机，这点加固很便宜）
function secretEq(a, b) {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  if (x.length !== y.length) return false;
  try { return crypto.timingSafeEqual(x, y); } catch (e) { return false; }
}
function checkDeviceSecret(secret, deviceId) {
  if (!secret) return false;
  const want = deviceSecretOf(deviceId);
  if (want && secretEq(secret, want)) return true;
  // ★★ 兼容窗口（dual）：新密钥刚发放、而设备还在发公共兜底的那段时间，**公共兜底也认** ✓
  //   为什么必须要它 ✗：一发放就只认它自己的密钥 ⇒ 还没升级到"会领取"的固件立刻 401、彻底不能用 ✗
  //   而设备要能领取又必须先有新固件 ⇒ 先有鸡还是先有蛋
  //   ⇒ S2 发放时记一个到期时间，到期后公共兜底对该设备彻底失效（那才是一机一密真正生效的时刻 ✓）
  const c = camSecrets();
  const until = c.dual && c.dual[deviceId];
  if (until && Date.now() < until) {
    const fb = c.fallbackSecret || CAM_DEVICE_SECRET;
    if (fb && secretEq(secret, fb)) return true;
  }
  return false;
}
/** 按密钥反查设备号（查不到返回 '' —— 共用公共密钥的设备查不到，属正常） */
function deviceIdBySecret(secret) {
  if (!secret) return '';
  camSecrets();                                  // 确保索引是新的
  return _secReverse.get(String(secret)) || '';
}
/**
 * 凭据种类（★ 只有"领取密钥"那个端点用得上）：
 *   'own'      自己的密钥（正常状态）
 *   'prev'     轮换宽限期的**旧**密钥 —— ★ 只允许用来"重新领取"，不能用于任何其它接口 ✓
 *   'fallback' 公共兜底密钥（还没一机一密的设备，首次领取走这条）
 *   其他 ⇒ ''（拒绝）
 */
function credentialKind(secret, deviceId) {
  if (!secret) return '';
  const c = camSecrets();
  if (deviceId && (c.revoked || []).indexOf(deviceId) >= 0) return '';   // 已吊销 ⇒ 连领取都不行
  if (deviceId && (c.devices || {})[deviceId] && secretEq(secret, c.devices[deviceId])) return 'own';
  if (deviceId && (c.prev || {})[deviceId] && secretEq(secret, c.prev[deviceId])) return 'prev';
  if (c.fallbackSecret && secretEq(secret, c.fallbackSecret)) return 'fallback';
  if (!c.fallbackSecret && secretEq(secret, CAM_DEVICE_SECRET)) return 'fallback';
  return '';
}
// 从 请求头 / query / body 三处取凭据（阶段2 过渡期三处都认）
function pickSecret(req, q, o) {
  return String(req.headers['x-device-secret'] || (q && q.secret) || (o && o.secret) || '').trim();
}
function pickDeviceId(req, q, o) {
  // ★★ 2026-10-19：**同时认 `dev_id`** ✗ —— CR-20261019-01 的契约写的是 `dev_id`
  //   （手表按规格书传 `dev_id` ✓ 而这里原来只认 `device_id` ⇒ **取不到设备号** ✓
  //     后果：`pull` 报 400 或按密钥兜底 ✓ 报错信息还是"缺少 dev_id" ✓ 很难查 ✓）
  //   ★ 判据：**契约名以规格书为准** ✗ —— 实现去适配契约，别让设备端改 ✓
  const explicit = String(req.headers['x-device-id']
    || (q && (q.device_id || q.dev_id))
    || (o && (o.device_id || o.dev_id)) || '').trim();
  if (explicit) return explicit;
  // ★ CR-20261009-01：没给设备号 ⇒ **用密钥反查**（一机一密后每台密钥唯一 ⇒ 能认出来 ✓）
  //   认不出来就返回 ''（= 保持老行为：按公共兜底校验、SOP 不过滤）⇒ 零回归 ✓
  return deviceIdBySecret(pickSecret(req, q, o));
}

/* ══════════ CR-20261009-01：密钥表从 S2 同步 ══════════
 * ★ 为什么是 S1 拉、不是 S2 推：S2 是唯一真源（后台在那里改），S1 只需定期同步；
 *   拉了就写本地文件 ⇒ 复用上面那套【按 mtime 热加载】的现成逻辑，校验路径一行不用改 ✓
 * ★ 三条纪律：
 *   ① **只在内容变化时才写盘**（写了 mtime 就变 ⇒ 每 5 分钟刷一行重载日志）
 *   ② **公共兜底密钥不外传**：同步只覆盖 devices/prev/revoked，
 *      fallbackSecret / requireSecretOnUpload 是 S1 本地策略，保持不动 ✓
 *   ③ **拉取失败绝不清空**（继续用现有表）—— 否则 S2 抖一下，全机队认证就崩 ✗
 */
// ★ 兜底周期从 5 分钟收紧到 60 秒：正常路径是后台改完**主动通知重载**
//   （见 /api/cam/secrets-reload），这个周期只是"万一通知失败"的兜底
//   ⇒ 越短越安全，代价只是每分钟一次内部 GET（可忽略 ✓）
//   ★ 动因：实测"revoke 后 1 秒再打，S1 还按老表放行" ⇒ 吊销有 5 分钟窗口，不能接受 ✗
const SEC_SYNC_MS = 60 * 1000;
// ★ CR-20261010-01：语音命令总开关 —— **声明必须在这里**（就在 syncCamSecrets 上方 ✓）
//   ⚠️ 别挪到下面那个大注释块里 ✗：`syncCamSecrets` 在下面会读写它，
//      而 `let` 在**模块求值到那一行之前**是 TDZ ⇒ 一旦哪天有人在启动早期同步密钥就 ReferenceError ✓
//      （现在只是"因为模块先求值完"才侥幸成立 ✗ —— 靠巧合的时序不是设计 ✓）
let VOICE_CMD_ENABLED = false;
let _secSyncAt = 0, _secSyncing = false;
// ★★ 为什么要单独记一个"同步状态"：第一版**只在内容变化时打日志**，
//   结果"同步成功但内容没变"完全看不见 —— 日志里只剩一条失败，
//   读日志的人（我自己）当场误判成"密钥表从来没同步成功" ✗✗
//   ⇒ 状态迁移（never→ok / ok→fail / 内容变化）都要留痕，"成功"与"从没跑过"必须分得开 ✓
let _secSyncState = 'never';
function syncCamSecrets(force) {
  if (_secSyncing) return Promise.resolve(false);
  if (!force && Date.now() - _secSyncAt < SEC_SYNC_MS) return Promise.resolve(false);
  _secSyncing = true;
  return fetch(`${CAM_S2_DBAPI}/api/cam-secrets`, {
    headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}` }
  }).then(r => {
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return r.json();
  }).then(d => {
    let cur = null;
    try { cur = JSON.parse(fs.readFileSync(CAM_SECRET_FILE, 'utf8')); } catch (e) {}
    const same = cur && JSON.stringify([cur.devices || {}, cur.prev || {}, cur.revoked || [], cur.dual || {}])
      === JSON.stringify([d.devices || {}, d.prev || {}, d.revoked || [], d.dual || {}]);
    const nDev = Object.keys(d.devices || {}).length, nRev = (d.revoked || []).length;
    const nDual = Object.keys(d.dual || {}).length;
    if (!same) {
      const next = Object.assign({}, cur || {}, {
        version: 2,
        devices: d.devices || {},
        prev: d.prev || {},
        revoked: d.revoked || [],
        // ★ 新密钥的"兼容窗口"（见 checkDeviceSecret）：到期后公共兜底对该设备彻底失效 ✓
        dual: d.dual || {},
        // ★ 本机策略，不被 S2 覆盖
        fallbackSecret: (cur && cur.fallbackSecret) || CAM_DEVICE_SECRET,
        requireSecretOnUpload: cur ? !!cur.requireSecretOnUpload : true,
        synced_at: new Date().toISOString(),
        updated: d.updated || (cur && cur.updated) || '',
      });
      fs.writeFileSync(CAM_SECRET_FILE, JSON.stringify(next, null, 2), { mode: 0o600 });
      log(`cam 密钥表已更新: 一机一密 ${nDev} 台 / 兼容窗口 ${nDual} 台 / 已吊销 ${nRev} 台`);
    } else if (_secSyncState !== 'ok') {
      // 内容没变但这是第一次成功（或刚从失败里恢复）⇒ 必须留一行，否则分不清"没变"和"没跑" ✗
      log(`cam 密钥表同步正常（内容无变化）: 一机一密 ${nDev} 台 / 兼容窗口 ${nDual} 台 / 已吊销 ${nRev} 台`);
    }
    _secSyncState = 'ok';
    _secSyncAt = Date.now();
    // ★ CR-20261010-01：顺带更新【语音命令】总开关（搭这条 60 秒同步的车 ✓ 不新增机制 ✓）
    //   ★ 缺省/读不到 ⇒ **一律当关** ✗（fail-closed ✓ 绝不因为拿不到就默认打开 ✓）
    //   ★ 状态迁移才打日志 —— "成功但没变"不该每分钟刷屏 ✓（同 _secSyncState 那套判据 ✓）
    const vc = !!(d.flags && d.flags.voice_cmd_enabled);
    if (vc !== VOICE_CMD_ENABLED) log(`cam 语音命令开关: ${VOICE_CMD_ENABLED ? '开' : '关'} → ${vc ? '开' : '关'}`);
    VOICE_CMD_ENABLED = vc;
    return true;
  }).catch(e => {
    _secSyncState = 'fail';
    log(`cam 密钥表同步失败（继续用现有表，不清空）：${e.message}`);
    _secSyncAt = Date.now() - SEC_SYNC_MS + 30000;   // 30 秒后重试
    return false;
  }).finally(() => { _secSyncing = false; });
}
setInterval(() => { syncCamSecrets(false); }, SEC_SYNC_MS).unref();
// ★ 启动后**尽快**同步（2 秒，原来是 20 秒）：
//   S1 重启后到首次同步之间用的是**本地旧表** —— 那段时间里
//   "刚被吊销的设备"仍会被放行（fail-open，危险方向 ✗）。
//   本地表只是"S2 挂了时的兜底"，不该在启动后长时间当权威用 ⇒ 尽早拉一次 ✓
setTimeout(() => { syncCamSecrets(true); }, 2 * 1000).unref();

/* ══════════ CR-20261009-01：OTA（S1 只是代理，**决策只在 S2 一处** ✓）══════════
 * ★★ 为什么不把版本比较/灰度决策抄一份到 S1：那样就有两份必须永远一致的实现 ✗
 *   （刚做完的 SOP 判定提示词就是栽在这上面：两边一飘，同一个输入两个结果）
 *   ⇒ S1 把 (device_id, ver) 转给 S2 的 `/api/cam-ota`，S2 用 ota-lib.decide() 出结论；
 *     S1 只做协议翻译（把"无更新"翻成 **204 空 body**）✓
 *   ⇒ S1 因此**不需要**自己拉策略表 —— 别顺手加一个没人调的 otaPolicy()（死代码 ✗）
 */
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

// ★★★ 多轮上下文表：device -> { msgs: [{role,content}...], at: 最后活跃时刻 }
//
// ★★★ 必须放在**模块级**（2026-10-09 修的一个真 bug，且是"多轮上下文从来没生效"这个级别的 ✗）：
//   它原来声明在**每个请求都会执行一次的 handler 函数内部** ✗ ——
//   于是**每次请求都新建一张空表** ⇒
//     · 写回的上下文，下一个请求**永远看不到** ⇒ **多轮对话完全失效** ✗✗
//     · 响应里的 `session`（= 上下文深度）**恒为 #1** ✗
//   设备端 2026-09-29 报的「`session` 在 reset=0 时也不前进（三轮全 #1）」**根因就是它** ✓
//   （他们当时猜了两种可能，②「多轮上下文其实没有真正保留」**猜对了** ✓）
//   ★ 判据：**"每请求一次的局部变量"不能承担跨请求的状态** ——
//     这类错误不报错、不崩，只是"记忆"永远为空 ✓（靠读响应看不出来，必须看变量的作用域 ✓）
const _chatSessions = new Map();

// ══════════════════════════════════════════════════════════════════════
// ★ CR-20261010-01：语音直接下命令（说「开始录制」就开录）
//
//   ★★ 为什么逻辑整条放这里（而不是"服务器推一条 start_record 指令"）✗：
//      设备**播放期间收不到远程指令**（语音流程占住 cmd_task ✓ 长回答能占几十秒 ✓）
//      ⇒ 推指令那条路会被拖到"播完之后" ✗ 用户体感"喊了半天没动" ✓
//      ⇒ 所以用**响应里带 action**（与 `reply` 同一趟回来 ✓ 零额外往返 ✓）
//
//   ★★★ 2026-10-12 **用户改设计**（推翻原先的「一句话陈述 + 一次确认」✗）：
//     · **情况一**：说「开始录制」⇒ **直接开录** ✓ **不播报任何内容** ✗
//       全部用**默认** ✓ **SOP 默认不选** ✓
//     · **情况二**：说「开始录制 仪容仪表自查」⇒ 按**指定那条 SOP** 开录 ✓
//     · ★★ **两种情况都直接开录** ✗ —— **不需要用户回任何话** ✓
//   用户原话：「总之，不管是哪种情况，都不需要用户做回复，直接开始录制」✓
//
//   ★ 因此本文件里的**状态机整套撤掉** ✗（原来的：提议 `ask_confirm` → 等 8 秒 →
//     「好」确认 / 「不用了」取消 / 「改只录音」改参数 ✓ 全部不要了 ✓）
//     ⇒ 命令判定从此**不需要跨轮状态** ✓（只靠这一句的词表 ✓ 更简单、更不容易错 ✓）
//
//   ★ 「不带 SOP」的语义**变了** ✗（这点很关键 ✓ 见下）：
//     · 旧设计：不带 SOP = 服务器**没听出来/没报** ⇒ 必须反问（绝不猜 ✗）
//     · 新设计：不带 SOP = **用户明确没指定** ⇒ 用默认（不选 SOP）✓ **不是猜** ✓
//     ⇒ ★ 设备端那句 `!has_sop ⇒ 拒绝开录`（依据旧规格书 §4.3 第 6 条）**需要放宽** ✗
//       （已出递件说明 ✓）—— 而"**按名字匹配**"这条仍然**绝不猜** ✓：
//       名字对不上 ⇒ 就当"没有 SOP"处理 ✓ 而不是硬塞一条 ✗
//
//   ★ 命令回合**不叫 LLM** ✗ ⇒ 直接回，最快 ✓（这次连 `reply` 都不拼了 ⇒ **零播报等待** ✓）
// ══════════════════════════════════════════════════════════════════════
const _sopCache = new Map();          // device -> { sops, last, at }  设备可见 SOP（60 秒缓存 ✓）
const SOP_CACHE_MS = 60 * 1000;

// ── 词表（★ 2026-10-12 用户改设计后**只剩两类** ✓ 肯定/否定/改参数三组已撤 ✗）──
const VC_RE = {
  // ★ 命令形状：句首是"开始/来/帮我"这类祈使，且必须落到"录制/录像/录音/记录"
  //   ⚠️ 故意**不认**"录音怎么弄""录制要注意什么"这类问句 ⇒ 用"问句词"做负向过滤 ✓
  start: /^(现在|马上|帮我|帮忙|来|请)?\s*(开始|启动|进行)?\s*(录制|录像|录音|录一段|录一个|录一下|记录拍摄)/,
  stop: /^(现在|马上|帮我|帮忙|请)?\s*(结束|停止|停|别录|不要录|录完|收工|关掉录制)/
};
const VC_ASK_WORDS = /(怎么|如何|为什么|啥|什么|吗|呢|哪|要不要|能不能|可不可以)/;   // 问句 ⇒ 不当命令 ✓

/** 归一化：去掉空白与常见标点（词表匹配前统一处理 ✓） */
function vcNorm(s) { return String(s || '').replace(/[\s，。！？!?、,.：:；;"'“”‘’()（）]/g, ''); }

/** 取"这台设备可见的 SOP 名单"（★ 从 S2 拿，60 秒缓存 ✓；失败 ⇒ 返回 null，调用方一律当"不可用" ✗） */
function fetchSopList(device) {
  const c = _sopCache.get(device);
  if (c && Date.now() - c.at < SOP_CACHE_MS) return Promise.resolve(c);
  return fetch(`${CAM_S2_DBAPI}/api/cam-sop-list?device_id=${encodeURIComponent(device)}`, {
    headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}` }
  }).then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
    .then(d => {
      const v = { sops: (d && d.sops) || [], last: (d && d.last_sop_id) || '', at: Date.now() };
      _sopCache.set(device, v);
      return v;
    })
    .catch(e => {
      log(`语音命令：取 SOP 名单失败（本次按"不可用"处理 ✗，绝不猜 SOP）：${e.message}`);
      return null;
    });
}

/** 在"设备可见名单"里按名字匹配（★ 命中唯一才叫可信 ✓ 命中多条 ⇒ 不可信 ✗） */
function matchSop(sops, text) {
  const t = vcNorm(text);
  const hit = sops.filter(s => { const n = vcNorm(s.name); return n && t.indexOf(n) >= 0; });
  if (hit.length === 1) return { ok: true, sop: hit[0] };
  if (hit.length > 1) return { ok: false, why: '命中多条', hit };
  // 退一步：名字里**包含**用户说到的关键词（≥2 字），同样要求唯一 ✓
  const loose = sops.filter(s => { const n = vcNorm(s.name); return n.length >= 2 && [...new Set(n)].some(ch => t.includes(ch) && n.includes(ch)); });
  return { ok: false, why: hit.length === 0 ? '没听出流程名' : '多义', hit: hit.length ? hit : loose };
}

function vcStartParams(sop) {
  // ★★★ 2026-10-12 用户改设计：**全部默认** ✓、**SOP 默认不选** ✓
  //   ★ 空值就是"默认"，这是**对方代码里写着的**语义（不是我猜的 ✓ 见 `voice.c`）：
  //     · `sop_id` 空 ⇒ 他们 `has_sop = (sop_id[0] != '\0')` = false ⇒ 日志打「（未指定 ✓）」
  //     · `mode`   空 ⇒ 他们日志打「（未给 ⇒ 设备默认 ✓）」+ `rec_mode_from_str(mode, REC_MODE_FULL)`
  //       （★ `downlink.h:49` `REC_MODE_FULL = 0 /* 拍照 + 录音（默认）*/` ⇒ 默认就是 full ✓）
  //     · `shot_interval_ms` = **0** ⇒ 他们注释：「0=指令未带→用设备默认 PHOTO_INTERVAL_MS」✓
  //   ★ 判据：**"默认"必须有对方代码作依据** ✗ —— 拍一个数字过去不叫默认，那叫"我替你定了" ✓
  //   ★★ 字段仍然**全给**（不省略 ✗）：他们规格书 §4.3 第 6 条写着「params 缺失 ⇒ 不发车」✗
  //     ⇒ 给全字段、值留空 ⇒ 既能表达"默认"，又不会踩"缺失"那条 ✓
  return {
    sop_id: sop ? sop.id : '',
    sop_name: sop ? sop.name : '',
    mode: '',                 // ★ 空 = 设备默认（full ✓）
    shot_interval_ms: 0       // ★ 0 = 设备默认（他们 `downlink.h:81` 明写 ✓）· 数字类型 ✓
  };
}

/**
 * ★ 语音命令总入口：**在 ASR 之后、KB/LLM 之前**调 ✓
 * ★★ 2026-10-12 用户改设计：**只有两类命令，且都直接执行、都不播报、都不需要用户回话** ✓
 *   ①「开始录制」           ⇒ `start_record` ＋ **全默认**（SOP 不选 ✓）
 *   ②「开始录制 <SOP 名>」  ⇒ `start_record` ＋ 指定那条 SOP ✓
 *   ★ 名字匹配**不可信时不再反问** ✗ ⇒ 按"没指定 SOP"处理（仍然开录 ✓）——
 *     理由：用户的主诉求是"**开录**" ✓ SOP 只是细化 ✓ 为了细化而**拒绝开录**是本末倒置 ✗
 *     ★ 但仍然**绝不猜** ✗ —— 对不上就当没指定 ✓ 绝不硬塞一条 ✓
 * @returns {null}                              ⇒ 不是命令 ⇒ 走原来的流程（LLM ✓ 且响应里**不加** action ✓）
 *          {reply:'', action, params}          ⇒ 是命令 ⇒ **直接回，不叫 LLM、不合成语音** ✓
 */
async function voiceCmdHandle(device, heard) {
  if (!VOICE_CMD_ENABLED || !device) return null;
  const t = vcNorm(heard);
  if (!t) return null;

  // ── ① 结束录制 ──
  if (VC_RE.stop.test(t) && !VC_ASK_WORDS.test(t)) {
    log(`语音命令: 「${heard}」⇒ 结束录制`);
    return { reply: '结束录制', action: 'stop_record', params: null };
  }

  // ── ② 开始录制（可不带 SOP 名 ✓）──
  if (VC_RE.start.test(t) && !VC_ASK_WORDS.test(t)) {
    // ★ 只有"用户确实报了名字"时才去查名单（省一次 S2 调用 ✓ 也让"没带名字"这条最快 ✓）
    let sop = null, why = '没报流程名 ⇒ 不选 SOP（全默认 ✓）';
    const list = await fetchSopList(device);
    if (list && list.sops.length) {
      // ★ 名单可用 ⇒ 看看用户有没有报名字（只在**该设备可见范围内**匹配 ✓）
      const named = list.sops.some(s => vcNorm(s.name) && t.indexOf(vcNorm(s.name)) >= 0);
      if (named) {
        const m = matchSop(list.sops, t);
        if (m.ok) { sop = m.sop; why = `匹配到「${m.sop.name}」`; }
        else { why = `报了名字但匹配不可信（${m.why}）⇒ 当没指定 ✓（绝不猜 ✗）`; }
      }
    } else {
      why = list ? '该设备可见 SOP 为 0 ⇒ 当没指定 ✓' : 'SOP 名单取不到 ⇒ 当没指定 ✓（仍开录 ✓）';
    }
    const params = vcStartParams(sop);
    log(`语音命令: 「${heard}」⇒ **直接开录**（${why}）`
      + `｜params: sop_id="${params.sop_id}" mode="${params.mode}" shot_interval_ms=${params.shot_interval_ms}`
      + `（空值/0 = 用设备默认 ✓）★ 无播报 ✓`);
    return { reply: '', action: 'start_record', params };   // ★ reply 空 ⇒ 设备不播报任何内容 ✓
  }

  // ── ③ ★ CR-20261012-01 期1：闹钟 / 备忘（★ 这两条**要播报** ⇒ reply 就是"确认"本身 ✓）──
  const extra = extraCmdHandle(heard);
  if (extra) return extra;

  return null;   // ── ④ 不是命令 ⇒ 交给原来的 LLM 流程 ✓ ──
}

// ══════════════════════════════════════════════════════════════════════
// ★★ CR-20261012-01 期1：语音控制的更多操作（**闹钟** / **备忘**）✓
//
//   ★ 架构不变（设备端规格书 §2 ✓）：**服务器判定意图 ⇒ 下发 `action`+`params`+一句 `reply`**
//     ⇒ 设备执行并**把 `reply` 念出来** ✓ —— ★★ 与「开始录制」**刻意不同** ✗：
//        那条用户要求「不播报任何内容」⇒ `reply` 为空 ✓；
//        这条**要靠那句 reply 做确认** ✓（用户 2026-10-12 拍板的口径是"不要用户回话"，
//        不是"不要它说话" ✓ ⇒ **它念、用户不用答** ✓）
//
//   ★★ 时间语义**必须在服务器算成 `day_offset`** ✗（设备端规格书 §3.1 ✓）：
//     · 服务器知道**真实日期**（"明天"是几号 ✓），设备只知道"现在是星期几/第几天" ✓
//     · 设备拿到 offset ⇒ 自己算 `due_dk` ⇒ **仍然只有一处实现** ✓
//     · ⚠️ 设备**时钟没同步（SNTP 没对上）时必须拒绝** ✓（那条在设备侧 ✓）
//
//   ★★★ 实测发现的硬事实（决定了下面的写法 ✗）：
//     **ASR 会把中文数字和阿拉伯数字【混着】给** ✓ —— 同一批测试里：
//       「定个明天九点的闹钟」⇒ `九点`（中文 ✓）
//       「十五分钟后提醒我」 ⇒ `15分钟后`（**阿拉伯** ✗✗）
//     ⇒ ★ **两种形态都必须收** ✓；★ 判据：**别假设 ASR 的数字形态** ✗ —— 跑一遍真实录音再写正则 ✓
//     ★ 另一条：ASR 回来**带句号**（「定个明天九点的闹钟。」✓）⇒ 先做标点归一化 ✓
// ══════════════════════════════════════════════════════════════════════

/** 中文数字（含"两"）→ 整数；纯数字串原样 ✓；认不出返回 null（**绝不猜** ✗）*/
function cnNum(s) {
  const t = String(s || '').trim();
  if (!t) return null;
  if (/^\d+$/.test(t)) return parseInt(t, 10);
  const D = { '零': 0, '〇': 0, '一': 1, '两': 2, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9 };
  if (!/^[零〇一两二三四五六七八九十]+$/.test(t)) return null;
  // 十 / 十X / X十 / X十Y（例：十=10 十五=15 二十=20 二十三=23）
  const i = t.indexOf('十');
  if (i < 0) return t.length === 1 ? D[t] : null;
  const head = t.slice(0, i), tail = t.slice(i + 1);
  const h = head === '' ? 1 : D[head];
  if (h === undefined) return null;
  let m = 0;
  if (tail !== '') { if (tail.length !== 1 || D[tail] === undefined) return null; m = D[tail]; }
  return h * 10 + m;
}
/** 数字类字符集（正则片段 ✓ 中文 + 阿拉伯都收 ✗）*/
const NUM_RE = '[0-9零〇一两二三四五六七八九十]+';

/** 北京时间"现在"（★ 一律 +8h 后用 **UTC getter** 读 ✗ —— 别依赖服务器时区配置 ✓）*/
function bjNow() {
  const d = new Date(Date.now() + 8 * 3600 * 1000);
  return { y: d.getUTCFullYear(), mo: d.getUTCMonth() + 1, day: d.getUTCDate(),
           wd: d.getUTCDay(), h: d.getUTCHours(), m: d.getUTCMinutes() };
}
function bjDayStr(offset) {
  const d = new Date(Date.now() + 8 * 3600 * 1000 + offset * 86400 * 1000);
  return (d.getUTCMonth() + 1) + '月' + d.getUTCDate() + '日';
}
/** "什么时候"的说法 → { dayOffset|null, hh, mm, rep, relMinutes, label }；说不清就 null ✓
 *  ★★ label 的算法（第一版写错过 ✗ 单测当场逮到）：
 *    必须把**所有**"时间类碎片"都摘掉 ✓ —— 第一版只摘了「三点」✗
 *    ⇒ 「提醒我下午三点吃药」的 label 变成了「**下午**吃药」✗
 *    ⇒ 「每天早上七点半叫我」的 label 变成了「**每天早上**」✗
 *    判据：**"剩下的就是 label"这种算法，必须把每一类碎片都列全** ✗
 *    做法：**边匹配边把匹配到的片段收进 `cuts`** ✓ 最后一次性摘 ✓（比"事后清理"可靠 ✓）
 */
function parseWhen(text) {
  let t = vcNorm(text);
  const cuts = [];                                   // ★ 所有"时间类"片段（label = 原文去掉它们 ✓）
  const cut = (s) => { if (s) { cuts.push(s); t = t.replace(s, ' '); } };

  let rep = 'once';
  const rm = t.match(/每(天|日|晚|个工作日|个周末)|工作日|上班日|周末|星期六日|周六日/);
  if (rm) {
    cut(rm[0]);
    rep = /每(天|日|晚)/.test(rm[0]) ? 'daily' : (/工作日|上班日/.test(rm[0]) ? 'workday' : 'weekend');
  }

  // ① 相对时间（"15 分钟后"/"半小时后"）★ 实测 ASR 会给阿拉伯数字 ✗
  let m = t.match(new RegExp('(' + NUM_RE + '|半)\\s*(分钟|分|小时|钟头)\\s*(后|之后|以后)'));
  if (m) {
    const n = m[1] === '半' ? 0.5 : cnNum(m[1]);
    if (n === null || !(n > 0)) return null;
    const mins = /小时|钟头/.test(m[2]) ? Math.round(n * 60) : Math.round(n);
    cut(m[0]);
    return { relMinutes: mins, rep, hh: null, mm: null, dayOffset: null, label: stripWhen(text, cuts) };
  }

  // ② 日偏移（今天/明天/后天…）
  let dayOffset = null;
  const dm = t.match(/(今天|今晚|明天|明晚|后天|大后天)/);
  if (dm) {
    const w = dm[1];
    dayOffset = /大后天/.test(w) ? 3 : /后天/.test(w) ? 2 : /明天|明晚/.test(w) ? 1 : 0;
    cut(w);
  }
  // ③ 上下午（★ 12 小时制 → 24 小时 ✓）
  const pmM = t.match(/(下午|晚上|傍晚|夜里|晚间|中午|上午|早上|早晨|凌晨|一大早)/);
  if (pmM) cut(pmM[0]);
  const pm = /(下午|晚上|傍晚|夜里|晚间)/.test(pmM ? pmM[0] : '');
  const noon = /中午/.test(pmM ? pmM[0] : '');
  const am = /(上午|早上|早晨|凌晨|一大早)/.test(pmM ? pmM[0] : '');

  // ④ 钟点：N点(半|M分)? / N:M
  let hh = null, mm = 0;
  const m2 = t.match(new RegExp('(' + NUM_RE + ')\\s*[点時时]\\s*(半|(' + NUM_RE + ')\\s*分?)?'));
  const m3 = t.match(/(\d{1,2})\s*[:：]\s*(\d{1,2})/);
  if (m3) { hh = parseInt(m3[1], 10); mm = parseInt(m3[2], 10); cut(m3[0]); }
  else if (m2) {
    hh = cnNum(m2[1]);
    if (hh === null) return null;
    if (m2[2] === '半') mm = 30;
    else if (m2[3] !== undefined && m2[3] !== '') { const v = cnNum(m2[3]); if (v === null || v > 59) return null; mm = v; }
    cut(m2[0]);
  }
  if (hh === null && !noon) return null;
  if (noon && hh === null) hh = 12;
  if (noon && hh !== null && hh < 12) hh = 12;                  // 中午十二点 ✓
  if (pm && hh < 12) hh += 12;                                   // 下午三点 ⇒ 15 ✓
  if (am && hh === 12) hh = 0;                                   // 凌晨十二点 ⇒ 0 ✓
  if (hh > 23 || mm > 59) return null;
  return { dayOffset, hh, mm, rep, relMinutes: 0, label: stripWhen(text, cuts) };
}
/** 从**原文**里摘掉所有时间类片段 + 触发词，剩下的当 label ✓（★ 摘不干净就返回空 ✓ 绝不硬凑 ✗）
 *  ★ 用原文而不是被改过的 t ✗ —— 因为 t 里那些片段已经被替换成空格了 ✓（从原文摘才摘得准 ✓）*/
function stripWhen(rawText, cuts) {
  let s = vcNorm(rawText);
  for (const c of (cuts || [])) s = s.split(c).join(' ');
  return s.replace(/(定个|订个|设个|设一个|加个|添加|闹钟|提醒我|提醒|叫我|叫醒我|叫醒|记住|帮我|一下|一个|吧|谢谢|的|要|个|着|了)/g, ' ')
          .replace(/\s+/g, '').trim();
}
/** 数字 → 中文（★ 给 TTS 念更自然 ✓：'9点' 会被念成"九点"多半没事，但"21点"会念成"二十一点"✓
 *  这里统一转中文 ⇒ 念出来最稳 ✓；★ 只处理 0~60 ✓ 够用 ✓）*/
const CN_STR = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九'];
function cnStr(n) {
  n = Number(n) || 0;
  if (n < 10) return CN_STR[n];
  if (n === 10) return '十';
  if (n < 20) return '十' + CN_STR[n - 10];
  const t = Math.floor(n / 10), o = n % 10;
  return CN_STR[t] + '十' + (o ? CN_STR[o] : '');
}
/** ★★ 把一个 24 小时时刻说成**人话** ✓ —— **只此一处** ✗（别处再写一遍必然写歪 ✓）
 *  ★ 为什么要有它：期2 的「取消九点的闹钟」第一版念成了「取消**十五**点的闹钟」✗
 *    中文里 15 点只能念「**下午三点**」✓（说"十五点"像机器 ✓ 用户听着别扭 ✓）
 *  ⇒ 时段分档 + 12 小时制 + 中文数字 ⇒「下午三点」「上午九点半」「晚上八点」✓
 *  ⚠️ 30 分念「半」✓（口语 ✓）；其它分钟念「X 分」✓ */
function clockCn(hh, mm) {
  const part = hh < 6 ? '凌晨' : hh < 12 ? '上午' : hh === 12 ? '中午' : hh < 18 ? '下午' : '晚上';
  const h12 = hh % 12 === 0 ? 12 : hh % 12;
  const s = cnStr(h12) + '点' + (mm === 0 ? '' : (mm === 30 ? '半' : cnStr(mm) + '分'));
  return part + s;
}
/** 闹钟的确认文案 —— ★★ **必须由 params 生成** ✗（"用户确认的对象 = 设备执行的对象" ✓ 老规矩 ✓）
 *  ★★ 措辞照设备端规格书 §5 第 1 条的期望：「**闹钟设在明天九点**」✓（联演时用户听到的就是这句 ✓）
 *  ★ 第一版写错过 ✗：把 20 点说成「下午8点」✗（中文里 20 点只能是"晚上八点"✓）⇒ 现已收进 `clockCn` ✓
 */
function alarmReply(p) {
  const day = p.day_offset === 0 ? '今天' : p.day_offset === 1 ? '明天'
            : p.day_offset === 2 ? '后天' : (cnStr(p.day_offset) + '天后');
  const part = p.hh < 6 ? '凌晨' : p.hh < 12 ? '上午' : p.hh === 12 ? '中午' : p.hh < 18 ? '下午' : '晚上';
  const t = clockCn(p.hh, p.mm).replace(/^(凌晨|上午|中午|下午|晚上)/, '');   // ★ 时段只留一处 ✓
  const repCn = p.rep === 'daily' ? '每天' : p.rep === 'workday' ? '每个工作日' : p.rep === 'weekend' ? '周末' : '';
  const when = repCn ? (repCn + part) : (day + part);
  return '闹钟设在' + when + t + (p.label ? '，提醒你' + p.label : '');
}

/** ★ 意图①：定闹钟（含"提醒我X点做Y" ✓ 也含"N分钟后"✓ 由服务器换算成绝对时刻 ✓）*/
function parseAlarmCmd(heard) {
  const t = vcNorm(heard);
  if (!/(闹钟|叫我|叫醒|提醒)/.test(t)) return null;
  if (VC_ASK_WORDS.test(t)) return null;                      // 问句 ⇒ 不当命令 ✓
  const w = parseWhen(t);
  if (!w) return null;
  const now = bjNow();
  let hh = w.hh, mm = w.mm, dayOffset = w.dayOffset;
  if (w.relMinutes > 0) {                                     // 相对 ⇒ 换成绝对时刻 ✓
    const total = now.h * 60 + now.m + w.relMinutes;
    dayOffset = Math.floor(total / 1440);
    hh = Math.floor((total % 1440) / 60);
    mm = total % 60;
  } else if (dayOffset === null) {
    // ★ 没说哪天 ⇒ **取下一次**（今天这个点还没到就是今天 ✓ 过了就是明天 ✓）
    //   ★ 这条要如实报出来（reply 里会说"今天/明天"✓）⇒ 用户听得出我理解成了哪天 ✓
    dayOffset = (hh * 60 + mm) > (now.h * 60 + now.m) ? 0 : 1;
  }
  const label = (w.label || '').replace(/^[的，,、]+/, '').trim();
  const p = { hh, mm, rep: w.rep || 'once', day_offset: dayOffset };
  if (label) p.label = label;                                  // ★ 没标签就**别带这个字段** ✗（别用空值覆盖 ✓）
  return p;
}

// ★★ 2026-10-12 设备端回执指出的一处真缺陷（**我错了** ✗）：
//   他们 `memo.h` 写的是 `#define MEMO_TEXT_MAX 120`，单位是 **字节** UTF-8（≈40 汉字 ✓）
//   而我第一版按 **"120 字"** 限 ✗ ⇒ **超他们上限 3 倍** ✗（120 汉字 = 360 字节 ✓）
//   ⇒ ★ 判据（同 #101 那一族）：**跨端说长度必须带单位** ✗ —— 同一个 120，
//     "字"和"字节"差了 3 倍 ✓ 而这类不一致**不会报错**，只会被下游**静默截断** ✓
//   ★ 可用上限取 **119**（不是 120 ✗）—— 他们那边是 `char text[MEMO_TEXT_MAX]`，
//     120 里要有 1 字节给结尾 NUL ✓（他们自己的校验也是 `tl >= MEMO_TEXT_MAX ⇒ 拒绝` ✓）
const MEMO_TEXT_MAX_BYTES = 119;
/** 按**字节**安全截断 UTF-8（不切碎半个汉字 ✓ —— 与 SOP 名称那道守卫同一套路 ✓）*/
function clipUtf8(s, maxBytes) {
  let out = String(s || '');
  while (out.length && Buffer.byteLength(out, 'utf8') > maxBytes) out = out.slice(0, -1);
  return out;
}

/** ★ 意图②：记备忘（★ 内容是**去掉引导语之后的那句** ✓ 设备端规格书 §5 第 2 条 ✓）
 *  ★ 超长 ⇒ **我方先按字节截断**（而不是丢给设备去截 ✗）：设备截断时**只打日志** ✓
 *    用户看不到 ✗ ⇒ 由我方截 + **在 reply 里如实说一句** ✓（"太长，后面的没记住"✓）
 *  ⇒ 返回 { text, truncated } ✓ —— ★ `truncated` **不进 params** ✗（别塞协议外的字段 ✓）
 *
 *  ★★ 2026-10-12 端到端实测逮到的一条 ✗：
 *    「记一下**备忘录**要买牛奶」⇒ ASR 给「记一下备忘录。要买牛奶。」⇒ 正文存成
 *    「**备忘录**。要买牛奶」✗（把"备忘录"三个字也当正文存进去了 ✓ 用户会看见 ✓）
 *    ⇒ 修法：正文再去掉**开头**的「备忘录/备忘」这类词 ✓（它们是"这是什么"、不是"内容" ✓）
 *    ★ 判据：**触发词与"宾语名词"要一起摘** ✗ —— 只摘动词（"记一下"✓）会漏掉后面的名词 ✓
 */
function parseMemoCmd(heard) {
  const raw = String(heard || '').replace(/[。！!？?，,、\s]+$/, '');
  // ★★ 2026-10-12 第二条实测缺陷 ✗：只说「**备忘录**」⇒ 被当成"记备忘"，正文成了「**录**」✗
  //   根因：触发词只列到「备忘」✗ ⇒ 剩下的"录"就被当正文 ✓
  //   ⇒ 把「备忘录 / 备忘条」也列为触发词 ✓（它们是同一个词的不同说法 ✓）
  const m = raw.match(/^(?:帮我)?(?:记一下|记一笔|记一条|记住|记录一下|记下|备忘一下|备忘录|备忘条|备忘|添加备忘|新增备忘)[：:，,]?\s*(.+)$/);
  if (!m) return null;
  const text = m[1]
    .replace(/^[：:，,、\s]+/, '')
    .replace(/^(备忘录|备忘条|备忘)[：:，,、。\.\s]*/, '')      // ★ 摘掉开头的宾语名词 ✓
    .trim();
  if (!text) return null;                                      // 没内容 ⇒ 不当作命令 ✓（别塞空备忘 ✗）
  // ★ 正文至少 2 个字符 ✗ —— 1 个字的"备忘"没有意义 ✓ 而且基本都是**切错词**的产物 ✓
  //   （判据：**"看起来像个词"不等于"是个能存的内容"** ✗）
  if (text.length < 2) return null;
  if (VC_ASK_WORDS.test(text) && text.length <= 6) return null; // "记一下怎么弄"这类问句别当备忘 ✓
  const clipped = clipUtf8(text, MEMO_TEXT_MAX_BYTES);
  return { text: clipped, truncated: clipped !== text };
}

// ══════════════════════════════════════════════════════════════════════
// ★★ CR-20261012-01 期2：**删 / 读**（`del_alarm` / `del_memo` / `read_memo`）✓
//
//   ★★★ 一条**顺序铁律**（不遵守必错 ✗✗）：
//     「**取消九点的闹钟**」里也有"闹钟"二字 ✗ ⇒ 期1 的 `parseAlarmCmd` 会把它当**新增** ✗
//     ⇒ 判据：**动词（取消/删/念）优先于名词（闹钟/备忘）** ✗
//     ⇒ 所以 `extraCmdHandle` 里的顺序是：**删闹钟 → 删备忘 → 读备忘 → 定闹钟 → 记备忘** ✓
//       （新增一律排最后 ✓ 因为它只凭名词就能命中 ✓）
//
//   ★ `index` 的基准：规格书 §3.2 写的是 `0 = **最后一条**` ✓（最新那条 ✓）
//     ⇒ 我方按此发 ✓ —— ★ 但"第 N 条"这种说法**我方不发** ✗：
//       数组从哪头数是**设备侧的实现细节** ✓ 我方不知道 ⇒ **绝不猜** ✗（说"第N条"就交给 LLM ✓）
//
//   ★★ `reply` 的**语义**（这条要跟设备端对齐 ✗）：
//     设备是"**先播 reply、后执行**"还是"执行完再播"？按 `voice.c` 的流程 ⇒ `reply` 播在前 ✓
//     ⇒ ★ 所以 reply **不能宣称结果** ✗（"已取消"✗ 万一没这条闹钟就打脸了 ✓）
//     ⇒ 我方一律用**意图口径**：「好，取消九点的闹钟」✓（成没成由**设备自己的**表情/提示去说 ✓）
// ══════════════════════════════════════════════════════════════════════

/** ★ 意图③：取消闹钟 ⇒ `del_alarm {hh,mm}` ✓（★ 只用钟点 ✗）
 *  ★★ 2026-10-12 晚按设备端 `回执4` §2 改 ✗：
 *    · 原来还有一条 `{index:0}`（"取消最后那个闹钟"）✗ —— ★ **删掉了** ✓
 *    · 理由（设备端给的 ✓ 很硬 ✗）：**闹钟表里只有槽位、没有"新旧"** ✓
 *      新建时填的是**第一个空槽** ⇒ ★ **槽位序 ≠ 创建序** ✗
 *      ⇒ ★★ **"最后那个闹钟""第三个闹钟"在设备侧【给不出】稳定含义** ✓
 *    · ⇒ 遇到这种 ⇒ ★ **回一句问清** ✗（不猜 ✗ 也不静默丢弃 ✗ —— 静默丢弃会让用户以为删掉了 ✓）
 *  ★ 判据（设备端原话 ✓ 值得记）：**"第 N 个"必须先定义"从哪头数、按什么排"** ✗ ——
 *    不定死 ⇒ 双方各按各的算 ⇒ ★ **删错一条、两边都觉得自己对** ✓
 *  ★ 要真支持"第 N 个闹钟" ⇒ ★ 那是一条**独立需求**（要设备端加新的排序口径 ✓）⇒ 登记 CR ✓ */
function parseDelAlarmCmd(heard) {
  const t = vcNorm(heard);
  if (!/(闹钟|叫我|叫醒|提醒)/.test(t)) return null;
  if (!/(取消|删掉|删除|去掉|撤销|关掉|不要了|别响)/.test(t)) return null;   // ★ 必须**有删除动词** ✗
  if (VC_ASK_WORDS.test(t)) return null;
  // ① 报了钟点 ⇒ 按 hh:mm 删 ✓（"取消九点的闹钟" ✓ —— ★ 这是我方**唯一**敢发的形态 ✓）
  const w = parseWhen(t);
  if (w && !w.relMinutes && w.hh !== null) return { hh: w.hh, mm: w.mm };
  // ② ★ 说的是"第几个 / 最后那个 / 最新那个" ⇒ **反问** ✗（★ 不猜、也不当没听见 ✓）
  if (/(第\s*[0-9零〇一两二三四五六七八九十]+\s*(个|条)|最后|最新|末尾|刚定|刚才那|上一个)/.test(t)) {
    return { needClarify: true };
  }
  // ③ 其它 ⇒ 不是命令 ✓（交给 LLM ✓）
  return null;
}

/** ★ 意图④：删备忘 ⇒ `del_memo {index:N}`
 *  ★★ 2026-10-12 晚补：**支持"第 N 条"** ✗（设备端 `回执4` §2 给了口径 ✓）
 *    · 口径 = **从新往旧数** ✓ `0` = **最新那条** ✓（设备侧 `memo_delete()` 本来就是这个口径 ✓）
 *    · 口语是 1-based（"第2条"✓）⇒ ★ 转成 `index = N-1` ✗（★ 别把 1-based 直接发出去 ✓）
 *    · 没写序号（"最后一条"✓）⇒ `index:0` ✓
 *  ★ 判据：**口语序号与数组下标差 1 ⇒ 换算必须在一个地方做完** ✗（这里 ✓ 别让设备再猜 ✓）*/
function parseDelMemoCmd(heard) {
  const t = vcNorm(heard);
  if (!/备忘/.test(t)) return null;
  if (!/(删掉|删除|去掉|消掉|清掉|不要了)/.test(t)) return null;
  if (VC_ASK_WORDS.test(t)) return null;
  const m = t.match(new RegExp('第\\s*(' + NUM_RE + ')\\s*(条|个)'));
  if (m) {
    const n = cnNum(m[1]);
    if (n === null || n < 1) return null;
    return { index: n - 1 };                       // ★ 口语 1-based ⇒ 下标 n-1 ✓
  }
  if (!/(最后|最新|末尾|刚记|刚才那|上一条)/.test(t)) return null;   // ★ 没序号也没说"最后" ⇒ 不发 ✗
  return { index: 0 };
}

/** ★ 意图⑤：读备忘 ⇒ `read_memo {index:N}`（设备侧复用 `voice_speak_text()` 念出来 ✓）
 *  ★★ 同样支持"第 N 条" ✓（口径与 `del_memo` **必须一致** ✗ —— 都是**从新往旧数** ✓）*/
function parseReadMemoCmd(heard) {
  const t = vcNorm(heard);
  if (!/备忘/.test(t)) return null;
  if (!/(念|读|说给我听|念念|读一下|念一下)/.test(t)) return null;
  if (/(怎么|如何|在哪|哪里)/.test(t)) return null;              // "备忘怎么用"不是命令 ✓
  if (/(删|去掉|清)/.test(t)) return null;                        // 带删的不当读 ✓（前面已处理 ✓）
  const m = t.match(new RegExp('第\\s*(' + NUM_RE + ')\\s*(条|个)'));
  if (m) {
    const n = cnNum(m[1]);
    if (n === null || n < 1) return null;
    return { index: n - 1 };
  }
  return { index: 0 };                                            // ★ 没说序号 ⇒ 最新那条 ✓
}

// ══════════════════════════════════════════════════════════════════════
// ★★ CR-20261012-01 期3：**倒计时**（`set_timer` / `stop_timer`）✓
//
//   ★ 与期1「定闹钟」的边界（**刻意不混** ✗）：
//     · 带「**倒计时 / 计时**」字样的 ⇒ 走 `set_timer` ✓（例：「定个五分钟的倒计时」✓）
//     · 「**N 分钟后提醒我**」⇒ ★ **仍然走 `add_alarm`** ✗（期1 已定的口径 ✓ 设备端已验过 ✓）
//       ⇒ 理由：那条是"到点提醒"（进闹钟列表 ✓ 界面上是一条 15:41 的闹钟 ✓）
//         而倒计时是**看着剩余时间走**✓ —— 两者语义不同 ✓ 别混 ✓
//       ⚠️ 但用户可能更想要后者 ⇒ ★ 这条**留给用户拍板** ✗（改起来是一行 ✓ 不擅自改 ✗）
//
//   ★★ 又一处**顺序陷阱**（与期2 同族 ✗）：「**取消倒计时**」里也有"倒计时" ✗
//     ⇒ 必须先判 **stop** 再判 **set** ✓（动词优先 ✓ 见 `extraCmdHandle` ✓）
//
//   ★ `seconds` 的上下限：设备端口径**还没给** ✗（回执里问了 ✓）
//     ⇒ 我方先按 **1 秒 ~ 24 小时** ✓ 并把界限**收在一个常量里** ✓（他们一答就改一行 ✓）
//     ★ 越界 ⇒ **不下发** ✗ ＋ 说清楚 ✓（★ 绝不像闹钟那样"按一个我自己挑的时长定下去" ✗）
// ══════════════════════════════════════════════════════════════════════
const TIMER_MIN_SEC = 1;
const TIMER_MAX_SEC = 24 * 3600;      // ★ 上限先拍 24 小时 —— 等设备端口径 ✓

/** 秒数说成人话 ✓（★ 与 `clockCn` 同一路数：**只此一处** ✗ 别处再写必然写歪 ✓）
 *  ⚠️ 分钟要用 **floor** 不是 round ✗✗：90 秒 = 1 分 30 秒 ✓
 *     第一版写 `Math.round(sec%3600/60)` ⇒ 1.5 被四舍五入成 **2** ⇒ 「二分三十秒」✗（单测逮到 ✓）
 *  ★ 有余秒时优先说"X 分 Y 秒" ✓（比"二分钟"精确 ✓ 用户能对上 ✓）*/
function durCn(sec) {
  sec = Math.round(Number(sec) || 0);
  if (sec < 60) return cnStr(sec) + '秒';
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), r = sec % 60;
  if (h && (m || r)) return cnStr(h) + '小时' + (m ? cnStr(m) + '分钟' : '') + (r ? cnStr(r) + '秒' : '');
  if (h) return cnStr(h) + '小时';
  if (r) return cnStr(m) + '分' + cnStr(r) + '秒';        // 90 ⇒ 一分三十秒 ✓
  if (m === 30) return '半小时';
  return cnStr(m) + '分钟';
}

/** ★ 意图⑥：定倒计时 ⇒ `set_timer {seconds[, label]}` ✓
 *  ★ 形态：「定个五分钟的倒计时」/「倒计时五分钟」/「计时十分钟」/「定个五分钟的倒计时，煮鸡蛋」✓
 *  ★ 时长数字**复用同一个 `NUM_RE` + `cnNum`** ✓（★ 不另写词表 ✗ —— 实测 ASR 中文/阿拉伯混着来 ✓）
 *  ⚠️ 两个**单测逮到**的坑 ✗：
 *    ① 「**一个半小时**」必须**先**判 ✗ —— 否则通用正则先匹配到「半小时」⇒ 只算 0.5h ✗（5400 变 1800 ✓）
 *    ② label 里必须把「**倒计时/计时/定时**」也摘掉 ✗ —— 否则回复会念成
 *       「好，五分钟倒计时开始了，**倒计时**」✗（第一版就是这样 ✓ 荒唐但很难自己看出来 ✓）
 */
function parseSetTimerCmd(heard) {
  const t = vcNorm(heard);
  if (!/(倒计时|计时|定时)/.test(t)) return null;
  if (VC_ASK_WORDS.test(t)) return null;                        // "倒计时怎么用"不是命令 ✓
  if (/(取消|停|停止|关掉|结束|不要了)/.test(t)) return null;    // 带"停/取消"的交给 stop ✓
  const cuts = [];
  let sec = null;
  // ① 先判「N 个半小时」✗（必须在通用式之前 ✓）
  const mh = t.match(new RegExp('(' + NUM_RE + ')\\s*个?\\s*半\\s*(个)?\\s*(小时|钟头)'));
  if (mh) {
    const n = cnNum(mh[1]);
    if (n === null) return null;
    sec = Math.round(n * 3600) + 1800;
    cuts.push(mh[0]);
  } else {
    // ② 通用：N(个)(天|小时|分钟|分|秒) / 半小时
    //   ★ 单位里**必须有"天"** ✗ —— 不是为了让两天能用（上限 24h ✓ 它会被下面的范围检查拦 ✓）
    //     而是为了**让越界由我方来说** ✓：少了它，「定个两天的倒计时」⇒ 压根不算倒计时命令 ✗
    //     ⇒ 交给 LLM 随口答 ✓（端到端实测就是这样 ✓ 效果不差、但**口径不确定** ✓）
    //     ★ 判据：**该由我拦的输入，必须先进我的解析器** ✗ —— 进不来就谈不上"拦" ✓
    const m = t.match(new RegExp('(' + NUM_RE + '|半)\\s*个?\\s*(天|小时|钟头|分钟|分|秒)'));
    if (!m) return null;
    const n = m[1] === '半' ? 0.5 : cnNum(m[1]);
    if (n === null || !(n > 0)) return null;
    const unit = m[2];
    sec = /天/.test(unit) ? Math.round(n * 86400)
        : (/小时|钟头/.test(unit) ? Math.round(n * 3600) : (/秒/.test(unit) ? Math.round(n) : Math.round(n * 60)));
    cuts.push(m[0]);
    // ★「一小时**半**」这种（不是"N个半小时"）⇒ 补上 ✓
    if (!/半/.test(m[1]) && /小时|钟头/.test(unit) && /半/.test(t)) sec += 1800;
  }
  if (sec === null) return null;
  if (sec < TIMER_MIN_SEC || sec > TIMER_MAX_SEC) return { outOfRange: true, seconds: sec };
  // ③ label = 原文去掉时长 + 去掉触发词 ✓（★ 触发词不摘 ⇒ 会念出"…开始了，倒计时"✗）
  const label = stripWhen(heard, cuts.concat(['倒计时', '计时', '定时']))
    .replace(/^(的|，|,|、)+/, '').replace(/(的|，|,|、)+$/, '').trim();
  const p = { seconds: sec };
  if (label) p.label = label;                                    // ★ 没标签就不带这个字段 ✗
  return p;
}

/** ★ 意图⑦：停倒计时 ⇒ `stop_timer`（无参数 ✓）*/
function parseStopTimerCmd(heard) {
  const t = vcNorm(heard);
  if (!/(倒计时|计时)/.test(t)) return null;
  if (!/(取消|停掉|停止|停|关掉|结束|不要了|别响)/.test(t)) return null;
  if (VC_ASK_WORDS.test(t)) return null;
  return { stop: true };
}

/** 把新意图接进总入口 ✓（★ 顺序见上面那条铁律 ✗ 动词优先 ✓）*/
function extraCmdHandle(heard) {
  // ── ① 先判**删闹钟**（"取消九点的闹钟"必须在这里被吃掉 ✗ 晚一步就被当成新增 ✓）──
  const da = parseDelAlarmCmd(heard);
  if (da) {
    // ★★ "第几个/最后那个闹钟" ⇒ **反问** ✗（设备端口径：闹钟只有槽位、没有新旧 ✓
    //   ⇒ 「最后那个」在设备侧**没有稳定含义** ✓ ⇒ ★ 绝不猜 ✓ 但也**不当没听见** ✗
    //   ★ 为什么必须回一句 ✗：静默丢弃会让用户**以为已经删掉了** ✓（比"做不到"更糟 ✓）
    if (da.needClarify) {
      const reply = '闹钟是按钟点记的，你说的那一个是几点的？';
      log(`语音命令: 「${heard}」⇒ del_alarm **口径不足 ⇒ 反问** ✗｜reply="${reply}"`
        + `（★ 闹钟表只有槽位没有新旧 ⇒ "第几个/最后那个"无稳定含义 ✓ 不猜 ✗）`);
      return { reply, action: null, params: null };
    }
    // ★ 用共用的 clockCn ✓ —— 第一版直接 `cnStr(hh)+'点'` ⇒ 15 点被念成「十五点」✗（应为"下午三点"✓）
    const reply = '好，取消' + clockCn(da.hh, da.mm) + '的闹钟';
    log(`语音命令: 「${heard}」⇒ del_alarm ${JSON.stringify(da)}｜reply="${reply}"`);
    return { reply, action: 'del_alarm', params: da };
  }
  // ── ② 删备忘 ──
  const dm = parseDelMemoCmd(heard);
  if (dm) {
    // ★ 口径 = 从新往旧数 ✓（`index:0` = 最新 ✓）；有"第 N 条"时也要念清是哪条 ✗
    const reply = dm.index === 0 ? '好，删掉最后一条备忘'
                                 : ('好，删掉最新往前的第' + cnStr(dm.index + 1) + '条备忘');
    log(`语音命令: 「${heard}」⇒ del_memo ${JSON.stringify(dm)}｜reply="${reply}"`);
    return { reply, action: 'del_memo', params: dm };
  }
  // ── ③ 读备忘（★ reply 要**极短** ✗：设备紧接着会把备忘正文念出来 ✓ 别抢话 ✓）──
  const rm = parseReadMemoCmd(heard);
  if (rm) {
    const reply = '好';
    log(`语音命令: 「${heard}」⇒ read_memo ${JSON.stringify(rm)}｜reply="${reply}"（★ 正文由设备念 ✓ 我方不抢话 ✓）`);
    return { reply, action: 'read_memo', params: rm };
  }
  // ── ④ ★ 期3：**先判"停倒计时"、再判"定倒计时"** ✗（"取消倒计时"里也有"倒计时" ✓ 同一条铁律 ✓）──
  const stp = parseStopTimerCmd(heard);
  if (stp) {
    const reply = '好，取消倒计时';
    log(`语音命令: 「${heard}」⇒ stop_timer（无参数 ✓）｜reply="${reply}"`);
    return { reply, action: 'stop_timer', params: null };
  }
  const st = parseSetTimerCmd(heard);
  if (st) {
    if (st.outOfRange) {
      // ★★ 越界 ⇒ **不下发** ✗ ＋ 说清楚 ✓ —— 绝不按一个我自己挑的时长定下去 ✗
      //   （对比："定闹钟"那条也有越界，但那边设备会拒绝 ✓；倒计时这边**先由我拦住** ✓ 省一趟 ✓）
      const reply = '倒计时' + (st.seconds > TIMER_MAX_SEC ? '最长只能定一天，这个我没法定' : '太短了，这个我没法定');
      log(`语音命令: 「${heard}」⇒ set_timer 越界（${st.seconds}s，界限 ${TIMER_MIN_SEC}~${TIMER_MAX_SEC}s）⇒ **不下发** ✗｜reply="${reply}"`);
      return { reply, action: null, params: null };
    }
    const reply = '好，' + durCn(st.seconds) + '倒计时开始了' + (st.label ? '，' + st.label : '');
    log(`语音命令: 「${heard}」⇒ set_timer ${JSON.stringify(st)}｜reply="${reply}"`);
    return { reply, action: 'set_timer', params: st };
  }
  // ── ⑤ 定闹钟 / 记备忘（**新增**一律排最后 ✓ 它们只凭名词就能命中 ✗）──
  const a = parseAlarmCmd(heard);
  if (a) {
    const reply = alarmReply(a);
    log(`语音命令: 「${heard}」⇒ add_alarm ${JSON.stringify(a)}｜reply="${reply}"（★ 由 params 生成 ✓ 同源 ✓）`);
    return { reply, action: 'add_alarm', params: a };
  }
  const memo = parseMemoCmd(heard);
  if (memo) {
    // ★★ reply 由 params **同源生成** ✓ —— 而且**超长时如实说** ✗：
    //   设备截断只打日志、用户看不到 ✓ ⇒ 我方先截（≤119 字节 ✓）＋ 这句提示 ✓
    //   ⇒ 用户听到的"记下了什么" = 实际存进去的东西 ✓（不会出现"说的和存的不一样" ✓）
    const reply = memo.truncated
      ? '有点长，我只记住了前半句：' + memo.text
      : '好，记住了：' + memo.text;
    const params = { text: memo.text };
    log(`语音命令: 「${heard}」⇒ add_memo ${JSON.stringify(params)}`
      + `${memo.truncated ? `（★ 原文 ${Buffer.byteLength(String(heard), 'utf8')} 字节 ⇒ 按设备上限 ${MEMO_TEXT_MAX_BYTES} 字节截断 ✓）` : ''}`
      + `｜reply="${reply}"`);
    return { reply, action: 'add_memo', params };   // ★ truncated **不进 params** ✗（协议外字段一律不加 ✓）
  }
  return null;
}

// ══════════════════════════════════════════════════════════════════════
// ★★★ CR-20261012-03 §9 期0：**影子模式**（shadow）—— 只记录，绝不执行 ✓
//
//   ★ 目的：在**不改线上任何行为**的前提下，拿到"**模型会怎么判**" vs "**现在代码怎么判**"的真实数据 ✓
//     ⇒ 再决定"哪些交给模型、哪些必须留确定性" ✓ 而不是凭感觉全换 ✗
//
//   ★★★ 三条红线（违反任何一条都会伤到线上 ✗）：
//     ① **绝不 await** ✓ —— fire-and-forget，请求该多快还多快 ✓
//     ② **绝不改任何返回值** ✓ —— 只读 `heard` 与"已知的旧判定" ✓ 不碰响应/设备状态 ✓
//     ③ **抛错只吞** ✓ —— 影子坏了不能影响主流程 ✓（整个函数体包 try/catch ✓）
//
//   ★ 成本控制：滑动窗口限频（`SHADOW_MAX_PER_MIN` ✓ 默认 30 ✓）
//     ★★ 被丢弃的调用要**计数并打日志** ✗ —— 否则"样本被限频削过"会让指标**系统性失真** ✗
//
//   ★ 开关：`SHADOW_MODE=on`（默认 **off** ✓ ⇒ 关掉即回退，不用改代码 ✓）
//   ★ 日志格式：`SHADOW {json}` 一行 ✓（给 `shadow-report.js` 聚合 ✓ 别改字段名 ✗）
// ══════════════════════════════════════════════════════════════════════
const SHADOW_MODE = process.env.SHADOW_MODE === 'on';
const SHADOW_MODEL = process.env.SHADOW_MODEL || 'deepseek-chat';
const SHADOW_MAX_PER_MIN = Math.max(1, parseInt(process.env.SHADOW_MAX_PER_MIN || '30', 10));
const SHADOW_TIMEOUT_MS = parseInt(process.env.SHADOW_TIMEOUT_MS || '9000', 10);
// ★★ 专用样本文件 ✗（理由见下面 appendFileSync 那段注释：日志会被 deploy.sh 清空 ✓）
const SHADOW_FILE = process.env.SHADOW_FILE || '/tmp/shadow.jsonl';
// ★ 工具表 = 规格书 §5 ✓：★★ 参数**只填原话片段**（`time_text` / `duration_text`）✗
//   探针实测：让模型填结构化数字 ⇒ 它会自己造名字（`set_alarm(time,date)` ✗）⇒ 必须只给"原话槽位" ✓
const SHADOW_TOOLS = [
  { type: 'function', function: { name: 'search_kb', description: '查店里的 SOP / 标准 / 流程 / 规定（问"怎么做""标准是什么"时用）',
    parameters: { type: 'object', properties: { query: { type: 'string', description: '检索词' } }, required: ['query'] } } },
  { type: 'function', function: { name: 'start_record', description: '让设备开始录制（用户说开始录制 / 开始录XX）',
    parameters: { type: 'object', properties: { sop_text: { type: 'string', description: '用户说的 SOP 名字原话，没提就不填' } } } } },
  { type: 'function', function: { name: 'stop_record', description: '让设备停止录制', parameters: { type: 'object', properties: {} } } },
  { type: 'function', function: { name: 'set_alarm', description: '定闹钟 / 到点提醒',
    parameters: { type: 'object', properties: { time_text: { type: 'string', description: '时间原话，如"明天九点""15分钟后"' }, label: { type: 'string', description: '提醒什么事，可空' } }, required: ['time_text'] } } },
  { type: 'function', function: { name: 'cancel_alarm', description: '取消闹钟',
    parameters: { type: 'object', properties: { time_text: { type: 'string', description: '时间原话；说"最后那个"就别填' }, which: { type: 'string', enum: ['last'], description: '指"最后一个"时填 last' } } } } },
  { type: 'function', function: { name: 'add_memo', description: '记一条备忘',
    parameters: { type: 'object', properties: { text: { type: 'string', description: '要记的内容本身' } }, required: ['text'] } } },
  { type: 'function', function: { name: 'delete_memo', description: '删备忘（只支持最后一条）',
    parameters: { type: 'object', properties: { which: { type: 'string', enum: ['last'] } } } } },
  { type: 'function', function: { name: 'read_memo', description: '念一条备忘（只支持最后一条）', parameters: { type: 'object', properties: {} } } },
  { type: 'function', function: { name: 'set_timer', description: '定倒计时',
    parameters: { type: 'object', properties: { duration_text: { type: 'string', description: '时长原话，如"五分钟""一个半小时"' }, label: { type: 'string' } }, required: ['duration_text'] } } },
  { type: 'function', function: { name: 'stop_timer', description: '取消倒计时', parameters: { type: 'object', properties: {} } } },
];
const SHADOW_SYS = [
  '你是门店带教助手「小星」，在「赋能02」设备上帮一线员工。',
  '先判断用户这句话属于哪一类，然后【只调用对应的工具】，不要解释、不要闲聊：',
  '1. 问店里的 SOP／标准／流程／规定／怎么做 ⇒ 调 search_kb',
  '2. 要操作设备本身（录制／闹钟／备忘／倒计时）⇒ 调对应的那个工具',
  '3. 只是闲聊、打招呼、问你是谁 ⇒ ★ **不要调任何工具** ✓',
].join('\n');
// ★ 写操作集合（用于判"危险分歧"✗ —— 分歧 + 写操作 = 会误操作 ⇒ 要逐条人工看 ✓）
//   ★★ 第一版写错了 ✗（本地假样本逮到 ✓）：只放了【旧 action 名】✗
//     而模型给的是【工具名】（`cancel_alarm` ✗ 不在集合里 ✓）
//     ⇒ 后果：**凡"模型提议写操作"的分歧都被漏标** ✗✗ —— 而那正是最该人工看的一类 ✓
//   ★ 判据：**判据里出现两个命名空间时，必须两边都收** ✗（否则漏掉的是"对方那一侧" ✓）
const SHADOW_WRITE_ACTIONS = new Set([
  // 旧 action 名
  'start_record', 'stop_record', 'set_timer', 'stop_timer',
  'add_alarm', 'add_memo', 'del_alarm', 'del_memo',
  // ★ 工具名（与上面一一对应 ✓ 别只写一边 ✗）
  'set_alarm', 'cancel_alarm', 'delete_memo',
]);
let _shadowCalls = [];      // 滑动窗口（只存时间戳 ✓）
let _shadowDone = 0, _shadowDropped = 0, _shadowErr = 0;
/** ★ 影子判定（★ 绝不 await ✓ 绝不改任何状态 ✓ 抛错只吞 ✓）
 *  @param heard     ASR 文本
 *  @param oldV     现有路径的判定（由调用方在**已知之后**传进来 ✓ 不重复跑旧逻辑 ✗ 免得重复打日志 ✓）
 *  @param device    设备码（只进日志 ✓） */
// ══════════════════════════════════════════════════════════════════════
// ★★ 语音会话记录（2026-10-14 用户点单：「我要的是乙，用户用设备端和小星的聊天记录」）
//
//   ★★★ 三条红线（★ 这个功能最怕的就是"记录把对话搞坏了" ✗）：
//     ① **绝不阻塞、绝不影响对话** ⇒ **fire-and-forget** ✓（与 `shadowJudge` 同一套做法 ✓）
//        · 不 await ✓ · 3 秒超时 ✓ · 任何失败**只打日志** ✓ · **绝不改返回值** ✓
//     ② **S2 挂了 ⇒ 语音照常可用** ✓（与"设备本地上报与指令下发不受影响"同一条纪律 ✓）
//     ③ **失败必须留证据** ✗ ⇒ 不写 `catch(e){}` 静默 ✓（本项目踩过这个 ✓）
//
//   ★ 一轮的定义（与服务端 §2.2 对齐 ✗）：
//     · 有 reply ⇒ 记 ✓
//     · **命令回合 reply 为空** ⇒ 也记 ✓（`action` 就是内容 ✓ —— "他让小星干了什么"有价值 ✓）
//     · 空 turn（无 reply 也无 action）⇒ **不记** ✗（省一次请求 ✓）
//     · 失败回合 ⇒ 记 ✓ 且带 `err` ✓
//   ★ 归属：查 S2 的 `current-binding` ✓（**在 fire-and-forget 里查** ⇒ 不占对话时间 ✓）
//     ★ 查询失败 ⇒ `student_name` 留空 ✓ **但记录照发** ✗（宁可真名缺失，也别整条丢 ✓）
//     ★ 按**当时**绑定归属 ✓ —— 换绑后历史**不搬家** ✗（那是服务端定死的口径 ✓）
// ══════════════════════════════════════════════════════════════════════
function logVoiceTurn(o) {
  try {
    const reply = String((o && o.reply) || '');
    const action = String((o && o.action) || '');
    const device = String((o && o.device) || '');
    if (!device) return;                                   // ★ 没设备号就没法归属 ⇒ 不记 ✗
    // ★★ 空 turn 不记 ✗ —— 但**失败回合要记** ✓（有 err 就算一轮 ✓）
    //   ⚠️ 反例：`asr_empty`（没听清）**不记** ✗ —— 那不是一轮对话 ✓（用户压根没说话 ✓）
    if (!reply.trim() && !action && !String((o && o.err) || '').trim()) return;
    const payload = {
      device_id: device, student_name: '',
      question: String((o && o.question) || ''),
      reply: reply, action: action,
      params: (o && o.params) || null,
      mood: String((o && o.mood) || ''),
      branch: String((o && o.branch) || ''),
      latency_ms: Number((o && o.latency_ms) || 0) || 0,
      err: String((o && o.err) || ''),
      created_at: new Date().toISOString(),
    };
    (async () => {
      const t = Date.now();
      try {
        // ① 先查绑定（★ 权威来源在 S2 ✓ 查不到不影响记录 ✓）
        try {
          const rb = await fetch(`${CAM_S2_DBAPI}/api/cam-devices/current-binding?device_id=${encodeURIComponent(device)}`,
            { headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}` }, signal: AbortSignal.timeout(3000) });
          if (rb && rb.ok) {
            const b = await rb.json();
            if (b && b.name) payload.student_name = String(b.name);   // ★ 按当时绑定归属 ✓
          }
        } catch (e) {
          log(`  ↳ 会话记录：查绑定失败（★ 不影响记录，按未绑定记 ✓）: ${e.message}`);
        }
        // ② 推送（★ 只写不读 ✓ 端点在 S2 db-api 18903 ✓ 与 cam 同一套 token ✓）
        const r = await fetch(`${CAM_S2_DBAPI}/api/voice-chat-log/ingest`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(3000),
        });
        if (!r || !r.ok) {
          log(`⚠️ 会话记录落库失败 HTTP ${r ? r.status : '-'}（★ 只记日志，不影响对话 ✗）`);
        } else {
          log(`  ↳ 会话记录已落库 ✓ ${device} ${payload.student_name || '(未绑定)'} `
            + `branch=${payload.branch || '-'} ${Date.now() - t}ms（★ 后台任务，不在对话链路里 ✓）`);
        }
      } catch (e) {
        log(`⚠️ 会话记录落库异常: ${e.message}（★ 只记日志，绝不影响对话 ✗）`);
      }
    })();
  } catch (e) {
    log(`⚠️ logVoiceTurn 组装失败: ${e.message}（★ 同样绝不影响对话 ✗）`);
  }
}

function shadowJudge(heard, oldV, device) {
  if (!SHADOW_MODE) return;
  try {
    const now = Date.now();
    _shadowCalls = _shadowCalls.filter(t => now - t < 60000);
    if (_shadowCalls.length >= SHADOW_MAX_PER_MIN) {
      _shadowDropped++;
      if (_shadowDropped % 20 === 1) log(`SHADOW ⚠️ 限频丢弃中（已丢 ${_shadowDropped} 条，上限 ${SHADOW_MAX_PER_MIN}/分）★ 指标会被削，聚合时注意 ✓`);
      return;
    }
    _shadowCalls.push(now);
    const t0 = Date.now();
    const ac = new AbortController();
    const tid = setTimeout(() => ac.abort(), SHADOW_TIMEOUT_MS);
    fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST', signal: ac.signal,
      headers: { 'Authorization': `Bearer ${DEEPSEEK_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: SHADOW_MODEL, temperature: 0, max_tokens: 160,
        messages: [{ role: 'system', content: SHADOW_SYS }, { role: 'user', content: String(heard || '') }],
        tools: SHADOW_TOOLS, tool_choice: 'auto',
      }),
    }).then(r => r.json()).then(j => {
      clearTimeout(tid);
      const m = j && j.choices && j.choices[0] && j.choices[0].message;
      const tc = m && m.tool_calls && m.tool_calls[0];
      let nv;
      if (tc) {
        let args = {};
        try { args = JSON.parse(tc.function.arguments || '{}'); } catch (e) { args = { __parse_error: String(tc.function.arguments).slice(0, 80) }; }
        nv = { tool: tc.function.name, args };
      } else {
        nv = { tool: null, text: String((m && m.content) || '').slice(0, 60) };   // ★ 不调工具 = 直接回话 ✓
      }
      // ★★ 判"分歧"的规则 —— ★ 第一版写错了 ✗（上线后第一批 3 条样本就抓到 ✓）：
      //   我原来写的是"知识库回合只要调了工具就算分歧" ✗✗
      //   ⇒ 可模型调 **`search_kb`** 恰恰就是"**和旧路径一致**"（都去查知识库 ✓）
      //   ⇒ 后果：**分歧率被系统性抬高** ✗（3 条样本里 2 条是假的 ✓）
      //   ★ 判据：**"分歧"要按"两条路想不想做同一件事"定义** ✗ ——
      //     不是按"有没有调工具"定义 ✓（工具是手段，不是目的 ✓）
      //   · 旧=命令 ⇒ 一致 ⟺ 模型选的工具**映射到同一个 action**
      //   · 旧=知识库 ⇒ 一致 ⟺ 模型调了 `search_kb`
      //   · 旧=人设自答 ⇒ 一致 ⟺ 模型**没调工具**（直接回话 ✓）
      let diverge;
      if (oldV.kind === 'command') diverge = !(nv.tool && SHADOW_TOOL_TO_OLD[nv.tool] === oldV.action);
      else if (oldV.kind === 'kb') diverge = (nv.tool !== 'search_kb');
      else diverge = (nv.tool !== null);
      // ★★ 危险分歧 = 分歧 且 任一侧涉及写操作 ✗（会误操作 ⇒ 逐条人工看 ✓）
      const dangerous = diverge && (SHADOW_WRITE_ACTIONS.has(oldV.action) || SHADOW_WRITE_ACTIONS.has(nv.tool));
      // ★ 漏检索 = 旧路径判"知识库问答" 而 模型没调 search_kb ✗（★ 本 CR 最担心的新失败模式 ✓）
      const missedKb = oldV.kind === 'kb' && nv.tool !== 'search_kb';
      _shadowDone++;
      const rec = { t: new Date().toISOString(), asr: String(heard || '').slice(0, 60), dev: device || '-',
        old: oldV, new: nv, diverge, dangerous, missedKb, ms: Date.now() - t0,
        n: _shadowDone, drop: _shadowDropped };
      const line = JSON.stringify(rec);
      // ★★ 另写一份**专用文件** ✗✗ —— 为什么必须有：
      //   `deploy.sh` 每次都用 `> /tmp/voice-server.log` **清空日志** ✓
      //   而设备端部署很频繁 ⇒ ★ 只写日志的话，**样本会被一次次清掉** ✓
      //   （"攒了一天数据，一部署全没了" ✗ —— 而且看不出是丢了，只觉得"样本怎么这么少" ✓）
      //   ★ 判据：**要跨时间积累的数据，不能只放在会被轮转/清空的通道里** ✗
      //   ★ appendFileSync 很小很稀（≤30/分 ✓）且**出错只吞** ✓ 不影响主流程 ✓
      try { fs.appendFileSync(SHADOW_FILE, line + '\n'); } catch (e) { /* 只吞 ✓ */ }
      log(`SHADOW ${line}`);
    }).catch(e => { clearTimeout(tid); _shadowErr++; log(`SHADOW ✗ 判定失败（${_shadowErr}）: ${e.message}`); });
  } catch (e) { _shadowErr++; try { log(`SHADOW ✗ 外层异常: ${e.message}`); } catch (e2) {} }
}
// ★ 工具名 → 现有 action 名（判分歧用 ✓ ★ 只用于比较，不参与执行 ✓）
const SHADOW_TOOL_TO_OLD = {
  start_record: 'start_record', stop_record: 'stop_record',
  set_alarm: 'add_alarm', cancel_alarm: 'del_alarm',
  add_memo: 'add_memo', delete_memo: 'del_memo', read_memo: 'read_memo',
  set_timer: 'set_timer', stop_timer: 'stop_timer',
  search_kb: '__kb__',
};

// ══════════════════════════════════════════════════════════════════════
// ★★ CR-20261012-02：语音回复的**语气字段** `reply_mood`（设备端出的 ✓ 让眼睛贴合说话内容）
//
//   ★ 设备端已做两层（不需要我方配合 ✓）：① 跟着 PCM 音量的韵律 ✓ ② 从文本标点/关键词推 ✓
//     本 CR 补**第三层：真语义情绪** ✓ —— ★ 这一层**只有我方知道** ✗
//
//   ★★ 三条硬规矩（照设备端规格书 §2.3 ✓ 一条都不能违 ✗）：
//     ① **缺字段 / 值不认识 ⇒ 设备自动降级** ✓ ⇒ ★ 我方**永远发安全的**：不在 6 个值里就**不发** ✗
//     ② **只描述语气** ✗ —— 不许发动作/时长/时间轴 ✓（动作归设备 ✓ 时间轴靠网络必抖 ✓）
//     ③ ★★ **必须与 `reply` 同源** ✗✗ —— "嘴上说好、脸上像道歉"是**最糟的** ✓
//        ⇒ 做法：**语气由产出这句话的同一次调用一起给出来** ✓（不是事后猜 ✓）
//
//   ★★ 什么时候**故意不发** ✗（让设备用自己那两层 ✓ 它们做得比"我瞎猜"好 ✓）：
//     · 值是 `neutral`（平常）✓ —— 设备自己的标点/关键词层正好覆盖这种情况 ✓
//     · 命令回合里 reply 为空的（如开始录制 ✓ 它根本不播报 ✓）
//     · 模型**没给**语气 ✓ —— ★ 宁可不说，也不猜 ✗（猜错 = 表情和内容打架 ✓）
//
//   ★★★ 定义位置本身就是一条判据 ✗（我第一版放错了 ✓ 当场踩坑）：
//     这三个函数被**处理函数内部**的 `voiceChatAnswer()` 用 ✓，
//     也被**模块级**的 `webAnswer()` 用 ✓
//     ⇒ ★ 必须放**模块级**（最深的那一层够不到 ✓ 反之才行 ✓）
//     ★ 判据：**新函数放哪一层，要看【所有】调用点在哪一层** ✗
//       —— 不是看"写的时候离谁近" ✓（离得近但作用域够不着 = 运行时才炸 ✓ `node --check` 查不出 ✗）
// ══════════════════════════════════════════════════════════════════════
const MOODS = ['neutral', 'happy', 'curious', 'sorry', 'excited', 'serious'];   // ★ 就这 6 个 ✗ 别自创 ✓

// ══════════════════════════════════════════════════════════════════════
// ★★★ 能力封闭清单（**模块级** ✗ —— 因为 [知识库/闲聊] 与 [联网] **两条路都要用** ✓）
//
//   ★ 为什么要单独抽出来 ✗✗（2026-10-12 用户报的真问题 ✓）：
//     用户说「他说能帮我订机票之类的，这不可能啊」✗
//     ★ 复现 5 次都没复现出来 ✓（5 次全都在**正确否认** ✓）—— 但**病根找到了** ✗：
//       我原来在【不许吹牛】那节里**举了例子** ✗：
//         「★ 除此之外的事（**订机票**、买东西、改工资…）老实说你做不了」
//     ★★ 判据（通用 ✗ 值得记）：**"不要提 X" 会把 X 带进上下文** ✗ ——
//       模型做自我介绍时，可能把那个词**当成自己的本事说出来** ✓
//       而用户听到"订机票"从它嘴里出来 ⇒ **第一反应就是"它说它能订机票"** ✓
//     ⇒ ★ 修法：**用规则，不用例子** ✗（例子会起反作用 ✓）
//
//   ★★ 另一条（同日同族 ✗）：原来这段只写在 `VOICE_CHAT_SYSTEM` 里 ✓
//     而**联网那条路用自己的 system** ✗ ⇒ 它**根本没有能力边界** ✓（联网回合就能随便吹 ✗）
//     ⇒ ★ 抽到模块级 ⇒ 两条路**共用同一份** ✓（"同一个事实只能一处" ✓）
// ══════════════════════════════════════════════════════════════════════
/**
 * ★★ 2026版企业文化（源：`2026版企业文化.pdf` ✓ 全文 526 字 ✓ 用户 2026-10-20 提供）
 *   ★ 用户要求：「每一次对话都要从企业文化出发，有的没的要聊一下企业文化，
 *     要从企业文化出发去讲解一些内容、去回答用户」
 *   ★★ 提示词写法上有三个讲究 ✗：
 *     ① **给"怎么用"，不给"必须用"** ✗ —— 只写"必须提"⇒ 模型会**硬塞** ✓
 *        写成"落到某一条上，但**一句话带过**"✓ 才自然 ✓
 *     ② **给示范**（few-shot）✗ —— ★ 一段示范胜过十句要求 ✓
 *     ③ **红线/禁忌只给"别做什么"的语义** ��� —— ★ 不要让它主动念红线 ✗
 *        （★ 谁会对着员工念"九大红线"✓ 那是 HR ✓ 不是带教 ✓）
 */
const CORP_CULTURE = {
  core: [
    ['真诚', '讲真话，做实事，爱客户，守承诺'],
    ['高效', '成果导向，沟通简单，标准明确，推进有力'],
    ['专业', '精流程，定标准，练技能，严考核'],
    ['担当', '会放权，勇挑战，敢行动，定决策，担责任'],
  ],
  vision: '让胤隆会开到每一位员工的家乡',
  mission: '构筑休闲乐园 · 缔造幸福空间 · 搭建创业平台 · 实现员工梦想',
  slogan: '“浴”见胤隆，“浴”见美好，风里雨里，胤隆会等你',
  ideas: [
    ['经营理念', '客户第一，诚信为本，精益求精'],
    ['产品理念', '专业、健康、简单、创新'],
    ['服务理念', '一切为了顾客的微笑'],
    ['团队理念', '互敬互爱，责任担当，目标一致，优胜劣汰'],
  ],
  // ★ 只给"别做什么"的语义 ✓（★ 不主动念 ✓）
  taboos: '不传播负能量 · 不怠慢顾客 · 不议论顾客隐私 · 不弄虚作假 · 不推卸责任 · 不醉酒上岗',
};
/** 企业文化 → 提示词片段（★ 一处生成 ✗ 别在别处手抄一遍 ✓） */
function corpCultureBlock() {
  const L = [];
  L.push('【胤隆会 2026 版企业文化 · ★ 这是你说话的地基 ✗】');
  L.push('核心价值观（四句，要能脱口而出）：');
  CORP_CULTURE.core.forEach(function (c, i) { L.push('  ' + (i + 1) + '. ' + c[0] + '：' + c[1]); });
  L.push('愿景：' + CORP_CULTURE.vision);
  L.push('使命：' + CORP_CULTURE.mission);
  L.push('口号：' + CORP_CULTURE.slogan);
  CORP_CULTURE.ideas.forEach(function (x) { L.push(x[0] + '：' + x[1]); });
  L.push('');
  L.push('★★ 怎么用它（★ 这一节比上面那些条文更重要 ✗✗）：');
  L.push('  · **每次回答都要落到其中一条上** ✓ —— 但★★ **一句话带过** ✗，别背条文、别念口号；');
  L.push('  · 讲标准/流程/考核 ⇒ ★ 从「专业」出发（为什么要定标准 ✓）；');
  L.push('  · 讲顾客/服务/投诉 ⇒ ★ 从「真诚」「一切为了顾客的微笑」出发 ✓；');
  L.push('  · 讲配合/团队/带新人 ⇒ ★ 从「担当」「互敬互爱」出发 ✓；');
  L.push('  · 讲效率/执行/拖沓 ⇒ ★ 从「高效」出发 ✓；');
  L.push('  · ★★ **闲聊、逗乐、讲笑话** ⇒ ★ **也要带一句** ✓（这就是用户要的"有的没的都聊"✓）');
  L.push('    例：「讲完了，乐一乐；咱这行图的不就是"一切为了顾客的微笑"嘛。」✓');
  L.push('  · ★ **技术性追问**（"再说一遍/没听清"）⇒ 可以不提 ✓ **别每句都硬塞** ✗（会烦人 ✓）');
  L.push('  · ★★ **不许把红线/禁忌念给员工听** ✗ —— 那是入职培训的事 ✓ 你只在他们**要违规**时');
  L.push('    轻轻点一句（例：别议论客人隐私 ✓ 这与「真诚」「不议论顾客隐私」是一致的 ✓）');
  L.push('★★ 三条示范（★ 学这个语感 ✗ 不是背句子 ✓）：');
  L.push('  · 问"班前会怎么开" ⇒ 「班前会别念稿子，讲清今天的目标和昨天差在哪。咱讲『高效』——沟通简单、标准明确 ✓ 十分钟就够。」');
  L.push('  · 问"客人投诉怎么办" ⇒ 「先听着，别急着解释。咱的服务理念是"一切为了顾客的微笑" ✓ 他笑了，事儿才算完。」');
  L.push('  · 问"讲个笑话" ⇒ 「（讲完）乐呵完接着干活 —— 咱这行，乐子也得是"为了顾客的微笑"那种乐子。」');
  return L.join('\n');
}

const PERSONA_CAPS = [
  '【你实际会做什么 · ★ 只许说这六件 ✗✗】',
  '★ 你的能力**只有下面这六样，多一样都没有** ✗：',
  '  1. 查**店里的标准**（SOP / 流程 / 规定 / 考核标准）✓',
  '  2. 查**外部实时信息**（天气 / 新闻这类，有联网）✓',
  '  3. **操作这台设备**：开始/结束录制 · 定闹钟 · 记备忘 · 倒计时 ✓',
  '  4. **查他自己的学习数据**（积分 · 考试成绩 · 场景练习 · 课程进度 · 学习计划 · 今天的职责任务）✓',
  '     ★★ 只能查**当前这台设备绑定的人** ✗ —— 问别人的一律说查不到 ✓ **不许编** ✗',
  '     ★★ 有【系统查询到的真实数据】时**必须照着说** ✗ —— 别再说"查不到/做不了" ✓',
  '  5. **讲笑话 / 逗乐**（★ 用户 2026-10-20 明确要的 ✓）',
  '     ★ 每次**换一个新的** ✗ —— 别重复用过的段子 ✓；★ 讲完带一句企业文化更自然 ✓',
  '  6. 陪人**聊两句**（打招呼 / 自我介绍 / 闲话）✓',
  '★★ 除了这六样，**别的一律不会** ✓ —— 有人问你能不能做别的，就答：',
  '   「这个我做不了，我就管店里这些活儿。」✓',
  '★★ 做自我介绍时，**只许说这六样** ✓；★ **不要举任何"我会做某件别的事"的例子** ✗',
  '★★ 也**不要举"我不会做什么"的例子** ✗ —— ★ 说到那些词，听起来就像你会做 ✓',
  '  （★ 为什么：**"不要提 X"会把 X 带进上下文** ✗ 用户会以为你在说你会 X ✓）',
];
// ★ 观测用（★ 不是拦截 ✗）：回复里出现这些词 ⇒ **打一行警告 + 把原话记下来** ✓
//   ★ 为什么不直接拦 ✗：这些词在**否认句**里是正常的 ✓（"订机票…我真没那本事"✓）
//     ⇒ ★ 拦了会误伤 ✓；但**记下来**就能回答"到底有没有真的吹" ✓
const CAP_ALERT_WORDS = /(订机票|订酒店|订餐|买票|下单|代购|转账|付款|汇款|改工资|发消息|发微信|打电话|发邮件|查订单|查快递|预约|代订)/;
function capAlert(reply) {
  const m = String(reply || '').match(CAP_ALERT_WORDS);
  return m ? m[0] : null;
}
/** ★ 收口：只发【在 6 个值里且不是 neutral】的 ✓ 其余一律不发 ✗ */
function normMood(m) {
  const v = String(m || '').trim().toLowerCase();
  if (!MOODS.includes(v)) return null;     // ★ 不认识 ⇒ 不发 ✓（设备会降级 ✓）
  if (v === 'neutral') return null;        // ★ 平常 ⇒ 交给设备自己推断 ✓
  return v;
}
/** ★★ 命令回合的语气：由 **`action`** 决定 ✓（同源 ✓ 因为 reply 就是同一处按 action 生成的 ✓）
 *  ★ 只有"确实做了件好事 / 确实做不到"这两类才给 ✗；其余一律不给 ✓
 *  ⚠️ 那两条"做不到"的回复（倒计时越界 / 备忘太长）**文本是写死的** ✓
 *    ⇒ 这里用**包含关键短语**来判 ✓（★ 比让模型猜可靠 ✓ 而且那两句就在本文件里 ✓）*/
function moodForCmd(vc) {
  if (!vc || !String(vc.reply || '').trim()) return null;        // ★ 不播报的回合 ⇒ 不给 ✓
  const r = String(vc.reply);
  if (r.includes('没法定')) return 'sorry';                      // 倒计时越界 ✓
  if (r.includes('只记住了前半句')) return 'sorry';               // 备忘超长 ✓
  switch (vc.action) {
    case 'add_alarm': case 'add_memo':
    case 'set_timer':  return 'happy';                           // ★ 帮上忙了 ✓
    case 'del_alarm': case 'del_memo': case 'stop_timer':
      return null;                                               // ★ 删/停 ⇒ 平常 ✓
    default: return null;                                        // read_memo（就一个"好"✓）/ 其它 ✓
  }
}

// ══════════════════════════════════════════════════════════════════════
// ★★★ CR-20261012-03 §6.6 方案 B：**联网搜索**（用户 2026-10-12 拍板「把 B 接上」✓）
//
//   ★ 用户原话（逐字 ✓）：「可是我想的，比方说我刚问"明天天气怎么样"，
//     他是**真的会去搜**…然后给我回答，**这是我想要的呀**」✓
//
//   ★★ 实现形状（来自探针结论 ✓ 别改成别的 ✗）：
//     · `enable_search` 是**模型级开关**、**不是"工具"** ✗
//       ⇒ 正确姿势：**路由器**先判"要不要外部信息" ✓
//         ⇒ 要 ⇒ **生成那一轮带上 `enable_search:true`** ✓（★ 同一次调用 ✓ 不多一次往返 ✓）
//         ⇒ 不要 ⇒ 不带 ✓（零额外成本/延迟 ✓）
//     · ★ 必须走**原生端点** ✗✗：`/api/v1/services/aigc/text-generation/generation`
//       —— 实测**兼容模式会静默忽略 `enable_search`** ✗（模型答"我不连接互联网"✓
//          这正是我第一轮误判成"这条路是陷阱"的原因 ✗）
//     · ★ 打开 `search_options.enable_source=true` ⇒ 才拿得到 `search_info.search_results[]`
//       （带 `site_name`/`title`/`url` ✓ ⇒ ★ 我方把来源**写日志** ✓ 答案可核对 ✓）
//     · ★★ 实测数字：简单问题 2.5s ✓ ／ 天气 5.3~5.9s ✓ ／ 要求列链接 9.5s ✗（别要求列链接 ✓）
//       每次 input ≈3000+ tokens ✓（搜索结果进了 prompt ✓）
//
//   ★★★ 三条纪律（★ 缺一不可 ✗）：
//     ① **只在路由器判定需要时才开** ✗（否则每句都搜 ⇒ 成本延迟全上去 ✓）
//     ② **超时严格**（WEB_TIMEOUT_MS ✓ 默认 9s ✓）—— 超时 ⇒ **退回原路** ✓ 不干等 ✓
//     ③ **日均额度**（WEB_DAILY_MAX ✓）—— 超了就**退回原路** ✓（不是报错 ✓）
//
//   ★ 失败一律**退回原路** ✗✗：路由失败 / 搜索失败 / 超时 / 超额度 ⇒ 都走原来的
//     知识库＋deepseek 那条 ✓ ⇒ ★ **最坏情况 = 今天的行为** ✓（不会比现在差 ✓）
// ══════════════════════════════════════════════════════════════════════
const WEB_SEARCH_ON = process.env.WEB_SEARCH !== '0';
const WEB_MODEL = process.env.WEB_MODEL || 'qwen-plus';
const WEB_TIMEOUT_MS = parseInt(process.env.WEB_TIMEOUT_MS || '9000', 10);
const WEB_DAILY_MAX = Math.max(1, parseInt(process.env.WEB_DAILY_MAX || '200', 10));
const DASHSCOPE_URL = 'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation';
let _webDayKey = '', _webUsed = 0, _webFail = 0;
function webQuotaOk() {
  const d = new Date(Date.now() + 8 * 3600 * 1000).toISOString().slice(0, 10);   // ★ 北京时间日 ✓
  if (d !== _webDayKey) { _webDayKey = d; _webUsed = 0; log(`WEB 额度重置（${d}）上限 ${WEB_DAILY_MAX}/天`); }
  return _webUsed < WEB_DAILY_MAX;
}
/** ★★ 把模型给的 markdown 洗成"能念"的纯文本 ✗✗
 *  ★ 为什么必须有：探针实测 qwen 带搜索的答案长这样 ✗
 *    「**天气现象**：多云转晴 - **最高气温**：22℃…」
 *  ⇒ ★ 设备是 **TTS 逐字念** ✗ ⇒ 会把"星号""井号""短横线"念出来 ✓
 *  ★ 判据：**"给屏幕看"的格式和"给喇叭念"的格式不是一回事** ✗（本项目 §121 老规矩 ✓）*/
function speakable(s) {
  let t = String(s || '');
  t = t.replace(/```[\s\S]*?```/g, ' ');          // 代码块
  t = t.replace(/\*\*|__|`|\*|#+\s*/g, '');        // ★ 粗体/斜体/行内码/标题符
  t = t.replace(/^\s*[-•·]\s*/gm, '');             // 行首项目符号
  t = t.replace(/^\s*\d+[.、)]\s*/gm, '');         // 行首数字编号
  t = t.replace(/\[([^\]]*)\]\([^)]*\)/g, '$1');   // [文字](链接) ⇒ 文字
  t = t.replace(/^\s*答[：:]\s*/,'');              // 我方那套前缀（若模型也用了 ✓）
  t = t.replace(/[\r\n]+/g, ' ').replace(/\s{2,}/g, ' ').trim();
  return t;
}
/** ★ 路由器：这一轮要不要【外部/实时】信息？（★ 与 KB 检索**并行发** ✓ 见 §6.1 ✓）
 *  ★ 判据：认不准就 false ✗（= 不搜 ✓ 退回原路 ✓ 安全 ✓）*/
/**
 * ★★ 意图路由（2026-10-20 扩：一次调用判两件事 ✗）
 *   ① 要不要**联网**（外部实时信息）
 *   ② 要不要**查学习数据**（积分/考试/练习/课程/计划/职责）
 * ★★ 为什么合并成一次 ✗：拆两次 ⇒ 多 0.7~1.1s ✓
 *   （★ 语音回合最贵的就是"说完到第一声"✓ 实测已 6.5s 上下 ✓ 不能再加）
 * ★ 返回 `{ web, learning }` —— ★ `learning` 是 `kind` 枚举或 `null` ✓
 *   ★ 枚举**写进工具参数** ✓ ⇒ ★ 模型选不了枚举外的值 ✓（S2 侧还会 400 ✓ 双保险）
 */
const LEARN_KINDS = ['overview', 'points', 'exams', 'practices', 'courses', 'plan', 'duty'];
async function routeIntent(text) {
  const NONE = { web: false, learning: null, roleplay: false };
  if (!DEEPSEEK_KEY) return NONE;              // ★ 学习数据也要模型判定 ⇒ 没 key 两个都不做 ✓
  const ac = new AbortController();
  const tid = setTimeout(() => ac.abort(), 6000);
  try {
    const r = await fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST', signal: ac.signal,
      headers: { 'Authorization': `Bearer ${DEEPSEEK_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'deepseek-chat', temperature: 0, max_tokens: 60,
        messages: [
          { role: 'system', content: [
            '你有两个工具，按用户这句话判断要调用哪个（**也可以都不调**）。',
            '',
            '① web_search —— 需要【外部/实时信息】时调用',
            '   ★ 要：天气、新闻、今天几号、股价汇率、赛事、航班、任何"眼下的事实"',
            '   ★ 不要：店里的SOP/标准/流程/规定 · 操作设备 · 打招呼/闲聊/问你是谁',
            '',
            '② get_learning_summary —— 需要【这位员工自己的学习数据】时调用（kind 选最贴近的一个）',
            '   overview  = 本周练习/考试/平均分 ＋ 当前积分（"我最近学得怎么样/这周"）',
            '   points    = 积分余额 ＋ 最近变动（"我多少分/积分怎么来的"）',
            '   exams     = 考试成绩（"我考得怎么样/上次考多少"）',
            '   practices = 场景练习记录（"我练过什么/练习成绩"）',
            '   courses   = 课程进度（"我看了哪些课/还有多少没看"）',
            '   plan      = 学习计划 / 储备人才计划进度',
            '   duty      = 今天的岗位职责任务（"我今天要做什么/还有什么没做"）',
            '   ★★ 只能查【本人】✗ —— 问别人的一律**不调**（工具查不到 ✓ 你也别编 ✓）',
            '',
            '③ start_roleplay —— 用户想**练场景 / 做对练 / 角色扮演**时调用（无参数）',
            '   ★ 例如：「开始对练」「练个场景」「我想练练」「来一次情景模拟」「陪我练一下投诉」',
            '   ★ 不是："跟我聊聊天"（那是闲聊）、"我考试考得怎么样"（那是查数据）',
          ].join('\n') },
          { role: 'user', content: String(text || '') },
        ],
        tools: [
          { type: 'function', function: { name: 'web_search', description: '需要外部实时信息时调用',
            parameters: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } } },
          { type: 'function', function: { name: 'get_learning_summary', description: '查这位员工自己的学习数据时调用',
            parameters: { type: 'object', properties: {
              kind: { type: 'string', enum: LEARN_KINDS },
            }, required: ['kind'] } } },
          { type: 'function', function: { name: 'start_roleplay', description: '用户想练场景/做对练/角色扮演时调用',
            parameters: { type: 'object', properties: {} } } },
        ],
        tool_choice: 'auto',
      }),
    });
    const j = await r.json();
    const tcs = (j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.tool_calls) || [];
    let web = false, learning = null, roleplay = false;
    for (const tc of tcs) {
      const fn = tc && tc.function && tc.function.name;
      if (fn === 'web_search' && WEB_SEARCH_ON) web = true;
      if (fn === 'start_roleplay' && RP_ON) roleplay = true;
      if (fn === 'get_learning_summary') {
        let k = null;
        try { k = JSON.parse(tc.function.arguments || '{}').kind; } catch (e) { k = null; }
        // ★ 模型给错/没给 ⇒ **兜底 overview** ✗ —— ★ 不猜别的 ✓ 也不因为参数错就整个不查 ✓
        learning = (LEARN_KINDS.indexOf(k) >= 0) ? k : 'overview';
      }
    }
    return { web, learning, roleplay };
  } catch (e) {
    _webFail++;
    log(`ROUTE ⚠️ 意图判定失败（第 ${_webFail} 次，按"都不做"处理 ✓）: ${e.message}`);
    return NONE;
  } finally { clearTimeout(tid); }
}

/**
 * ★★ 取学习数据摘要（2026-10-20）—— **数字由 S2 的 SQL 算** ✓ 这里只搬运 ✓
 *   ★★ 走**公网域名** ✗ —— 18901 不直接对外开放 ✓（S1 只能经 nginx ✓）
 *   ★ 失败返回 null ✓ ⇒ 调用方**按"没查到"处理** ✗（★ 绝不编数据 ✓）
 */
async function learningSummary(device, kind) {
  if (!device || !kind) return null;
  const t0 = Date.now();
  try {
    const r = await fetch(`https://ai.hiiin.com/api/voice/learning-summary?dev_id=${encodeURIComponent(device)}&kind=${encodeURIComponent(kind)}`,
      { headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}` }, signal: AbortSignal.timeout(4000) });
    if (!r || !r.ok) { log(`LEARN ⚠️ 取学习数据失败 HTTP ${r ? r.status : '-'}（★ 按"没查到"处理 ✓ 不编 ✗）`); return null; }
    const j = await r.json();
    if (!j || !j.ok || !j.found) { log(`LEARN ⚠️ 学习数据 not found (${j && (j.reason || j.error)})`); return null; }
    log(`LEARN ✓ 取到 ${kind}（${Date.now() - t0}ms ✓）`);
    return j;
  } catch (e) {
    log(`LEARN ⚠️ 取学习数据异常: ${e.message}（★ 按"没查到"处理 ✓）`);
    return null;
  }
}

/* ════════════════════════════════════════════════════════════════════════
 * ★★★ 语音对练 · 独立智能体（2026-10-21 定格「丙」✗）
 *
 *   ★★ 用户原话：「能不能『对练功能』走第二个智能体路由？这样就不怕和小星的
 *      数据搞混了，且练习完成以后的数据是要进 S2 服务器数据库的」
 *   ★ 用户拍板：① **丙**（同一个 HTTP 入口 ✓ 内部两套完全独立的会话池 ✓）
 *                ② **认**（对练静默 5 分钟自动收尾 ✓）
 *                ③ **丙：不能**（对练期间不许用小星别的功能 ✓ 说天气也按角色回 ✓）
 *
 *   ★★★ "独立"体现在四样东西**全部私有** ✗（★ 与小星**零共享** ✓）：
 *     ① `_rpSessions`  ← 会话池（★ 与小星的 `_chatSessions` **不是一张表** ✗）
 *     ② `rpSystemPrompt()` ← 提示词（★ **不含**小星人设/企业文化/学习数据工具 ✗）
 *     ③ 历史（★ 对练的 msgs **永远不进**小星的上下文 ✓ 反之亦然 ✓）
 *     ④ 出口与超时（★ 对练静默 5 分钟 ⇒ 只收对练 ✓ 不影响小星 TTL ✓）
 *
 *   ★★ 为什么④必须独立 ✗：★ 小星的 TTL 是"话题过期" ✓ 对练的 5 分钟是"人走了 ✓
 *     别一直演客人" ✓ —— ★ **语义不同** ✓ 共用一个会互相误伤 ✓
 *
 *   ★★ 三种出口（★ 缺一不可 ✗ 规格书 §2 ✓）：
 *     ① 用户明说：结束 / 不练了 / 停 ✓
 *     ② 满 6 轮 ✓（★ 防止一直聊 ✓）
 *     ③ 静默 5 分钟 ✓（★ 进程重启 ⇒ 表清空 ⇒ 自动回 normal ✓ 安全降级 ✓）
 *
 *   ★ 数据落 S2：`POST /api/voice/practice-log`（★ 已上线 ✓ 实测写进 practices ✓）
 *     ★ 归属 `dev_id → device_binding` ✓ 未绑定 ⇒ 不落库也不报错 ✓
 * ════════════════════════════════════════════════════════════════════════ */
const _lastDebrief = new Map();   // device -> { total, grade, scores, comment }（★ end 响应要用 ✗）
const _rpSessions = new Map();          // device -> { mode, scene, sceneId, sceneName, module, turns, msgs:[], at }
const _rpStartPending = new Set();      // device：本回合被判定为"开始对练" ✓ 下一次调用进 picking
const RP_MAX_TURNS = 6;                 // ★ 满 6 轮自动收尾 ✓（用户拍板 ✓）
// ★ 静默超时（★ 用户拍板 5 分钟 ✓）
//   ★★ 2026-10-21：允许 env 覆盖 ✗ —— ★ 否则"静默 5 分钟自动退出"这条**永远没法验** ✓
//     （★ 真等 5 分钟 = 让用户干等 ✗ 违反本项目的输出纪律 ✓）
//   ★ 生产不设 ⇒ 默认 5 分钟 ✓；验的时候临时设 6 秒 ✓ 验完删掉 ✓
const RP_IDLE_MS = Math.max(3000, parseInt(process.env.ROLEPLAY_IDLE_MS, 10) || (5 * 60 * 1000));
const RP_ON = String(process.env.ROLEPLAY_ON || '1') !== '0';   // ★ 回退开关 ✓ 置 0 立即回到"只当小星" ✓
// ★★ 对练自己的超时（★ 端点里的 LLM_TIMEOUT_MS 是**函数内**的 ✓ 这里在模块级 ⇒ 用不了 ✗
//   ★ 这就是 2026-10-21 那次 LLM_TIMEOUT_MS is not defined 的根因 ✓
const RP_LLM_TIMEOUT_MS = 20000;
const RP_END_WORDS = /(结束|不练了|不玩了|停下|停止|退出|算了|够了|到此为止|结束练习|结束对练)/;

// ★ 场景缓存（★ 200 个 48KB ✓ 别每次读盘 ✓ 10 分钟 ✓）
let _rpSceneCache = { at: 0, data: null };
async function rpLoadScenes() {
  if (_rpSceneCache.data && Date.now() - _rpSceneCache.at < 10 * 60 * 1000) return _rpSceneCache.data;
  try {
    const r = await fetch('https://ai.hiiin.com/api/voice/scenes',
      { headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}` }, signal: AbortSignal.timeout(5000) });
    if (!r || !r.ok) return _rpSceneCache.data;
    const j = await r.json();
    if (j && j.ok && Array.isArray(j.scenes)) {
      _rpSceneCache = { at: Date.now(), data: j };
      log(`ROLEPLAY 场景库已缓存：${j.total} 个 / ${(j.modules || []).length} 个模块 ✓`);
    }
    return _rpSceneCache.data;
  } catch (e) { log(`ROLEPLAY ⚠️ 场景库拉取失败: ${e.message}`); return _rpSceneCache.data; }
}

// ★★ 对练提示词（★ **只演** ✗ 不评价 ✓ 不带小星人设/文化/工具 ✗✗）
function rpSystemPrompt(scene, turn) {
  return [
    '★★ 你现在**不是**助手，你是场景里的一个**具体角色**（客人 / 员工 / 同事）✗✗',
    '',
    '【场景】' + scene.name + '（' + (scene.module || '') + ' · 难度 ' + (scene.difficulty || '') + '）',
    '【情境】' + (scene.description || ''),
    '【对方身份】一线门店员工（学员）· 【你的身份】这个场景里的对手方',
    '',
    '【硬规则】',
        '1. ★★ 每轮只说 **1~2 句** ✗（★ **总字数 ≤ 40 字** ✗✗）口语 ✓ 像真人在说话 ✓ 别写成小说 ✗',
    '   ★ 为什么卡这么死：★ 设备上要念出来 ✓ 长了没人听 ✓ 而且**生成字数直接等于等待时间** ✗',
    '2. ★ 第一句要把情境**自然带出** ✓ —— ★ **不许**念"场景：xxx"✗（那是播报 ✓ 不是演戏 ✓）',
    '3. ★★ **绝不点评、绝不指导、绝不提"你应该…"** ✗✗ —— 你现在是客人/员工 ✓ 不是教练 ✓',
    '4. ★ 用户说什么都**顺着演** ✓ 不纠正 ✓ 不跳出 ✓ 不解释规则 ✓',
    '5. ★ 有情绪就带情绪 ✓（客人会不耐烦 ✓ 员工会委屈 ✓）—— 这才像真场景 ✓',
    '6. ★★ 只输出角色的台词 ✗ —— **不要**旁白、不要括号解释、不要任何前缀 ✗',
    '',
    '【进度】第 ' + turn + ' / ' + RP_MAX_TURNS + ' 轮'
  ].join('\n');
}

// ★★ 收尾点评（★ 此时才切回教练 ✓ 可用小星的语气 ✓）
function rpDebriefPrompt(scene, msgs) {
  const dialog = msgs.map(function (m) {
    return (m.role === 'user' ? '员工' : '对方') + '：' + m.content;
  }).join('\n');
  return [
    '你是门店带教教练，刚陪员工练完一个场景，现在给**收尾点评**。',
    '',
    '【场景】' + scene.name + '（' + (scene.module || '') + '）',
    '【情境】' + (scene.description || ''),
    '',
    '【对话记录】',
    dialog,
    '',
    '【评分维度】★ 就用这三个 ✗（与学员端口径一致 ✓ 别自造维度 ✗）',
    '  · 交流深度（0~34）：有没有问清问题、听懂对方真正在意的',
    '  · 专业度（0~33）：流程/标准有没有说到位',
    '  · 应对合理性（0~33）：处理是否得当、有没有踩坑',
    '★ 三项加起来满分 100 ✓',
    '',
    '★★ 只输出**严格 JSON** ✗（不要解释、不要 markdown 代码块）：',
    '{"交流深度":28,"专业度":30,"应对合理性":26,"点评":"不超过 80 字，先说一个亮点，再说一个改进点"}',
    '★★ 点评**必须 ≤ 80 字** ✗ —— 设备上要念出来的 ✓ 长了没人听 ✓'
  ].join('\n');
}

// ★ 调模型（★ 对练专用 ✓ 温度 0.6 ✓ 演戏需要点变化 ✓）
async function rpCallLLM(system, msgs, maxTokens) {
  const ac = new AbortController();
  const tid = setTimeout(() => ac.abort(), RP_LLM_TIMEOUT_MS);
  try {
    const r = await fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST', signal: ac.signal,
      headers: { 'Authorization': `Bearer ${DEEPSEEK_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'deepseek-chat', temperature: 0.6, max_tokens: maxTokens || 160,
        messages: [{ role: 'system', content: system }].concat(msgs)
      }),
    });
    const j = await r.json();
    if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
    return String((j.choices && j.choices[0] && j.choices[0].message.content) || '').trim();
  } finally { clearTimeout(tid); }
}

// ★ 落库到 S2（★ 失败只记日志 ✓ 不能让"写库失败"毁掉用户的练习体验 ✓）
async function rpSaveLog(device, scene, scores, total, grade, turns, feedback) {
  try {
    const r = await fetch('https://ai.hiiin.com/api/voice/practice-log', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ dev_id: device, scene: scene.name, scores, total, grade, turns, feedback }),
      signal: AbortSignal.timeout(5000),
    });
    const j = await r.json();
    if (j && j.ok) log(`ROLEPLAY ✓ 已落库 id=${j.id || '-'} ${j.student || '(未绑定)'} ${total}分 ${grade}（${turns} 轮）`);
    else log(`ROLEPLAY ⚠️ 落库失败: ${JSON.stringify(j).slice(0, 120)}`);
  } catch (e) { log(`ROLEPLAY ⚠️ 落库异常: ${e.message}（★ 不影响用户 ✓）`); }
}

// ★★ 收尾：点评 ＋ 打分 ＋ 落库 ＋ **清会话** ✗
async function rpDebrief(device, s) {
  const scene = s.scene || { name: s.sceneName || '', module: s.module || '', description: '' };
  let parsed = null;
  try {
    const out = await rpCallLLM(rpDebriefPrompt(scene, s.msgs), [{ role: 'user', content: '请给收尾点评（严格 JSON）' }], 220);
    const m = out.match(/\{[\s\S]*\}/);
    if (m) parsed = JSON.parse(m[0]);
  } catch (e) { log(`ROLEPLAY ⚠️ 收尾点评失败: ${e.message}（★ 用兜底分 ✓）`); }
  const scores = {
    '交流深度': Math.max(0, Math.min(34, parseInt(parsed && parsed['交流深度'], 10) || 20)),
    '专业度': Math.max(0, Math.min(33, parseInt(parsed && parsed['专业度'], 10) || 20)),
    '应对合理性': Math.max(0, Math.min(33, parseInt(parsed && parsed['应对合理性'], 10) || 20))
  };
  const total = scores['交流深度'] + scores['专业度'] + scores['应对合理性'];
  const grade = total >= 90 ? 'S' : (total >= 80 ? 'A' : (total >= 70 ? 'B' : (total >= 60 ? 'C' : 'D')));
  let comment = String((parsed && parsed['点评']) || '').trim();
  if (comment.length > 80) comment = comment.slice(0, 78) + '…';
  if (!comment) comment = '练完了。下次记得先把对方的话听完再答，别急着解释。';
  await rpSaveLog(device, scene, scores, total, grade, s.turns, comment);
  // ★★ 2026-10-21 依设备端递件补 ✗：★★ end 原来只回 debrief_text ✗ —— 而规格 §2.4
  //   明确要 { total, grade, scores, comment, debrief_text } ✓ ⇒ ★ 实现与规格不一致 ✓ 我认 ✗
  //   这里把结构化结果**存一份** ✓（★ 会话马上要 delete ✓ 存不进会话里 ✗）
  _lastDebrief.set(device, { total: total, grade: grade, scores: scores, comment: comment });
  _rpSessions.delete(device);      // ★★ **清会话** ✗ 对练历史绝不进小星的上下文 ✓
  const reply = (comment.indexOf('练完了') === 0 ? comment : ('练完了——' + comment))
    + ' 三个分：交流深度 ' + scores['交流深度'] + '、专业度 ' + scores['专业度']
    + '、应对合理性 ' + scores['应对合理性'] + '，合计 ' + total + ' 分，' + grade + ' 级。'
    + ' 想再来一遍就说「开始对练」。';
  log(`ROLEPLAY ✓ 收尾（${scene.name} / ${s.turns} 轮 / ${total}分 ${grade}）`);
  return reply;
}

/**
 * ★★ 对练回合的统一入口（★ 两个端点都调它 ✓ 不重复写 ✓）
 *   ★ 返回 `null` ⇒ **不是对练回合** ✓ 调用方走原路（★ 小星路径**一字不改** ✓）
 *   ★ 返回 `{reply, handled:true}` ⇒ ★ 这个回合归对练 ✓ 调用方**直接回** ✗
 */
async function rpHandleTurn(device, heard) {
  if (!RP_ON || !device) return null;
  const _rt0 = Date.now();
  const r = await _rpHandleTurnInner(device, heard);
  // ★★ 2026-10-21 加：★ 对练回合**也记会话日志** ✗
  //   ★ 起因：用户问"能看日志吗" ⇒ ★ 我说"分数在、对话没记" ✗
  //     因为记日志的钩子挂在**正常问答分支** ✓ 而★ 对练分支是提前 return 的 ⇒ **跳过了** ✗
  //   ★ 判据：**凡是有独立 return 的分支，都要自己记日志** ✗ 别指望主路径兜住 ✓
  if (r && r.handled && r.reply) {
    // ★★ 2026-10-21 依设备端回执 §3 加断言 ✗
    //   ★ 用户报过一次「它把我说的话重复了一遍」✓ 设备端查出真因是
    //     `/api/voice-chat` 失败后走"ASR 回声"回退 ⇒ ★ **把 heard 当回复念** ✗✓
    //   ★ 他们说"只要服务器偶尔回 reply == heard，症状就会重现"✓ 完全正确 ✓
    //   ⇒ ★ 这里硬断言 ✗：对练里 reply 只要等于 heard ⇒ **换掉** ✓ 并告警 ✓
    //   （★ 换掉而不是只告警 ✓ —— ★ 因为念出来的就是错的 ✓ 日志留证据就够了 ✓）
    if (String(r.reply).trim() === String(heard).trim()) {
      log(`ROLEPLAY ⚠️ reply===heard（★ 复读风险 ✗）heard=「${heard}」⇒ 已替换为兜底句 ✓`);
      r.reply = '嗯……你怎么看？';
    }
    try {
      logVoiceTurn({
        device: device, question: heard, reply: r.reply,
        action: '', params: null, mood: '',
        latency_ms: Date.now() - _rt0, branch: 'roleplay',
      });
    } catch (e) {}
  }
  return r;
}
async function _rpHandleTurnInner(device, heard) {
  const s = _rpSessions.get(device);

  // ── ① 已在对练 / 选场景中 ──
  if (s) {
    // ★ 出口③：静默超时 ✓（★ 不点评 ✓ 不算一次练习 ✓）
    if (Date.now() - s.at > RP_IDLE_MS) {
      log(`ROLEPLAY 静默 ${Math.round((Date.now() - s.at) / 1000)}s ⇒ 自动退出（★ 不点评 ✓ 不算练习 ✓）`);
      _rpSessions.delete(device);
      return { reply: '你走开了吧？那这次不算练完。想练的时候说「开始对练」。', handled: true };
    }
    s.at = Date.now();

    // ★ 出口①：用户明说结束 ✓
    if (RP_END_WORDS.test(heard)) {
      if (s.mode !== 'roleplay' || !s.msgs || !s.msgs.length) {
        _rpSessions.delete(device);
        return { reply: '行，那先不练了。想练了再叫我。', handled: true };
      }
      return { reply: await rpDebrief(device, s), handled: true };
    }

    // ── 选场景中：★ **两层** ✗ —— 先说模块 ✓ 再说场景名 ✓（规格书 §3 ✓）──
    if (s.mode === 'picking') {
      // ① 还在选模块 ⇒ 认模块名 ✓ 然后报该模块下的场景（★ 最多 3 个 ＋ "还有 N 个" ✓）
      if (s.stage !== 'scene') {
        const mods = s.modules || [];
        const mHit = mods.find(function (m) {
          return heard.indexOf(m.module) >= 0 || m.module.indexOf(heard) >= 0;
        });
        if (!mHit) {
          const head = mods.slice(0, 6).map(function (m) { return m.module; });
          return { reply: '这几个方向里挑一个：' + head.join('、') + '。', handled: true };
        }
        const inMod = (s.allScenes || []).filter(function (x) { return x.module === mHit.module; });
        const top = inMod.slice(0, 3);
        // ★ 只留这个模块的场景 ✓ 下一句按场景名匹配 ✓
        s.pending = inMod;
        s.stage = 'scene';
        s.at = Date.now();
        const moreN = inMod.length > 3 ? ('，还有 ' + (inMod.length - 3) + ' 个我没念') : '';
        return {
          reply: mHit.module + '有 ' + inMod.length + ' 个。先说这三个：'
            + top.map(function (x) { return x.name; }).join('、') + moreN + '。说名字就行。',
          handled: true,
          scenes: inMod.map(function (x) { return { id: x.id, name: x.name }; })
        };
      }
      // ② 选场景 ⇒ 匹配上就开场 ✓
      const list = s.pending || [];
      const hit = list.find(function (x) {
        return heard.indexOf(x.name) >= 0 || x.name.indexOf(heard) >= 0
          || (x.name.length > 4 && heard.indexOf(x.name.slice(0, 5)) >= 0);
      });
      if (!hit) {
        return { reply: '没对上号。你就说场景名字，比如「' + ((list[0] && list[0].name) || '客户投诉空调太冷') + '」。', handled: true };
      }
      s.mode = 'roleplay'; s.scene = hit; s.sceneId = hit.id; s.sceneName = hit.name; s.module = hit.module;
      s.msgs = []; s.turns = 0; s.at = Date.now();
      log(`ROLEPLAY ▶ 开始「${hit.name}」(${hit.id} / ${hit.module}) device=${device}`);
      let open = '';
      try {
        open = await rpCallLLM(rpSystemPrompt(hit, 1), [{
          role: 'user', content: '（对练开始。请以角色身份说第一句话，把上面的情境自然带出来，1~3 句，不要旁白。）'
        }], 70);
      } catch (e) {
        open = '你好，我这边有个事想跟你说一下。';
        log(`ROLEPLAY ⚠️ 开场失败: ${e.message}（★ 用兜底开场 ✓）`);
      }
      s.msgs.push({ role: 'assistant', content: open });
      s.turns = 1;
      return { reply: open, handled: true };
    }

    // ── 扮演中：★ 只演 ✓ 不评价 ✓ ──
    s.msgs.push({ role: 'user', content: heard });
    if (s.msgs.length > RP_MAX_TURNS * 3) s.msgs.shift();
    let out = '';
    try {
      out = await rpCallLLM(rpSystemPrompt(s.scene, Math.min(s.turns + 1, RP_MAX_TURNS)), s.msgs.slice(-8), 70);
    } catch (e) {
      log(`ROLEPLAY ⚠️ 演回合失败: ${e.message}`);
      return { reply: '（对方没接话，好像信号断了。再说一遍？）', handled: true };
    }
    s.msgs.push({ role: 'assistant', content: out });
    s.turns++;
    s.at = Date.now();
    // ★ 出口②：满 6 轮 ✓ 自动收尾 ✓
    if (s.turns >= RP_MAX_TURNS) {
      const debrief = await rpDebrief(device, s);
      return { reply: out + ' ' + debrief, handled: true };
    }
    return { reply: out, handled: true };
  }

  // ── ② 不在对练中 ──
  //   ★★ "开始对练"由 `routeIntent` 判定 ✓（★ 模型决定 ✓ 不硬编码关键词 ✓）
  if (_rpStartPending.has(device)) {
    _rpStartPending.delete(device);
    const sj = await rpLoadScenes();
    if (!sj || !sj.scenes || !sj.scenes.length) {
      return { reply: '场景库这会儿没拉到，等下再试试。', handled: true };
    }
    // ★ 一层一层报 ✗ —— 先报模块（★ 10 个 ⇒ 只念前 6 ＋ "还有 N 个" ✓ 规格书 §3 ✓）
    const mods = sj.modules || [];
    const head = mods.slice(0, 6).map(function (m) { return m.module; });
    const more = mods.length > 6 ? ('，还有 ' + (mods.length - 6) + ' 个别的') : '';
    // ★ 记住全量场景 ✓ 选模块后按 module 过滤 ✓（★ stage='module' ⇒ 先认模块 ✓）
    _rpSessions.set(device, {
      mode: 'picking', stage: 'module',
      modules: mods, allScenes: sj.scenes || [],
      pending: [], turns: 0, msgs: [], at: Date.now()
    });
    return {
      reply: '行，练哪个方向？' + head.join('、') + more + '。说一个就行，屏幕上也看得到全部。',
      handled: true,
      modules: mods
    };
  }
  return null;
}

/**
 * ★★ 服务器侧把一句回复合成成 **WAV** 并返回 URL（2026-10-21 ✓ 依设备端回执 CR-20261021-02 §2 ✗）
 *
 *   ★ 起因：★ 现在的链路里**有一次 TTS 往返是设备自己补的** ✗
 *     设备 → /api/voice-chat → 只拿到 {reply}（没 audio ✗）→ 设备再 POST /api/tts
 *     → 等整段 WAV → 下载 → 播放 ⇒ ★ 实测多花 **0.9~1.4 s** ✗
 *   ⇒ ★ 服务器**顺手合成好** ✓ 响应里直接给 audio ✓ 设备拿到就播 ✓
 *   ★★ 首字预期：★ 2.7 s → **1.6~1.8 s** ✓（★ 不改设备任何音频路径 ✓ 风险≈0 ✓）
 *
 *   ★★ 必须给 **RIFF WAV** ✗ —— ★ 设备只解析 PCM16 / μ-law ✓ **没有 mp3 解码器** ✗
 *     （★ 所以不能直接用 `handleTTS()` 的 mp3 ✓）
 *   ★ 复用：`ttsToPcmFile()`（合成 → pcm）＋ `pcmToWav()`（pcm → wav 头）✓ 都现成 ✓
 */
async function ttsWavUrl(text) {
  const s = String(text || '').trim();
  if (!s) return '';
  let pcmPath = '';
  try {
    pcmPath = await ttsToPcmFile(s);
    const pcm = fs.readFileSync(pcmPath);
    const name = 'rtts-' + Date.now() + '-' + Math.random().toString(36).slice(2, 7) + '.wav';
    fs.writeFileSync(path.join(AUDIO_DIR, name), pcmToWav(pcm));
    return '/api/voice-audio/' + name;
  } catch (e) {
    log(`TTS-AUDIO ⚠️ 合成失败（★ 只回文本 ✓ 设备会自己补 ✓）: ${e.message}`);
    return '';
  } finally {
    if (pcmPath) { try { fs.unlinkSync(pcmPath); } catch (e) {} }
  }
}

async function routeNeedsWeb(text) {
  if (!WEB_SEARCH_ON || !DEEPSEEK_KEY) return false;
  const ac = new AbortController();
  const tid = setTimeout(() => ac.abort(), 6000);
  try {
    const r = await fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST', signal: ac.signal,
      headers: { 'Authorization': `Bearer ${DEEPSEEK_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'deepseek-chat', temperature: 0, max_tokens: 60,
        messages: [
          { role: 'system', content: [
            '判断用户这句话需不需要【外部/实时信息】。需要就调用工具，不需要就别调用。',
            '★ 需要：天气、新闻、今天几号、股价/汇率、赛事、航班、任何"眼下的事实"。',
            '★ 不需要：店里的SOP/标准/流程/规定（我们有内部知识库）· 操作设备（录制/闹钟/备忘/倒计时）',
            '  · 打招呼/闲聊/自我介绍/问你是谁 · 靠常识就能答的通用做法。',
          ].join('\n') },
          { role: 'user', content: String(text || '') },
        ],
        tools: [{ type: 'function', function: { name: 'web_search', description: '需要外部实时信息时调用',
          parameters: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } } }],
        tool_choice: 'auto',
      }),
    });
    const j = await r.json();
    const tc = j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.tool_calls;
    return !!(tc && tc.length);
  } catch (e) {
    _webFail++;
    log(`WEB ⚠️ 路由判定失败（第 ${_webFail} 次，按"不搜"处理 ✓）: ${e.message}`);
    return false;
  } finally { clearTimeout(tid); }
}
/** ★ 联网回答（qwen-plus ＋ enable_search ＋ enable_source ✓）
 *  ⇒ 返回 { reply, sources } ／ null（失败 ✓ 调用方退回原路 ✓）*/
async function webAnswer(heard, lenHint) {
  if (!DASHSCOPE_API_KEY) { log('WEB ⚠️ 没有 DASHSCOPE_API_KEY ⇒ 退回原路'); return null; }
  const ac = new AbortController();
  const tid = setTimeout(() => ac.abort(), WEB_TIMEOUT_MS);
  const t0 = Date.now();
  try {
    const sys = [
      '你是「赋能02」门店带教助手"小星"，正在给一线员工答话（会被语音念出来）。',
      '★ 性格幽默接地气，像店里业务最强又爱开玩笑的前辈 ✓ 但**别贫嘴**、一两句就收 ✓。',
      // ★★ 2026-10-12 修 ✗：这条**原来没有**能力边界 ⇒ 联网回合能随便吹 ✓（用户报的问题的一半 ✓）
      //   ⇒ 现在**共用模块级同一份** ✓（别在这里再写一遍 ✗）
      ...PERSONA_CAPS,
      '★ 用**口语**回答，**不要 markdown、不要星号、不要分点符号、不要 emoji** —— 会被逐字念出来。',
      '★ 搜到就用搜到的 ✓ 说清"这是查到的"✓；**搜不到或不确定就直说，别编** ✗。',
      `★ 长度控制在 ${lenHint || '180 字以内'}。`,
      // ★ CR-20261012-02：同一句话里一起给出【语气】⇒ ★ 天然与 reply 同源 ✓
      '★★ 最后**另起一行**写一行语气（给设备显示表情用 ✓），只能是这 6 个词之一：',
      '  语气：happy ／ curious ／ sorry ／ excited ／ serious ／ neutral',
      '  （★ 不许自造词 ✗；语气必须与你上面说的话一致 ✗ ——「嘴上说好、脸上道歉」最糟 ✗）',
    ].join('\n');
    const r = await fetch(DASHSCOPE_URL, {
      method: 'POST', signal: ac.signal,
      headers: { 'Authorization': `Bearer ${DASHSCOPE_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: WEB_MODEL,
        input: { messages: [{ role: 'system', content: sys }, { role: 'user', content: String(heard || '') }] },
        parameters: { enable_search: true, search_options: { enable_source: true }, result_format: 'message' },
      }),
    });
    const j = await r.json();
    const o = j.output || {};
    const msg = (o.choices && o.choices[0] && o.choices[0].message) || {};
    const raw = String(msg.content || '');
    const sr = (o.search_info && o.search_info.search_results) || [];
    // ★★ 判据不靠"它说它搜了"✗ —— 看 usage.plugins.search（真搜过的铁证 ✓）
    const searched = !!((j.usage || {}).plugins || {}).search;
    // ★ CR-20261012-02：语气行 —— ★ 必须**先抽掉**再清洗 ✗（否则 speakable 之后
    //   文字里还留着"语气：happy"✓ 会被 TTS 念出来 ✗✗）
    const moodM = String(raw).match(/语气\s*[:：]\s*([A-Za-z]+)/);
    const mood = normMood(moodM && moodM[1]);
    const reply = speakable(String(raw).replace(/^\s*语气\s*[:：]\s*[A-Za-z]+\s*$/gmi, ''));
    if (!reply) { log(`WEB ⚠️ 空答案 ⇒ 退回原路（${Date.now() - t0}ms）`); return null; }
    _webUsed++;
    log(`WEB ✓ ${Date.now() - t0}ms 搜过=${searched ? 'Y' : 'N'} 来源${sr.length}条 额度${_webUsed}/${WEB_DAILY_MAX}`
      + (sr.length ? ` ｜ ${sr.slice(0, 3).map(x => String(x.site_name || '') + ':' + String(x.title || '').slice(0, 20)).join(' / ')}` : ''));
    if (sr.length) log(`WEB 来源URL: ${sr.slice(0, 3).map(x => x.url).join(' ')}`);   // ★ 可核对 ✓
    return { reply, sources: sr, searched };
  } catch (e) {
    _webFail++;
    log(`WEB ✗ 失败（第 ${_webFail} 次，退回原路 ✓）: ${e.message}（${Date.now() - t0}ms）`);
    return null;
  } finally { clearTimeout(tid); }
}
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

// ★ 找出本次 tts.py 产出的分段文件（/tmp/tts-chunk-<n>.mp3），按序号排序
//   为什么需要：tts.py 超过 MAX_CHARS(200) 会按句切段输出 -1/-2/-3…，
//   而原来只 copy 了 -1 ⇒ 回复 >200 字时播到一半突然停（CR-20260929-03 §3）
function globTtsChunks() {
  try {
    return fs.readdirSync('/tmp')
      .filter(n => /^tts-chunk-\d+\.mp3$/.test(n))
      .sort((a, b) => parseInt(a.match(/(\d+)/)[1], 10) - parseInt(b.match(/(\d+)/)[1], 10))
      .map(n => '/tmp/' + n);
  } catch (e) { return []; }
}

// TTS: 文字 → 音频
//   ★ audioFmt（CR-20260929-03 §16.4，设备端建议的字段名）：
//     'pcm16'（缺省）/ 'ulaw8' ⇒ 生成的 .wav 用 8bit μ-law（tag=7, 16k）
//     ⇒ ★ 体积直接减半（16k/16bit=32KB/s → 16k/8bit=16KB/s）
//     ⇒ 设备端 §16 已验完 μ-law 解码（查表 + 正经遍历 RIFF 块找 fmt/data）✓
//     ⚠️ 默认必须是 pcm16 —— 浏览器/WebChat 也在用 /api/tts，它们没有 μ-law 解码器 ✗
async function handleTTS(text, audioFmt) {
  const ulaw = (audioFmt === 'ulaw8');
  const timestamp = Date.now();
  const ttsMp3 = `tts-${timestamp}.mp3`;
  const ttsWav = `tts-${timestamp}.wav`;
  const mp3Target = path.join(AUDIO_DIR, ttsMp3);
  const wavTarget = path.join(AUDIO_DIR, ttsWav);

  try {
    // ★★★ CR-20260929-03 §3 修：tts.py 超过 MAX_CHARS(200) 会按句切成多段
    //   （/tmp/tts-chunk-1.mp3、-2.mp3…，脚本注释自己就写着"大文件易截尾"），
    //   而这里原来【只 copy 了第 1 段】⇒ 回复 >200 字时**播到一半突然停** ✗✗
    //   实测 260 字：产出 435,975B + 459,799B 两段，服务器只发出 869,434B/27.2s（=只有第一段）✗
    //   ⇒ ① 先清掉旧段（否则会把上一次的段也 concat 进来 ✗）② 按序号全部合并
    //   ⚠️ /tmp/tts-chunk-*.mp3 是【固定路径】⇒ 并发调用会互相覆盖（既有缺陷，本次不扩大；
    //      彻底解决要改 tts.py 支持自定义前缀，另开）
    for (const oldChunk of globTtsChunks()) { try { fs.unlinkSync(oldChunk); } catch (e) {} }

    // ★ 2026-10-12：把**模型**也显式传下去 ✗ —— 原来只传 `--voice`，
    //   而 `tts.py` 的模型默认是写死的 `cosyvoice-v1` ⇒ 服务商下线 v1 后**全线不出声** ✗
    //   （实测：光改音色没用，模型和音色**必须配套** ✓ 见上面 TTS_MODEL 的注释 ✓）
    const ttsRun = await execFileAsync('python3',
      [TTS_SCRIPT, text, '--model', TTS_MODEL, '--voice', TTS_VOICE], { env: execEnv });

    const chunks = globTtsChunks();
    if (!chunks.length) {
      // ★★ 2026-10-12：**带上脚本原话** ✗ —— `tts.py` 全段失败时**仍退出 0** ✗，
      //   旧代码于是只报一句"输出文件不存在" ✓ ⇒ 看不出是**服务商**的问题（误导排障 ✓）
      //   ⇒ 把 stdout/stderr 尾部拼进来，下次一眼能看出是 "Model not exist" 还是网络 ✓
      const raw = String((ttsRun && (ttsRun.stdout || ttsRun.stderr)) || '').trim().slice(-400);
      throw new Error('TTS 输出文件不存在（model=' + TTS_MODEL + ' voice=' + TTS_VOICE + '）'
        + (raw ? '；脚本原话: ' + raw.replace(/\s+/g, ' ') : '；脚本无输出'));
    }
    if (chunks.length === 1) {
      fs.copyFileSync(chunks[0], mp3Target);
    } else {
      // 多段合并：各段同为 libmp3lame/128k ⇒ ffmpeg concat 用 -c copy 无损且快
      const listFile = path.join(AUDIO_DIR, `tts-${timestamp}-list.txt`);
      fs.writeFileSync(listFile, chunks.map(f => `file '${f}'`).join('\n'), 'utf8');
      execSync(`ffmpeg -y -f concat -safe 0 -i "${listFile}" -c copy "${mp3Target}"`, { stdio: 'pipe', timeout: 30000 });
      try { fs.unlinkSync(listFile); } catch (e) {}
      log(`TTS 分段合并: ${chunks.length} 段 → ${path.basename(mp3Target)}`);
    }

    // 同时生成 WAV 版本（SoX 无 libmad 无法播 MP3）
    //   ★ ulaw8 时改用 pcm_mulaw（8bit，tag=7）—— 设备端按 fmt 块自适应 ✓
    try {
      const ac = ulaw ? 'pcm_mulaw' : 'pcm_s16le';
      execSync(`ffmpeg -y -i "${mp3Target}" -acodec ${ac} -ar 16000 -ac 1 "${wavTarget}"`, { stdio: 'pipe', timeout: 15000 });
    } catch(e) {
      log(`WAV 转码失败(不影响 MP3): ${e.message}`);
    }

    // ★★★ 2026-10-21 依设备端回执修 ✗✗
  //   ★ 原来**永远**返回 `.mp3` ✗ —— 即使 ulaw 分支已经把 WAV 生成好了（L2178 ✓）
  //   ⇒ ★ 设备只解析 RIFF WAV ✓ **没有 mp3 解码器** ✗ ⇒ 拿到 .mp3 = 播不了 ✗
  //   ★★ 判据：**"生成了" ≠ "返回了"** ✗ —— 生成在 L2178 ✓ 返回在 L2183 ✓ 两者不通 ✓
  //   ★ 三种格式 ✗：
  //     · `format=ulaw8` ⇒ μ-law WAV（★ 设备要的 ✓ 体积约 PCM16 的一半 ✓）
  //     · `format=wav`   ⇒ PCM16 WAV（★ 兼容其它要 WAV 的消费者 ✓）
  //     · 不传           ⇒ .mp3（★ **保持原样** ✓ 浏览器/WebChat 一直在用 ✗ 不能改 ✓）
  if (audioFmt === 'ulaw8' || audioFmt === 'wav') {
    if (!fs.existsSync(wavTarget)) throw new Error('WAV 转码失败（format=' + audioFmt + '）');
    return { success: true, audio: `/api/voice-audio/${ttsWav}`, format: audioFmt };
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
  // ★ 2026-10-12：与 handleTTS 一致，**模型与音色配套传** ✗（只传音色会落回写死的 v1 ✓ 已下线 ✓）
  await execFileAsync('python3', [TTS_SCRIPT, text, '--model', TTS_MODEL, '--voice', TTS_VOICE], { env: execEnv });
  if (!fs.existsSync(mp3)) throw new Error('TTS 无输出（model=' + TTS_MODEL + ' voice=' + TTS_VOICE + '）');
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

// ══════════════════════════════════════════════════════════════════════
// ★★ 2026-10-12：命令回合的「静音垫」—— 为什么需要它 ✗
//
//   用户要求：说「开始录制」⇒ **不播报任何内容** ✓
//   ⇒ 我方自然想到"reply 空 ⇒ 不发音频" ✗ —— **但设备端会判为失败** ✗✗：
//     他们 `voice.c` 的流式收尾是 `if (s_stream.audio_bytes > 0) { 成功 } `
//     否则「**一个字节都没播 ⇒ 退回老端点**」✓（这是他们的"永不比老版差"设计 ✓ 本身没错 ✓）
//     ⇒ 0 字节音频 = 被判失败 ⇒ 白跑一轮回退 + `action` 可能被应用两次 ✗
//
//   ⇒ 做法：命令回合送 **~40 ms 静音** ✓ ⇒ `audio_bytes > 0` ⇒ 判成功 ✓
//     而**人耳听不到任何内容** ✓ ⇒ 与用户要求一致 ✓
//   ★ 判据：**"不播报"不能用"不发音频"来表达** ✗ —— 在对方协议里，
//     "没有音频"是一个**错误信号**，不是一个**空信号** ✓（这两者不能混 ✓）
// ══════════════════════════════════════════════════════════════════════
const SILENCE_MS = 40;
/** 生成一段静音 PCM（按当前格式 ✓ 16k/单声道 ⇒ pcm16 = 32000 B/s，ulaw8 = 16000 B/s ✓） */
function silencePcm(afmt) {
  const bytes = Math.round((afmt === 'ulaw8' ? 16000 : 32000) * (SILENCE_MS / 1000));
  // ★ μ-law 的**静音不是 0** ✗✗ —— 0x00 解出来是满幅负值（很响 ✗）
  //   查表：μ-law 里 0xFF 才是"接近 0" ✓（他们解码器照标准表 ✓）⇒ 该填 0xFF ✓
  return Buffer.alloc(bytes, afmt === 'ulaw8' ? 0xff : 0);
}
/** 命令回合给老端点（非流式）用的静音音频文件（懒创建一次 ✓ 之后复用同一个 URL ✓） */
let _silenceUrl = '';
function silenceAudioUrl() {
  if (_silenceUrl) return _silenceUrl;
  try {
    const name = 'silence-16k-mono.wav';
    const p = path.join(AUDIO_DIR, name);
    if (!fs.existsSync(p)) {
      const n = Math.round(16000 * (SILENCE_MS / 1000));      // 16 kHz / 16 bit / 单声道
      const data = Buffer.alloc(n * 2);
      const h = Buffer.alloc(44);
      h.write('RIFF', 0); h.writeUInt32LE(36 + data.length, 4); h.write('WAVE', 8);
      h.write('fmt ', 12); h.writeUInt32LE(16, 16); h.writeUInt16LE(1, 20); h.writeUInt16LE(1, 22);
      h.writeUInt32LE(16000, 24); h.writeUInt32LE(32000, 28); h.writeUInt16LE(2, 32); h.writeUInt16LE(16, 34);
      h.write('data', 36); h.writeUInt32LE(data.length, 40);
      fs.writeFileSync(p, Buffer.concat([h, data]));
      log(`命令回合静音垫已生成: ${name}（${44 + data.length} B ✓ ${SILENCE_MS}ms ✓）`);
    }
    _silenceUrl = '/api/voice-audio/' + name;
    return _silenceUrl;
  } catch (e) {
    log(`⚠️ 静音垫生成失败（命令回合将不带音频）: ${e.message}`);
    return '';
  }
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

  // ══════════════════════════════════════════════════════════════════════
  // ★★ 赋能03 手表「岗位职责追踪」转发（CR-20261019-01 ✓）
  //
  //   ★ 为什么在 S1 转发 ✗：★ 手表**没有学员 token** ✓ 而数据在 S2 ✓
  //     ⇒ ★ 复用 `/api/cam/*` 那条既有路：**S1 验设备密钥 ⇒ 带内部 token 打 S2** ✓
  //     （★ 不另造鉴权 ✓ 与 cam 完全同一套 `checkDeviceSecret` ✓）
  //
  //   ★★ 安全要点 ✗✗：**`dev_id` 一律用"验过密钥的那个"** ✗ ——
  //      ★ 绝不用客户端自己报的（★ 否则任何设备都能替别人点「收到」✓）
  //      report 的 body 里如果带了 dev_id ⇒ **被这里覆盖** ✓
  //
  //   ★ "推送"= 设备**拉** ✓（手表在 NAT 后 ✓ 推不进去 ✓）
  //     ⇒ ★ 服务器在 `pull` 里按当前时间算好"现在该弹什么" ✓（`should_alert` ✓）
  // ══════════════════════════════════════════════════════════════════════
  /* ══════════════════════════════════════════════════════════════════
   * ★★★ 期1：边录边识别（CR-20261021-02 ✓ 方案：协同/specs/CR-20261021-02 ✓）
   *
   *   ★★ 为什么做这个 ✗：★ 实测首字延迟 ~2.7s ✓ 其中 **ASR 占 1.5~1.8s** ✗
   *      ★ 只改 TTS/LLM 流式 ⇒ 下限被 ASR 锁死 ✓ 只省 0.35s ✗
   *      ⇒ ★ **要 1 秒级，必须让 ASR 与用户说话并行** ✗✓（★ 边录边识别 ✓）
   *
   *   ★★ 设计要点（★ 最简单的正确做法 ✗）：
   *     ① 设备边录边传分片 ✓ 服务器**累积**
   *     ② 每次对**累积的全部音频**做一次全量识别 ✓ ⇒ ★ 返回"到目前为止的全文"✗
   *        （★ 不是增量 ✗ —— ★ 增量会反复 ✓ 全量天然稳定 ✓✓）
   *     ③ ★ **并发保护**：同一设备同时只跑一次识别 ✓ 正在跑就**直接返回上次结果** ✗
   *        （★ 20 片 × 全量识别 = O(n²) ✗ ⇒ 必须丢弃中间态 ✓）
   *     ④ ★ **静音处切 / 片间重叠** 是**设备侧**的事 ✓（★ 方案 §3 期1 三坑之一 ✓）
   *
   *   ★ 端点（★ 只增不改 ✓ 老固件完全不受影响 ✓）：
   *     POST /api/voice/asr-chunk?device=xxx&seq=1   body = 音频字节
   *       → { ok:true, seq, text:"到目前为止的全文", bytes }
   *     POST /api/voice/asr-commit?device=xxx        body = 最后一片（可空）
   *       → { ok:true, text:"最终全文" }
   * ══════════════════════════════════════════════════════════════════ */
  // ★ 期1 专用响应（只 4 个字段 ✓ 不回文本 ✗）
  function jsonOut4(code, obj) { try { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' }); res.end(JSON.stringify(obj)); } catch (e) {} }
  const _asrSess = new Map();       // device -> { parts: Map<seq,Buffer>, text, at, busy }
  const ASR_SESS_TTL = 120 * 1000;  // ★ 2 分钟没动静 ⇒ 丢 ✓（★ 防止内存漏 ✗）

  function _asrSessGet(device) {
    let s = _asrSess.get(device);
    if (s && Date.now() - s.at > ASR_SESS_TTL) { _asrSess.delete(device); s = null; }
    if (!s) { s = { parts: new Map(), text: '', at: Date.now(), busy: false }; _asrSess.set(device, s); }
    s.at = Date.now();
    return s;
  }
  // ★ 按 seq 顺序拼（★ 并发到达也不会乱 ✓ 缺号就跳过 ✓ 不抛错）
  function _asrConcat(s) {
    const keys = Array.from(s.parts.keys()).sort(function (a, b) { return a - b; });
    return Buffer.concat(keys.map(function (k) { return s.parts.get(k); }));
  }

  if (req.method === 'POST' && req.url.startsWith('/api/voice/asr-chunk')) {
    const q = getQuery(req.url);
    const dev = String(q.device || q.dev_id || q.device_id || '').trim();
    const seq = parseInt(q.seq, 10) || 0;
    if (!dev) { jsonOut4(400, { ok: false, error: '缺少 device' }); return; }
    const bufs = [];
    req.on('data', c => bufs.push(c));
    req.on('end', async () => {
      const data = Buffer.concat(bufs);
      if (!data.length) { jsonOut4(400, { ok: false, error: '空分片' }); return; }
      const s = _asrSessGet(dev);
      s.parts.set(seq, data);
      // ★★ 并发保护 ✗ —— 正在识别就**直接回上次结果** ✓（★ 丢弃中间态 ✓ O(n²) 也扛得住 ✓）
        if (s.busy) { jsonOut4(200, { ok: true, seq: seq, final: false, len: _asrConcat(s).length }); return; }
      s.busy = true;
      try {
        const all = _asrConcat(s);
        let text = '';
        try {
          const asr = await handleASR(all);      // ★ 与 /api/voice-chat 用**同一个**识别 ✓
          text = String((asr && asr.text) || '').trim();
          s.text = text;                          // ★ 只记"最近一次成功"的全文 ✓
        } catch (e) {
          log(`ASR-CHUNK ⚠️ 识别失败（${dev} seq=${seq}）: ${e.message}（★ 返回上次全文 ✓）`);
        }
        jsonOut4(200, { ok: true, seq: seq, final: false, len: all.length });
      } finally { s.busy = false; }
    });
    return;
  }

  if (req.method === 'POST' && req.url.startsWith('/api/voice/asr-commit')) {
    const q = getQuery(req.url);
    const dev = String(q.device || q.dev_id || q.device_id || '').trim();
    if (!dev) { jsonOut4(400, { ok: false, error: '缺少 device' }); return; }
    const bufs = [];
    req.on('data', c => bufs.push(c));
    req.on('end', async () => {
      const tail = Buffer.concat(bufs);
      const s = _asrSess.get(dev);
      if (!s) {
        // ★ 没有分片 ⇒ 只有最后一片 ✓ 直接识别它 ✓（★ 兼容"设备没分片直接 commit"✓）
        if (!tail.length) { jsonOut4(400, { ok: false, error: '没有音频' }); return; }
        try {
          const asr = await handleASR(tail);
          jsonOut4(200, { ok: true, text: String((asr && asr.text) || '').trim(), chunks: 0 });
        } catch (e) { jsonOut4(500, { ok: false, error: e.message }); }
        return;
      }
      if (tail.length) s.parts.set(99999, tail);       // ★ 最后一片放最后 ✓
      s.at = Date.now();
      try {
        const all = _asrConcat(s);
        const asr = await handleASR(all);
        const text = String((asr && asr.text) || '').trim();
        const n = s.parts.size;
        _asrSess.delete(dev);                          // ★★ 交卷即**清会话** ✗（★ 下一次对练从头开始 ✓）
        log(`ASR-COMMIT ✓ ${dev} ${n} 片 / ${all.length} 字节 ⇒ ${text.length} 字`);
        jsonOut4(200, { ok: true, text: text, chunks: n, bytes: all.length });
      } catch (e) {
        log(`ASR-COMMIT ⚠️ 失败（${dev}）: ${e.message}`);
        jsonOut4(500, { ok: false, error: e.message });
      }
    });
    return;
  }
  /* ══════════════════════════════════════════════════════════════════
   * ★★ 设备端「模拟对练」屏上入口（CR-20261021-01 ✓ 规格：协同/specs/CR-20261021-01 ✓）
   *
   *   ★ 用户 2026-10-21：「这个对练的激活方式不光是语音激活，用户也可以在设备端
   *     进入『模拟对练』，在里面选择类型、选择场景，然后开始练习。」
   *   ★★ 这套端点是给**设备屏**用的 ✗ —— 与语音入口**共用同一个会话池** ✓
   *      （`_rpSessions` ✓）⇒ ★ 语音说"开始对练"和屏上点"开始练习"**是同一个会话** ✓
   *
   *   ★ 鉴权：与 /api/cam/* 同一套 `X-Device-Secret` ✓（★ 一机一密 ✓）
   *   ★★ 对练的**回合**仍然走 /api/voice-chat（音频 ✓）—— ★ 这里不加音频端点 ✗
   * ══════════════════════════════════════════════════════════════════ */

  /* ══════════════════════════════════════════════════════════════════
   * ★★ 期2：纯文本入口 ＋ 流式（CR-20261021-02 §4 ✓ 设备端回执 §2.2 ✗）
   *
   *   ★ 设备端固件**已经写好并烧进去了**（ac12859 ✓ 按行解析 `data: {"delta":"…"}` ✓
   *     遇 `"done"` 收工 ✓ 端点不在就自动退回音频老路 ✓）
   *   ⇒ ★ 他们等的就是这个端点 ✓
   *
   *   ★★ 为什么它快 ✗：★ 音频入口是"服务器还要 ASR 一遍" ✓
   *     而期1 之后设备**已经有文本** ✓ ⇒ ★ 别再识别第二次 ✗
   *     且 ★ 这里**流式**返 ⇒ 设备**收到第一段就能开始 TTS** ✓（"首句先播"✓）
   *
   *   ★★ 对练模式下**暂时不流式** ✗ —— ★ `rpHandleTurn` 是整体返回的 ✓
   *     ⇒ ★ 对练时一次性给全文 ✓（★ 等 §3 期2 的"句子队列"再优化 ✓）
   *
   *   响应格式（★ 设备按行切 ✓ 不需要 SSE 的 event/id ✗）：
   *     data: {"delta":"是啊，"}
   *     data: {"delta":"我都说了…"}
   *     data: {"done":true,"reply":"全文","audio":"/api/voice-audio/x.wav"}
   * ══════════════════════════════════════════════════════════════════ */
  if (req.method === 'POST' && req.url === '/api/voice-chat-text') {
    let tb = '';
    req.on('data', c => { tb += c; });
    req.on('end', async () => {
      let o = {};
      try { o = JSON.parse(tb || '{}'); } catch (e) { o = {}; }
      const device = String(o.device || o.dev_id || '').trim();
      const text = String(o.text || '').trim();
      const reset = String(o.reset || '') === '1';
      const send = (obj) => { try { res.write('data: ' + JSON.stringify(obj) + '\n\n'); } catch (e) {} };
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no' });
      if (!text) { send({ done: true, error: 'empty_text' }); res.end(); return; }
      log(`voice-chat-text: device=${device || '-'} text=「${text.slice(0, 40)}」`);

      // ★★ 对练会话中 ⇒ 走对练（★ 暂不流式 ✓ 一次性给全文 ✓）
      try {
        if (RP_ON && device && _rpSessions.has(device) && !reset) {
          const rp = await rpHandleTurn(device, text);
          if (rp && rp.handled) {
            const au = '';   // ★ 2026-10-21 止损：不在同步路径上合成 ✓
            send({ done: true, reply: rp.reply, audio: au, roleplay: true });
            res.end();
            return;
          }
        }
      } catch (e) { log(`voice-chat-text ⚠️ 对练分支异常: ${e.message}（★ 继续走普通 ✓）`); }

      // ★ 普通模式：★ 流式 delta ✓（★ 复用与小星同一套提示词 ✓ 不含对练 ✗）
      try {
        // ★★ 2026-10-21：★ 这里**不能**用 `kbSearchForVoice` / `VOICE_CHAT_SYSTEM` ✗
        //   它们都是**后面才声明的 const** ✓ 我这段在它们前面 ⇒ **TDZ**
        //   （实测报错：Cannot access 'VOICE_CHAT_SYSTEM' before initialization ✗）
        //   ★ 判据：★ **缩进一样只说明"同一个作用域"，不说明"能用"** ✗
        //     还要看**声明顺序** ✗ —— 这是今天第 5 个同类坑 ✓
        //   ⇒ ★ 这个端点**自带提示词** ✗ 不依赖任何外层声明 ✓ 最稳 ✓
        const sys = [
          '你是「赋能02」设备上的带教教练，名字叫小星，给门店一线员工帮忙。',
          '★★ 幽默、接地气 ✓ 一两句就够 ✗ 别贫 ✓',
          '★ 直接从"答："之后开始说 ✓ 不要加任何前缀/解释 ✗',
          '★ 不知道就说不知道 ✗ 绝不编造店里制度 ✗',
          '★ 每次都要落到企业文化的一条上（真诚：讲真话做实事爱客户守承诺 · ',
          '   高效：成果导向沟通简单标准明确推进有力 · 专业：精流程定标准练技能严考核 · ',
          '   担当：会放权勇挑战敢行动定决策担责任）✓ 一句话带过 ✗ 别背条文 ✓',
        ].join('\n');
        const ac = new AbortController();
        const tid = setTimeout(() => ac.abort(), 25000);
        let full = '';
        try {
          const r = await fetch('https://api.deepseek.com/chat/completions', {
            method: 'POST', signal: ac.signal,
            headers: { 'Authorization': `Bearer ${DEEPSEEK_KEY}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({
              model: 'deepseek-chat', temperature: 0.4, max_tokens: 220, stream: true,
              messages: [{ role: 'system', content: sys }, { role: 'user', content: text }],
            }),
          });
          if (!r.ok || !r.body) throw new Error('HTTP ' + r.status);
          const reader = r.body.getReader();
          const dec = new TextDecoder('utf-8');
          let buf = '';
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            buf += dec.decode(value, { stream: true });
            const lines = buf.split('\n');
            buf = lines.pop() || '';
            for (const ln of lines) {
              const s2 = ln.trim();
              if (!s2.startsWith('data:')) continue;
              const payload = s2.slice(5).trim();
              if (payload === '[DONE]') continue;
              try {
                const jj = JSON.parse(payload);
                const d = jj.choices && jj.choices[0] && jj.choices[0].delta && jj.choices[0].delta.content;
                if (d) { full += d; send({ delta: d }); }
              } catch (e) {}
            }
          }
        } finally { clearTimeout(tid); }
        // ★ 只取 `答：` 之后的内容（★ 与小星同一套契约 ✓）
        let reply = String(full || '').trim();
        const mm = reply.match(/答[：:]\s*([\s\S]+)/);
        if (mm) reply = mm[1].trim();
        const au2 = '';  // ★ 同上 ✓
        send({ done: true, reply: reply, audio: au2 });
        res.end();
      } catch (e) {
        log(`voice-chat-text ⚠️ 失败: ${e.message}（★ 设备会自动退回音频老路 ✓）`);
        try { send({ done: true, error: String(e.message || e) }); res.end(); } catch (e2) {}
      }
    });
    return;
  }
  // GET /api/voice-roleplay/scenes —— 场景列表（★ 设备屏直接渲染 ✓）
  if (req.method === 'GET' && req.url.startsWith('/api/voice-roleplay/scenes')) {
    const q = getQuery(req.url);
    const dev = pickDeviceId(req, q, null);
    if (!checkDeviceSecret(pickSecret(req, q, null), dev)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '设备密钥错误' }));
      return;
    }
    // ★★ 2026-10-21 依设备端回执修（★ 真 bug ✗）：
    //   设备下行缓冲 **4 KB**（`downlink.c` `char buf[4096]` ✓ 读满即止 ✓ **不报错** ✗）
    //   而 200 条场景 ≈ 十几 KB ⇒ ★ **静默截断** ✓ 设备分不出"被截断" ✗✗
    //   ⇒ ★★ **不带 `module` 时只回模块清单**（★ 约 1 KB ✓ 安全 ✓）
    //      ★ 设备先取清单 ⇒ 再按 module 取该模块 20 条（★ 约 2 KB ✓）
    //   ★ 另给 `count` 与 `total` ✗ —— ★ 设备拿 `count` 和"解析出几条"对账 ✓ 对不上就报错 ✓
    const wantMod = String(q.module || '').trim();
    rpLoadScenes().then(function (sj) {
      if (!sj || !sj.scenes) {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: 'scenes_unavailable' }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      if (!wantMod) {
        // ★ 只回菜单 ✓ 不回 scenes ✗（★ 免得十几 KB 把 4 KB 缓冲撑爆 ✓）
        res.end(JSON.stringify({ ok: true, total: sj.total, modules: sj.modules || [], scenes: [], count: 0 }));
        return;
      }
      const allInMod = sj.scenes.filter(function (x) { return x.module === wantMod; });
      // ★★ 2026-10-21 实测：★ 20 条 = **4006 字节** ✗ 而设备缓冲 **4096** ⇒ **只剩 90 字节** ✗✗
      //   ★ 场景名一变长 / 加一个字段 ⇒ **立刻回到静默截断** ✓
      //   ⇒ ★ **默认只回 12 条**（约 2.4 KB ✓ 留一半余量 ✓）＋ 支持 `?limit=`（上限 20 ✓）
      //   ★ 判据：**贴着缓冲边界跑 = 迟早出事** ✗ 余量必须留一半 ✓
      let lim = parseInt(q.limit, 10);
      if (!(lim > 0)) lim = 12;
      if (lim > 20) lim = 20;
      const scenes = allInMod.slice(0, lim);
      res.end(JSON.stringify({
        ok: true, total: sj.total, modules: sj.modules || [],
        module: wantMod, in_module: allInMod.length, limit: lim,
        scenes: scenes, count: scenes.length
      }));
    }).catch(function (e) {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    });
    return;
  }

  // GET /api/voice-roleplay/state —— 当前状态（★ 屏幕轮询用 ✓ 建议 10~15 秒一次 ✓）
  if (req.method === 'GET' && req.url.startsWith('/api/voice-roleplay/state')) {
    const q = getQuery(req.url);
    const dev = pickDeviceId(req, q, null);
    if (!checkDeviceSecret(pickSecret(req, q, null), dev)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '设备密钥错误' }));
      return;
    }
    const s = dev ? _rpSessions.get(dev) : null;
    const idle = s ? (Date.now() - s.at > RP_IDLE_MS) : false;
    if (!s || idle) {
      if (s && idle) _rpSessions.delete(dev);      // ★ 顺手清理 ✓
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, active: false }));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({
      ok: true, active: true, mode: s.mode,
      scene: s.scene ? { id: s.scene.id, name: s.scene.name, module: s.scene.module } : null,
      turns: s.turns || 0, max_turns: RP_MAX_TURNS,
      // ★★ 2026-10-21 依设备端回执加 ✗：对方**最新一句**
      //   ★ 用途：① 日志里能自证"轮次真的在推进" ✓ ② 将来要恢复屏上文字也直接有数据 ✓
      last_reply: (function () {
        const mm = s.msgs || [];
        for (let i = mm.length - 1; i >= 0; i--) if (mm[i].role === 'assistant') return mm[i].content;
        return '';
      })()
    }));
    return;
  }

  // POST /api/voice-roleplay/start —— 屏上点「开始练习」（★ 直接指定 scene_id ✓ 不问模型 ✗）
  if (req.method === 'POST' && req.url === '/api/voice-roleplay/start') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      let o = {};
      try { o = JSON.parse(body || '{}'); } catch (e) { o = {}; }
      const dev = pickDeviceId(req, null, o);
      if (!checkDeviceSecret(pickSecret(req, null, o), dev)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '设备密钥错误' }));
        return;
      }
      if (!dev) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '缺少 dev_id' }));
        return;
      }
      const sid = String(o.scene_id || '').trim();
      if (!sid) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '缺少 scene_id' }));
        return;
      }
      try {
        const sj = await rpLoadScenes();
        if (!sj || !sj.scenes) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ ok: false, error: 'scenes_unavailable' }));
          return;
        }
        const hit = sj.scenes.find(function (x) { return x.id === sid; });
        if (!hit) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ ok: false, error: 'scene_not_found' }));
          return;
        }
        // ★★ 一旦 start ⇒ **重置**该设备的对练会话 ✓（★ 语音入口与屏上入口共用同一个 ✓）
        _rpStartPending.delete(dev);
        const s = {
          mode: 'roleplay', scene: hit, sceneId: hit.id, sceneName: hit.name,
          module: hit.module, msgs: [], turns: 0, at: Date.now()
        };
        _rpSessions.set(dev, s);
        log(`ROLEPLAY ▶（屏上）开始「${hit.name}」(${hit.id}) device=${dev}`);
        // ★ 开场：★ 角色第一句 ✓ 文本 ＋ 音频都给 ✓
        let open = '';
        try {
          open = await rpCallLLM(rpSystemPrompt(hit, 1), [{
            role: 'user', content: '（对练开始。请以角色身份说第一句话，把上面的情境自然带出来，1~3 句，不要旁白。）'
          }], 70);
        } catch (e) {
          open = '你好，我这边有个事想跟你说一下。';
          log(`ROLEPLAY ⚠️ 屏上开场生成失败: ${e.message}（★ 用兜底开场 ✓）`);
        }
        s.msgs.push({ role: 'assistant', content: open });
        s.turns = 1;
        // ★★ 开场**只给文本** ✗ —— ★ 设备本来就会调 `/api/tts`（★ 它天天在用 ✓）
        //   ★ 2026-10-21：我原来写了个 `ttsSynthesize()` ✗ —— ★ 那个函数**根本不存在** ✓
        //     （★ `node --check` 查不出来 ✓ 与 `LLM_TIMEOUT_MS` 同一类坑 ✗）
        //   ★ 判据：**跨模块复用前先 grep 确认函数真的存在** ✗ 别凭印象写名字 ✓
        const audioUrl = '';
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          ok: true,
          session: {
            mode: 'roleplay',
            scene: { id: hit.id, name: hit.name, module: hit.module, description: hit.description || '' },
            turns: 1, max_turns: RP_MAX_TURNS
          },
          opening: { text: open, audio_url: audioUrl }
        }));
      } catch (e) {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
    });
    return;
  }

  // POST /api/voice-roleplay/end —— 屏上点「结束练习」（★ 点评＋打分＋落库 ✓ 服务器做 ✓）
  if (req.method === 'POST' && req.url === '/api/voice-roleplay/end') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      let o = {};
      try { o = JSON.parse(body || '{}'); } catch (e) { o = {}; }
      const dev = pickDeviceId(req, null, o);
      if (!checkDeviceSecret(pickSecret(req, null, o), dev)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '设备密钥错误' }));
        return;
      }
      const s = dev ? _rpSessions.get(dev) : null;
      // ★ 没在练（或没开场）⇒ ★ **不点评、不算一次练习** ✗（★ 与静默超时同一条口径 ✓）
      if (!s || s.mode !== 'roleplay' || !s.msgs || !s.msgs.length) {
        if (dev) _rpSessions.delete(dev);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: true, skipped: 'not_in_roleplay' }));
        return;
      }
      try {
        const debriefText = await rpDebrief(dev, s);     // ★ 内部已落库 ✓ ＋ 已存 _lastDebrief ✓
        // ★★ 2026-10-21 依设备端递件补全 ✗（★ 原来只回 debrief_text ✓ 屏上显示"没拿到分数" ✗）
        //   ★ 规格 §2.4 的五件套：total / grade / scores / comment / debrief_text ✓
        //   ★ 三条口径按他们要求 ✓：
        //     ① `total` = **三个维度之和** ✓（0~100 ✓）
        //     ② `comment` 与 `debrief_text` **都给** ✓ 允许相同 ✓
        //     ③ `scores` 键名**保持中文** ✓（★ 他们按 key 直取 ✓ 改英文就取不到 ✗）
        const lb = _lastDebrief.get(dev) || null;
        _lastDebrief.delete(dev);                       // ★ 用完即清 ✓ 防串下一局 ✗
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          ok: true,
          result: {
            total: lb ? lb.total : 0,
            grade: lb ? lb.grade : '',
            scores: lb ? lb.scores : { '交流深度': 0, '专业度': 0, '应对合理性': 0 },
            comment: lb ? lb.comment : '',
            debrief_text: debriefText,
          }
        }));
      } catch (e) {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
    });
    return;
  }
  if (req.method === 'GET' && req.url.startsWith('/api/duty/pull')) {
    const q = getQuery(req.url);
    const dev = pickDeviceId(req, q, null);
    if (!checkDeviceSecret(pickSecret(req, q, null), dev)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '设备密钥错误' }));
      return;
    }
    if (!dev) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: '缺少 dev_id' }));
      return;
    }
    // ★ 转发到 S2（★ 用**公网域名**走 nginx ✓ —— 18901 不直接对外开放 ✓）
    fetch(`https://ai.hiiin.com/api/duty/pull?dev_id=${encodeURIComponent(dev)}`, {
      headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}` },
      signal: AbortSignal.timeout(15000),
    }).then(async r => {
      const txt = await r.text();
      res.writeHead(r.ok ? 200 : 502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(txt);
    }).catch(e => {
      log(`⚠️ duty/pull 转发失败 ${dev}: ${e.message}`);
      // ★ 失败**必须留证据** ✗ 且**不能装成"没有任务"** ✗（那会让用户以为今天没事 ✓）
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'upstream_unavailable', detail: e.message }));
    });
    return;
  }

  if (req.method === 'POST' && req.url === '/api/duty/report') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      let o = {};
      try { o = JSON.parse(body || '{}'); } catch (e) { o = {}; }
      const dev = pickDeviceId(req, null, o);
      if (!checkDeviceSecret(pickSecret(req, null, o), dev)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '设备密钥错误' }));
        return;
      }
      if (!dev) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: '缺少 dev_id' }));
        return;
      }
      // ★★ 用**验过密钥的 dev** 覆盖 body 里的 dev_id ✗（★ 这是本端点的安全核心 ✓）
      o.dev_id = dev;
      fetch('https://ai.hiiin.com/api/duty/report', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${CAM_INGEST_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(o),
        signal: AbortSignal.timeout(15000),
      }).then(async r => {
        const txt = await r.text();
        res.writeHead(r.ok ? 200 : 502, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(txt);
      }).catch(e => {
        log(`⚠️ duty/report 转发失败 ${dev}: ${e.message}`);
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'upstream_unavailable', detail: e.message }));
      });
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
    // ★ 2026-10-07（CR-20261007-01）：**把设备号一起转发给 S2** ——
    //   S2 要按"这台设备绑定学员的岗位"过滤 SOP 列表（★ 设备密钥已在本函数上面校验过
    //   ⇒ 这个 dev 是可信的，设备无法冒充别人 ✓）
    //   ✗ 不带 device 时 S2 会**不过滤**（兼容老固件）—— 所以这一行不能漏
    const sopUrl = CAM_S2_DBAPI + '/api/cam-sop' + (dev ? '?device=' + encodeURIComponent(dev) : '');
    fetch(sopUrl, {
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

  /* ══════════ CR-20261009-01：OTA 版本查询 + 一机一密领取 ══════════ */

  // GET /api/cam/ota?device_id=&ver=  —— 版本查询（灰度 = 服务器按 device_id 决定 ✓ 不加字段）
  //   ★★ 这个端点的**失败语义**与别处不同，别照抄：
  //     · 缺参       ⇒ 400（规格书 §8 明确"不要 500"）
  //     · 没有新版本  ⇒ **204 + 空 body**（✗ 不要 {"version":null}）
  //     · S2 不可达   ⇒ **也是 204**（= 没有更新 ⇒ 设备什么都不做，最安全）
  //       ✗✗ 绝不能把 404/5xx 甩给设备：404 会被它当成"功能没上线 ⇒ **永久回退**"，
  //          而 5xx 会让它每轮都重试、日志刷屏 —— 上线了就不该出现这两种 ✓
  if (req.method === 'GET' && (req.url === '/api/cam/ota' || req.url.startsWith('/api/cam/ota?'))) {
    const q = getQuery(req.url);
    const dev = pickDeviceId(req, q, null);
    const curVer = String(q.ver || '').trim();
    const jsonOut = (code, obj) => {
      res.writeHead(code, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(obj));
    };
    if (!dev) { jsonOut(400, { error: '缺少 device_id' }); return; }
    if (!curVer) { jsonOut(400, { error: '缺少 ver（设备当前版本号）' }); return; }
    if (!checkDeviceSecret(pickSecret(req, q, null), dev)) { jsonOut(401, { error: '设备密钥错误' }); return; }
    logAuthSource(req, dev, 'ota');
    // ★ 决策只在 S2 一处（ota-lib.decide）—— S1 只做协议翻译，不抄一份逻辑 ✗
    fetch(`${CAM_S2_DBAPI}/api/cam-ota?device_id=${encodeURIComponent(dev)}&ver=${encodeURIComponent(curVer)}`, {
      headers: { 'Authorization': 'Bearer ' + CAM_INGEST_TOKEN }
    }).then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(d => {
        if (!d || d.ok !== true || d.none === true || !d.version) {
          log(`OTA 查询 ${dev} ver=${curVer} ⇒ 204（无更新）`);
          res.writeHead(204); res.end();
          return;
        }
        log(`OTA 查询 ${dev} ver=${curVer} ⇒ ${d.version}（${d.size} 字节）`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ version: d.version, url: d.url, size: d.size, sha256: d.sha256 || '' }));
      })
      .catch(e => {
        log(`OTA 查询降级为 204（不打扰设备）：${e.message}`);
        try { res.writeHead(204); res.end(); } catch (e2) {}
      });
    return;
  }

  // GET/POST /api/cam/device/secret?device_id=  —— 一机一密领取
  //   ★★ 只在【后台预登记过】的设备才发（这条是防"拿公共密钥枚举设备号领完全机队密钥"的核心 ✗）
  //   ★ 凭据可以是三种（见 credentialKind）：
  //       自己的密钥 / 轮换宽限期的旧密钥（只允许用在这个端点 ✓）/ 公共兜底（首次领取的兼容路径）
  //   ★ 领到后**立刻同步一次密钥表** —— 否则这台设备要等最多 5 分钟才能用上自己的新密钥 ✗
  if ((req.method === 'GET' || req.method === 'POST') &&
      (req.url === '/api/cam/device/secret' || req.url.startsWith('/api/cam/device/secret?'))) {
    const q = getQuery(req.url);
    const jsonOut = (code, obj) => {
      res.writeHead(code, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(obj));
    };
    const doClaim = (o) => {
      const dev = pickDeviceId(req, q, o);
      const sec = pickSecret(req, q, o);
      if (!dev) { jsonOut(400, { error: '缺少 device_id' }); return; }
      const kind = credentialKind(sec, dev);
      if (!kind) {
        log(`设备领密钥被拒（凭据无效）: ${dev}`);
        jsonOut(401, { error: '设备密钥错误' });
        return;
      }
      log(`设备领密钥请求: ${dev}（现有凭据=${kind}）`);
      fetch(`${CAM_S2_DBAPI}/api/cam-secret-claim`, {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + CAM_INGEST_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify({ device_id: dev, via: kind })
      }).then(r => r.json().then(d => ({ code: r.status, d: d })))
        .then(out => {
          const d = out.d;
          if (!d || d.ok !== true || !d.secret) {
            const code = out.code === 400 ? 400 : (out.code === 409 ? 409 : 403);
            log(`设备领密钥被拒: ${dev} ⇒ HTTP ${out.code} ${(d && d.error) || ''}`);
            jsonOut(code, { error: (d && d.error) || '不能领取密钥' });
            return;
          }
          syncCamSecrets(true);                       // 让它马上生效
          // ★ 日志只打**指纹**，绝不打印明文 secret（规格书 §6 硬要求）
          const fp = crypto.createHash('sha256').update(String(d.secret)).digest('hex').slice(0, 12);
          log(`设备已领取密钥: ${dev}（key_id=${d.key_id} fp=sha256:${fp}）`);
          jsonOut(200, { secret: d.secret, key_id: d.key_id });
        })
        .catch(e => {
          log(`设备领密钥失败: ${e.message}`);
          jsonOut(502, { error: '领取通道暂时不可用，请稍后重试' });
        });
    };
    if (req.method === 'GET') { doClaim(null); return; }
    let cbody = '';
    req.on('data', c => {
      cbody += c;
      if (cbody.length > 4096) { try { req.destroy(); } catch (e) {} }   // 这个端点不需要大 body
    });
    req.on('end', () => { let o = {}; try { o = JSON.parse(cbody || '{}'); } catch (e) {} doClaim(o); });
    return;
  }

  /* ══════════ CR-20261009-01：密钥表【主动重载】（后台改完立刻生效）══════════
   * ★★ 为什么必须有它：S1 的密钥表本来是**每 5 分钟**拉一次，
   *   而"吊销"是安全动作 —— 设备丢了/泄露了，**不能等 5 分钟**才失效 ✗✗
   *   （2026-10-09 实测：revoke 后 1 秒再打，S1 还按老表放行 ⇒ 返回 204 而不是 401，
   *     即"吊销了但没生效" —— 这种静默延迟比报错还危险）
   * ⇒ 后台改完（预登记/轮换/吊销/删除）**同步调这个端点**，等它回 ok 才算生效 ✓
   *   ★ 鉴权用内部 ingest token（这是"触发一次同步"，滥用最多是让 S1 多拉一次）
   */
  if (req.method === 'POST' && req.url === '/api/cam/secrets-reload') {
    const tok = String(req.headers['x-ingest-token'] || (req.headers.authorization || '').replace(/^Bearer\s+/i, '')).trim();
    // ★ 2026-10-12：过渡期结束 ⇒ **只认新 token** ✓（旧的 cam-ingest-token-2026 从此 401 ✓）
    if (!CAM_INGEST_TOKEN || tok !== CAM_INGEST_TOKEN) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }
    // ★ 等同步真的跑完再回 —— 否则调用方以为生效了，其实还在路上 ✗
    _secSyncAt = 0;                                  // 清掉节流，强制本次真的去拉
    syncCamSecrets(true).then(() => {
      const c = camSecrets();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: _secSyncState === 'ok',
        devices: Object.keys(c.devices || {}).length,
        dual: Object.keys(c.dual || {}).length,
        revoked: (c.revoked || []).length,
      }));
    }).catch(e => {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: e.message }));
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
        // ★ CR-20261009-01：OTA 升级结果上报（fw / ota_state / ota_from）
        // ★ CR-20261009-02 期1：新增 `ota_latest` —— 设备**查到有新版本但还没装**时带上它 ✓
        //   ⚠️ 与上面几条**同一套合并写入语义**：不带该字段的包**不许清掉已有值** ✗
        //      （参照 vbat 那次的三条边界：老设备不带这些字段 ⇒ 忽略、不报错、不清空 ✓）
        //   ★★ 为什么 `ota_latest` **单独一个字段**、不复用 `ota_state`（规格书 §4.1 ✓）：
        //      `ota_state` = 「**上一次 OTA 的结果**」（历史事件 ✓）
        //      `ota_latest` = 「**现在有一个新版本等你装**」（当前状态 ✓）
        //      ⇒ 两个语义，混了就再也分不清"升完了"和"还有得升" ✗
        //   ⚠️ 另外记一条（我方在回执 §10.2 提过）：本字段**会陈旧** ✗ ——
        //      设备装完之后就不再发它 ⇒ 按"缺省不清旧值"服务端会**永远留着**旧版本号 ✓
        //      ⇒ **判断"有没有可升级版本"必须用 `fw` × 服务器 target**（都是当前状态 ✓），
        //        本字段只作"设备也已经知道了"的辅助信号 ✓
        let otaTouched = false;
        ['fw', 'ota_state', 'ota_from', 'ota_latest'].forEach(k => {
          const v = o[k];
          if (v !== undefined && v !== null && String(v).trim() !== '') {
            camDeviceState[dev][k] = String(v).trim();
            otaTouched = true;
          }
        });
        if (o.ota_state === 'ok') camDeviceState[dev].ota_at = Date.now();
        if (otaTouched) {
          // ★★ 为什么必须分两类日志（2026-10-09 实测踩到）：
          //   设备端发 1.2.2 起，**常规上报（每 5 分钟一次）也会带上 `fw`** ——
          //   而上面 `otaTouched` 对 `fw` 同样置真 ⇒ 若一律记成「升级上报」，
          //   日志就会**每分钟满屏"升级上报"**，而实际上**没有任何设备在升级** ✗✗
          //   （同族：**日志说谎比不记更糟** —— 读日志的人会据此做错误判断 ✓）
          const isResult = (o.ota_state !== undefined && o.ota_state !== null && String(o.ota_state).trim() !== '')
            || (o.ota_from !== undefined && o.ota_from !== null && String(o.ota_from).trim() !== '');
          log(isResult
            ? `cam 升级结果上报 -> ${dev}: fw=${camDeviceState[dev].fw || '-'}`
              + ` ota_state=${camDeviceState[dev].ota_state || '-'}`
              + ` ota_from=${camDeviceState[dev].ota_from || '-'}`
            : `cam 版本上报（常规）-> ${dev}: fw=${camDeviceState[dev].fw || '-'}`
              + (camDeviceState[dev].ota_latest ? ` ota_latest=${camDeviceState[dev].ota_latest}` : '')
              + `（未带 ota_state/ota_from ⇒ 按合并语义保留旧值 ✓）`);
          // ★ 落 S2 台账（做持久化：S1 是内存态，重启就没了）
          //   ★ 失败必须**留日志**，不许静默（"降级可用"的 catch 一定要留证据 ✗）
          fetch(`${CAM_S2_DBAPI}/api/cam-ota-report`, {
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + CAM_INGEST_TOKEN, 'Content-Type': 'application/json' },
            body: JSON.stringify({ device_id: dev, fw: o.fw, ota_state: o.ota_state, ota_from: o.ota_from, ota_latest: o.ota_latest })
          }).then(r => { if (!r.ok) log(`OTA 台账上报失败：HTTP ${r.status}（${dev}）`); })
            .catch(e => log(`OTA 台账上报异常：${e.message}（${dev}）`));
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

        // ★ CR-20261009-02 期3：用户手动触发升级（`ota_install`）
        //   ★ 必须**显式加这一支** ✗ —— 本函数末尾是"其它 action 一律当 start/stop 录制"的兜底，
        //     不写这一支的话，`action:'ota_install'` 会被当成**开始录制** ✗✗（凭空录一段）
        //   ⚠️ 本接口只负责【入队】✓ —— "该不该给这台设备发"由 **S2** 判定
        //     （策略与台账都在那边 ✓ 规格书 §5 #7「只对确实有可用版本的设备发」✓）
        //   ★ 不带 version 字段：设备收到后会自己 `GET /api/cam/ota` 取版本 ✓
        //     （塞一个设备不认识的字段没有好处 ✗ —— "缺省 = 自己查/保持原值"是本项目一贯口径 ✓）
        if (action === 'ota_install') {
          camCmdQueue[dev] = { cmd: 'ota_install', ts: Date.now() };
          log(`cam 升级指令入队 -> ${dev}（用户手动触发 ✓）`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, cmd: 'ota_install' }));
          return;
        }
        // ⚠️ 如实记一条隐患（不改行为，留日志）：走到这里的**未知 action** 会被当成录制 ✗
        //   （现有调用方只有 start/stop/take_photo/set_config/ota_install ✓ 所以现在打不到 ✓）
        if (action !== 'start' && action !== 'stop') log(`⚠️ cam cmd/send 收到未知 action="${action}" ⇒ 按录制处理（沿用旧行为 ✗ 见代码注释）`);

        const cmd = action === 'start' ? 'start_record' : 'stop_record';
        // ★★ 2026-10-12 用户改设计后，这两行**删掉了** ✗ ——
        //   原来在这里记"上次用的 mode / 频率"，给语音**提议**当默认值 ✓
        //   而新设计是「**全部默认**」⇒ 由**设备**决定默认（我方发空值/0 ✓ 见 vcStartParams ✓）
        //   ⇒ 再记一份就变成"我替你定了" ✗（正如当时的注释所警示的 ✓）⇒ 整段撤掉 ✓
        //   ★ 教训：**改了上游语义，要顺手把只为它存在的状态删干净** ✗
        //     （这两行当时没人引用 —— 只有 `_voiceLastMode` 那两个 Map ✗ 而它们已随状态机一起删掉 ✓
        //       ⇒ 不删就是**运行时 ReferenceError** ✓ 语法检查抓不到 ✓ 靠的是"删符号后全文件回查" ✓）
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
        // ★ CR-20261009-01：OTA / 固件版本（设备升级后随状态上报带上来）
        //   ⚠️ 没上报过就是 null（前端显示"未知"），**不要显示成 0 或空版本号** ✗
        fw: st.fw || null,
        ota_state: st.ota_state || null,
        ota_from: st.ota_from || null,
        ota_latest: st.ota_latest || null,   // ★ CR-20261009-02：设备已知的可升级版本（★ 会陈旧 ⇒ 判据用 fw×target ✓）
        ota_at: st.ota_at || null,
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

        // ★★★ 2026-10-21 依设备端回执修 ✗✗
        //   ★ 我原来写：`handleTTS(body.text)` —— **只传了 1 个参数** ✗
        //   ⇒ `audioFmt` 恒为 undefined ⇒ `ulaw = (undefined === 'ulaw8')` = false ⇒ **永远 PCM16** ✗
        //   ⇒ ★ 所以设备端两种写法（query / body）都"失败"✓ —— 其实是被**静默忽略** ✓
        //   ★★ 而我还跟他们说"`/api/tts` 本来就支持 format=ulaw8" ✗ ⇒ **我说错了** ✓
        //   ★ 判据：**"代码里有这个能力" ≠ "这个端点暴露了这个能力"** ✗
        //     必须**看端点到底传了什么** ✓ 而不是看被调函数支持什么 ✓
        //   ★ 现在两个位置都认（★ query 优先 ✓ body 次之 ✓ 两种都能用 ✓）
        const fmtArg = String((body && body.format) || (getQuery(req.url) || {}).format || '').trim();
        const result = await handleTTS(body.text, fmtArg);
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
  const VOICE_REPLY_MAX = 300;          // 兜底上限（实际按 detail 档位走 DETAIL_STYLES）
  // ★★ 回答长度三档（用户可选，2026-09-29）—— 设备在请求里带 detail=1|2|3
  //   缺省/非法值一律按【中等】处理（老固件不带这个字段也能用，和 reset 同一套兼容规则）
  //   ★ 2026-10-07 三档整体翻倍（用户实测反馈"说话太少，步骤讲不全"）：
  //     ⚠️ 长度上限直接决定 TTS 音频大小，而设备端 WAV 缓冲 VOICE_WAV_MAX = 1.5MB：
  //        中文 TTS 实测 ≈5.0 字/秒（4.6~5.3）⇒ 16k 单声道 pcm16 = 32KB/s：
  //        · 90  字 ≈ 18s ≈ 0.58MB ✓
  //        · 180 字 ≈ 36s ≈ 1.15MB ✓
  //        · 300 字 ≈ 60s ≈ 1.92MB ✗ 超设备缓冲 ⇒ 这条档位必须走 ulaw8(0.5×) = 0.96MB ✓
  //      ★ 实测锚点：247 字 → 1,487KB（离 1.5MB 只剩 13KB ✗）。
  //        ⇒ **设备端 1.5MB 缓冲 ≈ pcm16 243 字封顶** —— 这就是 wavCapFor() 的来由。
  //      ⇒ 设备端选「3 具体」时应带 audio_fmt=ulaw8；流式那条路不受此限（边生成边播）
  //   ★ 简约档 80 → 90（2026-10-07 二次调整）：端到端实测模型写了 81 字、撞上 80 的截断线
  //     打了个 `…` ⇒ 用户要求提到 90。★ 保留观察：模型对"最短档"最容易略微超限，
  //     若再出现 `…`，正解是**给截断线留 10% 余量**（提示词 90 / 截断 100），不是无限往上抬
  //   ★★ 具体档 300 → 400（2026-10-07 第三次调整，用户报"具体档没念完"后定位）：
  //     提示词仍写 **300 字以内**（内容口径不变），但**代码线放到 400** ——
  //     实测模型对"最长档"会正常超额（301 / 341 字，超 14%），
  //     而 300 的代码线一到就把句子从中间切断 ⇒ 用户听到的正是"没有全部念完" ✗
  //     400 字 ulaw8 ≈ **1.25MB**（≤ 1.4MB 预算）⇒ 装得下，没必要切 ✓
  //     ⚠️ 简约/中等**不跟着放宽**：实测 84/90、159/180 都没超线（没症状就别动）✗
  const DETAIL_STYLES = {
    1: { max: 90,  tokens: 280, len: '不超过 90 字，2~4 句，只给最核心的一步或结论' },
    2: { max: 180, tokens: 560, len: '180 字以内，4~6 句，把关键几步说清楚' },
    3: { max: 400, tokens: 800, len: '300 字以内，把关键的几步都说全，可以说 6~8 句' },
  };
  function detailOf(v) {
    const n = parseInt(String(v || '').trim(), 10);
    return DETAIL_STYLES[n] ? n : 2;   // 不传/空/非法 ⇒ 中等
  }
  // ★ 整段 WAV 那条路（老 /api/voice-chat）的**按格式**字数上限 —— 设备端把整个 WAV 收进内存，
  //   VOICE_WAV_MAX = 1.5MB，超了就是**静默截断**（念一半停住，用户以为是 bug）。
  //   实测 ≈5.0 字/秒（4.6~5.3）⇒ pcm16(32KB/s)：200 字 ≈ 38s ≈ 1.23MB（安全）；
  //                  ulaw8(16KB/s)：400 字 ≈ 80s ≈ 1.25MB（安全）。
  //   ★ 实测锚点：247 字 → 1,487KB ⇒ **pcm16 的物理封顶约 243 字**，取 200 留 20% 余量。
  //   ★ 流式那条路**也要看这个预算**（设备端把流式 PCM 收在同一个缓冲里 —— 2026-10-07 实测证实）✓
  function wavCapFor(afmt, want) {
    return afmt === 'ulaw8' ? Math.min(want, 400) : Math.min(want, 200);
  }
  // ★★ 设备端整段音频缓冲 = `VOICE_WAV_MAX` = 1.5MB ⇒ 服务端自己留 ~100KB 余量
  //   为什么流式也要按它收：2026-10-07 实测日志里 `detail=3` 推过 **2,075,594 字节** PCM，
  //   而设备端此前只验过 897KB 的长流 ⇒ 超出部分**尾巴没了**（用户报"没念完"）✗
  const DEV_AUDIO_BUDGET = 1.4 * 1024 * 1024;
  // 16k 单声道：pcm16 = 32KB/s、ulaw8 = 16KB/s；中文 TTS ≈5.0 字/秒
  //   ⇒ **每字 ≈6.4KB（pcm16）/ 3.2KB（ulaw8）**（用实测校准过：247 字 → 1,487KB ✓）
  const BYTES_PER_CHAR = { pcm16: 6400, ulaw8: 3200 };
  function audioBytesFor(n, fmt) { return n * (BYTES_PER_CHAR[fmt] || BYTES_PER_CHAR.pcm16); }
  // 流式：按字节预算选格式（pcm16 装得下就用 pcm16，装不下换 μ-law = 正好一半）
  //   ★ 必须在**发 JSON 头之前**定下来，否则头里写的 format 与实际字节流不一致 ✗
  function pickStreamFmt(afmt, n) {
    if (!AUTO_ULAW8 || afmt === 'ulaw8') return afmt;
    const need = audioBytesFor(n, 'pcm16');
    if (need <= DEV_AUDIO_BUDGET) return 'pcm16';
    log(`voice-chat-stream: ${n} 字 pcm16 ≈ ${(need / 1048576).toFixed(2)}MB > 预算 ` +
        `${(DEV_AUDIO_BUDGET / 1048576).toFixed(1)}MB ⇒ 自动改 ulaw8（≈${(need / 2 / 1048576).toFixed(2)}MB）`);
    return 'ulaw8';
  }
  // ★★ 老路自动降级 pcm16 → ulaw8（2026-10-07，用户拍板"顺手改"）
  //   要解决的问题：设备不带 audio_fmt 时，"具体"档被 pcm16 的 200 字线截掉一截
  //   （实测：模型写 235 字 ⇒ 白丢 35 字）。
  //   为什么敢换：设备端 `voice.c` 的 `wav_parse()` 是**逐块遍历 RIFF、按 fmt 的 tag/位深自适应**
  //   （CR-20260929-03 §16.2 真机验过 tag=7），且**老路与流式共用同一对
  //   voice_feed_pcm16()/voice_feed_ulaw8()** ⇒ 格式由**响应自己**说了算，设备无需改动 ✓
  //   ★ 只在"不换就要截断"时才换（不无谓改变既有行为），且留日志 + 在 JSON/响应头里回报实际格式
  //   ★ 关掉的办法：环境变量 VOICE_AUTO_ULAW8=0
  const AUTO_ULAW8 = process.env.VOICE_AUTO_ULAW8 !== '0';
  // ★★★ 2026-10-12 晚 **拆掉**「固定问答（人设类）」这段短路 ✗（用户明确要求 ✓）
  //
  //   ★ 用户原话（逐字 ✓ 设计真源）：
  //     「他的人设包括我问他"你是谁"、让他做下自我介绍等，**一定要让他灵活**。
  //       他的说话风格要**幽默**一点，幽默很重要，你知道吗？他的人设**一定要幽默**」
  //
  //   ★ 原来为什么有它（当时的理由 ✓ 不是没道理）：
  //     问「你是谁」⇒ 知识库 0 命中 ⇒ reliable=false ⇒ 代码直接回"查不到" ✗，
  //     而人设明明写在 system prompt 第一句 —— 它排在闸门后面，**根本没机会说话** ✓
  //     ⇒ 那时用"一张写死的表"兜：确定性 ✓ 零成本 ✓ 零延迟 ✓ 不可能答错 ✓
  //
  //   ★★ 现在为什么必须拆 ✗✗：
  //     **写死的表 = 每次一字不差** ⇒ 用户听到的永远是同一句 ✓ ⇒ ★ **那正是"不灵活"** ✗
  //     而且**幽默根本放不进去** ✗（表里的话是固定的，笑点会重复到腻 ✓）
  //   ★ 而且当初那条"闸门"现在也松了 ✓：检索不可信时**已经会交给模型凭通用经验回答** ✓
  //     （见下面那句"检索不可信 ⇒ 按用户要求交给模型凭通用经验回答"✓）
  //     ⇒ ★ 所以这条路已经通了 ✓ 不需要再用表兜 ✓
  //
  //   ★ 判据（值得记 ✗）：**"写死"能换来确定性，但换不来"灵活"** ✗ ——
  //     当需求是"灵活/有个性"时，写死的表**本身就是缺陷** ✓ 不是保险 ✓
  //   ★ 风险与对冲：模型可能吹牛（"我能帮你订机票"✗）
  //     ⇒ 已在 system prompt 里单开一节【你实际会做什么 · ★ 不许吹牛 ✗】✓
  //   ★ 代价：人设类问题从此走一次 LLM（多 ~1~2 秒 ✓ 多一次调用费 ✓）
  //     —— ★ 这是**用户明确要的取舍** ✓（"一定要让他灵活"✓），如实标在这里 ✗
  const SELF_QA_DISABLED = true;   // ★ 保留一个显式标记 ✗：别把这段删得无影无踪 ✓
  function selfAnswer(_text) { return null; }   // ★ 永远不短路 ✓ ⇒ 一律交给模型（人设见 system prompt ✓）
  // 历史留痕（已停用 ✗）：原来那四张写死的表 ——
  //   「你是谁」⇒ '我是店里的培训教练，你问我工作上的事…'
  //   「你能做什么」⇒ '店里的规定、SOP、考核标准…'
  //   「怎么用你」⇒ '按住按钮说话，松手等我说完就行。'
  //   「谁做的你」⇒ '我是店里赋能系统的一部分…'
  // ★★★ 上下文表在上面**模块级**声明（`_chatSessions`）——
  //   它原来就写在这一行 ✗ 而这里属于"每个请求执行一次"的 handler ⇒ **每次请求都是空表** ✗✗
  //   ⇒ 写回的上下文下个请求看不到 ⇒ **多轮对话完全失效**（见文件顶部那段注释 ✓）
  //   ⚠️ 别再把它挪回函数里 —— 这是"记忆"能不能跨请求的唯一前提 ✓

  // 设备专用提示词 —— ★ 独立一套，**不动**学员端那套（那套要"详细完整/8000 tokens"，正好相反）
  //
  // ★★ 为什么要求"强制二选一 + 固定前缀"（2026-09-29 实测教训）：
  //   第一版只在提示词里写"参考内容没有答案就说不知道"，实测**模型照样编** ——
  //   问「录像怎么开始」（知识库里根本没有设备说明），它答出了
  //   "录像设备开机后按录制键就开始" ✗ 那是它自己编的。
  //   ⇒ 光靠"不许编"这种祈使句约束不住；改成【强制二选一 + 代码校验前缀】：
  //     模型必须选一个分支输出，代码再检查格式 —— 格式不对就当成"查不到"。
  //   ⇒ 失败模式从"编一个答案"变成"说查不到"，这是我们要的方向
  //     （给学员用，编错流程比查不到坏得多）。
  const VOICE_CHAT_SYSTEM = [
    '你是「赋能02」设备上的带教教练，名字叫小星，给门店一线员工帮忙。',
    '',
    '【性格 · ★ 很重要，用户特别强调过 ✗】',
    '· ★★ 你**幽默**、接地气、嘴皮子利索 —— 像店里那个业务最强、又最爱开玩笑的前辈。',
    '· 幽默要有**分寸**：一两句就够 ✓ 别贫、别尬、别堆网络梗、别拿客人和同事开涮 ✗。',
    '· ★★ 但**讲标准/流程/规定**的时候 —— 先把准确的答案说全 ✓（可以收尾抖一句 ✓），',
    '  **绝不许拿标准本身开玩笑** ✗（员工会照着做，玩笑会出事故 ✓）。',
    '· ★★ 自我介绍、打招呼、闲聊时**要自然、每次说法可以不一样** ✓ —— **别背课文** ✗。',
    '  （用户原话：「我问他"你是谁"、让他做下自我介绍，一定要让他灵活…人设一定要幽默」✓）',
    '',
    // ★★ 能力清单**从模块级常量展开** ✗（★ 不再在这里写一遍 ✓）
    //   —— 因为**联网那条路也要同一份** ✓（"同一个事实只能一处"✓ 原来漏了联网路 ✗）
    ...PERSONA_CAPS,
    '',
    '【回答怎么来 · 严格按这个顺序】',
    '1. 先看下面的【知识库参考内容】—— 里面【有相关内容】时，优先依据它回答（那是我们店的标准）。',
    '2. 参考内容里【没有、或只有一点沾边】时 —— 不要生硬地说不知道、不要说去问店长；',
    '   凭你的酒店服务与培训经验，给一个【通用、稳妥、可操作】的建议。',
    '   但要讲清楚这是通用做法（例如：咱们店的具体规定我这边没查到，不过一般这么做…），',
    '   绝不要把通用做法说成【我们店就是这么规定的】。',
    '3. ★ 与工作无关、但你【答得上来的】（打招呼 ✓ 闲聊 ✓ 自我介绍 ✓）——',
    '   **幽默地接一句** ✓ 再自然地把话头引回工作 ✓（不用拒绝 ✗）。',
    '4. 只有当你【确实给不出任何有用内容】时，才只输出四个字：',
    '   无法回答',
    '',
    '【输出格式 · 必须严格照做】',
    '· 只要回答，就先输出一行：',
    '  答：<你的回答>',
    '  （清单、表格条目、流程步骤都算有信息；不用逐字照抄，把要点讲全讲顺就行；',
    '    知识库里给的是通用流程时，也可以直接用来回答某个具体情形）',
    '· ★★ 紧接着**再输出一行**语气（★ 给设备显示表情用 ✓）：',
    '  语气：<下面 6 个词里的**一个**>',
    '  · neutral = 平常（也可以不写这一行 ✓）· happy = 肯定/报喜/轻松',
    '  · curious = 疑问/不确定/需要对方补充 · sorry = 抱歉/做不到/出错',
    '  · excited = 特别好的消息 · serious = 严肃提醒/重要规定',
    '  ★★ 只能从这 6 个词里选，**不许自己造词** ✗（造了设备不认 ✓ 等于白写 ✓）',
    '  ★★ 语气必须与你【上面那句话】一致 ✗ ——「嘴上说好、脸上像道歉」是最糟的 ✗',
    '· 不给建议时，就只输出那四个字：无法回答（**不要**再写语气行 ✓）',
    '· 不要输出解释、不要输出别的任何内容、不要提参考内容或知识库这类词。',
    '',
    '【回答怎么写】',
    '· ★ 把关键步骤/要点【说全】，不要为了短而漏步骤；问怎么处理就把每一步都说出来。',
    '· 总长度控制在 __LEN__；不要客套话、不要复述问题、不要重复。',
    '· 口语化，像老员工教新人；★ 讲做法时直接说，**别在这时候贫嘴** ✗（幽默留给闲聊 ✓）。',
    '· 不要 markdown、不要分点、不要星号/括号/井号、不要 emoji —— 会被逐字念出来。',
    '',
    '【绝对不许 · 红线】',
    '· 不许编造【具体数字、时限、罚款、金额】（如 3 分钟内必须… / 罚 50 元 / 赔 200 元）',
    '· 不许编造【本店特有的制度条款、岗位规定、流程细节】',
    '· 依据知识库时可以引用它的具体规定；凭经验时只说通用做法，不说本店规定',
    '· 员工会照着做，编错了会出事 —— 拿不准就用【一般】【通常】这种留余地的说法',
    '· ★ 不许为了逗乐而**夸大或编造店里的事** ✗（幽默是语气，不是内容 ✗）',
    '',
    // ★★ 2026-10-20：企业文化（★ 用户要求「每一次对话都要从企业文化出发」✗）
    //   ★ 注意注入位置 ✗ —— 放在**最后** ✓（★ 比前面所有"不许编/别贫"的规则更靠后 ⇒ 更容易被执行 ✓
    //     但★ 它**不覆盖**上面的"不许编"✗：文化给的是**语气和出发点** ✓ 不是事实 ✓）
    corpCultureBlock(),
  ].join('\n');

  // 取该设备的上下文（★ 统一入口 —— 这是 2026-10-09 修的一个真 bug）
  //
  // ★★ 原来这里是 `chatHistoryOf(device)`，而"**清空**（reset=1）"和"**写回**"这两件事
  //    分别嵌在 `/api/voice-chat` 与 `/api/voice-chat-stream` 的 **`else` 分支**里 ✗✗：
  //      · `selfA`（人设固定问答，如「你是谁」）⇒ **既不写回、也不清空** ✗
  //      · `!kb.ok`（知识库不可用）⇒ 同样 ✗
  //    ⇒ 后果两条（设备端 2026-09-29 报的「`session` 恒为 #1」就是它）：
  //      ① **追问接不上**：上一轮走了那两条路 ⇒ 这一轮的上下文里没有它 ✗
  //      ② ★ **`reset=1` 会静默失效**：用户说"新话题"，但那一轮走了 selfA ⇒ **没清** ✗
  //         下一轮（reset=0）还把旧上下文带上 ⇒ 与用户意图相反 ✗
  //   ⇒ 修法：**把"取/清/记"收成三个纯函数**，两条端点都只调它们，
  //     并且**在任何分支之前**就把 reset 处理掉 ✓（不依赖走哪条路 ✓）
  //
  // ★ 另一个能力（设备端 §16.3 建议的"双保险"）：**服务端自己按时间过期** ✓
  //   设备端可能重烧/换固件 ⇒ **这边才是唯一真源** ✓
  const CHAT_CTX_TTL_MS = 5 * 60 * 1000;   // 5 分钟没有新请求 ⇒ 丢上下文（当新会话）
  function chatCtx(device, reset) {
    if (!device) return null;
    const now = Date.now();
    let s = _chatSessions.get(device);
    // ★ 插桩（排查"每轮条数都回到 0"✓ 一行、信息量大，平时也看得出上下文有没有在涨 ✓）
    const _before = (s && Array.isArray(s.msgs)) ? s.msgs.length : -1;
    const _why = (!s ? 'new-noentry' : (!Array.isArray(s.msgs) ? 'new-badshape' : 'reuse'));
    if (!s || !Array.isArray(s.msgs)) { s = { msgs: [], at: now }; _chatSessions.set(device, s); }
    let _ttl = false;
    if (now - (s.at || 0) > CHAT_CTX_TTL_MS) {           // ① 服务端按时间过期
      if (s.msgs.length) log(`voice-chat: 上下文过期（${Math.round((now - s.at) / 1000)}s 无请求）⇒ 当新会话（device=${device}）`);
      s.msgs.length = 0; _ttl = true;
    }
    let _rst = false;
    if (reset === '1') { s.msgs.length = 0; _rst = true; }  // ② 设备说"新话题" ⇒ 清空
    s.at = now;
    if (_before !== s.msgs.length || reset === '1' || _why !== 'reuse') {
      log(`voice-chat: ctx[${device}] ${_why} 前=${_before} 后=${s.msgs.length} ttl=${_ttl} reset=${_rst}（收到 reset="${reset}"）`);
    }
    if (_chatSessions.size > 500) {                       // 防无界增长
      const first = _chatSessions.keys().next().value;
      if (first !== device) _chatSessions.delete(first);
    }
    return s;
  }
  /** 取给模型用的历史（★ 会顺带处理"过期"与"新话题"✓ —— 所以必须在任何分支之前调一次 ✓） */
  function chatMsgsFor(device, reset) {
    const s = chatCtx(device, reset);
    return s ? s.msgs.slice(-VOICE_CHAT_ROUNDS * 2) : null;
  }
  /** 记一轮问答（★ 所有"确实答过"的分支都要调 ✓ —— 见上面那段注释） */
  function chatRecord(device, heard, reply) {
    const s = chatCtx(device, '0');                       // ★ 传 '0'：记账时**不许**再清一次
    if (!s) return;
    s.msgs.push({ role: 'user', content: heard });
    s.msgs.push({ role: 'assistant', content: reply });
    while (s.msgs.length > VOICE_CHAT_ROUNDS * 2) s.msgs.shift();
    s.at = Date.now();
  }
  /** 当前上下文深度（**只读**，供响应里的 `session` 用 ✓ 不给不存在的设备建条目 ✓） */
  function chatDepthOf(device) {
    if (!device) return 0;
    const s = _chatSessions.get(device);
    return s && Array.isArray(s.msgs) ? Math.ceil(s.msgs.length / 2) : 0;
  }

  // 查知识库。★ 返回 {ok, results, reliable} —— 三个状态必须分开：
  //   ok=false            ⇒ 检索服务不可用（跟用户说"稍后再问"，不是"没这条知识"）
  //   ok=true reliable=false ⇒ 检索到了但【都不靠谱】（弱命中）⇒ 直接说查不到，不叫模型
  //   ok=true reliable=true  ⇒ 有稀有词支撑 ⇒ 才敢让模型组织语言
  //   ★ "未知 ≠ 零"：把"服务挂了"和"没这条知识"说成同一句话，会让人查错方向
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
        if (!r.ok) { log(`⚠️ 知识库检索返回 HTTP ${r.status}`); return { ok: false, results: [], reliable: false }; }
        const j = await r.json();
        return { ok: true, results: (j.results || []).slice(0, 3),
                 reliable: j.reliable === true, maxScore: j.maxScore };
      } finally { clearTimeout(tid); }
    } catch (e) {
      log(`⚠️ 知识库检索失败: ${e.message}`);
      return { ok: false, results: [], reliable: false };
    }
  }

  // 兜底话术（检索不到 / 检索服务不可用）
  const NO_KB_REPLY = '不好意思，这个问题没办法回答你，你可以咨询一下门店领导。';
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

  // ★★ `reply_mood` 的收口函数（`MOODS` / `normMood` / `moodForCmd`）已移到**模块级** ✗
  //   —— 为什么必须：`webAnswer()` 在**模块级** ✓ 它也要用 `normMood` ✓
  //   ★ 我第一版把这三个写在**这个处理函数内部** ✗ ⇒ `webAnswer` 里调用会 ReferenceError ✗
  //   ★ 判据（今天第二次踩 ✓）：**新函数放在哪一层，要看【所有】调用点在哪一层** ✗ ——
  //     不是看"我写的时候离谁近" ✓（离得近但作用域够不着 = 运行时才炸 ✓ 语法检查查不出 ✗）
  //   ⇒ 定义见文件上方 `MOODS` / `normMood` / `moodForCmd` ✓

  // ★ 2026-10-20：加了 `toolData` ✗ —— 工具查到的真实数据（★ 可选 ✓ 不传就与原来完全一致 ✓）
  //   `toolData = { kind, data }` ／ null ✓
  async function voiceChatAnswer(text, kbResults, history, detail, toolData) {
    const style = DETAIL_STYLES[detail] || DETAIL_STYLES[2];
    let kbBlock = '';
    if (kbResults.length) {
      kbBlock = '【知识库参考内容】\n' + kbResults.map((r, i) =>
        `[${i + 1}] ${r.file}\n${r.snippet}`).join('\n\n');
    } else {
      kbBlock = '【知识库参考内容】\n（没有检索到任何相关内容）';
    }
    // ★★ 工具数据块（2026-10-20）：**两处都放** ✗
    //   ★★ 实测教训（第一版只放 system ✓）：模型照样说「查不到/积分是空的」✗
    //      因为 `VOICE_CHAT_SYSTEM` 里有「知识库没内容 ⇒ 说查不到」的规则 ✓
    //      ⇒ ★ **工具数据必须比"知识库没内容"优先级更高** ✗
    //   ① system 里加一条**最高优先级规则** ✓（明确"与上面所有规则冲突时以它为准"✓）
    //   ② 把它**塞进 user 消息** ✓（★ 那是模型正在回答的那一轮 ⇒ 注意力最高 ✓）
    let toolBlock = '';
    if (toolData && toolData.kind) {
      // ★★ 2026-10-20 修（★ 这个 bug 害模型说了两轮"数据是空的"✗）：
      //   `learningSummary()` 返回的是**整个 S2 响应对象**（★ 没有 `.data` 这一层 ✗）
      //   而我写的是 `toolData.data` ⇒ **undefined** ⇒ ★ 提示词里就是 `{kind:points, undefined}` ✓
      //   ⇒ ★ 模型看到 undefined ✓ **很诚实地**回答"数值是空的" ✓✓
      //   ★ 判据：**"模型说数据是空的"要先怀疑"我传进去的是不是空的"** ✗
      //     —— ★ 它有时在说谎 ✓ 但更多时候是**我真的给了它一个空的** ✓
      const payload = (toolData.data != null) ? toolData.data : toolData;
      toolBlock = '【系统查询到的真实数据】\n'
        + 'kind=' + toolData.kind + '\n'
        + JSON.stringify(payload);
    }
    const toolRule = toolBlock
      ? '\n\n★★【最高优先级 · 与上面任何规则冲突时以这条为准】★★\n'
        + '本轮有【系统查询到的真实数据】—— ★ 那是**确实查到的** ✓（来自系统数据库 ✓）\n'
        + '⇒ ★ 必须**据此回答** ✗：即使知识库里没有内容、即使你说不出道理 ✓\n'
        + '⇒ ★★ **严禁**说「查不到 / 数据是空的 / 没查到你的记录」✗✗（数据就在下面 ✓ 说了就是撒谎 ✓）\n'
        + '⇒ ★ 只说用户问的那一项 ✓ 别把整段数据全念出来 ✗；看不懂的字段就跳过 ✓'
      : '';
    const msgs = [{ role: 'system', content: VOICE_CHAT_SYSTEM.replace('__LEN__', style.len) + '\n\n' + kbBlock + toolRule }];
    if (history) for (const h of history) msgs.push(h);
    // ★★ 工具数据**也塞进 user 消息** ✗ —— 双保险（★ 模型对"当前这一轮"的注意力最高 ✓）
    msgs.push({ role: 'user', content: toolBlock ? (String(text) + '\n\n' + toolBlock) : String(text) });

    const ac = new AbortController();
    const tid = setTimeout(() => ac.abort(), LLM_TIMEOUT_MS);
    try {
      const r = await fetch('https://api.deepseek.com/chat/completions', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${DEEPSEEK_KEY}`, 'Content-Type': 'application/json' },
        // ★★ 2026-10-20：temperature 0 → **0.4** ✗
        //   ★ 为什么改（★ 两个真实问题都由它引起 ✓）：
        //     ① 用户要「**讲笑话**」✓ 而 temp=0 ⇒ ★ 每次都给**同一个段子** ✗✗
        //     ② 用户早先要「自我介绍**一定要灵活**」✓ 而 temp=0 ⇒ ★ 其实**每次一字不差** ✗
        //        （★ 我当时以为提示词能解决 ✓ 其实**温度把它锁死了** ✓✓）
        //   ★★ 为什么敢改 ✗：★ 我们有**代码闸门**（`答：` 格式校验 ＋ 检索不可信就不用模型 ✓）
        //      ⇒ ★ 不靠模型自觉 ✓ ⇒ 0.4 只会让它**换个说法** ✓ 不会让它**乱编** ✓
        body: JSON.stringify({ model: 'deepseek-chat', messages: msgs, temperature: 0.4, max_tokens: style.tokens }),
        signal: ac.signal
      });
      const j = await r.json();
      if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
      const content = j.choices && j.choices[0] && j.choices[0].message.content;
      // ★★ 代码校验契约：只接受 `答：xxx` 这一种格式；其余（含"查不到"、
      //    以及任何没按格式来的胡说）一律当成"没有可依据的答案"。
      //    这一步是防编造的最后一道闸门 —— 不依赖模型"自觉"。
      const raw = String(content || '').trim();
      // ★ CR-20261012-02：先把**语气行**抽出来（★ 必须在校验 `答：` 之前 ✗
      //   否则 `[\s\S]+` 会把语气行一起当成正文 ✓ 回复里就会念出"语气：happy" ✗✗）
      const moodM = raw.match(/语气\s*[:：]\s*([A-Za-z]+)/);
      const mood = normMood(moodM && moodM[1]);
      const body = raw.replace(/^\s*语气\s*[:：]\s*[A-Za-z]+\s*$/gmi, '').trim();
      const m = /^答\s*[:：]\s*([\s\S]+)$/.exec(body);
      const okAnswer = !!(m && m[1].trim());
      return { reply: okAnswer ? cleanReply(m[1]) : '', refused: !okAnswer,
               mood: okAnswer ? mood : null, raw: raw.slice(0, 60), usage: j.usage || null };
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
        const detailPart = parts.find(p => p.name === 'detail');
        const detail = detailOf(detailPart ? detailPart.data.toString('utf8') : '');
        // ★ audio_fmt 对老路也生效（CR-20260929-03 §16.4）—— fallback 那条路同样能省一半流量
        //   ★ 用 let：下面第 ④ 步可能在"不降级就要截断"时把它改成 ulaw8
        const fmtPartOld = parts.find(p => p.name === 'audio_fmt');
        let afmt = (fmtPartOld && String(fmtPartOld.data.toString('utf8')).trim() === 'ulaw8') ? 'ulaw8' : 'pcm16';

        if (!audioPart || !audioPart.data || !audioPart.data.length) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'no_audio' }));
          return;
        }

        // ★★ 2026-10-21 依设备端递件加分段打点 ✗（★ t0~t5 ✓ 他们只要这一行证据 ✓）
        //   ★ 判据：**t5 − t1 ≤ 3.0 s** ✓（★ 他们实测 5.4~7.0 ✗）
        const _t0 = Date.now();
        // ① ASR
        const asr = await handleASR(audioPart.data);
        const _t2 = Date.now();
        const heard = (asr.text || '').trim();
        if (!heard) {
          log(`voice-chat: 没听清（device=${device || '-'}）`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'asr_empty' }));
          return;
        }
        // ★★ 「新话题 / 过期」在**任何分支之前**统一处理（2026-10-09 修）——
        //   原来这步嵌在 else 分支里 ⇒ 走 selfA 的那一轮**静默不清** ✗
        //   ⇒ 用户说"新话题"，下一轮却还带着旧上下文，与意图相反 ✗
        chatCtx(device, reset);

        // ★ CR-20261010-01 期1：语音命令优先（★ 在 KB/LLM **之前** ✓ 命中就**不叫模型** ✓ ⇒ 这才是 ≤3 秒的来源 ✓）
        const vcOld = await voiceCmdHandle(device, heard);
        // ★ 期0 影子：命令回合的判定（★ 不 await ✓ 不改下面任何一行 ✓）
        if (vcOld) shadowJudge(heard, { kind: 'command', action: vcOld.action || null }, device);
        if (vcOld) {
          if (vcOld.action) chatRecord(device, heard, vcOld.reply);   // 命令也算一轮对话 ✓
      // ★★ 语音会话记录：命令回合**在这里**记 ✗ —— 本分支 L309x 会 `return` ✓
      //   所以不能只挂在函数末尾 ✓（★ 那是"改一处漏一处"的典型 ✓）
      //   ★ 命令回合 reply 可能为空（如"开始录制"✓）⇒ ★ 也记 ✓（`action` 就是内容 ✓）
      logVoiceTurn({
        device: device, question: heard, reply: vcOld.reply,
        action: vcOld.action || '', params: vcOld.params || null,
        mood: moodForCmd(vcOld), latency_ms: Date.now() - t0, branch: 'command',
      });
          log(`voice-chat ✓ device=${device || '-'} ★命令回合(不叫LLM) action=${vcOld.action || '-'} heard="${heard.slice(0, 20)}"`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          // ★ 2026-10-12：命令回合（reply 空）**也要给一个音频** ✗ ——
          //   给的是 40ms 静音垫 ✓（人耳听不到 ✓ 但对方/老端点不会把它当"没有音频" ✗ 详见上面那段注释 ✓）
          const silUrl = String(vcOld.reply || '').trim() ? '' : silenceAudioUrl();
          res.end(JSON.stringify(Object.assign({
            success: true, text: oneLine(heard), reply: vcOld.reply,
            session: device ? (device + '#' + chatDepthOf(device)) : ''
          }, silUrl ? { audio: silUrl, audio_is_silence: true } : {},
             vcOld.action ? { action: vcOld.action } : {}, vcOld.params ? { params: vcOld.params } : {},
             // ★ CR-20261012-02：命令回合的语气（★ 由 action 决定 ⇒ 与 reply 同源 ✓）
             //   ★ 不给时**不带这个字段** ✗（不是给空值 ✓ —— 设备端规格书 §2.3 第 1 条 ✓）
             moodForCmd(vcOld) ? { reply_mood: moodForCmd(vcOld) } : {})));
          return;
        }

        // ② 知识库检索（★ 这一步是"先检索再回答"的核心）
        // ② ★ 固定问答（人设类）**已拆掉** ✗（用户 2026-10-12：「人设一定要幽默、一定要灵活」✓）
        //    ⇒ `selfAnswer()` 现在恒返回 null ✓ 一律交给模型 ✓（人设写在 system prompt 里 ✓）
        const selfA = selfAnswer(heard);
        // ③ ★★ 知识库检索 与 【联网路由判定】**并行发** ✗（CR-20261012-03 §6.1 ✓）
        //    ★ 为什么并行：检索只要 0.12~0.27s ✓ 而路由要 0.7~1.1s ✓
        //      ⇒ 串行的话白等一次 ✓；并行则"等路由"的时间把检索藏掉了 ✓
        // ★ 2026-10-20：`routeNeedsWeb` → `routeIntent`（★ 一次判定"联网／查学习数据"两件事 ✓）
        // ★★ 语音对练（独立智能体 ✗）：已在会话中 ⇒ **直接处理并回** ✓
        //   ★ 关键：此时**不进 routeIntent** ✗ —— 省一次往返 ✓ 且对练回合归对练 ✓
        if (RP_ON && device && _rpSessions.has(device)) {
          try {
            const rpA = await rpHandleTurn(device, heard);
            if (rpA && rpA.handled) {
              // ★ 分段打点（★ 设备端 §3 要的证据 ✗ 判据 t5−t1 ≤ 3.0s ✓）
              try { log(`[T] ${device} t0→ASR=${_t2 - _t0}ms  ASR→回=${Date.now() - _t2}ms  合计=${Date.now() - _t0}ms`); } catch (e) {}
              // ★ 2026-10-21：★ 顺手合成 WAV 一起回 ✗ ⇒ 设备不用再走一次 TTS 往返 ✓
              // ★★★ 2026-10-21 **止损** ✗✗
              //   ★ 原因：★ 设备端实测 t1→t5 = **5.4~7.0 s** ✗ 而其中大头是**我在这里同步做 TTS** ✓
              //   ★ 我的 `ttsWavUrl()` = `ttsToPcmFile()`（python3 合成）＋ **ffmpeg 转码** ✗ = 2 次子进程 ✗
              //   ★★ 为了省他们 1 秒（自己调 /api/tts）✓ 我加了 **4 秒** ✗✗ ⇒ **净变慢** ✗
              //   ★ 而我的 `latency_ms` 只量 `rpHandleTurn`（LLM 719ms ✓）⇒ ★ 我一直以为自己很快 ✗
              //   ⇒ ★ 立刻回到"只回文本" ✓ 我们自己不合成 ✓ 设备调 `/api/tts` 更快 ✓
              //   ★ audio 字段**保留**（★ 值为空串 ✓ 设备按"没有 audio"处理 ✓ 不破坏契约 ✓）
              const rpAudio = '';
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ success: true, reply: rpA.reply, audio: rpAudio, heard: heard, roleplay: true, modules: rpA.modules || undefined, scenes: rpA.scenes || undefined }));
              return;
            }
          } catch (e) { log('ROLEPLAY ⚠️ 回合异常（★ 退回小星 ✓）: ' + e.message); }
        }        const intentP = routeIntent(heard);
        const kb = await kbSearchForVoice(heard);
        const intent = await intentP;
        // ★★ 路由判定"用户想开始对练" ⇒ **立刻进对练** ✓（★ 不落到小星的能力里 ✗）
        if (RP_ON && device && intent.roleplay && !_rpSessions.has(device)) {
          _rpStartPending.add(device);
          try {
            const rpB = await rpHandleTurn(device, heard);
            if (rpB && rpB.handled) {
              const rpAudio2 = '';   // ★ 同上：不在同步路径上合成 ✓（★ 2026-10-21 止损 ✗）
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ success: true, reply: rpB.reply, audio: rpAudio2, heard: heard, roleplay: true, modules: rpB.modules || undefined, scenes: rpB.scenes || undefined }));
              return;
            }
          } catch (e) { _rpStartPending.delete(device); log('ROLEPLAY ⚠️ 启动异常（★ 退回小星 ✓）: ' + e.message); }
        }
        const needWeb = intent.web;
        // ★★ 学习数据（★ 数字由 S2 算 ✓ 这里只搬运 ✓）；与知识库**并行**取 ✓ 不占路由时间 ✓
        const toolData = intent.learning ? await learningSummary(device, intent.learning) : null;
        // ★ 期0 影子：非命令回合的判定（都不 await ✓）
        shadowJudge(heard, selfA ? { kind: 'self_qa' } : { kind: 'kb', reliable: !!kb.reliable, maxScore: kb.maxScore || 0 }, device);

        // ③ 决定回答：检索服务挂了 / 检索结果不可信 / 没这条知识 ⇒ 直接用兜底话术，**不叫模型**
        //    ★★ 这是防"编答案"的关键闸门。实测（2026-09-29）：只靠提示词说"不许编"是不够的 ——
        //       问「录像怎么开始」（知识库里根本没有设备说明），模型照样答出
        //       "录像设备开机后按录制键就开始" ✗ 那是它自己编的。
        //       ⇒ 把判据挪到【代码】里：检索不可信 ⇒ 根本不给它编的机会。
        let reply, usage = null;
        let replyMood = null;   // ★ CR-20261012-02：语气（★ 不给就保持 null ⇒ 响应里不带该字段 ✗）
        let webHit = false;     // ★ 语音会话记录用：这一轮是不是走了联网分支 ✓（决定 branch 字段）
        // ★★★ CR-20261012-03 §6.6 方案 B：**联网分支**（用户拍板「把 B 接上」✓）
        //   ★ 只在【路由器说需要】且【额度够】时才走 ✓
        //   ★★ 失败/超时/超额度 ⇒ reply 仍是 null ⇒ **自动落到下面原有的知识库＋deepseek 那条** ✓
        //      ⇒ 最坏情况 = 今天的行为 ✓（不会比现在差 ✓）
        //   ⚠️ 注意顺序：联网分支**必须在最前** ✗ —— 因为"店里没有的知识"正是要走它的 ✓
        if (needWeb && webQuotaOk()) {
          // ★ 长度提示直接取 `DETAIL_STYLES` ✗ —— 不能写 `style.len`：
          //   `style` 定义在 `voiceChatAnswer()` **函数内部**（另一个作用域 ✓）
          //   ⇒ 在这里引用会 **ReferenceError** ✗（本地语法检查查不出来 ✓ 只有跑起来才炸 ✓）
          //   ★ 判据：**跨函数抄表达式时，先确认变量在【当前】作用域里** ✗
          const wa = await webAnswer(heard, (DETAIL_STYLES[detail] || DETAIL_STYLES[2]).len);
          if (wa && wa.reply) {
            reply = wa.reply; replyMood = normMood(wa.mood); webHit = true;   // ★ 语气与这句同源 ✓（同一次调用出的 ✓）
            log(`WEB 分支命中 ✓ 来源${(wa.sources || []).length}条 mood=${replyMood || '(不发)'}`);
          }
        }
        // ★★ 整段用 `reply == null` 包起来 ✗✗ —— 为什么必须：
        //   第一版我写成 `if (reply == null && selfA) {…} else if (!kb.ok) {…} else {…}` ✗
        //   ⇒ 联网分支**已经给了 reply** 时，第一个条件为假 ⇒ 落到 `else if (!kb.ok)` ✗
        //     ⇒ ★ **知识库一挂，就会把我刚搜到的答案覆盖成"知识库不可用"** ✗✗
        //   ★ 判据：**给一条分支链加"前置分支"时，要检查后面条件的【互斥性】** ✗ ——
        //     原来那两组条件是互斥的（selfA / kb.ok ✓），我插进来之后就**不互斥**了 ✓
        if (reply == null) {
          if (selfA) {
            reply = selfA;
          } else if (!kb.ok) {
            reply = KB_DOWN_REPLY;
          } else {
            if (!kb.reliable) log('检索不可信 ⇒ 按用户要求交给模型凭通用经验回答（不再生硬说不知道）');
            // ★ 历史由 chatMsgsFor 给（"新话题/过期"已在上面统一处理过 ⇒ 这里传 '0' 不再清 ✓）
            const history = chatMsgsFor(device, '0');
            try {
              const ans = await voiceChatAnswer(heard, kb.results, history, detail, toolData);
              reply = ans.reply;
              usage = ans.usage;
              replyMood = normMood(ans.mood);   // ★ 语气与这句同源 ✓（模型在同一次输出里给的 ✓）
              if (ans.refused) {
                log(`voice-chat: 模型判定参考内容里没有答案（raw="${ans.raw}"）⇒ 答"查不到"`);
                reply = NO_KB_REPLY;
                replyMood = null;   // ★ 换成兜底话术了 ⇒ 语气必须跟着清 ✗（否则"查不到"配个 happy 就荒唐了 ✓）
              }
            } catch (e) {
              log(`⚠️ voice-chat LLM 失败: ${e.message}`);
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ success: false, error: 'llm_failed' }));
              return;
            }
            // ★ 模型有时候还是会绕开"不知道"去编 ⇒ 再过一道：回复里出现明确兜底词就采用，
            //   否则要求它至少在参考内容里找得到依据（这里只做长度与空值兜底，不做语义判断）
            if (!reply) reply = NO_KB_REPLY;
          }
        }
        // ★★ 记账放在**分支之外**（2026-10-09 修）——
        //   原来它在 else 里 ⇒ `selfA`（人设自答）与 `!kb.ok`（知识库不可用）这两类轮次
        //   **完全不进上下文** ✗ ⇒ 用户"接着追问"时模型看不见上一句 ✗
        //   （设备端 2026-09-29 报的「`session` 恒为 #1」正是这个：数字停住 = 没在记 ✓）
        chatRecord(device, heard, reply);

        // ④ ★ 服务端自己截断（别指望设备端，规格书 §4.4 写死了）
        //   ★ 按 audio_fmt 收上限：整段 WAV 要装进设备 1.5MB 缓冲（见 wavCapFor）
        const want = (DETAIL_STYLES[detail] || DETAIL_STYLES[2]).max;
        let cap = wavCapFor(afmt, want);
        // ★★ 自动降级：只要 ulaw8 能**多留字**就换（不要求"ulaw8 一定装得下"——
        //   原来那个条件让 341 字的回复既没降级、又被 pcm16 的 200 字线砍掉 141 字 ✗）
        if (AUTO_ULAW8 && afmt === 'pcm16' && reply.length > cap) {
          const cap8 = wavCapFor('ulaw8', want);
          if (cap8 > cap) {
            log(`voice-chat: 回复 ${reply.length} 字 > pcm16 上限 ${cap} 字 ⇒ 自动降级 ulaw8（上限 ${cap8} 字，设备端按 RIFF fmt 自适应 ✓）`);
            afmt = 'ulaw8';
            cap = cap8;
          }
        }
        if (reply.length > cap) {
          log(`voice-chat: 回复 ${reply.length} 字 > ${afmt} 上限 ${cap} 字 ⇒ 截断（避免设备端 WAV 缓冲溢出）`);
          reply = reply.slice(0, cap) + '…';
        }
        if (!reply) reply = NO_KB_REPLY;

        // ⑤ TTS
        // ★ 2026-10-12 用户改设计后，**命令回合不会走到这里** ✓ ——
        //   它在上面 `voiceCmdHandle` 命中时**已经提前 return** 了（带 action/params、不带音频 ✓
        //   见"★ CR-20261010-01"那段早退 ✓）⇒ 所以这里**不需要**为命令回合加空 reply 分支 ✓
        //   ★ 判据：**改语义前先确认代码走哪条路** ✗ —— 我一度在这里加了个"空 reply 跳过 TTS"的守卫，
        //     后来发现那一整段对命令回合**不可达** ✓ ⇒ 撤掉（不留永远不执行的分支 ✗）
        let audioPath = '';
        try {
          const t = await handleTTS(reply, afmt);
          audioPath = t.audio || '';
        } catch (e) {
          log(`⚠️ voice-chat TTS 失败: ${e.message}`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'tts_failed' }));
          return;
        }

        // ★ 全过程都要留证据：否则出事故时只看到"回复很短"，分不清是
        //   "知识库没命中"（正常）还是"模型没答"（异常）
        // ★★ 2026-10-12 补：**把 reply 原话记下来** ✗✗
        //   为什么必须（今天的教训 ✓）：用户报「它说能帮我订机票」✗
        //   而日志**只记了字数** ✗ ⇒ 我查了半天只能复现、拿不到当时那一句 ✓
        //   ★ 判据：**用户报的是"它说了什么"，日志就必须记"它说了什么"** ✗
        //     （只记长度 = 把唯一能定性的证据丢了 ✓）
        //   ★ 同时也打能力越界的**警告**（★ 只警告不拦截 ✓ —— 这些词在否认句里是正常的 ✓）
        //   ★ 长度也照旧记（它决定 TTS 与设备缓冲 ✓）
        const capHit = capAlert(reply);
        if (capHit) log(`⚠️ 能力词命中「${capHit}」⇒ 请人工看一眼是否真的吹了 ✗｜reply="${reply.slice(0, 120)}"`);
        log(`voice-chat ✓ device=${device || '-'} d=${detail} fmt=${afmt}${selfA ? ' SELF' : ''} kb=${kb.ok ? kb.results.length : 'DOWN'} ` +
            `rel=${kb.reliable ? 'Y' : 'N'}(${kb.maxScore}) ` +
            // ★ 打出上下文深度（设备端 2026-09-29 报「session 恒 #1」时，光看响应看不出是"没记"还是"记了但被上限压住" ✗
            //   ⇒ 把"此刻表里有几条 / 深度几"打进日志，这类问题一眼可判 ✓（对后续任何多轮问题都有用 ✓）
            `ctx=${chatDepthOf(device)}条数=${(device && _chatSessions.get(device) ? _chatSessions.get(device).msgs.length : 0)}表内=${_chatSessions.size} ` +
            `heard="${heard.slice(0, 20)}" reply=${reply.length}字 ` +
            // ★★ 2026-10-12 补：**记原话** ✗（用户报"它说能订机票"时，只记字数就查不清 ✓）
            `reply="${reply.slice(0, 120)}" ` +
            `tokens=${usage ? (usage.prompt_tokens + '/' + usage.completion_tokens) : '-'} ` +
            `耗时=${((Date.now() - t0) / 1000).toFixed(1)}s`);

        // ★ 回报**实际**格式（字段名与流式头一致：format/bits/sample_rate/channels）——
        //   设备端本来按下载到的 WAV 的 fmt 块自适应，这里是给"不想解析 WAV 头"的消费端兜底，
        //   并且让"服务端自己换了格式"这件事**在响应里看得见**（不靠猜）
        res.writeHead(200, {
          'Content-Type': 'application/json; charset=utf-8',
          'X-Dyz-Format': afmt
        });
        // ★★ 语音会话记录：**fire-and-forget** ✓（非流式端点 = 设备回退路径 ✓ 同样要记 ✓）
        logVoiceTurn({
          device: device, question: heard, reply: reply,
          action: vcOld ? vcOld.action : '', params: vcOld ? vcOld.params : null,
          mood: replyMood, latency_ms: Date.now() - t0,
          branch: vcOld ? 'command' : (webHit ? 'web' : (kb.ok ? 'kb' : 'kb_down')),
        });
        res.end(JSON.stringify({
          success: true,
          text: heard,
          reply: reply,
          audio: audioPath,
          // ★ 实际音频格式（可能被服务端自动降级过 —— 见 AUTO_ULAW8）
          format: afmt === 'ulaw8' ? 'ulaw8' : 'pcm_s16le',
          bits: afmt === 'ulaw8' ? 8 : 16,
          sample_rate: 16000,
          channels: 1,
          session: device ? (device + '#' + chatDepthOf(device)) : '',
          // ★ CR-20261012-02：语气（★ 没有就**不带这个字段** ✗ —— 设备端会用自己的两层降级 ✓
          //   ★ 不用 `reply_mood: null` ✗：设备端规格书 §2.3 第 1 条说的是"缺字段/不认识就降级"✓
          //     给 null 等于给一个"不认识的值"✓ 虽然也能降级 ✓ 但**不带**更明确 ✓）
          ...(replyMood ? { reply_mood: replyMood } : {})
        }, null, 0));
      } catch (e) {
        log(`⚠️ voice-chat 异常: ${e.message}`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'asr_failed' }));
      }
    });
    return;
  }

  /* ============ CR-20260929-03 期2：流式问答端点 ============ */
  // 契约见协同 spec CR-20260929-03 §11（+ 设备端 §13 的三条补充）：
  //   ① 响应 = **1 行 JSON 头（以 \n 结尾）** + 连续裸 PCM（16k/单声道/16bit 小端）
  //   ② ★ **无论成功失败都先发 JSON 头** ⇒ 设备端逻辑统一："读第一行 → 再决定后续"
  //   ③ ★ **JSON 值里绝不能出现换行**（设备端判据 = "遇到第一个 \n 就是头结束"）
  //      ⇒ 服务端负责把 text/reply 里的换行压成空格，不让设备去处理
  //   ④ ★ **头可能跨块到达** ⇒ 我们自证时**不拿"首块能否解析"当判据**（用首字节时间）
  //   ⑤ ★ nginx 必须 `proxy_buffering off`（否则攒够一整块才转发 ⇒ 流式白做）
  //      另加 `X-Accel-Buffering: no` 做双保险
  //   ⑥ 服务端**只发 pcm_s16le**；将来加 μ-law 会显式写 `format:"ulaw8"`（设备端会明确报错，不会当 PCM 硬播）
  //
  // ★ 本版实现（v1）：**先发头 → 按句 TTS → 每句转 16k PCM 立刻推**
  //   头 ≈ ASR+检索+LLM ≈ 3.3s ⇒ **屏上先出字** ✓；首块音频 ≈ +首句 TTS ≈ 6~7s ✓
  //   ⚠️ 下一步（真流式）：`tts.py` 改 CosyVoice `streaming_call` + 24k→16k 重采样，
  //      可把**首块音频压到 ~4s**；本版先把"新契约 + 逐句推"跑通（设备端可先接 ✓ 不用等）
  function streamHeaders(res, afmt) {
    const ulaw = (afmt === 'ulaw8');
    res.writeHead(200, {
      'Content-Type': 'application/x-dyz-voice-stream',
      'Cache-Control': 'no-cache',
      'X-Accel-Buffering': 'no',        // ★ 双保险：即使 nginx 配错也不缓冲
      'X-Dyz-Sample-Rate': '16000', 'X-Dyz-Channels': '1',
      'X-Dyz-Bits': ulaw ? '8' : '16', 'X-Dyz-Format': ulaw ? 'ulaw8' : 'pcm_s16le'
    });
  }
  // ★ 值里禁换行（见上面 ③）
  function oneLine(s) {
    return String(s == null ? '' : s).replace(/[\r\n\t]+/g, ' ').replace(/\s{2,}/g, ' ').trim();
  }
  function headLine(res, obj) { res.write(JSON.stringify(obj) + '\n'); }
  // 一句话 → 16k/单声道/s16le PCM
  // ★ 用【异步】execFile 而不是 execSync：execSync 会阻塞事件循环，
  //   而 libuv 的 socket write 是异步的 ⇒ 阻塞期间已排队的响应数据发不出去 ✗
  //   （这正是设备端实测"头与首块 PCM 只差 1ms"的成因之一 ✓）
  function ffmpegToPcm(wav, fmt) {
    const ulaw = (fmt === 'ulaw8');
    return new Promise((resolve, reject) => {
      execFile('ffmpeg', ['-y', '-v', 'error', '-i', wav,
                          '-f', ulaw ? 'mulaw' : 's16le',
                          '-acodec', ulaw ? 'pcm_mulaw' : 'pcm_s16le',
                          '-ar', '16000', '-ac', '1', '-'],
               { maxBuffer: 64 * 1024 * 1024, encoding: 'buffer' },
               (err, stdout) => err ? reject(err) : resolve(stdout));
    });
  }
  async function sentencePcm(text, fmt) {
    const t = await handleTTS(text, fmt);
    const wav = path.join(AUDIO_DIR, path.basename(t.audio).replace(/\.mp3$/, '.wav'));
    if (!fs.existsSync(wav)) throw new Error('TTS WAV 不存在');
    return await ffmpegToPcm(wav, fmt);
  }
  // 【已删除】旧的 splitSentences(text, 60)：按标点切碎再合并到 ≤60 字，句子超长时 `p.slice(0, cap)` 硬切。
  //   ★ 2026-10-07 用户报"断句不自然"后确认它是病根之一（句子中间硬切 + 每段单独合成抹平句末停顿），
  //     已由下面的 splitSents() + 自适应分块取代（实测 264 字回答 7 块 → 3 块）。
  //   ★ 要恢复旧行为（不推荐）：`git show ac008d6^:voice-server.js | grep -A12 'function splitSentences'`
  /* ============ 流式的"只在句末断开"分块（2026-10-07 重做，治停顿不自然）============
     ★ 病根两条（实测）：
       ① `slice(0, cap)` **在句子中间硬切** ⇒ 听起来就是"不该停的地方停了" ✗
       ② 每段单独合成 ⇒ 句末的自然停顿（实测 0.5~1.05s）被抹成 0.23~0.45s 的**均匀小缝**
          （同一段 124 字：切 4 段 ⇒ 停顿 22 处/6.41s；整段一次 ⇒ 25 处/8.44s）✗
     ★ 改法：**只在句末标点断开**（句子太长才退到从句标点），并且**块尽量大**；
       代价是首块变大 ⇒ 出声变慢，所以用"**按播放队列自适应**"：
         队列里还有多少秒可播 → 决定下一块能放多大（合成速度实测 ≈18 字/秒，取 16 保守）
       ⇒ 首块仍是 60 字（出声不受影响），后面越攒越大（最多 240 字）✓
  */
  const STREAM_FIRST_CAP = 60;    // 首块上限（出声延迟由它决定，别再调大）
  const STREAM_REST_CAP = 240;    // 后续块上限（越大越自然；实测单次 248 字没问题 ✓）
  const STREAM_SYNTH_CPS = 16;    // 合成速度（字/秒）保守值（实测 230 字 12.8s ≈ 18）
  // 只在句末标点断开（保留标点）；没有标点的尾巴也算一句
  function splitSents(text) {
    return (String(text).match(/[^。！？!?；;]*[。！？!?；;]|[^。！？!?；;]+$/g) || [String(text)])
      .map(s => s.trim()).filter(Boolean);
  }
  // 一句超过 cap 时：**先从从句标点断**（，、：,），实在没有才硬切（极少数）
  function cutPiece(s, cap) {
    if (s.length <= cap) return [s, ''];
    const head = s.slice(0, cap);
    const m = head.match(/^[\s\S]*[，、：,]/);
    if (m && m[0].length >= Math.min(20, cap)) return [m[0], s.slice(m[0].length)];
    return [head, s.slice(cap)];
  }

  if (req.method === 'POST' && req.url === '/api/voice-chat-stream') {
    const ct = req.headers['content-type'] || '';
    const bm = ct.match(/boundary=(.+)/);
    if (voiceRateLimited(req)) {
      streamHeaders(res); headLine(res, { success: false, error: 'rate_limited' }); res.end(); return;
    }
    if (!bm) {
      streamHeaders(res); headLine(res, { success: false, error: 'no_boundary' }); res.end(); return;
    }
    const chunks = []; let got = 0, tooBig = false;
    req.on('data', c => {
      if (tooBig) return;
      got += c.length;
      if (got > VOICE_MAX_BODY) { tooBig = true; req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', async () => {
      if (tooBig) { streamHeaders(res); headLine(res, { success: false, error: 'too_large' }); res.end(); return; }
      const t0 = Date.now();
      let headSent = false;
      const fail = (code) => {
        try { if (!headSent) { streamHeaders(res); headSent = true; } headLine(res, { success: false, error: code }); } catch (e) {}
        try { res.end(); } catch (e) {}
      };
      try {
        const parts = parseMultipart(Buffer.concat(chunks), bm[1]);
        const audioPart = parts.find(p => p.name === 'audio');
        const devPart = parts.find(p => p.name === 'device');
        const resetPart = parts.find(p => p.name === 'reset');
        const detailPart = parts.find(p => p.name === 'detail');
        const fmtPart = parts.find(p => p.name === 'audio_fmt');
        const device = devPart ? String(devPart.data.toString('utf8')).trim() : '';
        const reset = resetPart ? String(resetPart.data.toString('utf8')).trim() : '0';
        const detail = detailOf(detailPart ? detailPart.data.toString('utf8') : '');
        // ★ audio_fmt：'pcm16'(缺省) / 'ulaw8'（设备端 §16.4 建议的字段名）—— 非法值一律按 pcm16，不报错
        //   ★ 用 let：下面可能要按"体积预算"自动改成 ulaw8（见 pickStreamFmt）
        let afmt = (fmtPart && String(fmtPart.data.toString('utf8')).trim() === 'ulaw8') ? 'ulaw8' : 'pcm16';
        if (!audioPart || !audioPart.data || !audioPart.data.length) { fail('no_audio'); return; }

        const asr = await handleASR(audioPart.data);
        const heard = (asr.text || '').trim();
        if (!heard) { log('voice-chat-stream: 没听清'); fail('asr_empty'); return; }
        // ★★ 与 /api/voice-chat 同一处修正：「新话题 / 过期」在任何分支之前统一处理 ✓
        chatCtx(device, reset);

        // ★ CR-20261010-01 期1：语音命令优先（与 /api/voice-chat 同一份判定 ✓ 只是这里**不能早退** ✗
        //   —— 流式端点是"一行 JSON 头 + 裸 PCM"✓ 早退会让设备等不到音频 ✓
        //   ⇒ 做法：命中命令时**跳过 KB/LLM**，但仍然走下面的"发头 + 合成" ✓）
        const vcS = await voiceCmdHandle(device, heard);

        // 人设固定问答 / 知识库检索 / 三档长度 —— 与 /api/voice-chat 同一套逻辑
        //   ★ 人设短路已拆 ✗（`selfAnswer` 恒 null ✓ 交给模型 ✓ 人设见 system prompt ✓）
        const selfA = vcS ? null : selfAnswer(heard);
        // ★★ 知识库检索 与 【联网路由判定】**并行发** ✗（CR-20261012-03 §6.1 ✓ 同非流式端点 ✓）
        // ★★ 知识库检索 与 【意图路由】**并行发** ✗（CR-20261012-03 §6.1 ✓）
        //   ★ 2026-10-20：`routeNeedsWeb` → `routeIntent`（★ 一次判"联网／查学习数据"✓）
        //     ★ 命令回合不叫模型 ⇒ 直接给空意图 ✓（★ 省一次往返 ✓）
        // ★★ 语音对练（独立智能体 ✗）：已在会话中 ⇒ **直接处理并回** ✓
        //   ★ 关键：此时**不进 routeIntent** ✗ —— 省一次往返 ✓ 且对练回合归对练 ✓
        if (RP_ON && device && _rpSessions.has(device)) {
          try {
            const rpA = await rpHandleTurn(device, heard);
            if (rpA && rpA.handled) {
              // ★ 分段打点（★ 设备端 §3 要的证据 ✗ 判据 t5−t1 ≤ 3.0s ✓）
              try { log(`[T] ${device} t0→ASR=${_t2 - _t0}ms  ASR→回=${Date.now() - _t2}ms  合计=${Date.now() - _t0}ms`); } catch (e) {}
              // ★ 2026-10-21：★ 顺手合成 WAV 一起回 ✗ ⇒ 设备不用再走一次 TTS 往返 ✓
              // ★★★ 2026-10-21 **止损** ✗✗
              //   ★ 原因：★ 设备端实测 t1→t5 = **5.4~7.0 s** ✗ 而其中大头是**我在这里同步做 TTS** ✓
              //   ★ 我的 `ttsWavUrl()` = `ttsToPcmFile()`（python3 合成）＋ **ffmpeg 转码** ✗ = 2 次子进程 ✗
              //   ★★ 为了省他们 1 秒（自己调 /api/tts）✓ 我加了 **4 秒** ✗✗ ⇒ **净变慢** ✗
              //   ★ 而我的 `latency_ms` 只量 `rpHandleTurn`（LLM 719ms ✓）⇒ ★ 我一直以为自己很快 ✗
              //   ⇒ ★ 立刻回到"只回文本" ✓ 我们自己不合成 ✓ 设备调 `/api/tts` 更快 ✓
              //   ★ audio 字段**保留**（★ 值为空串 ✓ 设备按"没有 audio"处理 ✓ 不破坏契约 ✓）
              const rpAudio = '';
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ success: true, reply: rpA.reply, audio: rpAudio, heard: heard, roleplay: true, modules: rpA.modules || undefined, scenes: rpA.scenes || undefined }));
              return;
            }
          } catch (e) { log('ROLEPLAY ⚠️ 回合异常（★ 退回小星 ✓）: ' + e.message); }
        }        const intentP = vcS ? Promise.resolve({ web: false, learning: null, roleplay: false }) : routeIntent(heard);
        const kb = vcS ? { ok: true, results: [], reliable: true, maxScore: 0 }
                       : await kbSearchForVoice(heard);
        const intent = await intentP;
        // ★★ 路由判定"用户想开始对练" ⇒ **立刻进对练** ✓（★ 不落到小星的能力里 ✗）
        if (RP_ON && device && intent.roleplay && !_rpSessions.has(device)) {
          _rpStartPending.add(device);
          try {
            const rpB = await rpHandleTurn(device, heard);
            if (rpB && rpB.handled) {
              const rpAudio2 = '';   // ★ 同上：不在同步路径上合成 ✓（★ 2026-10-21 止损 ✗）
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ success: true, reply: rpB.reply, audio: rpAudio2, heard: heard, roleplay: true, modules: rpB.modules || undefined, scenes: rpB.scenes || undefined }));
              return;
            }
          } catch (e) { _rpStartPending.delete(device); log('ROLEPLAY ⚠️ 启动异常（★ 退回小星 ✓）: ' + e.message); }
        }
        const needWeb = intent.web;
        // ★ 学习数据（★ 数字由 S2 算 ✓ 失败按"没查到"✓ 绝不编 ✗）
        const toolData = intent.learning ? await learningSummary(device, intent.learning) : null;
        // ★ 期0 影子（流式端点 = 设备主路径 ✓）：只记录，不 await、不改任何返回值 ✓
        if (vcS) shadowJudge(heard, { kind: 'command', action: vcS.action || null }, device);
        else shadowJudge(heard, selfA ? { kind: 'self_qa' } : { kind: 'kb', reliable: !!kb.reliable, maxScore: kb.maxScore || 0 }, device);
        let reply, usage = null;
        let replyMood = null;   // ★ CR-20261012-02：语气（★ 不给就保持 null ⇒ 头里不带该字段 ✗）
        let webHit = false;     // ★ 语音会话记录用：这一轮是不是走了联网分支 ✓（决定 branch 字段）
        // ★★★ CR-20261012-03 §6.6 方案 B：联网分支（★ 结构与非流式端点**完全一致** ✓ 别写两套 ✗）
        //   ⚠️ 流式端点这里有个已知取舍 ✗：`webAnswer` 是**整段**返回的（不带流式 ✓）
        //     ⇒ 联网回合会**多等一段**（头发出之前就开始等 ✓ 实测 2.5~5.9s ✓）
        //     ★ 这是"能用真搜索"换来的代价 ✓ 已如实记录（规格书 §6.6.2 ✓）
        if (!vcS && needWeb && webQuotaOk()) {
          // ★ 同非流式端点：长度提示取 `DETAIL_STYLES` ✓（★ 不是 `style.len` ✗ 那个在别的函数里 ✓）
          const wa = await webAnswer(heard, (DETAIL_STYLES[detail] || DETAIL_STYLES[2]).len);
          if (wa && wa.reply) {
            reply = wa.reply; replyMood = normMood(wa.mood); webHit = true;
            log(`WEB 分支命中（流式）✓ 来源${(wa.sources || []).length}条 mood=${replyMood || '(不发)'}`);
          }
        }
        if (reply == null && vcS) {
          reply = vcS.reply;                                  // ★ 命令回合不叫 LLM ✓
          replyMood = moodForCmd(vcS);                        // ★ 语气由 action 决定 ⇒ 与 reply 同源 ✓
          log(`voice-chat-stream ✓ device=${device || '-'} ★命令回合(不叫LLM) action=${vcS.action || '-'} mood=${replyMood || '(不发)'} heard="${heard.slice(0, 20)}"`);
        } else if (reply == null && selfA) {
          reply = selfA;
        } else if (reply == null && !kb.ok) {
          reply = KB_DOWN_REPLY;
        } else if (reply == null) {
          if (!kb.reliable) log('检索不可信 ⇒ 按用户要求交给模型凭通用经验回答（不再生硬说不知道）');
          // ★ 历史由 chatMsgsFor 给（"新话题/过期"上面已统一处理 ⇒ 传 '0' 不再清 ✓）
          const history = chatMsgsFor(device, '0');
          try {
            const ans = await voiceChatAnswer(heard, kb.results, history, detail, toolData);
            reply = ans.reply; usage = ans.usage;
            replyMood = normMood(ans.mood);                    // ★ 同一次输出里给的 ⇒ 同源 ✓
            if (ans.refused) { log('voice-chat-stream: 模型判定参考内容里没有答案 ⇒ 答"查不到"'); reply = NO_KB_REPLY; replyMood = null; }
          } catch (e) {
            log(`⚠️ voice-chat-stream LLM 失败: ${e.message}`);
            // ★ 失败也要留痕 ✓（★ 与"降级必须留证据"同族 ✗ —— 只记成功等于看不见故障率 ✓）
            logVoiceTurn({ device: device, question: heard, err: 'llm_failed',
              latency_ms: Date.now() - t0, branch: 'err' });
            fail('llm_failed'); return;
          }
          if (!reply) { reply = NO_KB_REPLY; replyMood = null; }
        }
        // ★★ 记账放在分支之外（与 /api/voice-chat 同一处修正 ✓）——
        //   原来在 else 里 ⇒ selfA / 知识库不可用 那两类轮次不进上下文 ✗
        chatRecord(device, heard, reply);
        const cap = (DETAIL_STYLES[detail] || DETAIL_STYLES[2]).max;
        if (reply.length > cap) reply = reply.slice(0, cap) + '…';
        // ★★ 流式也要看**体积预算**（2026-10-07 用户实测"具体档没念完"后定位）
        //   实测日志：`detail=3` 推过 **2,075,594 字节**的 PCM（301 字），
        //   而设备端 `VOICE_WAV_MAX` 只有 **1.5MB** —— 之前只验过 897KB 的长流 ✗
        //   ⇒ pcm16 装不下就改 μ-law（体积正好一半），**在发头之前**定下来，
        //     这样 JSON 头里的 format/bits 与实际字节流一致 ✓
        afmt = pickStreamFmt(afmt, reply.length, detail);

        // ★★★ 先把 JSON 头发出去（★ 不等 TTS）—— 设备屏上可以先出字
        //   ★★ 2026-09-29 设备端实测反馈修的一个真 bug：他们测到"头与首块 PCM 只差 1 毫秒"
        //      ⇒ "先出字"没兑现。两个成因，都在这三行里堵：
        //      ① 没关 Nagle ⇒ 内核把小包攒着，等后续数据一起发 ✗
        //      ② 写完立刻做 TTS（当时还是 execSync 阻塞事件循环）⇒ libuv 的 socket write 是异步的，
        //         被同步阻塞挡住 = 头根本没出去 ✗
        //   ⇒ 关 Nagle + flushHeaders + **让出一次事件循环**（setImmediate）把头真发出去，再开始合成
        streamHeaders(res, afmt); headSent = true;
        try { if (res.socket) res.socket.setNoDelay(true); } catch (e) {}
        try { if (typeof res.flushHeaders === 'function') res.flushHeaders(); } catch (e) {}
        headLine(res, {
          success: true, text: oneLine(heard), reply: oneLine(reply),
          session: device ? (device + '#' + chatDepthOf(device)) : '',
          sample_rate: 16000, channels: 1,
          bits: afmt === 'ulaw8' ? 8 : 16, format: afmt === 'ulaw8' ? 'ulaw8' : 'pcm_s16le',
          // ★ CR-20261010-01：命令回合才带这两个字段 ✓（不是命令 ⇒ 一个都不加 ⇒ 老固件 behavior 完全不变 ✓）
          ...(vcS && vcS.action ? { action: vcS.action } : {}),
          ...(vcS && vcS.params ? { params: vcS.params } : {}),
          // ★ CR-20261012-02：语气（★ 与 reply 同级 ✓ 头字段 ✓；★ 没有就**不带** ✗ ⇒ 设备降级 ✓）
          //   ★★ 同源保证：命令回合取 `action` ✓／联网与知识库回合取**同一次模型输出**里的那行 ✓
          ...(replyMood ? { reply_mood: replyMood } : {})
        });
        await new Promise(r => setImmediate(r));   // ★ 让 libuv 把上面这一行真正写到 socket 再继续
        const tHead = Date.now();
        // ★★ 2026-10-12 补：**能力词警告 + 记原话** ✗（同非流式端点 ✓
        //   用户报「它说能帮我订机票」时，这里只记字数 ⇒ 拿不到当时那一句 ✓）
        const capHitS = capAlert(reply);
        if (capHitS) log(`⚠️ 能力词命中「${capHitS}」⇒ 请人工看一眼是否真的吹了 ✗｜reply="${reply.slice(0, 120)}"`);
        log(`voice-chat-stream 头已发 d=${detail} fmt=${afmt}${selfA ? ' SELF' : ''} heard="${heard.slice(0, 16)}" ` +
            `reply=${reply.length}字 reply="${reply.slice(0, 120)}" 头耗时=${((tHead - t0) / 1000).toFixed(1)}s`);

        // ★★★ 自适应分块：只在句末断开，块大小按"播放队列还能撑多久"决定
        //   首块 60 字（出声快）→ 队列攒起来后放大到 240 字（语调自然、拼缝少）
        const sents = splitSents(reply);
        const bytesPerSec = afmt === 'ulaw8' ? 16000 : 32000;
        let pushed = 0, pieceN = 0, si = 0, carry = '';
        // ★★ 2026-10-12 命令回合：reply 空 ⇒ **送 40ms 静音垫** ✗
        //   ★ 为什么不能"什么都不送"：他们 `voice.c` 判成功的条件是 `audio_bytes > 0` ✓
        //     0 字节 ⇒ 判「一个字节都没播 ⇒ 退回老端点」✗（白跑一轮 + action 可能被应用两次 ✗）
        //   ⇒ 静音 40ms ⇒ 判成功 ✓ 而**人耳听不到** ✓（用户要的"不播报"仍然成立 ✓）
        if (vcS && !String(reply || '').trim()) {
          const sil = silencePcm(afmt);
          res.write(sil); pushed += sil.length; pieceN = 1;
          log(`voice-chat-stream ★命令回合 ⇒ 送静音垫 ${sil.length} 字节（${SILENCE_MS}ms ✓ 无播报 ✓）`
            + `（★ 不发的话他们那侧会判"0 字节 = 失败"并回退老端点 ✗）`);
        }
        while (si < sents.length || carry) {
          const queuedSec = pushed / bytesPerSec;            // 已推的音频还能播多少秒
          const target = Math.min(STREAM_REST_CAP,
            Math.max(STREAM_FIRST_CAP, Math.floor(queuedSec * 0.7 * STREAM_SYNTH_CPS)));
          let piece = carry; carry = '';
          while (si < sents.length) {
            const nx = sents[si];
            if (!piece && nx.length > target) {              // 单句比目标还长 ⇒ 从句标点断
              const cut = cutPiece(nx, target);
              piece = cut[0];
              if (cut[1]) carry = cut[1]; else si++;
              break;
            }
            if (piece && piece.length + nx.length > target) break;   // 装不下就收口
            piece += nx; si++;
            if (piece.length >= target) break;
          }
          if (!piece) break;
          pieceN++;
          try {
            const pcm = await sentencePcm(piece, afmt);
            if (pcm && pcm.length) {
              res.write(pcm); pushed += pcm.length;
              // ★ 首块 PCM 的时间要单独记 —— 否则"头与首块只差 1ms"这类问题看不出来（设备端实测反馈）
              if (pieceN === 1) {
                log(`voice-chat-stream 首块 PCM 已推 距头 ${Date.now() - tHead}ms（${pcm.length} 字节）`);
              } else {
                log(`voice-chat-stream 第 ${pieceN} 块：${piece.length} 字 → ${pcm.length} 字节（队列还剩 ` +
                    `${(pushed / bytesPerSec).toFixed(1)}s 可播）`);
              }
            } else log(`⚠️ voice-chat-stream 第 ${pieceN} 块 PCM 为空`);
          } catch (e) {
            // ★ 已推的音频照旧 + 正常结束（绝不发半个 PCM 就不管 —— §11.6）
            log(`⚠️ voice-chat-stream 第 ${pieceN} 块 TTS 失败，已推 ${pushed} 字节后正常结束: ${e.message}`);
            break;
          }
        }
        log(`voice-chat-stream ✓ ${pieceN} 块（共 ${reply.length} 字）→ 推 PCM ${pushed} 字节 ` +
            `tokens=${usage ? (usage.prompt_tokens + '/' + usage.completion_tokens) : '-'} ` +
            `总耗时=${((Date.now() - t0) / 1000).toFixed(1)}s`);
        // ★★ 语音会话记录：**fire-and-forget** ✓ —— 放在最后 ✓ **不 await** ✓ 绝不改返回值 ✓
        //   ★ branch 口径：命令 / 联网 / 知识库 / 知识库不可用 —— 便于事后按来源统计 ✓
        logVoiceTurn({
          device: device, question: heard, reply: reply,
          action: vcS ? vcS.action : '', params: vcS ? vcS.params : null,
          mood: replyMood, latency_ms: Date.now() - t0,
          branch: vcS ? 'command' : (webHit ? 'web' : (kb.ok ? 'kb' : 'kb_down')),
        });
        res.end();
      } catch (e) {
        log(`⚠️ voice-chat-stream 异常: ${e.message}`);
        fail('server_error');
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