/**
 * auth-hmac.js —— 设备接口签名校验（服务端，S1 自实现）
 *
 * 配套：固件 `esp32-cam-vision2/main/auth_hmac.c` · 契约见 `协同/specs/CR-20261103-02-设备端接口签名HMAC.md`
 *
 * ★★ 签名串（与设备端**逐字节一致**，差一个换行就 401）：
 *
 *      METHOD \n PATH_AND_QUERY \n DEVICE_ID \n TS \n NONCE \n SHA256_HEX(BODY)
 *
 *   · 6 行、`\n` 分隔、**无结尾换行**
 *   · PATH_AND_QUERY = `req.url` 原样（含 query，不含 scheme/host）
 *   · BODY 用**原始字节**算 sha256（multipart 也一样，**不许 parse 完再 stringify** ✗）
 *   · 无 body 时第 6 行 = 空串的 sha256 常量
 *
 * ★ 为什么自己实现、不直接抄对方那份 SDK ✗：
 *   鉴权代码要能独立复核 —— 自己的实现 + 用对方的**公开测试向量**交叉验证，
 *   比"把对方的实现搬进来"更能发现两端不一致（例：路径是否含 query、大小写、换行）。
 *   交叉验证见 `scripts/verify-auth-hmac.js`（跑 CR 里那条 Python/Node 一致的向量）。
 *
 * 用法（rawBody = 原始请求体的 Buffer；GET 传 null）：
 *   const r = verifyDeviceAuth(req, rawBody, {   // rawBody = 原始字节 Buffer；GET 传 null
 *     secretOf: id => deviceSecretOf(id),   // 一机一密：按设备号取
 *     allowLegacy: true,                    // 过渡期允许老固件（明文密钥头）
 *     legacySecret: PUBLIC_SECRET,          // 公共兜底密钥（仅 legacy 用）
 *   });
 *   // r = { ok, mode:'sig'|'legacy'|'none', code, reason, deviceId }
 */

'use strict';

const crypto = require('crypto');

const SKEW_SEC = 300;          // 时钟偏移窗口（±5 分钟）
const NONCE_TTL_SEC = 600;     // nonce 记忆时长（必须 ≥ 2×SKEW）
const SHA256_EMPTY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

/* ── nonce 防重放（进程内 Map；S1 是单进程 ✓ 重启会清空 —— 可接受，重放窗口回到 0） ── */
const _nonce = new Map();      // nonce -> 过期 ms
let _lastSweep = 0;

function _sweep(now) {
  if (now - _lastSweep < 30000) return;    // 每 30 秒清一次就够
  _lastSweep = now;
  for (const [k, exp] of _nonce) { if (exp <= now) _nonce.delete(k); }
}

function sha256Hex(buf) {
  return crypto.createHash('sha256').update(buf || Buffer.alloc(0)).digest('hex');
}

/** 签名串（导出，方便单测/排查） */
function canonicalString(method, url, deviceId, ts, nonce, bodyHashHex) {
  return [String(method || '').toUpperCase(), String(url || ''), String(deviceId || ''),
          String(ts || ''), String(nonce || ''), String(bodyHashHex || '')].join('\n');
}

function sign(secret, canonical) {
  return crypto.createHmac('sha256', String(secret)).update(canonical).digest('hex');
}

/** 定长比较（避免按字节提前返回泄露信息） */
function safeEq(a, b) {
  const x = Buffer.from(String(a || ''), 'utf8');
  const y = Buffer.from(String(b || ''), 'utf8');
  if (x.length !== y.length) return false;
  return crypto.timingSafeEqual(x, y);
}

/**
 * 校验一次设备请求。
 * @param {http.IncomingMessage} req
 * @param {Buffer|null} rawBody  原始请求体（GET 传 null）
 * @param {{secretOf:function, allowLegacy?:boolean, legacySecret?:string}} opt
 */
function verifyDeviceAuth(req, rawBody, opt) {
  const o = opt || {};
  const h = (req && req.headers) || {};
  const devHeader = String(h['x-device-id'] || '').trim();
  const ts = String(h['x-device-ts'] || '').trim();
  const nonce = String(h['x-device-nonce'] || '').trim();
  const sig = String(h['x-device-sign'] || '').trim();
  const hasSig = !!(ts && nonce && sig);

  // ── 没有签名头 ⇒ 老固件：走 legacy（明文密钥头 / body 里的 secret） ──
  if (!hasSig) {
    if (!o.allowLegacy) return { ok: false, mode: 'none', code: 401, reason: '缺少签名头且已禁止 legacy（请升级固件）', deviceId: devHeader };
    const legacy = String(h['x-device-secret'] || '').trim();
    if (!legacy) return { ok: false, mode: 'none', code: 401, reason: '缺少签名头，也没带 X-Device-Secret', deviceId: devHeader };
    if (o.legacySecret && safeEq(legacy, o.legacySecret)) return { ok: true, mode: 'legacy', deviceId: devHeader };
    // 老固件也可能已经拿到了**自己那台**的密钥（一机一密早期版本）
    const own = devHeader && o.secretOf ? o.secretOf(devHeader) : '';
    if (own && safeEq(legacy, own)) return { ok: true, mode: 'legacy', deviceId: devHeader };
    return { ok: false, mode: 'legacy', code: 401, reason: 'legacy 密钥不匹配', deviceId: devHeader };
  }

  // ── 签名路 ──
  const now = Date.now();
  const nowSec = Math.floor(now / 1000);
  const tsNum = Number(ts);
  if (!/^\d{10}$/.test(ts) || !isFinite(tsNum)) return { ok: false, mode: 'sig', code: 401, reason: 'ts 格式不对（要 10 位 Unix 秒）', deviceId: devHeader };
  const skew = nowSec - tsNum;
  if (Math.abs(skew) > SKEW_SEC) return { ok: false, mode: 'sig', code: 401, reason: `ts 过期（偏移 ${skew}s，检查设备时钟/SNTP）`, deviceId: devHeader };
  if (!/^[0-9a-f]{16}$/.test(nonce)) return { ok: false, mode: 'sig', code: 401, reason: 'nonce 格式不对（16 位小写 hex）', deviceId: devHeader };
  if (!devHeader) return { ok: false, mode: 'sig', code: 401, reason: '缺少 X-Device-Id', deviceId: '' };

  _sweep(now);
  if (_nonce.has(nonce)) return { ok: false, mode: 'sig', code: 401, reason: 'nonce 重复（疑似重放）', deviceId: devHeader };

  const secret = o.secretOf ? o.secretOf(devHeader) : '';
  if (!secret) return { ok: false, mode: 'sig', code: 401, reason: `未知设备 ${devHeader}`, deviceId: devHeader };

  const bodyHash = sha256Hex(rawBody || Buffer.alloc(0));
  const canon = canonicalString(req.method, req.url, devHeader, ts, nonce, bodyHash);
  const want = sign(secret, canon);
  if (!safeEq(want, sig.toLowerCase())) {
    return { ok: false, mode: 'sig', code: 401, reason: '签名不匹配', deviceId: devHeader };
  }

  // ★ 只有**验过**才记 nonce（验失败的请求不许占用 nonce，否则攻击者可拿乱签名把我们缓存打满）
  _nonce.set(nonce, now + NONCE_TTL_SEC * 1000);
  return { ok: true, mode: 'sig', deviceId: devHeader, bodyHash };
}

module.exports = { verifyDeviceAuth, canonicalString, sign, sha256Hex, safeEq, SHA256_EMPTY, SKEW_SEC, NONCE_TTL_SEC };
