#!/usr/bin/env node
/**
 * verify-auth-hmac.js —— 用**对方（设备端）公布的测试向量**交叉验证 S1 的签名实现
 *
 * 为什么必须有它 ✗：签名是"两端逐字节一致"的协议，差一个字符就全 401。
 *   单测自己跟自己一致没有意义 —— 必须拿**设备端产出的已知向量**比。
 *   向量来源：`协同/specs/CR-20261103-02-设备端接口签名HMAC.md` §5（Python/Node 各算一遍，值一致）
 *
 * 用法：node scripts/verify-auth-hmac.js
 */
'use strict';

const crypto = require('crypto');
const path = require('path');
const auth = require(path.join(__dirname, '..', 'auth-hmac.js'));

let pass = 0, fail = 0;
function ok(name, cond, extra) {
  if (cond) { pass++; console.log(`  ✓ ${name}`); }
  else { fail++; console.log(`  ✗ ${name}${extra ? '  → ' + extra : ''}`); }
}

/* ══════════ 1. 与 CR §5 公布的向量逐字节对齐 ══════════ */
const V = {
  secret: 'dev-secret-fn0009',
  method: 'POST',
  url: '/api/cam/status',
  deviceId: 'FN-0009',
  ts: '1793600000',
  nonce: '0123456789abcdef',
  bodyHash: '0faf458de42e8dce271de05b7d93ab296c2f6e83df364007c4bdc24a11cc7e0c',
  expectSign: '3e6e5af03e5b5ad0294add5144506671b03b17ba0a0ac9c5c6a2998157c57ec6',
};

console.log('[1] 与设备端公布的向量对齐');
const canon = auth.canonicalString(V.method, V.url, V.deviceId, V.ts, V.nonce, V.bodyHash);
const expectCanon = [
  'POST', '/api/cam/status', 'FN-0009', '1793600000', '0123456789abcdef', V.bodyHash,
].join('\n');
ok('签名串 6 行、换行/顺序一致', canon === expectCanon, JSON.stringify(canon));
ok('HMAC-SHA256 结果与设备端一致', auth.sign(V.secret, canon) === V.expectSign, auth.sign(V.secret, canon));

/* ══════════ 2. verifyDeviceAuth 行为（正反两面） ══════════ */
console.log('\n[2] 校验函数行为');
const SECRETS = { 'FN-0009': V.secret };
const LEGACY = 'lingmu-cam-2026-secret';

function makeReq(over) {
  const o = over || {};
  return { method: o.method || 'POST', url: o.url || '/api/cam/status', headers: o.headers || {} };
}
function signedHeaders(body, opts) {
  const o = opts || {};
  const ts = o.ts || String(Math.floor(Date.now() / 1000));
  const nonce = o.nonce || crypto.randomBytes(8).toString('hex');
  const bodyHash = crypto.createHash('sha256').update(body || Buffer.alloc(0)).digest('hex');
  const url = o.url || '/api/cam/status';
  const method = o.method || 'POST';
  const c = auth.canonicalString(method, url, 'FN-0009', ts, nonce, bodyHash);
  return {
    'x-device-id': 'FN-0009', 'x-device-ts': ts, 'x-device-nonce': nonce,
    'x-device-sign': auth.sign(SECRETS['FN-0009'], c),
  };
}

const body = Buffer.from('{"device_id":"FN-0009","status":"idle"}');
const opt = { secretOf: id => SECRETS[id], allowLegacy: true, legacySecret: LEGACY };

let r = auth.verifyDeviceAuth(makeReq({ headers: signedHeaders(body) }), body, opt);
ok('正确签名 → ok（mode=sig，identity=true：唯一可当身份的一种）', r.ok && r.mode === 'sig' && r.identity === true, JSON.stringify(r));

r = auth.verifyDeviceAuth(makeReq({ headers: signedHeaders(body) }), Buffer.from('{"device_id":"FN-0009","status":"busy"}'), opt);
ok('改了 body → 401 签名不匹配', !r.ok && /签名不匹配/.test(r.reason), r.reason);

const h2 = signedHeaders(body);
r = auth.verifyDeviceAuth(makeReq({ headers: h2 }), body, opt);
ok('第一次用该 nonce → ok', r.ok, r.reason);
r = auth.verifyDeviceAuth(makeReq({ headers: h2 }), body, opt);
ok('同一 nonce 再来 → 401 重放', !r.ok && /nonce 重复/.test(r.reason), r.reason);

r = auth.verifyDeviceAuth(makeReq({ headers: signedHeaders(body, { ts: String(Math.floor(Date.now() / 1000) - 400) }) }), body, opt);
ok('ts 超出 ±300s → 401', !r.ok && /ts 过期/.test(r.reason), r.reason);

r = auth.verifyDeviceAuth(makeReq({ headers: signedHeaders(body, { nonce: 'ABCDEF0123456789' }) }), body, opt);
ok('nonce 非小写 hex → 401 格式', !r.ok && /nonce 格式/.test(r.reason), r.reason);

{ // 未知设备
  const h = signedHeaders(body);
  h['x-device-id'] = 'FN-9999';
  const c2 = auth.canonicalString('POST', '/api/cam/status', 'FN-9999', h['x-device-ts'], h['x-device-nonce'],
    crypto.createHash('sha256').update(body).digest('hex'));
  h['x-device-sign'] = auth.sign(SECRETS['FN-0009'], c2);
  r = auth.verifyDeviceAuth(makeReq({ headers: h }), body, opt);
  ok('未知设备 → 401', !r.ok && /未知设备/.test(r.reason), r.reason);
}

console.log('\n[3] legacy 兼容（老固件）· ★★ 只能放行、不能给身份（CR §14.2）');
// ★ 设备端 2026-11-13 明确：legacy / legacy_no_ts 的 identity 必须是 false
//   判据："验得过" ≠ "有资格当身份" —— legacy 是明文密钥头（无签名/无完整性），只有 sig 能当身份
r = auth.verifyDeviceAuth(makeReq({ headers: { 'x-device-id': 'FN-0009', 'x-device-secret': SECRETS['FN-0009'] } }), null, opt);
ok('自己的密钥（明文头、无 ts）⇒ ok/mode=legacy_no_ts 且 identity=false',
  r.ok && r.mode === 'legacy_no_ts' && r.identity === false, JSON.stringify(r));

{
  const ts = String(Math.floor(Date.now() / 1000));
  r = auth.verifyDeviceAuth(makeReq({ headers: { 'x-device-id': 'FN-0009', 'x-device-secret': SECRETS['FN-0009'], 'x-device-ts': ts } }), null, opt);
  ok('自己的密钥 + 新鲜 ts ⇒ mode=legacy 且 identity=false', r.ok && r.mode === 'legacy' && r.identity === false, JSON.stringify(r));
  r = auth.verifyDeviceAuth(makeReq({ headers: { 'x-device-id': 'FN-0009', 'x-device-secret': SECRETS['FN-0009'], 'x-device-ts': String(Number(ts) - 400) } }), null, opt);
  ok('legacy 带过期 ts ⇒ 401（挡"剥掉签名头再重放"）', !r.ok && /ts 过期/.test(r.reason), r.reason);
}

r = auth.verifyDeviceAuth(makeReq({ headers: { 'x-device-id': 'FN-0009', 'x-device-secret': LEGACY } }), null, opt);
ok('已知设备用公共兜底密钥 ⇒ mode=fallback、identity=false',
  r.ok && r.mode === 'fallback' && r.identity === false, JSON.stringify(r));

r = auth.verifyDeviceAuth(makeReq({ headers: { 'x-device-id': 'FN-7777', 'x-device-secret': LEGACY } }), null, opt);
ok('未知设备 + 公共兜底 ⇒ mode=missing_key、identity=false（能自救，不是硬 401）',
  r.ok && r.mode === 'missing_key' && r.identity === false, JSON.stringify(r));

r = auth.verifyDeviceAuth(makeReq({ headers: { 'x-device-id': 'FN-0009', 'x-device-secret': LEGACY } }), null,
  { secretOf: id => SECRETS[id], allowLegacy: false, legacySecret: LEGACY });
ok('allowLegacy=false 时老固件 ⇒ 401', !r.ok && /不接受 legacy/.test(r.reason), r.reason);

r = auth.verifyDeviceAuth(makeReq({ headers: { 'x-device-id': 'FN-0009', 'x-device-secret': 'wrong' } }), null, opt);
ok('错的 legacy 密钥 ⇒ 401', !r.ok && /legacy 密钥不匹配/.test(r.reason), r.reason);

// ★ legacyPaths 白名单（P3 时只留升级/领密钥两条救回通道）
{
  const step = { secretOf: id => SECRETS[id], legacySecret: LEGACY, legacyPaths: ['/api/cam/ota', '/api/cam/device/secret'] };
  r = auth.verifyDeviceAuth(makeReq({ url: '/api/cam/status', headers: { 'x-device-id': 'FN-0009', 'x-device-secret': LEGACY } }), null, step);
  ok('白名单外的 path ⇒ 401（不再全放）', !r.ok && /不接受 legacy/.test(r.reason), r.reason);
  r = auth.verifyDeviceAuth(makeReq({ url: '/api/cam/ota', headers: { 'x-device-id': 'FN-0009', 'x-device-secret': LEGACY } }), null, step);
  ok('白名单内的 path ⇒ 放行（老固件救回通道）', r.ok, JSON.stringify(r));
}

console.log('\n[4] GET 无 body（空串 hash）');
const emptyHash = crypto.createHash('sha256').update(Buffer.alloc(0)).digest('hex');
ok('空 body 的 sha256 = 官方常量', emptyHash === auth.SHA256_EMPTY, emptyHash);
{
  const ts = String(Math.floor(Date.now() / 1000)), nonce = crypto.randomBytes(8).toString('hex');
  const c = auth.canonicalString('GET', '/api/cam/cmd?device_id=FN-0009', 'FN-0009', ts, nonce, emptyHash);
  const h = { 'x-device-id': 'FN-0009', 'x-device-ts': ts, 'x-device-nonce': nonce, 'x-device-sign': auth.sign(SECRETS['FN-0009'], c) };
  r = auth.verifyDeviceAuth(makeReq({ method: 'GET', url: '/api/cam/cmd?device_id=FN-0009', headers: h }), null, opt);
  ok('GET + query 参与签名 → ok', r.ok, r.reason);
}

console.log('\n[5] 未知设备救命通道 + dryRun（红队 P0-3 / 统计层语义）');
{
  // 未知设备"时间已同步 ⇒ 一定带签名头"，但服务端没有它的密钥 ⇒ 不能硬 401（否则上不了云、OTA 也查不了）
  //   只要同时带了合法 legacy 密钥 ⇒ 放行并标 missing_key（救回通道，identity=false）
  const ts = String(Math.floor(Date.now() / 1000)), nonce = crypto.randomBytes(8).toString('hex');
  const c = auth.canonicalString('GET', '/api/cam/ota', 'FN-8888', ts, nonce, auth.SHA256_EMPTY);
  const h = {
    'x-device-id': 'FN-8888', 'x-device-ts': ts, 'x-device-nonce': nonce,
    'x-device-sign': auth.sign('whatever', c), 'x-device-secret': LEGACY,
  };
  r = auth.verifyDeviceAuth(makeReq({ method: 'GET', url: '/api/cam/ota', headers: h }), null, opt);
  ok('未知设备 + 签名头 + 合法 legacy ⇒ missing_key 放行、identity=false',
    r.ok && r.mode === 'missing_key' && r.identity === false, JSON.stringify(r));
}
{
  // dryRun 不消耗 nonce：统计层对同一请求判两次，第二次不能自己把自己挡掉
  const h = signedHeaders(body);
  const o1 = auth.verifyDeviceAuth(makeReq({ headers: h }), body, Object.assign({}, opt, { dryRun: true }));
  const o2 = auth.verifyDeviceAuth(makeReq({ headers: h }), body, Object.assign({}, opt, { dryRun: true }));
  ok('dryRun 连判两次都 ok（不消耗 nonce）', o1.ok && o2.ok, JSON.stringify([o1.ok, o2.ok]));
  const o3 = auth.verifyDeviceAuth(makeReq({ headers: h }), body, opt);
  const o4 = auth.verifyDeviceAuth(makeReq({ headers: h }), body, opt);
  ok('真跑：第一次 ok / 第二次 nonce 重复', o3.ok && !o4.ok && /nonce 重复/.test(o4.reason), o4.reason || '');
}

console.log(`\n结果：通过 ${pass} 项，失败 ${fail} 项`);
process.exit(fail ? 1 : 0);
