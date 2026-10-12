#!/usr/bin/env node
/**
 * 用法：
 *   node shadow-report.js [样本文件] [--since=HH:MM] [--top=60]
 *   ★ 默认读 **`/tmp/shadow.jsonl`**（影子模式另写的专用样本文件 ✓）
 *     —— ★ 为什么不用 /tmp/voice-server.log ✗：`deploy.sh` 每次都 `>` 清空它 ✓
 *        样本会被一次次清掉（"攒了一天，一部署全没了"✗）✓
 *     旧格式（服务日志里带 `SHADOW ` 前缀的行）也兼容 ✓ 传日志路径即可 ✓
 *
 * ★★ 两条纪律（否则数字会骗人 ✗）：
 *   ① 必须同时报 **限频丢弃数**（`drop`）✓ —— 样本被削过，指标就有系统性偏差 ✓
 *   ② 必须报 **样本数** ✓ —— 10 条样本算出来的"分歧率"没有意义 ✓
 */
const fs = require('fs');

const arg = (process.argv[2] && !process.argv[2].startsWith('--')) ? process.argv[2] : null;
const DEF = '/tmp/shadow.jsonl';
const LOG = arg || (fs.existsSync(DEF) ? DEF : '/tmp/voice-server.log');
let since = null, top = 60;
for (const a of process.argv.slice(2)) {
  if (a.startsWith('--since=')) since = a.split('=')[1];
  if (a.startsWith('--top=')) top = parseInt(a.split('=')[1], 10);
}
if (!fs.existsSync(LOG)) { console.error(`✗ 日志不存在: ${LOG}`); process.exit(1); }

// ★ 只认两种形状 ✓：专用文件里是**裸 JSON 一行** ✓；服务日志里是 `… SHADOW {json}` ✓
const rows = [];
let bad = 0, fails = 0, drops = 0, lines = 0;
for (const raw of fs.readFileSync(LOG, 'utf8').split('\n')) {
  lines++;
  // ★ 容忍 BOM ✗（本项目老坑：PS `Set-Content -Encoding UTF8` 会给文件加 BOM ✓
  //   带头 BOM 时 `startsWith('{')` 恒假 ⇒ 一条都解析不出来，而且**看起来像"没有样本"** ✓）
  const line = raw.replace(/^\uFEFF/, '').replace(/\r$/, '');
  let rest = null, iso = null;
  if (line.startsWith('{')) {                       // ← 专用样本文件 /tmp/shadow.jsonl ✓
    rest = line;
  } else {
    const i = line.indexOf('SHADOW ');
    if (i < 0) continue;
    rest = line.slice(i + 7);
    iso = (line.match(/\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})/) || [])[1] || null;
  }
  if (rest.startsWith('⚠️')) { const m = rest.match(/已丢 (\d+)/); if (m) drops = Math.max(drops, +m[1]); continue; }
  if (rest.startsWith('✗')) { fails++; continue; }
  let j;
  try { j = JSON.parse(rest); } catch (e) { bad++; continue; }
  if (!j || !j.old || !j.new) { bad++; continue; }
  if (since) {                                      // ★ 只看 HH:MM 之后 ✓（jsonl 用记录自己的 t ✓）
    const t = iso || String(j.t || '').slice(11, 16);
    if (t && t < since) continue;
  }
  rows.push(j);
  if (typeof j.drop === 'number') drops = Math.max(drops, j.drop);
}

const KB = 'search_kb';
const kindCn = { command: '命令', kb: '知识库问答', self_qa: '人设自答' };
const esc = s => String(s == null ? '' : s).replace(/\s+/g, ' ').slice(0, 72);

console.log('══════ 影子模式报告（CR-20261012-03 §9）══════');
console.log(`  日志        ${LOG}（共 ${lines.toLocaleString()} 行）`);
console.log(`  样本        ${rows.length} 条` + (since ? `（--since=${since}）` : ''));
console.log(`  ★ 限频丢弃  ${drops} 条` + (drops > 0 ? '  ⚠️ 样本被削过 ⇒ 下面比例有系统性偏差，别当精确值 ✓' : ' ✓'));
console.log(`  解析失败    ${bad} 条 ／ 判定失败 ${fails} 条`);
if (!rows.length) { console.log('\n  （还没有影子样本 ⇒ 等有真实语音走过再来 ✓）'); process.exit(0); }

const n = rows.length;
const div = rows.filter(r => r.diverge);
const danger = rows.filter(r => r.dangerous);
const oldKb = rows.filter(r => r.old.kind === 'kb');
const missed = rows.filter(r => r.missedKb);
const falseKb = rows.filter(r => r.old.kind !== 'kb' && r.new.tool === KB);
const pct = (a, b) => b ? (a * 100 / b).toFixed(1) + '%' : '—';

console.log('\n── 分布（现有路径判成了什么）──');
for (const k of ['command', 'kb', 'self_qa']) {
  const c = rows.filter(r => r.old.kind === k).length;
  console.log(`  ${(kindCn[k] || k).padEnd(6)} ${String(c).padStart(5)}  ${pct(c, n)}`);
}
const tools = {};
for (const r of rows) { const t = r.new.tool || '(不调工具/直接回话)'; tools[t] = (tools[t] || 0) + 1; }
console.log('\n── 模型选了哪些工具 ──');
for (const [t, c] of Object.entries(tools).sort((a, b) => b[1] - a[1])) {
  console.log(`  ${t.padEnd(22)} ${String(c).padStart(5)}  ${pct(c, n)}`);
}

console.log('\n────────── 四个指标 ──────────');
console.log(`  ① 分歧率        ${pct(div.length, n)}   （${div.length}/${n}）`);
console.log(`  ② ★危险分歧     ${pct(danger.length, n)}   （${danger.length}/${n}）★ 涉及写操作，逐条人工看 ✓`);
console.log(`  ③ ★漏检索率     ${pct(missed.length, oldKb.length)}   （${missed.length}/${oldKb.length}）★ 本 CR 最担心的新失败模式 ✓`);
console.log(`  ④ 误检索率      ${pct(falseKb.length, n - oldKb.length)}   （${falseKb.length}/${n - oldKb.length}）`);

const show = (title, arr) => {
  if (!arr.length) { console.log(`\n── ${title}：无 ✓ ──`); return; }
  console.log(`\n── ${title}（前 ${Math.min(top, arr.length)} 条）──`);
  for (const r of arr.slice(0, top)) {
    console.log(`  「${esc(r.asr)}」`);
    console.log(`      旧: ${r.old.kind}${r.old.action ? ' ' + r.old.action : ''}${r.old.kind === 'kb' ? ' reliable=' + (r.old.reliable ? 'Y' : 'N') : ''}`);
    console.log(`      新: ${r.new.tool ? r.new.tool + ' ' + JSON.stringify(r.new.args || {}) : '直接回话「' + esc(r.new.text) + '」'}`);
  }
};
show('★★ 危险分歧（涉及写操作 ⇒ 优先人工看）', danger);
show('★ 漏检索（旧=知识库问答，模型没调 search_kb）', missed);
const plain = div.filter(r => !r.dangerous && !r.missedKb);
show('其余分歧', plain);

console.log(`\n★ 判据（本 CR §9.1）：① 样本要够（≥几百条）；② 危险分歧必须为 0 或逐条解释清楚；`);
console.log(`  ③ 漏检索率先量再定阈值 ✓；④ 有丢弃数时别把比例当精确值 ✓`);
