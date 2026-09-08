// Combined HTTP proxy for localtunnel
// Listens on one port, routes to Gateway and Voice Server
const http = require('http');
const fs = require('fs');
const path = require('path');

const PROXY_PORT = 18888;
const GATEWAY_PORT = 18789;
const VOICE_PORT = 18790;

// 可公开访问的工作区文件目录（只读）
const WORKSPACE_DIR = '/root/.openclaw/陈盛工作区';
const SAFE_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.pdf', '.txt', '.md'];

function serveStaticFile(req, res) {
  // 防止路径穿越
  const requestedPath = path.normalize(decodeURIComponent(req.url.replace('/workspace/', '')));
  const fullPath = path.resolve(WORKSPACE_DIR, requestedPath);
  if (!fullPath.startsWith(WORKSPACE_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('Forbidden');
    return;
  }
  // 仅允许安全扩展名
  const ext = path.extname(fullPath).toLowerCase();
  if (!SAFE_EXTENSIONS.includes(ext)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('File type not allowed');
    return;
  }
  fs.stat(fullPath, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
      return;
    }
    const mimeTypes = {
      '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
      '.gif': 'image/gif', '.webp': 'image/webp', '.pdf': 'application/pdf',
      '.txt': 'text/plain; charset=utf-8', '.md': 'text/markdown; charset=utf-8',
    };
    // 非图片/PDF → 下载；图片/PDF → 浏览器直接打开
    const isDownload = !['.png', '.jpg', '.jpeg', '.gif', '.webp', '.pdf'].includes(ext);
    const headers = {
      'Content-Type': mimeTypes[ext] || 'application/octet-stream',
      'Cache-Control': 'public, max-age=300',
    };
    if (isDownload) {
      headers['Content-Disposition'] = `attachment; filename="${encodeURIComponent(path.basename(fullPath))}"`;
    }
    res.writeHead(200, headers);
    fs.createReadStream(fullPath).pipe(res);
  });
}

function proxyRequest(req, res, targetPort) {
  const proxyReq = http.request(
    { host: '127.0.0.1', port: targetPort, path: req.url, method: req.method, headers: { ...req.headers, host: '127.0.0.1:' + targetPort } },
    (proxyRes) => {
      const headers = { ...proxyRes.headers };
      delete headers['transfer-encoding'];
      res.writeHead(proxyRes.statusCode, headers);
      proxyRes.pipe(res);
    }
  );
  proxyReq.on('error', (err) => {
    console.error(`[Proxy Error] ${req.url} → :${targetPort}: ${err.message}`);
    res.writeHead(502, { 'Content-Type': 'text/plain' });
    res.end(`Proxy error: ${err.message}`);
  });
  req.pipe(proxyReq);
}

// 桌面兄弟(laptop) AI 对话接口
function handleBrotherMessage(req, res) {
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    try {
      const body = JSON.parse(Buffer.concat(chunks).toString());
      const messages = body.messages || [];
      if (messages.length === 0) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'empty messages' }));
        return;
      }
      
      // 如果有 system 消息，添加到 messages 开头
      if (body.system) {
        messages.unshift({ role: 'system', content: body.system });
      }
      
      // 读取 Gateway auth token
      let gwToken = '';
      try {
        const cfg = JSON.parse(fs.readFileSync('/root/.openclaw/openclaw.json', 'utf-8'));
        gwToken = cfg.gateway && cfg.gateway.auth && cfg.gateway.auth.token;
      } catch(e) {}
      
      // 调用 Gateway chat completions
      const bodyStr = JSON.stringify({
        model: 'deepseek/deepseek-v4-flash',
        messages: messages
      });
      
      const opts = {
        host: '127.0.0.1',
        port: GATEWAY_PORT,
        path: '/v1/chat/completions',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(bodyStr),
          'Authorization': gwToken ? `Bearer ${gwToken}` : ''
        }
      };
      
      const gwReq = http.request(opts, (gwRes) => {
        let data = '';
        gwRes.on('data', c => data += c);
        gwRes.on('end', () => {
          try {
            const result = JSON.parse(data);
            const reply = result.choices && result.choices[0] && result.choices[0].message && result.choices[0].message.content || '';
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ response: reply }));
          } catch(e) {
            console.error(`[brother-message] parse error: ${e.message}`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ response: data }));
          }
        });
      });
      gwReq.on('error', (err) => {
        console.error(`[brother-message] gw error: ${err.message}`);
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      });
      gwReq.write(bodyStr);
      gwReq.end();
    } catch(e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  });
}

const server = http.createServer((req, res) => {
  const url = req.url;
  
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-openclaw-session-key');
  
  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }
  
  if (url.startsWith('/workspace/')) {
    serveStaticFile(req, res);
  } else if (url === '/api/brother-message' && req.method === 'POST') {
    handleBrotherMessage(req, res);
  } else if (url === '/api/tts-pending') {
    // 跳播轮询:目前没有待推消息
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ text: '' }));
  } else if (url === '/api/laptop-brother-log') {
    // 从笔记本拉取桌面兄弟日志
    const spawn = require('child_process').spawn;
    const ssh = spawn('ssh', ['-o', 'ConnectTimeout=5', '-o', 'StrictHostKeyChecking=no', '1@100.105.216.41', 'type C:\\temp\\lpt-brother.log']);
    let data = '';
    ssh.stdout.on('data', c => data += c);
    ssh.stderr.on('data', c => {});
    ssh.on('close', (code) => {
      if (code === 0 && data) {
        // 取最近100行
        const lines = data.split('\n');
        const recent = lines.slice(-100).join('\n');
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end(recent);
      } else {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('[日志获取失败，笔记本电脑可能未在线]\n');
      }
    });
  } else if (url === '/laptop-brother') {
    // 笔记本桌面兄弟日志页面（自动刷新，实时显示）
    const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🦞 笔记本桌面兄弟日志</title>
<style>
  body { background:#1a1d23; color:#c8d3da; font-family: monospace; font-size:13px; margin:0; padding:16px; }
  pre { white-space: pre-wrap; word-break: break-all; margin:0; }
  .header { display:flex; justify-content:space-between; align-items:center; margin-bottom:12px; }
  h1 { font-size:16px; margin:0; color:#8bd3ff; }
  .status { color:#8f9; font-size:12px; }
  .loading { color:#888; }
  .new { animation: blink 0.5s; }
  @keyframes blink { 0%{background:#3a4f5c} 100%{background:transparent} }
</style>
</head>
<body>
<div class="header">
  <h1>🦞 笔记本桌面兄弟 — 实时日志</h1>
  <span class="status" id="status">加载中...</span>
</div>
<pre id="log">正在获取日志...</pre>
<script>
let lastLen = 0;
async function fetchLog() {
  try {
    const r = await fetch('/api/laptop-brother-log');
    const text = await r.text();
    document.getElementById('log').textContent = text;
    document.getElementById('status').textContent = '✅ ' + new Date().toLocaleTimeString() + ' 更新';
    document.getElementById('status').className = 'status new';
    setTimeout(() => document.getElementById('status').className = 'status', 1000);
  } catch(e) {
    document.getElementById('status').textContent = '❌ 获取失败';
  }
}
fetchLog();
setInterval(fetchLog, 3000);
</script>
</body>
</html>`;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
  } else if (url.startsWith('/api/voice-input') || url.startsWith('/api/tts') || url.startsWith('/api/voice-audio/')) {
    proxyRequest(req, res, VOICE_PORT);
  } else {
    proxyRequest(req, res, GATEWAY_PORT);
  }
});

server.listen(PROXY_PORT, () => {
  console.log(`[Combined HTTP Proxy] http://0.0.0.0:${PROXY_PORT}`);
  console.log(`  /*                       -> :${GATEWAY_PORT}`);
  console.log(`  /api/voice-*             -> :${VOICE_PORT}`);
  console.log(`  /workspace/*             -> ${WORKSPACE_DIR}`);
});
