// WebChat HTTPS Proxy - routes both Gateway and Voice Server
// Access at: https://120.26.114.222:4433/webchat/
const https = require('https');
const http = require('http');
const fs = require('fs');

const SSL_PORT = 4433;
const GATEWAY_PORT = 18789;
const VOICE_PORT = 18790;
const GATEWAY_HOST = '127.0.0.1';

const options = {
  key: fs.readFileSync('/etc/ssl/selfsigned/server.key'),
  cert: fs.readFileSync('/etc/ssl/selfsigned/server.crt')
};

function proxyRequest(req, res, targetHost, targetPort) {
  const proxyReq = http.request(
    { host: targetHost, port: targetPort, path: req.url, method: req.method, headers: req.headers },
    (proxyRes) => {
      res.writeHead(proxyRes.statusCode, proxyRes.headers);
      proxyRes.pipe(res);
    }
  );
  proxyReq.on('error', (err) => {
    console.error(`[Proxy Error] ${req.url}: ${err.message}`);
    res.writeHead(502, { 'Content-Type': 'text/plain' });
    res.end(`Proxy error: ${err.message}`);
  });
  req.pipe(proxyReq);
}

const server = https.createServer(options, (req, res) => {
  const url = req.url;
  
  // Route voice API requests to the voice server
  if (url.startsWith('/api/voice-input') || url.startsWith('/api/tts') || url.startsWith('/api/voice-audio/')) {
    proxyRequest(req, res, GATEWAY_HOST, VOICE_PORT);
  } else {
    proxyRequest(req, res, GATEWAY_HOST, GATEWAY_PORT);
  }
});

server.listen(SSL_PORT, () => {
  console.log(`[HTTPS Proxy] https://0.0.0.0:${SSL_PORT}`);
  console.log(`  /webchat/* -> http://${GATEWAY_HOST}:${GATEWAY_PORT}`);
  console.log(`  /api/voice-* -> http://${GATEWAY_HOST}:${VOICE_PORT}`);
});
