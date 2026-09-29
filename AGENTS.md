# s1-services — S1 语音交互服务

> **上级**：`../CLAUDE.md`（工作区地图）｜**远端**：`https://github.com/cs111129/s1-services.git`
> **服务器**：S1 网关 `120.26.114.222`

## 这是什么

S1 网关服务器上的语音交互服务代码：`voice-server` + `cam_analyze`（实操考核采集分析）+ `asr`。
与 xydck 的区别：**xydck 是学员端业务系统（S2），s1-services 是 S1 上的语音/采集侧服务**。

## 关键命令

```bash
# S1 部署（语音交互服务，git 化后走 SSH 协议）
ssh root@120.26.114.222 "cd /root/.openclaw/陈盛工作区/项目/语音交互 && ./deploy.sh"
```

## 对外暴露的端点（★ 加鉴权/关端口前先看这里）

`voice-server.js`（`0.0.0.0:18790`，与 `/api/cam/*` **同一个进程**）对外有这几组：

| 端点 | 现状 | 消费者（★ 别只看一个） |
|:--|:--|:--|
| `POST /api/voice-input` | **无鉴权**，已开公网路由（经 `q3i.cn`） | 赋能02（ESP32）· **WebChat 前端 `voice-input.js`** · `openclaw-webchat-channel/dist/web/voice-input.js` · `combined-proxy.js`/`https-proxy.js` |
| `POST /api/tts` | 同上 | 同上 |
| `GET /api/voice-audio/<file>` | 同上 | 同上 |
| `/api/cam/*` | 设备密钥 `X-Device-Secret` | 赋能1号视觉板 · S2（`student-data.js` + `auth-service.js`） |

★★ **两个语音端点【不能】加 `X-Device-Secret`** —— WebChat 那条是**浏览器直调**，放不了密钥 ✗
⇒ 保护手段是 **app 内限频（120 次/分/IP）+ 限 body（2MB）+ TTS 文本 ≤1000 字**
（见配置手册 **§121.4**）。要动之前先 grep 消费者：
`grep -rn "voice-input\|/api/tts\|voice-audio" /root /var/www --include='*.js' --include='*.html'`

★★ **限频取 IP 必须用 `X-Real-IP`，不能用 `X-Forwarded-For` 的第一个** ✗
nginx 的 `$proxy_add_x_forwarded_for` 是**追加**语义 ⇒ 客户端能把自己的假 IP 排到最前面 ⇒
**限频可被一个请求头绕过**（2026-09-29 实测：轮换假 XFF 连打 130 次 **0 拦截** ✗；改 `X-Real-IP` 后第 **121** 次 429 ✓）
- 谁在用这套限频：`voiceClientIp()` / `voiceRateLimited()`（`VOICE_RATE_MAX` / `VOICE_MAX_BODY` 可用环境变量覆盖）
- ★ 门店 NAT 后设备 + 多台手机会**共用一个公网 IP** ⇒ 限额别调太紧（40 就会出假 429，现为 120）

## nginx 相关（S1 上的 cam/voice 路由）

- S1 的 nginx snippet 目录：`/etc/nginx/snippets/` — `cam-device-api.conf`（cam 白名单）、
  `voice-api.conf`（语音三条，2026-09-29 新增）
- 挂载点：`/etc/nginx/sites-enabled/q3i.cn`（**只有 443 块**挂了语音；80 块只做 301）
- ★★ **备份绝不能落在 `/etc/nginx/sites-enabled/` 里** —— nginx 是 `include sites-enabled/*;`，
  `.bak` **会被当配置加载** ⇒ 同名 `server_name` 重复。
  **危险点不是警告，是"先加载的生效"** ⇒ 可能**静默用上旧配置**（新路由配了却没生效，而 `nginx -t` 是绿的）✗
  ⇒ 备份放 **`/root/nginx-sites-backup/`**（2026-09-29 踩过：警告 4 条 → 挪走后 0 条）
- ⚠️ S1 端口/防火墙相关改动前先看 `../xydck/AGENTS.md` 的 `## 部署检查清单` ——
  **关端口前必须把"谁在用它"grep 全**（S2 曾写死公网 IP 直连，掐断过整条链路）。

## 改 `voice-server.js` 的两个已知雷区

1. **取音频 `GET /api/voice-audio/:filename`** —— 必须 `切查询串 → decodeURIComponent → path.basename`
   **三步全做**（2026-09-29 修的目录穿越，曾是 200 可读 `/etc/shadow` ✗）。
   ★ 只加 `path.basename` 会引入新 bug：`x.mp3?token=abc` 会被整个当文件名 ⇒ **正常取音频反而 404** ✗
2. **验证穿越必须 `curl --path-as-is` 且把 `..` 层数给够** —— `AUDIO_DIR` 有 **6 段** ⇒ 回到 `/` 需 **6 层**；
   不带 `--path-as-is` 时 curl 会自己规范化掉，**永远测不出洞** ✗
   ★ 一定要配**阳性对照**（同目录真实文件取到 200），否则"没测出来"和"没有洞"分不清

## 指针

- S2/S1 服务器与端口全表：`../xydck/AGENTS.md`
- 语音通路上线与踩坑全文：配置手册 **§121** · `高频踩坑速查.md`（四条）· `97-项目部署问题清单.md`
