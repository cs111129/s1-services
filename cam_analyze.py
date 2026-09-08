#!/usr/bin/env python3
"""
cam_analyze.py - 视觉采集板云端分析脚本

输入：batch 目录路径（cam-uploads/<batch>/，内含 snap_001..N.jpg + rec.wav）
流程：
  1. 读目录下所有 snap_*.jpg（按文件名排序保证时序）+ rec.wav
  2. 图片帧（≤60 帧，抽帧降本）→ 通义千问 Qwen-VL-Plus 视觉时序分析（阿里云 DashScope）
  3. rec.wav → 通义千问 paraformer ASR 转文字（复用同目录 asr_recognition.py）
  4. 图片分析 + 录音文字 → DeepSeek 综合分析（一段中文结论）
  5. 结果写 SQLite（cam-analysis.db / cam_analysis 表）+ 打印 JSON

⚠️ 模型变更（2026-09-03，见 docs/24）：图片分析从 deepseek-v4-flash-vision-exp 改为
   qwen-vl-plus（DashScope，非推理型、与 ASR 同平台、直接出 content），规避推理型 max_tokens 坑。

用法：python3 cam_analyze.py <batch目录路径>
退出码：0=成功，1=失败（stderr 打印原因）
"""
import sys
import os
import requests
import json
import base64
import sqlite3
import subprocess
import time
from pathlib import Path

# ============ 配置 ============
# 图片视觉分析：通义千问 Qwen-VL-Plus（阿里云 DashScope 兼容 OpenAI 接口，非推理型）
VISION_MODEL = "qwen-vl-plus"
VISION_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
# 综合分析：DeepSeek 文本模型（不变）
DEEPSEEK_CHAT = "deepseek-chat"
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
# AI 分析超时（qwen-vl-plus 处理多帧较久，给足）
VISION_TIMEOUT = 180
CHAT_TIMEOUT = 120
# 抽帧上限：超过该帧数则等间隔抽帧，控制 token 成本/耗时（60 帧抽 30 帧）
MAX_VISION_FRAMES = 30

# 本脚本与 voice-server.js / asr_recognition.py 同目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ASR_SCRIPT = os.path.join(SCRIPT_DIR, "asr_recognition.py")
DB_PATH = os.path.join(SCRIPT_DIR, "cam-analysis.db")

# ============ S2 对接（兄弟 27 号） ============
# S2 db-api 地址 + ingest / current-binding 端点
# 兄弟 29 号给了正式值：S2_DBAPI=http://8.136.146.89:18903, token=cam-ingest-token-2026
S2_DBAPI = os.environ.get("S2_DBAPI", "http://8.136.146.89:18903")
INGEST_PATH = "/api/cam-records/ingest"
BINDING_PATH = "/api/cam-devices/current-binding"
# CAM_INGEST_TOKEN：兄弟给的默认值；正式期可用环境变量覆盖。没有则跳过推送（仅本地落库）
CAM_INGEST_TOKEN = os.environ.get("CAM_INGEST_TOKEN", "cam-ingest-token-2026")
# 参数配置化：cam-config 端点（兄弟 S2 已做，S1 分析前拉取提示词）
CAM_CONFIG_PATH = "/api/cam-config"

# 图片视觉时序分析 prompt（按时间顺序，每张间隔约 5 秒）
VISION_PROMPT = (
    "以下是视觉采集设备按时间顺序连续拍摄的一组画面（共 {n} 张，相邻约 5 秒）。"
    "请按时间顺序分析这段时间内的场景变化：是否有人员活动/经过、物体移动、"
    "光照或环境状态变化等。用中文简洁分点描述，突出时序上的变化点；"
    "若画面基本无变化也请说明。"
)

# 综合分析 prompt
SUMMARY_PROMPT = (
    "视觉采集设备在某个时段记录到以下信息：\n"
    "【画面时序分析】\n{vision}\n"
    "【现场录音转写】\n{audio}\n\n"
    "请综合分析这段现场发生了什么，给出一段简洁的中文结论（2~4 句）。"
    "若录音无有效内容，则只基于画面分析结论。"
)

# SOP 比对 prompt（默认值，33号给；S2 cam-config.json 可配置 sopComparePrompt 覆盖）
SOP_COMPARE_PROMPT = (
    "你是酒店服务标准考核员。员工演示了「{sop_name}」流程，以下是录像分析结果：\n"
    "【画面时序分析】{vision}\n"
    "【现场录音转写】{audio}\n\n"
    "该流程的 SOP 标准如下：\n{sop_content}\n\n"
    "请逐条比对员工演示与 SOP 标准，输出：\n"
    "1. 符合项：哪些动作/步骤/话术做到了\n"
    "2. 缺失/不规范项：哪些没做到或做错了\n"
    "3. 话术评估：语言表达是否达标\n"
    "4. 综合评分（0~100）与一句结论"
)

# 参数配置化：提示词全局变量，实际值从 S2 /api/cam-config 拉取（拉不到用上面默认）
vision_prompt = VISION_PROMPT
summary_prompt = SUMMARY_PROMPT
sop_compare_prompt = SOP_COMPARE_PROMPT
SOP_PATH = "/api/cam-sop"
# 34号问题4：DeepSeek 文本模型输出上限 + 失败重试次数（S2 cam-config 可配置，默认兜底）
max_tokens = 2000
retry_count = 1


def fetch_cam_config():
    """从 S2 拉取实操考核配置（提示词/参数）。失败用默认值，不阻塞分析。
    参数配置化（30/31 号）：后台配置提示词 + interval_ms/max_seconds，分析前拉取提示词。
    34号问题4：新增 maxTokens（DeepSeek 输出上限）+ retryCount（失败重试次数）。"""
    global vision_prompt, summary_prompt, sop_compare_prompt, max_tokens, retry_count
    try:
        if not CAM_INGEST_TOKEN or not S2_DBAPI:
            return
        r = requests.get(f"{S2_DBAPI}{CAM_CONFIG_PATH}",
                         headers={"Authorization": f"Bearer {CAM_INGEST_TOKEN}"},
                         timeout=10)
        if r.status_code == 200:
            c = r.json()
            if c.get("visionPrompt"):
                vision_prompt = c["visionPrompt"]
            if c.get("summaryPrompt"):
                summary_prompt = c["summaryPrompt"]
            if c.get("sopComparePrompt"):
                sop_compare_prompt = c["sopComparePrompt"]
            # 34号问题4：读 maxTokens / retryCount
            if c.get("maxTokens"):
                max_tokens = int(c["maxTokens"])
            if c.get("retryCount") is not None:
                retry_count = int(c["retryCount"])
            log(f"已拉取 cam-config：visionPrompt={len(vision_prompt)}字, summaryPrompt={len(summary_prompt)}字, "
                f"sopComparePrompt={len(sop_compare_prompt)}字, maxTokens={max_tokens}, retryCount={retry_count}, "
                f"intervalMs={c.get('intervalMs')}, maxSeconds={c.get('maxSeconds')}")
        else:
            log(f"cam-config 拉取失败({r.status_code})，用默认提示词")
    except Exception as e:
        log(f"cam-config 拉取异常: {e}，用默认提示词")


def fetch_sop(sop_id):
    """从 S2 拉取 SOP 标准内容（33号 /api/cam-sop）。失败或空返回 None。"""
    if not sop_id or not CAM_INGEST_TOKEN or not S2_DBAPI:
        return None
    try:
        r = requests.get(f"{S2_DBAPI}{SOP_PATH}", params={"id": sop_id},
                         headers={"Authorization": f"Bearer {CAM_INGEST_TOKEN}"}, timeout=10)
        if r.status_code == 200:
            d = r.json()
            if d.get("id"):
                log(f"已拉取 SOP: {d.get('name') or sop_id} (content {len(d.get('content',''))} 字)")
                return d
        else:
            log(f"拉取 SOP {sop_id} 失败({r.status_code})")
    except Exception as e:
        log(f"拉取 SOP {sop_id} 异常: {e}")
    return None


def log(msg):
    sys.stderr.write(f"[cam_analyze] {msg}\n")
    sys.stderr.flush()


def load_key(env_name):
    """从环境变量或 /root/.bashrc 读 API key（同 voice-server.js 逻辑）"""
    v = os.environ.get(env_name)
    if v:
        return v.strip()
    try:
        for line in open("/root/.bashrc", encoding="utf-8", errors="ignore"):
            if env_name in line and "=" in line:
                val = line.split("=", 1)[1].strip()
                val = val.strip('"').strip("'")
                if val:
                    return val
    except OSError:
        pass
    return ""


def http_json_post(url, key, payload, timeout=180):
    """用 requests POST JSON，返回解析后的 JSON 对象。
    34号问题4：失败/超时自动重试 retry_count 次（固定 1 秒间隔，KISS 不用指数退避）。
    call_chat / call_vision 都走这里，所以两者都获得重试。"""
    import requests
    last_err = None
    for attempt in range(retry_count + 1):
        try:
            r = requests.post(
                url,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
                timeout=timeout,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < retry_count:
                log(f"请求失败，重试 {attempt + 1}/{retry_count}: {e}")
                time.sleep(1)
    raise last_err


def call_vision(key, image_paths):
    """图片帧一次性给 Qwen-VL-Plus 做时序分析，返回文字。

    - 帧数 > MAX_VISION_FRAMES 时等间隔抽帧，控制 token 成本/耗时。
    - qwen-vl-plus 非推理型，直接出 content（规避 DeepSeek 推理型耗尽 max_tokens 的坑）。
    """
    # 抽帧降本：超过上限按等间隔取样，保留首尾帧
    if len(image_paths) > MAX_VISION_FRAMES:
        step = len(image_paths) // MAX_VISION_FRAMES
        sampled = image_paths[::step]
        # 确保包含最后一帧
        if sampled[-1] != image_paths[-1]:
            sampled.append(image_paths[-1])
        log(f"抽帧降本: {len(image_paths)} 帧 → {len(sampled)} 帧（等间隔）")
        image_paths = sampled

    content = [{"type": "text", "text": vision_prompt.format(n=len(image_paths))}]
    for p in image_paths:
        b64 = base64.b64encode(open(p, "rb").read()).decode()
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
        })
    payload = {
        "model": VISION_MODEL,
        "messages": [{"role": "user", "content": content}],
        "max_tokens": 1500,
        "temperature": 0.3,
    }
    log(f"调用 Qwen-VL-Plus 视觉模型，{len(image_paths)} 张图 ...")
    j = http_json_post(VISION_URL, key, payload, timeout=VISION_TIMEOUT)
    msg = j["choices"][0]["message"]
    text = (msg.get("content") or "").strip()
    if not text:
        # qwen-vl-plus 一般直接出 content；为空则用 reasoning_content 兜底（防御）
        text = (msg.get("reasoning_content") or "").strip()
    # 成本埋点：打印本次 token 用量（供 S2 记 API 费用）
    usage = j.get("usage") or {}
    log(f"视觉分析 token 用量: prompt={usage.get('prompt_tokens')}, "
        f"completion={usage.get('completion_tokens')}, total={usage.get('total_tokens')}")
    return text


def call_chat(key, prompt, max_tokens=500):
    """DeepSeek 文本模型综合分析"""
    payload = {
        "model": DEEPSEEK_CHAT,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }
    j = http_json_post(DEEPSEEK_URL, key, payload, timeout=CHAT_TIMEOUT)
    return j["choices"][0]["message"]["content"].strip()


def call_asr(wav_path):
    """复用 asr_recognition.py 转文字；失败/无内容返回空串。
    ASR 前先 ffmpeg 预处理：高通(去80Hz低频噪声) + afftdn降噪 + dynaudnorm动态归一化增益，
    缓解「轻声/远距离录音信号弱」导致的识别不准。"""
    if not os.path.exists(wav_path):
        return ""
    # 预处理：降噪 + 动态增益（失败则回退用原音频，不阻塞 ASR）
    proc_path = wav_path + ".proc.wav"
    try:
        p = subprocess.run(
            ["ffmpeg", "-y", "-i", wav_path,
             "-af", "highpass=f=80,afftdn=nf=-25,dynaudnorm=f=150:g=15",
             "-ac", "1", proc_path],
            capture_output=True, timeout=60,
        )
        asr_input = proc_path if (p.returncode == 0 and os.path.exists(proc_path)) else wav_path
    except Exception as e:
        log(f"ffmpeg 预处理失败({e})，直接用原音频")
        asr_input = wav_path
    try:
        r = subprocess.run(
            ["python3", ASR_SCRIPT, asr_input],
            capture_output=True, text=True, timeout=180,
        )
        out = r.stdout.strip()
        if not out:
            return ""
        return json.loads(out).get("text", "")
    except Exception as e:
        log(f"ASR 失败: {e}")
        return ""
    finally:
        if os.path.exists(proc_path):
            try:
                os.remove(proc_path)
            except OSError:
                pass


def get_binding(device_id):
    """查 S2 的 current-binding，拿设备当前绑定的学员快照。
    返回 dict（student_id/store/name/position），失败返回全 null。
    """
    if not device_id:
        return {"student_id": None, "store": None, "name": None, "position": None}
    if not CAM_INGEST_TOKEN:
        return {"student_id": None, "store": None, "name": None, "position": None}
    try:
        import requests
        r = requests.get(
            f"{S2_DBAPI}{BINDING_PATH}",
            params={"device_id": device_id},
            headers={"Authorization": f"Bearer {CAM_INGEST_TOKEN}"},
            timeout=15,
        )
        j = r.json()
        return {
            "student_id": j.get("student_id"),
            "store": j.get("store"),
            "name": j.get("name"),
            "position": j.get("position"),
        }
    except Exception as e:
        log(f"查 current-binding 失败: {e}（用空快照）")
        return {"student_id": None, "store": None, "name": None, "position": None}


def push_ingest(record):
    """把分析结果推送到 S2 的 ingest 端点（UPSERT 幂等）。
    成功返回 True；无 token / 失败（含重试一次）返回 False。
    """
    if not CAM_INGEST_TOKEN:
        log("无 CAM_INGEST_TOKEN，跳过推 S2（仅本地落库）")
        return False
    try:
        import requests
        headers = {"Authorization": f"Bearer {CAM_INGEST_TOKEN}", "Content-Type": "application/json"}
        r = requests.post(f"{S2_DBAPI}{INGEST_PATH}", json=record, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            log(f"已推送 S2 ingest: batch={record['batch']} (status={r.status_code})")
            return True
        # 非 2xx：重试一次（网络抖动/幂等安全）
        r2 = requests.post(f"{S2_DBAPI}{INGEST_PATH}", json=record, headers=headers, timeout=30)
        if r2.status_code in (200, 201):
            log(f"重试推送成功: batch={record['batch']}")
            return True
        log(f"推送 S2 ingest 失败: status={r.status_code}/{r2.status_code}, resp={r.text[:200]}")
        return False
    except Exception as e:
        log(f"推送 S2 ingest 异常: {e}")
        return False


def init_db():
    """建表（幂等）"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cam_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch TEXT NOT NULL,
            ts INTEGER,
            image_analysis TEXT,
            audio_text TEXT,
            summary TEXT,
            raw_json TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cam_analysis_batch ON cam_analysis(batch)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cam_upload_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ts         TEXT,
            device_id  TEXT,
            batch      TEXT,
            mode       TEXT,
            frames     INTEGER,
            files      TEXT,
            bytes      INTEGER,
            target     TEXT,
            http       INTEGER
        )
    """)
    conn.commit()
    return conn


def save_upload_log(batch_dir, batch, device_id):
    """读 _upload_log.txt，解析 key:value 写本地 cam-analysis.db + 推 S2（37号文档，追溯上传）"""
    upload_log_path = os.path.join(batch_dir, "_upload_log.txt")
    if not os.path.exists(upload_log_path):
        return
    try:
        text = open(upload_log_path, encoding="utf-8").read()
        data = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("-"):
                continue
            if ":" in line:
                k, v = line.split(":", 1)
                data[k.strip()] = v.strip()

        def to_int(s):
            s = str(s)
            return int(s) if s.lstrip("-").isdigit() else None

        rec = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id": data.get("device_id") or device_id,
            "batch": data.get("batch") or batch,
            "mode": data.get("mode"),
            "frames": to_int(data.get("frames", "")),
            "files": data.get("files"),
            "bytes": to_int(data.get("bytes", "")),
            "target": data.get("target"),
            "http": 200,
        }

        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cam_upload_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         TEXT,
                device_id  TEXT,
                batch      TEXT,
                mode       TEXT,
                frames     INTEGER,
                files      TEXT,
                bytes      INTEGER,
                target     TEXT,
                http       INTEGER
            )
        """)
        conn.execute(
            "INSERT INTO cam_upload_log (ts, device_id, batch, mode, frames, files, bytes, target, http) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                rec["ts"], rec["device_id"], rec["batch"], rec["mode"],
                rec["frames"], rec["files"], rec["bytes"], rec["target"], rec["http"],
            ),
        )
        conn.commit()
        conn.close()
        log(f"已写本地 cam_upload_log: batch={batch}, device_id={device_id or '(无)'}")

        push_upload_log(rec)
    except Exception as e:
        log(f"写 cam_upload_log 失败: {e}")


def push_upload_log(rec):
    """把上传日志推到 S2 的 cam-upload-log ingest 端点（追加，管理后台查 S2）。"""
    if not CAM_INGEST_TOKEN:
        log("无 CAM_INGEST_TOKEN，跳过推 S2 上传日志")
        return
    try:
        headers = {"Authorization": f"Bearer {CAM_INGEST_TOKEN}", "Content-Type": "application/json"}
        r = requests.post(f"{S2_DBAPI}/api/cam-upload-log/ingest", json=rec, headers=headers, timeout=20)
        if r.status_code in (200, 201):
            log(f"已推 S2 上传日志: batch={rec['batch']}")
        else:
            log(f"推 S2 上传日志失败: status={r.status_code}, resp={r.text[:200]}")
    except Exception as e:
        log(f"推 S2 上传日志异常: {e}")


def main():
    if len(sys.argv) < 2:
        log("缺少 batch 目录参数")
        sys.exit(1)

    batch_dir = sys.argv[1]
    batch = os.path.basename(batch_dir.rstrip("/"))
    device_id = sys.argv[2] if len(sys.argv) > 2 else ""   # 可选：设备码（cam-upload 传入）
    sop_id = sys.argv[3] if len(sys.argv) > 3 else ""      # 可选：SOP 标准 id（33号）
    log(f"开始分析 batch={batch} 目录 {batch_dir}, device_id={device_id or '(无)'}")

    # 37号文档：上传日志入库（读 _upload_log.txt，无论分析成败都先记录）
    save_upload_log(batch_dir, batch, device_id)

    # 参数配置化：分析前拉取 S2 的提示词配置（失败用默认，不阻塞）
    fetch_cam_config()

    if not os.path.isdir(batch_dir):
        log(f"目录不存在: {batch_dir}")
        sys.exit(1)

    # 1. 收集图片（按文件名排序保证时序）与录音
    imgs = sorted(
        str(p) for p in Path(batch_dir).glob("snap_*.jpg")
    )
    wav = os.path.join(batch_dir, "rec.wav")
    log(f"找到 {len(imgs)} 张图, 录音={'有' if os.path.exists(wav) else '无'}")

    # 34号 audio 模式：无图片（只有 rec.wav），跳过视觉分析，仅基于录音
    no_image = (len(imgs) == 0)
    if no_image:
        if not os.path.exists(wav):
            log("无图片且无录音，无可分析内容")
            sys.exit(1)
        log("本次无图片（audio 模式），跳过视觉分析，仅基于录音分析")

    # 图片视觉分析用 DashScope key（qwen-vl-plus / 与 ASR 同平台）；无图片时不要求该 key
    vision_key = ""
    if not no_image:
        vision_key = load_key("DASHSCOPE_API_KEY")
        if not vision_key:
            log("缺少 DASHSCOPE_API_KEY")
            sys.exit(1)
    # 综合分析用 DeepSeek key（无论有无图片都需要，用于综合/SOP 比对）
    chat_key = load_key("DEEPSEEK_API_KEY")
    if not chat_key:
        log("缺少 DEEPSEEK_API_KEY")
        sys.exit(1)

    # 2. 图片视觉时序分析（Qwen-VL-Plus）；audio 模式无图跳过
    if no_image:
        vision = ""                                  # image_analysis 字段置空（无图片）
        vision_prompt_text = "（本次未采集画面）"      # 综合/SOP prompt 里 {vision} 占位
    else:
        try:
            vision = call_vision(vision_key, imgs)
        except Exception as e:
            log(f"视觉分析失败: {e}")
            vision = f"视觉分析失败: {e}"
        vision_prompt_text = vision
        log(f"视觉分析结果:\n{vision}")

    # 3. 录音 ASR
    audio_text = call_asr(wav) if os.path.exists(wav) else ""
    log(f"录音转写: {audio_text or '(空)'}")

    # 4. 综合分析（DeepSeek）
    try:
        summary = call_chat(chat_key, summary_prompt.format(
            vision=vision_prompt_text,
            audio=audio_text or "(无有效录音内容)",
        ), max_tokens=max_tokens)
    except Exception as e:
        log(f"综合分析失败: {e}")
        # 降级：有视觉结论用视觉，无视觉（audio 模式）用录音文字兜底
        summary = vision if vision else (audio_text or "（无有效内容）")
    log(f"综合分析:\n{summary}")

    # 4b. SOP 标准比对（33号：选了 SOP 后，用 deepseek 比对画面+话术是否符合 SOP 标准）
    sop_compare = ""
    if sop_id:
        sop_doc = fetch_sop(sop_id)
        if sop_doc:
            try:
                sop_compare = call_chat(chat_key, sop_compare_prompt.format(
                    sop_name=sop_doc.get("name") or sop_id,
                    sop_content=sop_doc.get("content") or "",
                    vision=vision_prompt_text,
                    audio=audio_text or "(无有效录音内容)",
                ), max_tokens=max_tokens)
                log(f"SOP 比对:\n{sop_compare}")
            except Exception as e:
                log(f"SOP 比对失败: {e}")
                sop_compare = f"SOP 比对失败: {e}"
        else:
            sop_compare = f"(未找到 SOP，sop_id={sop_id})"
    else:
        log("本次无 SOP（未选流程标准），不做比对")

    # 5. 组装完整 record（含设备码、帧数、状态、绑定快照）
    binding = get_binding(device_id)   # 查 S2 当前绑定快照
    record = {
        "batch": batch,
        "device_id": device_id,
        "student_id": binding.get("student_id"),
        "store": binding.get("store"),
        "name": binding.get("name"),
        "position": binding.get("position"),
        "ts": int(time.time() * 1000),
        "frame_count": len(imgs),
        "image_analysis": vision,
        "audio_text": audio_text,
        "summary": summary,
        "sop_id": sop_id,
        "sop_compare": sop_compare,
        "status": "analyzed",
        "raw_json": json.dumps({"device_id": device_id, "frame_count": len(imgs)}, ensure_ascii=False),
    }

    # 5b. 写本地 SQLite（A 验证期过渡库，正式期以 S2 为准）
    try:
        conn = init_db()
        conn.execute(
            "INSERT INTO cam_analysis (batch, ts, image_analysis, audio_text, summary, raw_json) "
            "VALUES (?,?,?,?,?,?)",
            (batch, record["ts"], vision, audio_text, summary, json.dumps(record, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
        log(f"已写入 SQLite: {DB_PATH} (batch={batch})")
    except Exception as e:
        log(f"本地落库失败（不影响推 S2）: {e}")

    # 5c. 推送到 S2 ingest（S2 落库后前端才能查到）
    push_ingest(record)

    # 打印最终 JSON 结果（voice-server.js 可读）
    print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
