#!/usr/bin/env python3
"""
asr_recognition.py — 语音转文字（实操考核用）

★ 2026-09-14 换模型：paraformer-realtime-v2（DashScope SDK）→ qwen3-asr-flash（原生 multimodal HTTP）
  实测理由（同一段 168s 真实录音，见 scripts/ 里的对照脚本）：
    - 内容 648 字 → 854 字（+32%）；且与另一个独立模型 fun-asr 交叉印证，
      新模型多听到的内容（如"五十万"）两个模型一致 → 是真内容不是幻觉
    - 更快：168s 音频 4.8s 出结果
    - **附带 emotion / language 标注**（考核可用来判断服务态度/情绪）
    - 直接吃 base64，**不需要公网 URL**（paraformer-v2 那条路要先把音频传到 DashScope 临时空间）
  回退：qwen3 失败时自动退回 paraformer-realtime-v2（老代码保留）；
       也可用环境变量 ASR_ENGINE=paraformer 强制走老路径。

⚠️ 硬限制（实测）：**音频最长 300 秒**。
   API 原话 `AUDIO_DURATION_TOO_LONG: audio duration (420000.0ms) over service process (300000ms)`
   实测 120/180/240/300s 可过，420s/524s 失败；另有 data-URI 上限 20MB。
   压缩成 mp3 也救不了 —— 限的是**时长**不是体积。
   → 所以超过 MAX_SEG_SEC 的录音会**自动分段**转写再拼接。

⚠️ 分段切点策略：在**原始音频**上做静音检测，切点尽量落在静音中点（避免把字切一半）。
   这也是为什么 ffmpeg 预处理放在本脚本里而不是调用方 —— dynaudnorm 会把静音段底噪放大，
   在归一化之后的音频上做静音检测**检测不到静音**（实测 524s 只剪掉 1s）。

用法：python3 asr_recognition.py <音频文件路径>
输出：stdout 打印一个 JSON（至少含 text；另带 engine/segments/annotations 便于排查）
退出码：0=成功，1=失败（stderr 打印原因）
"""
import sys
import os
import json
import base64
import math
import wave
import subprocess
import tempfile
import shutil
import requests

# ============ 配置 ============
ASR_MODEL = os.environ.get("ASR_MODEL", "qwen3-asr-flash-2026-02-10")
NATIVE_URL = "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation"
# 引擎选择：qwen3（默认）| paraformer（回退/强制老路径）
ASR_ENGINE = os.environ.get("ASR_ENGINE", "qwen3").strip().lower()

# 单次调用上限 300s，留 20s 余量
MAX_SEG_SEC = float(os.environ.get("ASR_MAX_SEG_SEC", "280"))
# 每段最短长度（避免切出几秒钟的碎片段）
MIN_SEG_SEC = 20.0
# data-URI 上限 20MB（实测报错原文 max bytes per data-uri item : 20971520）
DATA_URI_LIMIT = 20 * 1024 * 1024

# ffmpeg 预处理链：高通去低频噪声 + 动态归一化增益
# ⚠️ 刻意不含 afftdn：实测 AI 降噪在低声压录音上会把语音连同噪声一起削掉（+7% 内容量）
FFMPEG_AF = "highpass=f=80,dynaudnorm=f=150:g=15"

_HTTP_TIMEOUT = 300


def log(msg):
    sys.stderr.write(f"[asr] {msg}\n")
    sys.stderr.flush()


def load_key(env_name):
    """从环境变量或 /root/.bashrc 读 API key（与 voice-server.js / cam_analyze.py 一致）"""
    v = os.environ.get(env_name)
    if v:
        return v.strip()
    try:
        for line in open("/root/.bashrc", encoding="utf-8", errors="ignore"):
            if env_name in line and "=" in line:
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                if val:
                    return val
    except OSError:
        pass
    return ""


# ============ 音频工具 ============
def probe_duration(path):
    """优先用 wave 读（无外部依赖），失败再退回 ffprobe"""
    try:
        with wave.open(path, "rb") as f:
            return f.getnframes() / float(f.getframerate() or 1)
    except Exception:
        pass
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "default=nw=1:nk=1", path],
                           capture_output=True, text=True, timeout=60)
        return float((r.stdout or "").strip())
    except Exception:
        return 0.0


def preprocess(src, dst):
    """ffmpeg 预处理成 16k mono wav。成功返回 True。"""
    try:
        r = subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", src,
                            "-af", FFMPEG_AF, "-ac", "1", "-ar", "16000", dst],
                           capture_output=True, timeout=300)
        return r.returncode == 0 and os.path.exists(dst) and os.path.getsize(dst) > 1000
    except Exception as e:
        log(f"预处理失败: {e}")
        return False


def find_silences(path, noise_db=-35, min_sil=0.4):
    """在**原始音频**上找静音区间，返回 [(start, end), ...]。
    必须在 dynaudnorm 之前做 —— 归一化会把静音底噪抬起来，之后就检测不到了。"""
    try:
        r = subprocess.run(["ffmpeg", "-i", path,
                            "-af", f"silencedetect=noise={noise_db}dB:d={min_sil}",
                            "-f", "null", "-"], capture_output=True, text=True, timeout=300)
        txt = r.stderr or ""
        import re as _re
        starts = [float(x) for x in _re.findall(r"silence_start: ([\d.]+)", txt)]
        ends = [float(x) for x in _re.findall(r"silence_end: ([\d.]+)", txt)]
        out = []
        for i, s in enumerate(starts):
            e = ends[i] if i < len(ends) else None
            if e is not None and e > s:
                out.append((s, e))
        return out
    except Exception as e:
        log(f"静音检测失败（退回硬切）: {e}")
        return []


def plan_segments(duration, silences, max_seg=MAX_SEG_SEC):
    """把 [0, duration] 切成若干段，**每段都 <= max_seg**，切点尽量落在静音中点。
    返回 [(start, end), ...]

    设计：先算需要几段（ceil），再按等分位置找切点 —— 而不是"切到 280s 再看尾段怎么办"。
    后者有个真 bug：281s 时尾段只有 1s，把它并进上一段会得到 281s > 上限（单测抓到过）。
    等分后每段天然 <= max_seg，静音吸附只在等分点附近小幅挪动。
    """
    if duration <= max_seg:
        return [(0.0, duration)]

    n = int(math.ceil(duration / max_seg))
    ideal = duration / n                      # 每段理论长度（一定 <= max_seg）
    segs = []
    pos = 0.0
    for i in range(1, n):
        target = i * ideal
        # 静音吸附窗口：等分点前后各 35%，且不能把这一小段顶出上限 / 压得太短
        lo = max(pos + MIN_SEG_SEC, target - ideal * 0.35)
        hi = min(target + ideal * 0.35, pos + max_seg, duration - MIN_SEG_SEC)
        best = None
        if hi > lo:
            for (s, e) in silences:
                mid = (s + e) / 2.0
                if lo <= mid <= hi and (best is None or abs(mid - target) < abs(best - target)):
                    best = mid
        cut = best if best is not None else target
        # 双保险：绝不允许超过上限，也绝不允许倒退
        cut = min(cut, pos + max_seg)
        cut = max(cut, pos + MIN_SEG_SEC)
        segs.append((pos, cut))
        pos = cut
    segs.append((pos, duration))
    return segs


def extract_segment(src, start, end, dst):
    """从原始音频裁一段并预处理"""
    try:
        r = subprocess.run(["ffmpeg", "-y", "-loglevel", "error",
                            "-ss", f"{start:.3f}", "-t", f"{max(0.1, end - start):.3f}",
                            "-i", src, "-af", FFMPEG_AF, "-ac", "1", "-ar", "16000", dst],
                           capture_output=True, timeout=300)
        return r.returncode == 0 and os.path.exists(dst) and os.path.getsize(dst) > 1000
    except Exception as e:
        log(f"裁段失败 ({start:.0f}-{end:.0f}s): {e}")
        return False


# ============ 引擎1：qwen3-asr-flash ============
def call_qwen3(wav_path, key):
    """返回 (text, annotations)。失败抛异常。"""
    raw = open(wav_path, "rb").read()
    b64 = base64.b64encode(raw).decode()
    uri = f"data:audio/wav;base64,{b64}"
    if len(uri.encode()) > DATA_URI_LIMIT:
        raise RuntimeError(f"分段仍超过 data-URI 上限 20MB（{len(uri)/1048576:.1f}MB）")
    payload = {
        "model": ASR_MODEL,
        "input": {"messages": [{"role": "user", "content": [{"audio": uri}]}]},
        "parameters": {},
    }
    r = requests.post(NATIVE_URL,
                      headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                      json=payload, timeout=_HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    j = r.json()
    msg = j["output"]["choices"][0]["message"]
    text = "".join(c.get("text", "") for c in (msg.get("content") or []))
    return text.strip(), (msg.get("annotations") or []), (j.get("usage") or {})


# ============ 引擎2：paraformer-realtime-v2（回退） ============
def call_paraformer(wav_path):
    """老引擎，作兜底。需要 dashscope SDK。"""
    from dashscope.audio.asr import Recognition

    try:
        with wave.open(wav_path, "rb") as w:
            sr = w.getframerate()
    except Exception:
        sr = 16000
    model, rate = ("paraformer-realtime-v2", 16000) if sr >= 16000 else ("paraformer-realtime-8k-v2", 8000)

    class _CB:
        def on_open(self): pass
        def on_close(self): pass
        def on_complete(self): pass
        def on_error(self, result): pass
        def on_event(self, result): pass

    result = Recognition(model=model, callback=_CB(), format="wav", sample_rate=rate).call(wav_path)
    if not result or result.status_code != 200:
        return ""
    sents = result.get_sentence()
    if isinstance(sents, list):
        return "".join(s.get("text", "") for s in sents if isinstance(s, dict)).strip()
    return ""


# ============ 主流程 ============
def recognize_audio(audio_path):
    """返回 dict: {text, engine, segments, annotations}"""
    duration = probe_duration(audio_path)
    tmpdir = tempfile.mkdtemp(prefix="asr_")
    try:
        key = load_key("DASHSCOPE_API_KEY")
        if ASR_ENGINE == "qwen3" and not key:
            log("缺少 DASHSCOPE_API_KEY，回退 paraformer")
            engine = "paraformer"
        else:
            engine = ASR_ENGINE if ASR_ENGINE in ("qwen3", "paraformer") else "qwen3"

        # ---- paraformer 老路径：整段（它没有 300s 限制）----
        if engine == "paraformer":
            proc = os.path.join(tmpdir, "full.wav")
            target = proc if preprocess(audio_path, proc) else audio_path
            return {"text": call_paraformer(target) or "", "engine": "paraformer",
                    "segments": 1, "annotations": []}

        # ---- qwen3 路径：超长则分段 ----
        silences = find_silences(audio_path) if duration > MAX_SEG_SEC else []
        segs = plan_segments(duration, silences)
        if len(segs) > 1:
            log(f"音频 {duration:.0f}s 超过单次上限 {MAX_SEG_SEC:.0f}s → 分 {len(segs)} 段转写")
            log("切点: " + ", ".join(f"{a:.0f}-{b:.0f}s" for a, b in segs))

        parts, anns, total_usage = [], [], {}
        for i, (a, b) in enumerate(segs):
            seg_path = os.path.join(tmpdir, f"seg{i}.wav")
            ok = (preprocess(audio_path, seg_path) if len(segs) == 1
                  else extract_segment(audio_path, a, b, seg_path))
            if not ok:
                log(f"第{i+1}段预处理失败，跳过")
                continue
            try:
                t, an, u = call_qwen3(seg_path, key)
                if t:
                    parts.append(t)
                anns.extend(an)
                for k, v in (u or {}).items():
                    if isinstance(v, (int, float)):
                        total_usage[k] = total_usage.get(k, 0) + v
                log(f"第{i+1}/{len(segs)}段（{a:.0f}-{b:.0f}s）→ {len(t)} 字")
            except Exception as e:
                log(f"第{i+1}段 qwen3 失败: {e}")

        if parts:
            return {"text": "".join(parts), "engine": "qwen3-asr-flash",
                    "segments": len(segs), "annotations": anns, "usage": total_usage}

        # ---- qwen3 全军覆没 → 回退 paraformer ----
        log("qwen3 未取到任何文字，回退 paraformer")
        proc = os.path.join(tmpdir, "fb.wav")
        target = proc if preprocess(audio_path, proc) else audio_path
        return {"text": call_paraformer(target) or "", "engine": "paraformer-fallback",
                "segments": len(segs), "annotations": []}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "缺少音频文件路径参数"}), file=sys.stderr)
        sys.exit(1)

    audio_path = sys.argv[1]
    if not os.path.exists(audio_path):
        print(json.dumps({"error": f"音频文件不存在: {audio_path}"}), file=sys.stderr)
        sys.exit(1)

    try:
        out = recognize_audio(audio_path)
        if out.get("text"):
            print(json.dumps(out, ensure_ascii=False))
        else:
            log("识别结果为空")
            print(json.dumps({"error": "识别结果为空"}, ensure_ascii=False), file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)
