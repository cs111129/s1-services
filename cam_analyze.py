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
import re
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
# AI 分析超时（qwen-vl-plus 处理多帧较久，250 帧给足 500s）
VISION_TIMEOUT = 500
CHAT_TIMEOUT = 120
# 抽帧上限：超过该帧数则等间隔抽帧（qwen-vl-plus base64 硬上限 250 张，设满不抽帧）
MAX_VISION_FRAMES = 250

# ★★ 合并分析（2026-09-14 大哥定的方案）★★
# 背景：原来要 3 次调用（qwen 画面分析 → deepseek 综合 → deepseek 做 SOP 比对），
#      第 2、3 步其实看不到图片，只吃第 1 步输出的文字，等于"隔了一层"。
# 改法：用 deepseek-flash 一次调用同时完成「画面时序 + 录音转写 + SOP 逐条比对」，全部表格输出。
# 实测依据（S1 真机 + 真实采集帧）：
#   - deepseek-flash 就是 deepseek-v4.1-flash，**能看图**
#   - 图片硬上限 **600 张**（API 报错原文 `Too many images: max 600 images per request`），
#     600 张 = 117,035 prompt token 能过；瓶颈是上下文窗口而不是张数
#   - 每张图约 **195 token**（qwen 只要 82，所以换模型后 token 上升，不是"合并"造成的）
#   - **默认开推理**：max_tokens 给小了 content 会变空串（实测 2000/4000 全被推理吃光）
#     → 所以 max_tokens 必须给足；大哥定 100000（max_tokens 只是上限，按实际输出计费，不额外花钱）
#   - 实测一次调用：17 帧 33.5s/11,054 token；80 帧 27.5s/22,054 token，finish=stop 未截断
# 开关：S2 的 cam-config.json 里 **mergedPrompt 非空 → 走合并**；为空 → 回退原来的 3 步（可随时回滚）
DEEPSEEK_FLASH = "deepseek-flash"
MERGED_MAX_TOKENS = 100000   # 兜底值；实际以 cam-config.json 的 maxTokens 为准
MAX_MERGED_FRAMES = 600      # deepseek-flash 平台硬上限，超过必须抽帧
MERGED_TIMEOUT = 900         # 合并是一次大调用：实测 17 帧 33s / 80 帧 27s，给足余量

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
# CAM_DRYRUN=1：只分析、只写本地库，**不往 S2 推任何数据**。
# 用途：在生产上安全复跑验证（比如改了提示词想拿真实批次试，又不想往生产库塞记录）。
DRYRUN = os.environ.get("CAM_DRYRUN") == "1"
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
# ★ 合并分析提示词：非空则走「一次调用完成三段分析」；为空则回退原来的 3 步（回滚开关）
merged_prompt = None

# ============ 合并分析 prompt（默认值，S2 cam-config.json 的 mergedPrompt 可覆盖）============
# ⚠️ 占位符用 {{key}} 而不是 {key}：prompt 末尾有 JSON 示例，里面就是 { }，
#    用 str.format 会把这些大括号当成占位符直接抛异常。
DEFAULT_MERGED_PROMPT = """你是「胤隆会」实操考核的评分分析员。视觉采集设备按时间顺序连续拍摄了 {{n}} 张画面（相邻约 {{interval_sec}} 秒），并有现场录音转写。请严格按顺序分三步完成分析。

━━━ 第一步：画面时序分析 ━━━
按时间顺序还原画面里**真实发生**了什么，输出表格：
| 时间段 / 帧 | 画面里发生了什么 | 关键动作 / 变化 |
|---|---|---|
⚠️ 硬规则：只写你在这组图里**真实看到**的内容。不要因为第三步的 SOP 里出现了某个场景（前台 / 接待 / 客人 / 房卡等），就假设画面里发生了那个场景。若画面内容与 SOP 场景无关，必须在表格里如实写明「画面内容与 SOP 场景无关」。**编造画面里没有的内容属于严重错误。**

━━━ 第二步：录音转写分析 ━━━
【现场录音转写】
{{audio}}

对上面的录音转写做分析，输出表格：
| 时间点 / 段落 | 录音内容要点 | 说明（是否含话术要求、语气、与画面是否对应） |
|---|---|---|
若录音为空或无有效内容，表格里如实写「本次无有效录音」，**不要编造对话**。

━━━ 第三步：SOP 标准比对 ━━━
{{sop_block}}

━━━ 输出要求 ━━━
只输出一个 JSON 对象（不要 markdown 代码围栏、不要任何额外解释），三个字段都是 Markdown 表格：
{
  "image_analysis": "第一步的表格（含表头行）",
  "summary": "第二步的表格（含表头行）",
  "sop_compare": "第三步的表格（含表头行），SOP 含话术要求时在后面接一段「话术评估」"
}"""

# 第三步的两种写法（有 SOP / 设备端没选 SOP）
SOP_BLOCK_WITH = """流程名：{{sop_name}}
SOP 标准：
{{sop_content}}

对 SOP 里**每一条**标准逐条判断，输出表格（表头固定，不要改）：
| 序号 | 标准项 | 是否做到 | 说明 |
|---|---|---|---|
| 1 | （SOP 第 1 条标准原样照抄） | 做到 / 未做到 | （判断依据：要具体到画面或录音里的什么） |

判断值只用「做到」或「未做到」两个。若 SOP 标准里包含话术要求，在表格后补一段「话术评估」；若 SOP 没有话术要求，则不写话术评估。不要打分、不要评分。"""

SOP_BLOCK_NONE = """本次未指定 SOP 标准（设备端没选流程）。
请在 sop_compare 字段里只写一句：「本次未选择 SOP 流程，未做标准比对。」不要编造标准。"""


def fill_prompt(tpl, **kw):
    """把 {{key}} 替换成实际值。
    刻意不用 str.format：prompt 末尾的 JSON 示例含 { }，format 会把它们当占位符抛异常。"""
    out = tpl
    for k, v in kw.items():
        out = out.replace('{{' + k + '}}', str(v))
    return out


def fetch_cam_config():
    """从 S2 拉取实操考核配置（提示词/参数）。失败用默认值，不阻塞分析。
    参数配置化（30/31 号）：后台配置提示词 + interval_ms/max_seconds，分析前拉取提示词。
    34号问题4：新增 maxTokens（DeepSeek 输出上限）+ retryCount（失败重试次数）。
    2026-09-14：新增 mergedPrompt —— 非空则走「一次调用完成三段分析」的合并模式。"""
    global vision_prompt, summary_prompt, sop_compare_prompt, max_tokens, retry_count, merged_prompt
    try:
        if not CAM_INGEST_TOKEN or not S2_DBAPI:
            return
        r = requests.get(f"{S2_DBAPI}{CAM_CONFIG_PATH}",
                         headers={"Authorization": f"Bearer {CAM_INGEST_TOKEN}"},
                         timeout=10)
        if r.status_code == 200:
            c = r.json()
            if c.get("mergedPrompt"):
                merged_prompt = c["mergedPrompt"]
            else:
                merged_prompt = None      # 显式置空 → 回退 3 步老流程（回滚开关）
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
            log(f"已拉取 cam-config：模式={'合并(1次调用)' if merged_prompt else '老流程(3次调用)'}, "
                f"mergedPrompt={len(merged_prompt) if merged_prompt else 0}字, "
                f"visionPrompt={len(vision_prompt)}字, summaryPrompt={len(summary_prompt)}字, "
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


# cam 分析专用的 DeepSeek key 文件（2026-09-14）。
# 为什么需要它：这台机器上 DEEPSEEK_API_KEY 有多个来源且互相打架 ——
#   ① voice-server 进程环境里是 sk-d05… ② /root/.bashrc 里是 sk-2d0… ③ 大哥另给了一把新 key
# 而 load_key() 是「环境变量优先」，cam_analyze.py 又是 voice-server spawn 出来的、继承它的环境，
# 所以光改 .bashrc 对这脚本**不生效**。为了不重启 voice-server（不影响语音服务）就能换 key，
# 这里给 cam 分析留一个优先级最高的专用文件。
CAM_DEEPSEEK_KEYFILE = os.environ.get("CAM_DEEPSEEK_KEYFILE", "/root/.deepseek-key")


def load_deepseek_key():
    """取 cam 分析用的 DeepSeek key。
    顺序：① 专用文件（优先，改它即可换 key，无需重启任何服务）→ ② 环境变量 → ③ .bashrc。
    只记录"来源"，不打印 key 明文。"""
    p = CAM_DEEPSEEK_KEYFILE
    try:
        if os.path.exists(p):
            v = open(p, encoding="utf-8").read().strip()
            if v:
                log(f"DeepSeek key 来源: 专用文件 {p}")
                return v
            log(f"专用文件 {p} 是空的，回退环境变量")
    except OSError as e:
        log(f"读专用文件 {p} 失败({e})，回退环境变量")
    v = load_key("DEEPSEEK_API_KEY")
    log("DeepSeek key 来源: " + ("环境变量 / .bashrc" if v else "未找到"))
    return v


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


def call_vision(key, image_paths, interval_sec=5):
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

    content = [{"type": "text", "text": vision_prompt.format(n=len(image_paths), interval_sec=interval_sec)}]
    for p in image_paths:
        b64 = base64.b64encode(open(p, "rb").read()).decode()
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
        })
    payload = {
        "model": VISION_MODEL,
        "messages": [{"role": "user", "content": content}],
        "max_tokens": 8192,
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


def call_merged(key, image_paths, audio_text, sop_doc, interval_sec=5):
    """★ 合并分析：一次调用完成「画面时序 + 录音转写 + SOP 逐条比对」，三段都是 Markdown 表格。

    返回 dict(image_analysis, summary, sop_compare)。
    解析失败时**不丢内容**：原文塞进 image_analysis，另两个字段标注原因。

    为什么用 deepseek-flash（= deepseek-v4.1-flash）：
      - 能看图；图片硬上限 **600 张**（API 报错原文 `Too many images: max 600 images per request`）
      - 每张图约 195 token（qwen 只要 82，所以换模型后 token 会涨，这不是"合并"造成的）
      - **默认开推理**，max_tokens 给小了 content 会变空串（实测 2000/4000 全被推理吃光）
        → max_tokens 取 cam-config 的 maxTokens（大哥定 100000；它只是上限，按实际输出计费）
    """
    # 抽帧到平台硬上限（600）
    if len(image_paths) > MAX_MERGED_FRAMES:
        step = (len(image_paths) + MAX_MERGED_FRAMES - 1) // MAX_MERGED_FRAMES
        sampled = image_paths[::step]
        if sampled[-1] != image_paths[-1]:
            sampled.append(image_paths[-1])
        log(f"抽帧降本: {len(image_paths)} 帧 → {len(sampled)} 帧（等间隔，上限 {MAX_MERGED_FRAMES}）")
        image_paths = sampled

    # 第三步：有 SOP 就逐条比对，没选就如实说明（不编造标准）
    if sop_doc:
        sop_block = fill_prompt(SOP_BLOCK_WITH,
                                sop_name=sop_doc.get("name") or sop_doc.get("id") or "",
                                sop_content=sop_doc.get("content") or "")
    else:
        sop_block = SOP_BLOCK_NONE

    prompt = fill_prompt(
        merged_prompt or DEFAULT_MERGED_PROMPT,
        n=len(image_paths),
        interval_sec=interval_sec,
        audio=audio_text or "(无有效录音内容)",
        sop_block=sop_block,
    )
    if not image_paths:
        # audio 模式（只有录音没有图）：明确告诉模型别去"想象"画面
        prompt = ("【注意】本次未采集到任何画面（audio 模式），第一步「画面时序分析」请如实写"
                  "「本次未采集画面」，不要编造画面内容。\n\n" + prompt)

    content = [{"type": "text", "text": prompt}]
    for p in image_paths:
        b64 = base64.b64encode(open(p, "rb").read()).decode()
        content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}})

    payload = {
        "model": DEEPSEEK_FLASH,
        "messages": [{"role": "user", "content": content}],
        "max_tokens": max_tokens or MERGED_MAX_TOKENS,
        "temperature": 0.3,
    }
    log(f"调用 {DEEPSEEK_FLASH} 合并分析：{len(image_paths)} 张图，max_tokens={payload['max_tokens']} ...")
    j = http_json_post(DEEPSEEK_URL, key, payload, timeout=MERGED_TIMEOUT)
    choice = j["choices"][0]
    msg = choice.get("message") or {}
    text = (msg.get("content") or "").strip()
    if not text:
        # 推理型兜底：极少数情况下正文落在 reasoning_content
        text = (msg.get("reasoning_content") or "").strip()
    usage = j.get("usage") or {}
    rt = ((usage.get("completion_tokens_details") or {}).get("reasoning_tokens")) or 0
    log(f"合并分析 token: prompt={usage.get('prompt_tokens')}, completion={usage.get('completion_tokens')}"
        f"(推理{rt}), total={usage.get('total_tokens')}, finish={choice.get('finish_reason')}")

    # 解析 JSON（容错去掉 ```json 围栏）
    body = re.sub(r"^```(?:json)?|```$", "", text, flags=re.M).strip()
    try:
        d = json.loads(body)
        if not isinstance(d, dict):
            raise ValueError("返回的不是 JSON 对象")
    except Exception as e:
        log(f"合并分析 JSON 解析失败({e})，原文存进 image_analysis 以免丢内容")
        return {
            "image_analysis": text or "(合并分析返回空)",
            "summary": f"（合并分析未返回合法 JSON，无法拆分字段：{e}）",
            "sop_compare": "",
        }
    return {
        "image_analysis": (d.get("image_analysis") or "").strip(),
        "summary": (d.get("summary") or "").strip(),
        "sop_compare": (d.get("sop_compare") or "").strip(),
    }


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
    if DRYRUN:
        log("CAM_DRYRUN=1，跳过推 S2（本地库已写）")
        return False
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
    if DRYRUN:
        log("CAM_DRYRUN=1，跳过推 S2 上传日志")
        return
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
    interval_ms = sys.argv[4] if len(sys.argv) > 4 else ""  # 可选：拍照间隔（41号文档）
    interval_sec = 5
    if interval_ms.isdigit() and int(interval_ms) > 0:
        v = int(interval_ms) / 1000.0
        interval_sec = int(v) if v == int(v) else v
    log(f"开始分析 batch={batch} 目录 {batch_dir}, device_id={device_id or '(无)'}, interval={interval_sec}s")

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

    # 录音 ASR —— ★ 提前到分析之前：合并模式需要"录音转写"作为同一次调用的输入
    audio_text = call_asr(wav) if os.path.exists(wav) else ""
    log(f"录音转写: {audio_text or '(空)'}")

    # SOP 标准内容（合并模式 / 老流程都要用，提前拉）
    sop_doc = fetch_sop(sop_id) if sop_id else None
    if sop_id and not sop_doc:
        log(f"未找到 SOP（sop_id={sop_id}）")

    # DeepSeek key：两种模式都要用（优先用 /root/.deepseek-key 专用文件）
    chat_key = load_deepseek_key()
    if not chat_key:
        log("缺少 DEEPSEEK_API_KEY")
        sys.exit(1)

    if merged_prompt:
        # ★★ 合并模式（2026-09-14 起）：deepseek-flash 一次调用出「画面时序 + 录音转写 + SOP 比对」
        try:
            r = call_merged(chat_key, imgs, audio_text, sop_doc, interval_sec)
            vision = r["image_analysis"]
            summary = r["summary"]
            sop_compare = r["sop_compare"]
            log(f"合并分析完成：画面 {len(vision)} 字 / 录音分析 {len(summary)} 字 / SOP 比对 {len(sop_compare)} 字")
        except Exception as e:
            log(f"合并分析失败: {e}")
            vision = f"合并分析失败: {e}"
            summary = ""
            sop_compare = ""
    else:
        # ===== 老流程（3 次调用：qwen 画面 + deepseek 综合 + deepseek SOP）=====
        # 保留此路径作为回滚：把 S2 cam-config.json 的 mergedPrompt 清空即回到这里
        vision_key = ""
        if not no_image:
            vision_key = load_key("DASHSCOPE_API_KEY")
            if not vision_key:
                log("缺少 DASHSCOPE_API_KEY")
                sys.exit(1)

        # 2. 图片视觉时序分析（Qwen-VL-Plus）；audio 模式无图跳过
        if no_image:
            vision = ""                                  # image_analysis 字段置空（无图片）
            vision_prompt_text = "（本次未采集画面）"      # 综合/SOP prompt 里 {vision} 占位
        else:
            try:
                vision = call_vision(vision_key, imgs, interval_sec)
            except Exception as e:
                log(f"视觉分析失败: {e}")
                vision = f"视觉分析失败: {e}"
            vision_prompt_text = vision
            log(f"视觉分析结果:\n{vision}")

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
