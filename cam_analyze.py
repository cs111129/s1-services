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
# ★★ 2026-10-12：ingest token 从**文件**读（/root/.cam-ingest-token，0600、不进 git）
#   优先级：环境变量 > 文件；★ 都没有则返回空串 —— 本文件已有的逻辑会
#   "跳过推 S2、只落本地库"（见下面 CAM_INGEST_TOKEN 为空时的分支），不会静默用默认值 ✓
#   ★ 为什么要动：这个默认值一直躺在**源码（git）**里 ⇒ 密钥不该在版本库里
def _load_ingest_token():
    v = (os.environ.get("CAM_INGEST_TOKEN") or "").strip()
    if v:
        return v
    try:
        with open("/root/.cam-ingest-token", "r", encoding="utf-8") as f:
            v = (f.read() or "").strip()
            if v:
                return v
    except Exception:
        pass
    print("[cam_analyze] ✗ 取不到 CAM_INGEST_TOKEN（环境变量与 /root/.cam-ingest-token 都没有）"
          " ⇒ 本次只落本地库、不推 S2", flush=True)
    return ""

CAM_INGEST_TOKEN = _load_ingest_token()
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
# ★ 新增：按步骤逐项判定的模板（只含人格/框架）。None ⇒ 老路径按老行为（零窗口回退 ✓）
sop_compare_steps_prompt = None
SOP_PATH = "/api/cam-sop"
# 34号问题4：DeepSeek 文本模型输出上限 + 失败重试次数（S2 cam-config 可配置，默认兜底）
max_tokens = 2000
retry_count = 1
# ★ 合并分析提示词：非空则走「一次调用完成三段分析」；为空则回退原来的 3 步（回滚开关）
merged_prompt = None

# ★ ASR 元信息（call_asr 填充）：情绪/语种标注、用的引擎、分了几段
#   qwen3-asr-flash 会返回 annotations: [{"emotion":"happy","language":"zh"}]，
#   一段录音一个标注；超长分段后会有多个 → 取主导值（见 dominant_of）
asr_meta = {"annotations": [], "engine": "", "segments": 0}


def dominant_of(anns, field):
    """从（可能多段的）标注里取主导值：出现次数最多的；并列时取先出现的。"""
    vals = [a.get(field) for a in (anns or []) if isinstance(a, dict) and a.get(field)]
    if not vals:
        return ""
    counts = {}
    for v in vals:
        counts[v] = counts.get(v, 0) + 1
    best = max(counts.values())
    for v in vals:          # 保持原始顺序，并列时取先出现的
        if counts[v] == best:
            return v
    return ""

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

# ══════════════════════════════════════════════════════════════════════
# SOP 标准块 + 输出格式要求（CR-20261008-01 期2：按步骤逐项判定）
#
# ★★ 为什么不把「输出格式要求」放进 cam-config.json 的可配置提示词：
#   它和**数据模型绑死**（steps 有哪些字段 ⇒ 矩阵有哪些列）。
#   一旦放进配置，就会出现"改了代码默认值、线上仍是旧配置"的静默失效 ✗
#   —— 2026-10-08 实测：线上 mergedPrompt / sopComparePrompt **两个都被覆盖**，
#      只改本文件的默认值等于没改（这是本 CR 最容易踩的坑，故在此写明）。
#   ⇒ 规范：**人格/框架可配置，输出格式不可配置**（格式跟着数据模型走，只在代码里维护一份）。
#
# ★ 必须与 xydck/server/student-data.js 的同名实现**逐字节一致**
#   （两条判定链路：S1 首次分析 / 学员端「换个 SOP 重新分析」）
#   校验脚本：xydck/scripts/verify-sop-prompt-consistency.js ✓
# ══════════════════════════════════════════════════════════════════════
SOP_STEP_FIELDS = (("action", "动作"), ("speech", "话术"), ("tool", "工具"),
                   ("note", "注意事项"), ("duration", "完成时间"))

SOP_SPEC_STEPS = """请按【每一步 × 每一项】逐项判定，先输出矩阵表（表头固定，一个字都不要改）：
| 序号 | 步骤 | 动作 | 话术 | 工具 | 注意 | 时间 |
|---|---|---|---|---|---|---|
| 1 | （步骤名原样照抄） | ✅ | — | ✅ | ✅ | ✅ |

矩阵里的列名含义：「注意」= 注意事项，「时间」= 完成时间。判定值只能用这 4 个符号：
✅ 做到 ｜ ❌ 未做到 ｜ — 这一步没有这项要求（标准里没写） ｜ ❓ 画面或录音没覆盖到，无法判断
★ ❓ 和 ❌ 不是一回事：**没拍到、没录到就填 ❓**，不要因为"我没看到"就判「未做到」。
★ 步骤名原样照抄，不要改写、不要合并、不要漏掉任何一步。

矩阵之后必须再写两段（某段没有内容就写「无」）：
## 需要改进
- **第 N 步 <步骤名> · <项目>**：差在哪 + 判断依据（具体到画面时间点或录音原话）
## 无法判断
- **第 N 步 <步骤名> · <项目>**：为什么无法判断

不要打分、不要评分、不要给总分；也不要再单独写「话术评估」段落（话术已经在矩阵里逐项判定过了）。
若画面内容与 SOP 场景无关，如实写明「画面内容与 SOP 场景无关」，**不要编造画面里没有的动作或对话**。"""

SOP_SPEC_PLAIN = """请逐条比对员工演示与 SOP 标准，输出表格（表头固定，不要改）：
| 序号 | 标准项 | 是否做到 | 说明 |
|---|---|---|---|
| 1 | （SOP 第 1 条标准原样照抄） | 做到 / 未做到 | （判断依据：要具体到画面或录音里的什么） |

判断值只用「做到」或「未做到」两个。某一项如果画面/录音没覆盖到，在「说明」里写明「无法判断：…」，**不要直接算成未做到**。
若 SOP 标准里包含话术要求，在表格后补一段「话术评估」；若 SOP 没有话术要求，则不写话术评估。不要打分、不要评分。"""

SOP_BLOCK_NONE = """本次未指定 SOP 标准（设备端没选流程）。
请在 sop_compare 字段里只写一句：「本次未选择 SOP 流程，未做标准比对。」不要编造标准。"""

# ══════════════════════════════════════════════════════════════════════
# ★★ CR-20261126-01：主角锚定（实操考核「谁是主角」）
#   背景：多人场景（班组会：主持人 + 员工）模型不知道评谁 ⇒ 会把配角当主角
#   做法：学员开录前用手机自拍一张「主角照」⇒ 上传到 S1 ⇒ 复制成 <batch>/ref.jpg
#         ⇒ 分析时把这张图作为 messages 的**第 1 张**，并在最终 prompt **前面**插入本段
#   ★★ 为什么放代码层而不是 cam-config.json：线上 mergedPrompt 是被配置**覆盖**的
#      （改代码默认值等于没改，本项目 2026-10-08 踩过）⇒ 与"语音情绪前缀"同一种做法：
#      **代码层前置一段**，配置一行不用动 ✓（回滚 = 不传 ref.jpg）
# ══════════════════════════════════════════════════════════════════════
PROTAGONIST_BLOCK = """【主角锚定（本次考核对象）】
- 消息里**第 1 张图**是「主角参照照」（被考核人手机自拍）；后续 {n} 张才是设备按时间顺序拍的录制画面。
- 主角 = 参照照里的那个人。第一步每行请注明主角是否在画面（在 → 画面左/中/右、远/近）；不在 → 写「主角不在画面」；并写明识别依据（发型、衣着、体型等）。
- 第三步只评判**主角本人**的动作与话术；画面里的其他人（同事/顾客）只作为场景上下文，**不评分、不替代主角**。
- 若某段画面认不出主角（不在画面 / 背对 / 遮挡 / 光线差），在「无法判断」里如实写明；**禁止把配角当成主角评**。
- 参照照只用于认人：**不要**把它当作第 1 帧录像，也不要据此评价仪容仪表。

"""

# ★ CR-20261126-03：形象备案（非本次录制）作为参照 —— 只能认人，绝不能评价仪容
PORTRAIT_BLOCK = """【主角锚定（本次考核对象 · 形象备案）】
- 消息里**前 {k} 张图**是主角的「形象备案照」（手机+设备机位，**不是本次录制**）；后续 {n} 张才是设备按时间顺序拍的录制画面。
- 主角 = 备案照里的那个人。第一步每行请注明主角是否在画面（在 → 画面左/中/右、远/近）；不在 → 写「主角不在画面」；并写明识别依据（发型、体型、衣着等）。
- 第三步只评判**主角本人**的动作与话术；画面里的其他人（同事/顾客）只作为场景上下文，**不评分、不替代主角**。
- 若某段画面认不出主角（不在画面 / 背对 / 遮挡 / 光线差），在「无法判断」里如实写明；**禁止把配角当成主角评**。
- 备案照只用于认人：**绝不能**用备案照评价本次的仪容仪表/服装是否达标（衣着可能已换）；它也不是本次录像的一帧。

"""


def render_steps_for_prompt(steps):
    """结构化步骤 → 给模型看的文本。空字段【不输出】= 该步对此项无要求（对应矩阵里的「—」）。

    ★ 与 student-data.js 的 renderStepsForPrompt() 逐字节一致（有校验脚本比对）。
    """
    out = []
    for i, st in enumerate(steps, 1):
        st = st or {}
        lines = ["【步骤 %d】%s" % (i, st.get("name") or "")]
        for key, label in SOP_STEP_FIELDS:
            v = str(st.get(key) or "").strip()
            if v:
                lines.append("  %s：%s" % (label, v))
        out.append("\n".join(lines))
    return "\n".join(out)


def sop_standard_text(sop_doc):
    """SOP 标准正文：有 steps 按步骤渲染；没有就用原文（老 SOP 不中断 ✓）"""
    steps = sop_doc.get("steps") or []
    if steps:
        head = ("流程名：%s\n共 %d 个步骤（每步只列出**有要求**的项目；"
                "没列出的项 = 该步对此项无要求）\n" % (sop_doc.get("name") or "", len(steps)))
        return head + render_steps_for_prompt(steps)
    return "流程名：%s\nSOP 标准：\n%s" % (sop_doc.get("name") or "", sop_doc.get("content") or "")


def sop_spec_for(sop_doc):
    """输出格式要求：有 steps ⇒ 逐项矩阵；没有 steps ⇒ 老的四列表格"""
    return SOP_SPEC_STEPS if (sop_doc.get("steps") or []) else SOP_SPEC_PLAIN


def sop_content_with_spec(sop_doc):
    """给 {sop_content} 占位用：标准正文 + 输出要求（**整块都由代码生成**，配置里不再写格式 ✗）"""
    return sop_standard_text(sop_doc) + "\n\n" + sop_spec_for(sop_doc)


def sop_block_for_prompt(sop_doc):
    """合并流程用的整块（占位符 {{sop_block}} 的值）= 标准 + 输出要求"""
    return sop_content_with_spec(sop_doc)


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
    2026-09-14：新增 mergedPrompt —— 非空则走「一次调用完成三段分析」的合并模式。
    2026-10-08（CR-20261008-01 期2）：新增 sopCompareStepsPrompt ——
      **只放人格/框架，不含输出格式**（格式由代码按数据模型生成，见文件上方说明）。
      没配这个键时**完全退回老行为**（用 sopComparePrompt 的原文 + 平铺 content）
      ⇒ 配置与代码的部署先后都不会出现中间态 ✗（零窗口）✓"""
    global vision_prompt, summary_prompt, sop_compare_prompt, sop_compare_steps_prompt
    global max_tokens, retry_count, merged_prompt
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
            # ★ 2026-10-08：新的「按步骤逐项判定」模板（只含人格/框架，不含输出格式）
            #   没配 ⇒ 保持 None ⇒ 老路径完全按老行为跑（零窗口回退 ✓）
            if c.get("sopCompareStepsPrompt"):
                sop_compare_steps_prompt = c["sopCompareStepsPrompt"]
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


def fetch_sop(sop_id, device_id=""):
    """从 S2 拉取 SOP 标准内容（33号 /api/cam-sop）。失败或空返回 None。

    ★ 2026-10-07（CR-20261007-01）：带上 device_id —— S2 会做**二次校验**
      （这台设备绑定的岗位能不能用这条 SOP）。不在范围内 ⇒ S2 返回 {id:null}
      ⇒ 本函数返回 None ⇒ 分析照跑，只是**没有 SOP 标准可比**（降级，不报错）✓
    """
    if not sop_id or not CAM_INGEST_TOKEN or not S2_DBAPI:
        return None
    try:
        params = {"id": sop_id}
        if device_id:
            params["device"] = device_id
        r = requests.get(f"{S2_DBAPI}{SOP_PATH}", params=params,
                         headers={"Authorization": f"Bearer {CAM_INGEST_TOKEN}"}, timeout=10)
        if r.status_code == 200:
            d = r.json()
            if d.get("id"):
                log(f"已拉取 SOP: {d.get('name') or sop_id} (content {len(d.get('content',''))} 字)")
                return d
            if d.get("error") == "scope_denied":
                log(f"⚠️ SOP {sop_id} 不在设备 {device_id or '(未给)'} 的岗位范围内 ⇒ 本轮无标准可比（降级）")
                return None
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


def call_vision(key, image_paths, interval_sec=5, ref_images=None, ref_kind="selfie"):
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

    # ★ CR-20261126-01/03：主角锚定前缀（自拍 = 第 1 张；备案 = 前 k 张）
    ref_images = [p for p in (ref_images or []) if p]
    _txt = vision_prompt.format(n=len(image_paths), interval_sec=interval_sec)
    if ref_images:
        if ref_kind == "portrait":
            _txt = PORTRAIT_BLOCK.format(k=len(ref_images), n=len(image_paths)) + _txt
        else:
            _txt = PROTAGONIST_BLOCK.format(n=len(image_paths)) + _txt
        log(f"启用主角锚定（{ref_kind}，{len(ref_images)} 张参照）")
    content = [{"type": "text", "text": _txt}]
    for rp in ref_images:
        try:
            _rb = base64.b64encode(open(rp, "rb").read()).decode()
            content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{_rb}"}})
        except Exception as e:
            log(f"参照照读取失败（忽略）: {e}")
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


def call_merged(key, image_paths, audio_text, sop_doc, interval_sec=5, ref_images=None, ref_kind="selfie"):
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

    # 第三步：有 SOP 就逐项比对（标准 + 输出格式都由代码生成），没选就如实说明（不编造标准）
    if sop_doc:
        sop_block = sop_block_for_prompt(sop_doc)
        log("SOP 块：%s（%d 步）" % ("按步骤逐项判定" if (sop_doc.get("steps") or []) else "纯文本条目判定",
                                  len(sop_doc.get("steps") or [])))
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

    # ★ 把语音情绪作为**辅助**信息给模型（写在提示词最前面，这样不必改已保存的模板）
    emo_now = dominant_of(asr_meta.get("annotations"), "emotion")
    if emo_now:
        prompt = (
            "【系统附带的语音情绪识别（机器判断，仅供参考）】\n"
            f"主导情绪：{emo_now}\n"
            "⚠️ 这是对**整段录音**的判断，录音里可能有多人说话，不一定是该学员本人的情绪；\n"
            "   只有当 SOP 标准本身涉及服务态度/精神面貌时，才把它作为辅助依据，"
            "**不得据此单独判定不合格**。\n\n"
        ) + prompt

    # ★ CR-20261126-01/03：主角锚定（代码层前置，不改线上 cam-config.json）
    #   自拍 ⇒ 第 1 张参照；备案 ⇒ 前 k 张参照（且只用认人）
    ref_images = [p for p in (ref_images or []) if p]
    if ref_images:
        if ref_kind == "portrait":
            prompt = PORTRAIT_BLOCK.format(k=len(ref_images), n=len(image_paths)) + prompt
        else:
            prompt = PROTAGONIST_BLOCK.format(n=len(image_paths)) + prompt
        log(f"启用主角锚定（{ref_kind}，{len(ref_images)} 张参照）")

    content = [{"type": "text", "text": prompt}]
    for rp in ref_images:
        try:
            _rb = base64.b64encode(open(rp, "rb").read()).decode()
            content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{_rb}"}})
        except Exception as e:
            log(f"参照照读取失败（忽略）: {e}")
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
    """调用 asr_recognition.py 转文字；失败/无内容返回空串。

    ★ 2026-09-14 起，ffmpeg 预处理**搬到 asr_recognition.py 里**了，这里直接传原始音频。
    原因：新引擎 qwen3-asr-flash 有 300s 硬限，超长要分段，而**分段切点要在原始音频上做静音检测**；
    如果在调用方先 dynaudnorm 归一化，静音段底噪会被一起放大，之后根本检测不到静音
    （实测"剪静音"时 524s 只剪掉 1s，就是踩了这个）。
    预处理链仍是 highpass=f=80,dynaudnorm=f=150:g=15（不含 afftdn，见 asr_recognition.py）。

    超时给 600s：长录音要分 2~3 段、每段一次 HTTP + 一次 ffmpeg，比原来慢不少。
    """
    if not os.path.exists(wav_path):
        return ""
    global asr_meta
    try:
        r = subprocess.run(
            ["python3", ASR_SCRIPT, wav_path],
            capture_output=True, text=True, timeout=600,
        )
        out = r.stdout.strip()
        if not out:
            # 不静默返回空：把 rc 和 stderr 打出来，否则"转写为空"和"脚本报错"长得一模一样
            log(f"ASR 无输出: rc={r.returncode}, stderr={(r.stderr or '').strip()[:300]}")
            return ""
        j = json.loads(out)
        text = j.get("text", "") or ""
        eng = j.get("engine", "?")
        segs = j.get("segments", 1)
        anns = j.get("annotations") or []
        # 情绪/语种标注记进 asr_meta，后面写进 record 推给 S2（考核要看）
        asr_meta = {"annotations": anns, "engine": eng, "segments": segs}
        # 记一条可判断的日志：字数 / 时长 / 用了哪个引擎 / 分了几段。
        # ⚠️ 别只看"字/总时长"就断定转写差 —— 采集录音常常 80~90% 是静音，
        #    按总时长算会得出 0.4 字/秒的假象；按「字/有效语音秒」算才准（正常 3~5）。
        dur = 0.0
        try:
            import wave as _w
            with _w.open(wav_path, "rb") as _f:
                dur = _f.getnframes() / float(_f.getframerate() or 1)
        except Exception:
            pass
        log(f"ASR 结果 {len(text)} 字 / 音频 {dur:.0f}s / 引擎 {eng} / {segs} 段"
            + (f"（按总时长 {len(text)/dur:.2f} 字/秒；若偏低请先确认是不是大部分为静音）" if dur else ""))
        if anns:
            log("情绪/语种标注: " + json.dumps(anns[:6], ensure_ascii=False))
        return text
    except Exception as e:
        log(f"ASR 失败: {e}")
        return ""


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


# ══════════════════════════════════════════════════════════════════════
# ★★ 2026-11-27 SOP v2（用户拍板）：
#   · 有 SOP：① 录音分析（纪要/关键信息/表达问题）② 照片 × SOP 逐项研判（带转写辅助话术）
#              —— 不再单独输出"画面时序分析"
#   · 无 SOP：**只做录音分析**，不调用视觉模型（照片只留档，供日后补做 SOP）
#   · 研判分三层：L1 有没有做（语义容错）/ L2 做得怎么样（好坏+证据）/ L3 怎么提升（示范）
#   · 证据来源按 SOP 类型加权：会议→录音为主；服务→画面动作+录音话术；安全/操作→画面为主
# ══════════════════════════════════════════════════════════════════════

AUDIO_ANALYSIS_PROMPT = """你是门店现场的会议/沟通记录分析员。下面是现场录音的转写文本。
⚠️ 转写可能有同音字、断句、错字，请按语义理解，不要因为个别字不对就否定整句。

请只基于转写内容，输出三段 Markdown（不要输出 JSON）：

## 一、纪要
按时间顺序概括这段录音里发生了什么（谁在讲什么、大致流程）。

## 二、关键信息
列出录音里出现的关键信息：要求、规定、时间节点、人名/地点、数字、待办事项等。
（逐条列，能引用原话就引用原话）

## 三、表达问题
从"表达是否清楚、重点是否突出、有没有跑题/重复/含糊"的角度，指出 2~5 条问题；
每条要引用原话作为依据。若录音质量差/内容太少，如实说明。

硬规则：
- 不评价 SOP、不打分；
- 不编造录音里没有的内容；
- 若没有有效录音，三段都写"本次无有效录音"。"""

SOP_JUDGE_PROMPT = """你是门店 SOP 的教练型研判员。本次材料：
- ① 按时间顺序连续拍摄的 {{n}} 张现场画面（相邻约 {{interval_sec}} 秒）
- ② 现场录音转写（可能有同音字/断句错误，按语义理解，不要死抠字眼）
- ③ SOP 标准（类型：{{sop_type}}）

请对 SOP 的**每一步**做三层研判，输出一个 JSON 对象：{"sop_compare": "按步骤的 Markdown"}

每一步的格式固定为：

### 第 N 步 · 步骤名
**L1 有没有做**：做到 / 没做到 / 无法判断
- 证据：画面第 X 帧 / 录音原话「……」
**L2 做得怎么样**
- 优点：……
- 问题：……（对应维度：完整性 / 规范性 / 时机顺序 / 话术专业度 / 服务意识）
**L3 怎么提升**
- 下次这样做/这样说：「……」（结合本步问题给具体示范）

研判规则（必须遵守）：
1. **L1 只有三种结果**：做到 / 没做到 / 无法判断。"做了但不符合标准"必须判**做到**，把不符合的地方写进 L2，绝不能判成"没做到"或"无法判断"。
2. **语义容错**：只要意思到了就算做到。问候说"你好/您好/欢迎光临"都算；口号只要**有集合、有喊口号的动作**就算做到，口号文字与 SOP 不完全一致只写在 L2；话术只要求"表达了同样的意思"，不要求逐字一致。
3. **无法判断的门槛**：只有"画面没拍到 + 录音也没提到"才能判无法判断，并且必须写清"缺什么"（例：该步要求出示登记表，画面未拍到桌面、录音未提登记）。
4. **证据来源按 SOP 类型加权**：
   - 会议类：以**录音**为主要证据，画面只作佐证（有没有开会/有没有集合）；
   - 服务类：**动作看画面、话术看录音**；
   - 安全/操作类：以**画面**为主。
5. **L2 必须具体**：至少写 1 条优点 + 1 条问题（该步完全没做则写"未执行，无优点"），问题要能对应到画面或录音的证据。
6. **L3 每步都要给**：具体到"下次这句话怎么说""下一步怎么做"，给示范话术；不要写"加强培训/提高意识"这类空话。
7. **不许编造**：没看到就写"未拍到"，没听到就写"录音未提"；不要因为 SOP 里写了某个场景就假设现场发生了。
8. 步骤名原样照抄，不要合并/漏步；SOP 没有步骤时按条目逐条研判。

SOP 标准如下：
{{sop_block}}"""


def sop_category(sop_doc):
    """按名称/内容猜 SOP 类型（会议 / 服务 / 安全 / 操作）——只用于给研判定证据权重。"""
    text = ((sop_doc.get("name") or "") + " " + (sop_doc.get("content") or ""))
    if any(k in text for k in ("班前会", "班后会", "例会", "晨会", "会议", "开会", "传达")):
        return "会议"
    if any(k in text for k in ("安全", "消防", "应急", "演练", "点检")):
        return "安全"
    if any(k in text for k in ("操作", "设备", "清洁", "打扫", "收银", "开单", "登记")):
        return "操作"
    return "服务"


def sop_steps_text(sop_doc):
    """把 SOP 的 steps 摊平成给模型看的文本（不掺旧矩阵格式）。"""
    steps = sop_doc.get("steps") or []
    if not steps:
        return (sop_doc.get("content") or "").strip() or "（SOP 内容为空）"
    out = []
    for i, st in enumerate(steps, 1):
        name = st.get("name") or st.get("step") or ("第 %d 步" % i)
        parts = ["### 第 %d 步 · %s" % (i, name)]
        for k, label in (("action", "动作"), ("speech", "话术"), ("tool", "工具"),
                         ("note", "注意事项"), ("duration", "完成时间")):
            v = (st.get(k) or "").strip()
            if v:
                parts.append("- %s：%s" % (label, v))
        out.append("\n".join(parts))
    return "\n\n".join(out)


def call_audio_analysis(key, audio_text, max_tokens=4000):
    """录音分析（纪要 / 关键信息 / 表达问题）——文本模型，不涉及画面。"""
    prompt = AUDIO_ANALYSIS_PROMPT + "\n\n【录音转写】\n" + (audio_text or "(无有效录音)")
    return call_chat(key, prompt, max_tokens=min(max_tokens or 4000, 8000))


def call_sop_judge(key, image_paths, audio_text, sop_doc, interval_sec=5,
                   ref_images=None, ref_kind="selfie", max_tokens=100000):
    """照片 × SOP 三层研判（L1/L2/L3）——视觉模型，带录音转写辅助话术判定。"""
    # 超 600 张必须抽帧（平台硬上限）
    if len(image_paths) > MAX_MERGED_FRAMES:
        step = (len(image_paths) + MAX_MERGED_FRAMES - 1) // MAX_MERGED_FRAMES
        sampled = image_paths[::step]
        if sampled[-1] != image_paths[-1]:
            sampled.append(image_paths[-1])
        log("SOP 研判抽帧: %d → %d 帧（上限 %d）" % (len(image_paths), len(sampled), MAX_MERGED_FRAMES))
        image_paths = sampled

    content = []
    # 主角参照照放最前（不是现场画面，只用于识别主角）
    if ref_images:
        for rp in ref_images:
            try:
                b64 = base64.b64encode(open(rp, "rb").read()).decode()
                content.append({"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + b64}})
            except Exception:
                pass
    for ip in image_paths:
        b64 = base64.b64encode(open(ip, "rb").read()).decode()
        content.append({"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + b64}})

    prompt = fill_prompt(
        SOP_JUDGE_PROMPT,
        n=len(image_paths),
        interval_sec=interval_sec,
        sop_type=sop_category(sop_doc),
        sop_block=sop_steps_text(sop_doc),
    )
    if ref_images:
        prompt = ("【说明】最前面 %d 张是「主角参照照」（%s），只用于识别画面里的主角是谁，"
                  "不是现场画面；其后 %d 张才是现场画面。\n\n" % (len(ref_images), ref_kind, len(image_paths))) + prompt
    prompt += "\n\n【现场录音转写】\n" + (audio_text or "(无有效录音)")
    content.append({"type": "text", "text": prompt})

    payload = {
        "model": DEEPSEEK_FLASH,
        "messages": [{"role": "user", "content": content}],
        "max_tokens": int(max_tokens or 100000),
        "temperature": 0.2,
    }
    log("调用 %s 做 SOP 三层研判：%d 张图 / SOP 类型=%s ..." % (DEEPSEEK_FLASH, len(image_paths), sop_category(sop_doc)))
    j = http_json_post(DEEPSEEK_URL, key, payload, timeout=MERGED_TIMEOUT)
    msg = j["choices"][0]["message"]
    text = (msg.get("content") or "").strip() or (msg.get("reasoning_content") or "").strip()
    body = re.sub(r"^```(?:json)?|```$", "", text, flags=re.M).strip()
    try:
        d = json.loads(body)
        out = (d.get("sop_compare") or "").strip()
        if out:
            return out
    except Exception as e:
        log("SOP 研判 JSON 解析失败(%s)，原文直接返回" % e)
    return text


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
    # ★ CR-20261126-01/03：主角参照 —— ① 本次自拍 ref.jpg 优先 ② 否则用形象备案 ref_face/body/dev
    ref_images, ref_kind = [], "selfie"
    _selfie = os.path.join(batch_dir, "ref.jpg")
    if os.path.isfile(_selfie):
        ref_images, ref_kind = [_selfie], "selfie"
        log("检测到本次自拍参照照 ref.jpg ⇒ 启用主角锚定（自拍）")
    else:
        for _nm in ("ref_face.jpg", "ref_body.jpg", "ref_dev.jpg"):
            _p = os.path.join(batch_dir, _nm)
            if os.path.isfile(_p):
                ref_images.append(_p)
        if ref_images:
            ref_kind = "portrait"
            log(f"检测到形象备案参照 {len(ref_images)} 张 ⇒ 启用主角锚定（备案）")
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
    sop_doc = fetch_sop(sop_id, device_id) if sop_id else None
    if sop_id and not sop_doc:
        log(f"未找到 SOP（sop_id={sop_id}）")

    # DeepSeek key：两种模式都要用（优先用 /root/.deepseek-key 专用文件）
    chat_key = load_deepseek_key()
    if not chat_key:
        log("缺少 DEEPSEEK_API_KEY")
        sys.exit(1)

    if os.environ.get("SOP_V2", "1") != "0":
        # ★★ SOP v2：有 SOP 才看画面；无 SOP 只分析录音
        summary = ""
        vision = ""
        sop_compare = ""
        if sop_id and sop_doc and not no_image:
            from concurrent.futures import ThreadPoolExecutor
            def _audio_job():
                return call_audio_analysis(chat_key, audio_text, max_tokens)
            def _sop_job():
                return call_sop_judge(chat_key, imgs, audio_text, sop_doc, interval_sec,
                                      ref_images=ref_images, ref_kind=ref_kind, max_tokens=max_tokens)
            with ThreadPoolExecutor(max_workers=2) as ex:
                fa, fs = ex.submit(_audio_job), ex.submit(_sop_job)
                try:
                    summary = fa.result()
                except Exception as e:
                    log("录音分析失败: %s" % e)
                    summary = "录音分析失败: %s" % e
                try:
                    sop_compare = fs.result()
                except Exception as e:
                    log("SOP 研判失败: %s" % e)
                    sop_compare = "SOP 研判失败: %s" % e
            log("v2 完成：录音分析 %d 字 / SOP 研判 %d 字（画面时序不再单独输出）"
                % (len(summary), len(sop_compare)))
        else:
            try:
                summary = call_audio_analysis(chat_key, audio_text, max_tokens)
            except Exception as e:
                log("录音分析失败: %s" % e)
                summary = "录音分析失败: %s" % e
            if sop_id and sop_doc and no_image:
                sop_compare = "本次未采集画面（audio 模式），无法按画面做 SOP 研判。"
            elif sop_id and not sop_doc:
                sop_compare = "(未找到 SOP，sop_id=%s)" % sop_id
            else:
                sop_compare = "本次未选择 SOP 流程，未做标准比对。"
            log("v2 完成（无画面 / 无 SOP）：录音分析 %d 字" % len(summary))
    else:
        if merged_prompt:
            # ★★ 合并模式（2026-09-14 起）：deepseek-flash 一次调用出「画面时序 + 录音转写 + SOP 比对」
            try:
                r = call_merged(chat_key, imgs, audio_text, sop_doc, interval_sec, ref_images=ref_images, ref_kind=ref_kind)
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
                    vision = call_vision(vision_key, imgs, interval_sec, ref_images=ref_images, ref_kind=ref_kind)
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
                        # ★ 2026-10-08：配了 sopCompareStepsPrompt（只含人格/框架）时，
                        #   把「标准 + 输出格式」整块塞进 {sop_content} —— 格式只在代码里维护一份 ✓
                        #   没配 ⇒ 老行为（原文-content 进 {sop_content}），零窗口回退 ✓
                        if sop_compare_steps_prompt:
                            tpl = sop_compare_steps_prompt
                            sop_content_val = sop_content_with_spec(sop_doc)
                        else:
                            tpl = sop_compare_prompt
                            sop_content_val = sop_doc.get("content") or ""
                        sop_compare = call_chat(chat_key, tpl.format(
                            sop_name=sop_doc.get("name") or sop_id,
                            sop_content=sop_content_val,
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
        # ★ 语音情绪/语种（qwen3-asr-flash 的 annotations）—— 写进考核记录，管理端会展示
        # ⚠️ 这是对**整段录音**的机器判断：录音里可能有多人说话（实录里就出现过同事闲聊），
        #    所以它不等于"该学员本人的情绪"，只作服务态度的**辅助**参考，不能单独当评分依据。
        "emotion": dominant_of(asr_meta.get("annotations"), "emotion"),
        "language": dominant_of(asr_meta.get("annotations"), "language"),
        "asr_engine": asr_meta.get("engine") or "",
        "raw_json": json.dumps({
            "device_id": device_id, "frame_count": len(imgs),
            # 多段录音会有多条标注，原始列表留在 raw_json 里便于追溯
            "asr": {"engine": asr_meta.get("engine"), "segments": asr_meta.get("segments"),
                    "annotations": asr_meta.get("annotations") or []},
        }, ensure_ascii=False),
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
