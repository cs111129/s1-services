#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""portrait_check.py — 形象备案照片质检（CR-20261126-03 优化点3）

用法：python3 portrait_check.py <staging目录>
输出：stdout 最后一行 JSON：{"ok":bool,"score":0-100,"issues":[{"slot","reason"}],"summary":str}
退出码：0=已给出结论（无论 ok 真假）；1=质检服务异常（调用方按 fail-closed 处理）

★ 为什么单独脚本：语音服务跑在 Node，图片质检要用 qwen-vl（Python 侧已有 key/请求封装）
  ⇒ 复用 cam_analyze 的 http_json_post / load_key，避免第二套实现
"""
import base64
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from cam_analyze import (DEEPSEEK_FLASH, DEEPSEEK_URL, http_json_post,
                             load_deepseek_key, log)
except Exception as e:  # pragma: no cover
    sys.stderr.write(f"[portrait_check] 导入 cam_analyze 失败: {e}\n")
    sys.exit(1)

SLOT_LABEL = {
    "face_center": "正脸（正对镜头）",
    "face_left": "左脸（向左转头约45°）",
    "face_right": "右脸（向右转头约45°）",
    "half_front": "半身正面（腰部以上）",
    "half_left": "左半身（身体左侧朝镜头）",
    "half_right": "右半身（身体右侧朝镜头）",
    "body_front": "半身正面（旧版）",
    "body_back": "背面（旧版）",
    "body_side": "侧面（旧版）",
    "device_front": "设备机位·正面",
    "device_side": "设备机位·侧面",
}
ORDER = ["face_center", "face_left", "face_right", "half_front", "half_left", "half_right",
         "device_front", "device_side", "body_front", "body_back", "body_side"]
REQUIRED = ["face_center", "face_left", "face_right", "half_front", "half_left", "half_right"]

PROMPT = """你是「胤隆会」实操考核的**形象备案质检员**。下面是同一个人用于人脸/形象识别的备案照（按顺序附上，每张前面会说明它应该是什么）。

请逐张检查，并给出整体结论。检查项：
1. **有人**：画面里有一个清晰可见的人（不是空镜/只剩背景）；
2. **脸可辨**：正脸/左右脸这三张，脸要清晰可辨（允许轻微模糊，但不能糊到认不出）；
3. **朝向对**：正脸≈正对镜头；左脸≈向左转头；右脸≈向右转头（左右反了也能用，但要指出）；
4. **半身**：半身照要能看到腰以上（不需要全身）；
5. **遮挡**：口罩/帽子/墨镜遮住关键五官 ⇒ 不合格；
6. **同一人**：所有照片应是同一个人；
7. **可用性**：这些照片合起来，能不能在监控画面里认出这个人。

判定口径（**宽松但守住底线**）：只要每张都"有人 + 关键部位可辨"，就应判 ok；轻微模糊、构图不完美、背景杂乱都**不**算不通过。
只有"空镜/严重模糊/遮挡关键五官/明显不是同一人/朝向完全不对"才判不通过，并在 issues 里指出是哪一张（slot）。

只输出一个 JSON 对象（不要 markdown 围栏、不要解释），格式：
{"ok": true/false, "score": 0-100, "issues": [{"slot": "face_left", "reason": "..."}], "summary": "一句话结论"}
issues 里的 slot 必须用我给的文件名（不含 .jpg）。"""


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("用法: python3 portrait_check.py <dir>\n")
        sys.exit(1)
    d = sys.argv[1]
    imgs = []
    for slot in ORDER:
        p = os.path.join(d, slot + ".jpg")
        if os.path.isfile(p):
            imgs.append((slot, p))
    if not imgs:
        print(json.dumps({"ok": False, "score": 0, "issues": [], "summary": "没有找到任何照片"}, ensure_ascii=False))
        return
    missing = [s for s in REQUIRED if not os.path.isfile(os.path.join(d, s + ".jpg"))]
    key = load_deepseek_key()
    if not key:
        sys.stderr.write("[portrait_check] 缺少 DEEPSEEK_API_KEY\n")
        sys.exit(1)

    content = [{"type": "text", "text": PROMPT}]
    for slot, path in imgs:
        content.append({"type": "text", "text": f"【{slot}】{SLOT_LABEL.get(slot, slot)}"})
        b64 = base64.b64encode(open(path, "rb").read()).decode()
        content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}})
    payload = {"model": DEEPSEEK_FLASH, "messages": [{"role": "user", "content": content}],
               "max_tokens": 8000, "temperature": 0.1}
    try:
        j = http_json_post(DEEPSEEK_URL, key, payload, timeout=120)
        text = (j["choices"][0]["message"].get("content") or "").strip()
        if not text:
            text = (j["choices"][0]["message"].get("reasoning_content") or "").strip()
    except Exception as e:
        sys.stderr.write(f"[portrait_check] 调用视觉模型失败: {e}\n")
        sys.exit(1)

    import re
    body = re.sub(r"^```(?:json)?|```$", "", text, flags=re.M).strip()
    try:
        r = json.loads(body)
        if not isinstance(r, dict):
            raise ValueError("not object")
    except Exception:
        # 模型没按 JSON 返回 ⇒ 不让用户白等：给保守结论（不通过 + 原文摘要）
        print(json.dumps({"ok": False, "score": 0, "issues": [],
                          "summary": "质检模型返回格式异常，请重试（" + text[:80] + "）"},
                         ensure_ascii=False))
        return
    issues = r.get("issues") if isinstance(r.get("issues"), list) else []
    ok = bool(r.get("ok"))
    score = r.get("score")
    try:
        score = int(score)
    except Exception:
        score = 0
    # ★ 缺必填槽位 ⇒ 一律不通过（即使模型说 ok）
    if missing:
        ok = False
        issues = issues + [{"slot": s, "reason": "缺这张"} for s in missing]
    print(json.dumps({"ok": ok, "score": score, "issues": issues,
                      "summary": str(r.get("summary") or "")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
