#!/usr/bin/env python3
import sys
import json
import os
import wave
from dashscope.audio.asr import Recognition


def extract_text_from_result(result):
    """从 Recognition.call() 的返回结果中提取文字"""
    if not result or result.status_code != 200:
        return None

    # get_sentence() in call() mode returns a list of sentence dicts
    sentences = result.get_sentence()
    if isinstance(sentences, list):
        full_text = ""
        for s in sentences:
            if isinstance(s, dict) and s.get('text'):
                full_text += s['text']
        return full_text.strip() or None

    # Also try result.output for safety
    output = getattr(result, 'output', None) or {}
    if isinstance(output, dict):
        sentence_list = output.get('sentence', [])
        if isinstance(sentence_list, list):
            full_text = ''
            for s in sentence_list:
                if isinstance(s, dict) and s.get('text'):
                    full_text += s['text']
            return full_text.strip() or None

    return None


def recognize_audio(audio_path):
    """识别音频文件，返回文字结果。
    按 WAV 实际采样率选择模型+采样率：8kHz 录音用 8k 模型，16kHz 用 16k 模型
    （固件已降采样到 8kHz，sample_rate 必须与音频一致，否则语速错位识别乱）。"""
    try:
        with wave.open(audio_path, 'rb') as w:
            sr = w.getframerate()
    except Exception:
        sr = 16000   # 读不到则默认 16k

    if sr >= 16000:
        models_to_try = [('paraformer-realtime-v2', 16000)]
    else:
        models_to_try = [('paraformer-realtime-8k-v2', 8000)]

    for model, rate in models_to_try:
        try:
            cb = _EmptyCallback()
            recognition = Recognition(
                model=model,
                callback=cb,
                format='wav',
                sample_rate=rate
            )
            result = recognition.call(audio_path)
            text = extract_text_from_result(result)
            if text:
                return text
        except Exception:
            continue  # 尝试下一个模型

    return None


class _EmptyCallback:
    """DashScope Recognition 必需的回调（call() 模式下回调不触发，但必须提供）"""
    def on_open(self):
        pass

    def on_close(self):
        pass

    def on_complete(self):
        pass

    def on_error(self, result):
        pass

    def on_event(self, result):
        pass


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(json.dumps({'error': '缺少音频文件路径参数'}), file=sys.stderr)
        sys.exit(1)

    audio_path = sys.argv[1]

    if not os.path.exists(audio_path):
        print(json.dumps({'error': f'音频文件不存在: {audio_path}'}), file=sys.stderr)
        sys.exit(1)

    try:
        text = recognize_audio(audio_path)
        if text:
            print(json.dumps({'text': text}, ensure_ascii=False))
        else:
            print(json.dumps({'error': '识别结果为空'}), file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(json.dumps({'error': str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)
