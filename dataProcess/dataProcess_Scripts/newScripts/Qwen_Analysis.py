from __future__ import annotations

import os
from pathlib import Path
import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[3]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)
print("[Qwen] .env path:", ENV_PATH)
print("[Qwen] .env exists:", ENV_PATH.exists())
print("[Qwen] key loaded:", bool(os.getenv("DASHSCOPE_API_KEY")))

BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MODEL_NAME = "qwen-plus"
DEFAULT_SYSTEM_ROLE = "你是一个资深的大数据分析师与戏曲文化推广专家。"
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 2


def ask_qwen(prompt: str, system_role: str | None = None) -> str | None:
    api_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        print("[Qwen] API key missing")
        return None

    messages = [
        {"role": "system", "content": system_role or DEFAULT_SYSTEM_ROLE},
        {"role": "user", "content": prompt},
    ]
    payload = {
        "model": MODEL_NAME,
        "messages": messages,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    url = f"{BASE_URL}/chat/completions"

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()
            choices = data.get("choices")
            if not isinstance(choices, list) or not choices:
                print("[Qwen] request failed: empty choices")
                if attempt < MAX_RETRIES:
                    continue
                return None
            message = choices[0].get("message", {})
            content = message.get("content") if isinstance(message, dict) else None
            if not isinstance(content, str) or not content.strip():
                print("[Qwen] request failed: empty content")
                if attempt < MAX_RETRIES:
                    continue
                return None
            print("[Qwen] success")
            return content
        except Exception as exc:
            print(f"[Qwen] request failed: {exc}")
            if attempt >= MAX_RETRIES:
                return None

    return None


if __name__ == "__main__":
    result = ask_qwen(
        "请只输出 JSON：{\"ok\": true, \"message\": \"测试成功\"}",
        system_role="你是一个 JSON 输出助手。",
    )
    print(result)
