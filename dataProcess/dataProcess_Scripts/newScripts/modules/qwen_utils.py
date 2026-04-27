from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


DEFAULT_SYSTEM_ROLE = "你是一个资深的大数据分析师与戏曲文化推广专家。"
MAX_CONSECUTIVE_FAILURES = 3


def _mark_qwen_failure(qwen_state: Dict[str, Any]) -> None:
    failure_count = int(qwen_state.get("consecutive_failures", 0)) + 1
    qwen_state["consecutive_failures"] = failure_count
    if failure_count >= MAX_CONSECUTIVE_FAILURES:
        qwen_state["disabled"] = True


def _mark_qwen_success(qwen_state: Dict[str, Any]) -> None:
    qwen_state["consecutive_failures"] = 0


def load_qwen_ask_func(qwen_script_path: Path, module_name: str) -> Optional[Callable[..., Optional[str]]]:
    if not qwen_script_path.exists():
        return None
    spec = importlib.util.spec_from_file_location(module_name, qwen_script_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception:
        return None
    ask_func = getattr(module, "ask_qwen", None)
    return ask_func if callable(ask_func) else None


def try_parse_json_block(raw_text: str) -> Optional[Any]:
    if not raw_text:
        return None
    cleaned = raw_text.strip().replace("```json", "").replace("```", "").strip()
    decoder = json.JSONDecoder()
    try:
        return json.loads(cleaned)
    except Exception:
        pass
    for idx, char in enumerate(cleaned):
        if char not in {"{", "["}:
            continue
        try:
            obj, _ = decoder.raw_decode(cleaned[idx:])
            return obj
        except Exception:
            continue
    return None


def ask_qwen_json(
    ask_qwen_func: Optional[Callable[..., Optional[str]]],
    qwen_state: Dict[str, Any],
    prompt: str,
    fallback: Dict[str, Any],
    system_role: str = DEFAULT_SYSTEM_ROLE,
) -> Dict[str, Any]:
    if ask_qwen_func is None or qwen_state.get("disabled", False):
        return fallback
    try:
        raw_text = ask_qwen_func(prompt, system_role=system_role)
    except TypeError:
        try:
            raw_text = ask_qwen_func(prompt)
        except Exception:
            _mark_qwen_failure(qwen_state)
            return fallback
    except Exception:
        _mark_qwen_failure(qwen_state)
        return fallback

    if not raw_text:
        _mark_qwen_failure(qwen_state)
        return fallback
    parsed = try_parse_json_block(raw_text)
    if not isinstance(parsed, dict):
        _mark_qwen_failure(qwen_state)
        return fallback
    _mark_qwen_success(qwen_state)
    return parsed


def ask_qwen_list(
    ask_qwen_func: Optional[Callable[..., Optional[str]]],
    qwen_state: Dict[str, Any],
    prompt: str,
    fallback: List[Dict[str, Any]],
    system_role: str = DEFAULT_SYSTEM_ROLE,
) -> List[Dict[str, Any]]:
    if ask_qwen_func is None or qwen_state.get("disabled", False):
        return fallback
    try:
        raw_text = ask_qwen_func(prompt, system_role=system_role)
    except TypeError:
        try:
            raw_text = ask_qwen_func(prompt)
        except Exception:
            _mark_qwen_failure(qwen_state)
            return fallback
    except Exception:
        _mark_qwen_failure(qwen_state)
        return fallback

    if not raw_text:
        _mark_qwen_failure(qwen_state)
        return fallback
    parsed = try_parse_json_block(raw_text)
    if not isinstance(parsed, list):
        _mark_qwen_failure(qwen_state)
        return fallback
    _mark_qwen_success(qwen_state)
    return parsed


def qwen_cooldown(qwen_state: Dict[str, Any], seconds: float = 0.35) -> None:
    if not qwen_state.get("disabled", False):
        time.sleep(seconds)
