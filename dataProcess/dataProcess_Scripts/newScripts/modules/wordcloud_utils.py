from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any, Callable, Dict, List, Optional

import jieba
import pandas as pd

from .common_utils import safe_float
from .qwen_utils import ask_qwen_list


# 复用 nationalWordCloud.py 的停用词逻辑
STOP_WORDS = {
    "哈哈哈哈", "哈哈", "这个", "那个", "每个", "一个", "两个", "有些", "就是", "不是", "还是", "但是",
    "而且", "然后", "所以", "因为", "真的", "确实", "其实", "简直", "有点", "好像", "一样", "这样",
    "那样", "怎么", "什么", "为什么", "哪里", "现在", "刚才", "之后", "之前", "感觉", "觉得", "认为",
    "以为", "发现", "知道", "看到", "起来", "出来", "你们", "我们", "他们", "人家", "自己", "这里",
    "那里", "每周", "必看", "弹幕", "视频", "画质", "老师", "演员", "前面", "后面", "这段", "可以", "没有",
    "还有", "应该", "可能", "意思", "真是", "好好",
}


def _segment_text(text: str) -> List[str]:
    if not text:
        return []
    return jieba.lcut(text)


def build_word_cloud(danmaku_df: pd.DataFrame, top_n: int = 100) -> List[Dict[str, Any]]:
    if danmaku_df.empty:
        return []
    words = _segment_text("".join(danmaku_df["text"].fillna("").astype(str).tolist()))
    clean_words: List[str] = []
    for word in words:
        w = str(word).strip()
        if len(w) <= 1:
            continue
        if w in STOP_WORDS:
            continue
        if re.fullmatch(r"[0-9]+", w):
            continue
        if re.fullmatch(r"[\u4e00-\u9fa5]+", w):
            clean_words.append(w)
    return [{"name": word, "value": int(count)} for word, count in Counter(clean_words).most_common(top_n)]


def build_province_word_clouds(danmaku_df: pd.DataFrame, video_df: pd.DataFrame, top_n: int = 100) -> Dict[str, List[Dict[str, Any]]]:
    if danmaku_df.empty or video_df.empty:
        return {}
    merged = danmaku_df.merge(video_df[["bvid", "province"]], on="bvid", how="left")
    merged = merged[merged["province"].astype(str).str.strip().ne("")].copy()
    if merged.empty:
        return {}
    return {str(province): build_word_cloud(group, top_n=top_n) for province, group in merged.groupby("province")}


def format_sentiment_score(score_value: Any) -> str:
    score = safe_float(score_value, default=0.0)
    score = max(-1.0, min(1.0, score))
    if abs(score) < 1e-12:
        return "0.00"
    return f"{score:+.2f}"


def normalize_word_cloud_items(word_cloud: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in word_cloud:
        name = str(item.get("name", item.get("word", ""))).strip()
        if not name:
            continue
        value = int(round(safe_float(item.get("value", item.get("count", 0.0)), default=0.0)))
        normalized.append({"name": name, "value": value, "word": name, "count": value})
    return normalized


def fallback_word_cloud_result(words: List[str]) -> List[Dict[str, Any]]:
    return [{"word": word, "sentiment": "中性", "analysis": "暂无分析", "score": "0.00"} for word in words]


def build_word_cloud_sentiment_prompt(words: List[str]) -> str:
    return (
        "你是一名文本情感分析专家。\n\n"
        "以下是来自戏曲视频弹幕的高频词：\n\n"
        f"{json.dumps(words, ensure_ascii=False)}\n\n"
        "请对每个词进行分析：\n\n"
        "输出 JSON 数组：\n\n"
        "[\n"
        "  {\n"
        '    "word": "词语",\n'
        '    "sentiment": "情感类别（如：高度喜爱 / 正向 / 中性 / 调侃 / 负面）",\n'
        '    "analysis": "一句话解释该词反映的观众情绪",\n'
        '    "score": "情感强度，范围 -1.00 到 +1.00"\n'
        "  }\n"
        "]\n\n"
        "要求：\n"
        "1. 必须覆盖所有输入词\n"
        "2. score 为字符串格式，如 \"+0.93\"\n"
        "3. 不要输出 Markdown\n"
        "4. 不要遗漏字段\n"
        "5. 情感判断要符合弹幕语境（如“哈哈哈”通常是正向）"
    )


def merge_word_cloud_sentiment(
    word_cloud: List[Dict[str, Any]],
    ask_qwen_func: Optional[Callable[..., Optional[str]]],
    qwen_state: Dict[str, bool],
    top_n: int = 15,
) -> List[Dict[str, Any]]:
    items = normalize_word_cloud_items(word_cloud)
    if not items:
        return []

    top_words = [item["word"] for item in items[:top_n]]
    fallback_list = fallback_word_cloud_result(top_words)
    prompt = build_word_cloud_sentiment_prompt(top_words)
    qwen_list = ask_qwen_list(ask_qwen_func, qwen_state, prompt, fallback_list)

    sentiment_map: Dict[str, Dict[str, str]] = {}
    for row in qwen_list:
        if not isinstance(row, dict):
            continue
        word = str(row.get("word", "")).strip()
        if not word:
            continue
        sentiment_map[word] = {
            "sentiment": str(row.get("sentiment", "")).strip() or "中性",
            "analysis": str(row.get("analysis", "")).strip() or "暂无分析",
            "score": format_sentiment_score(row.get("score", "0.00")),
        }

    for item in items:
        payload = sentiment_map.get(item["word"], {"sentiment": "中性", "analysis": "暂无分析", "score": "0.00"})
        item.update(payload)
    return items
