from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import jieba
import pandas as pd

from .common_utils import find_column, read_csv, to_numeric
from .qwen_utils import ask_qwen_json, load_qwen_ask_func


# 复用 nationalWordCloud.py 的停用词逻辑
STOP_WORDS = {
    "哈哈哈哈", "哈哈", "这个", "那个", "每个", "一个", "两个", "有些", "就是", "不是", "还是", "但是",
    "而且", "然后", "所以", "因为", "真的", "确实", "其实", "简直", "有点", "好像", "一样", "这样",
    "那样", "怎么", "什么", "为什么", "哪里", "现在", "刚才", "之后", "之前", "感觉", "觉得", "认为",
    "以为", "发现", "知道", "看到", "起来", "出来", "你们", "我们", "他们", "人家", "自己", "这里",
    "那里", "每周", "必看", "弹幕", "视频", "画质", "老师", "演员", "前面", "后面", "这段", "可以", "没有",
    "还有", "应该", "可能", "意思", "真是", "好好",
}

VIDEO_ALIASES = {
    "province": ["省份", "province"],
    "opera": ["剧种", "opera"],
    "bvid": ["BV号", "bvid", "BVID", "bv"],
    "title": ["标题", "title"],
    "view": ["播放量", "view", "views"],
    "like": ["点赞数", "like", "likes"],
    "coin": ["投币数", "coin"],
    "favorite": ["收藏数", "favorite", "favorites"],
    "comment": ["评论数", "reply", "comment", "comments", "评论总数"],
    "danmaku": ["弹幕总量", "弹幕总数", "danmaku", "danmaku_count", "danmakuCount"],
}

COMMENTS_ALIASES = {
    "bvid": ["BV号", "bvid", "BVID", "bv"],
    "content": ["评论内容", "comment", "content"],
}

DANMAKU_ALIASES = {
    "bvid": ["BV号", "bvid", "BVID", "bv"],
    "progress_sec": ["视频进度(秒)", "弹幕时间", "progress", "progress_sec"],
    "text": ["弹幕内容", "弹幕文本", "danmaku", "content", "text"],
}


def _canonicalize_video_df(df: pd.DataFrame) -> pd.DataFrame:
    data = pd.DataFrame()
    for canonical_name, aliases in VIDEO_ALIASES.items():
        src_col = find_column(df, aliases)
        data[canonical_name] = df[src_col] if src_col is not None else ""

    if data["bvid"].astype(str).str.strip().eq("").all():
        raise ValueError("video_info.csv 未识别到 BV 号字段（如 BV号/bvid/BVID）。")

    text_cols = ["province", "opera", "bvid", "title"]
    for col in text_cols:
        data[col] = data[col].astype(str).str.strip()
        data[col] = data[col].replace({"nan": "", "None": "", "none": ""})

    numeric_cols = ["view", "like", "coin", "favorite", "comment", "danmaku"]
    for col in numeric_cols:
        data[col] = to_numeric(data[col])

    data = data[data["bvid"].ne("")].copy()
    data = data.drop_duplicates(subset=["bvid"], keep="first").reset_index(drop=True)
    data["title"] = data["title"].replace("", pd.NA).fillna(data["bvid"])
    return data


def _canonicalize_comments_df(df: pd.DataFrame) -> pd.DataFrame:
    bvid_col = find_column(df, COMMENTS_ALIASES["bvid"])
    content_col = find_column(df, COMMENTS_ALIASES["content"])
    if bvid_col is None:
        return pd.DataFrame(columns=["bvid", "content"])

    data = pd.DataFrame()
    data["bvid"] = df[bvid_col].astype(str).str.strip()
    data["content"] = df[content_col].astype(str).str.strip() if content_col is not None else ""
    data = data[data["bvid"].ne("")].copy()
    return data


def _canonicalize_danmaku_df(df: pd.DataFrame) -> pd.DataFrame:
    bvid_col = find_column(df, DANMAKU_ALIASES["bvid"])
    progress_col = find_column(df, DANMAKU_ALIASES["progress_sec"])
    text_col = find_column(df, DANMAKU_ALIASES["text"])
    if bvid_col is None:
        return pd.DataFrame(columns=["bvid", "progress_sec", "text"])

    data = pd.DataFrame()
    data["bvid"] = df[bvid_col].astype(str).str.strip()
    data["progress_sec"] = to_numeric(df[progress_col]) if progress_col is not None else 0.0
    data["text"] = df[text_col].astype(str).str.strip() if text_col is not None else ""
    data = data[data["bvid"].ne("")].copy()
    return data


def _min_max_normalize(series: pd.Series) -> pd.Series:
    min_val = float(series.min()) if len(series) else 0.0
    max_val = float(series.max()) if len(series) else 0.0
    if max_val == min_val:
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - min_val) / (max_val - min_val)


def _fill_count_by_detail(base_df: pd.DataFrame, detail_df: pd.DataFrame, target_col: str) -> None:
    if detail_df.empty:
        return
    grouped_count = detail_df.groupby("bvid").size()
    mapped_count = base_df["bvid"].map(grouped_count).fillna(0.0)
    base_df[target_col] = base_df[target_col].where(base_df[target_col] > 0, mapped_count)


def _calculate_indexes(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    view_norm = _min_max_normalize(result["view"])
    like_norm = _min_max_normalize(result["like"])
    favorite_norm = _min_max_normalize(result["favorite"])
    coin_norm = _min_max_normalize(result["coin"])
    comment_norm = _min_max_normalize(result["comment"])
    danmaku_norm = _min_max_normalize(result["danmaku"])
    spread_heat = (
        0.35 * view_norm
        + 0.20 * like_norm
        + 0.15 * favorite_norm
        + 0.15 * coin_norm
        + 0.10 * comment_norm
        + 0.05 * danmaku_norm
    )
    result["spreadHeat"] = (spread_heat * 100).round(1)

    view_safe = result["view"].replace(0, pd.NA)
    like_rate = (result["like"] / view_safe).fillna(0.0)
    coin_rate = (result["coin"] / view_safe).fillna(0.0)
    favorite_rate = (result["favorite"] / view_safe).fillna(0.0)
    comment_rate = (result["comment"] / view_safe).fillna(0.0)
    danmaku_rate = (result["danmaku"] / view_safe).fillna(0.0)
    interaction_quality = (
        0.25 * _min_max_normalize(like_rate)
        + 0.20 * _min_max_normalize(coin_rate)
        + 0.20 * _min_max_normalize(favorite_rate)
        + 0.20 * _min_max_normalize(comment_rate)
        + 0.15 * _min_max_normalize(danmaku_rate)
    )
    result["interactionQuality"] = (interaction_quality * 100).round(1)
    result["score"] = (0.6 * result["spreadHeat"] + 0.4 * result["interactionQuality"]).round(1)
    return result


def _segment_text(text: str) -> List[str]:
    if not text:
        return []
    return jieba.lcut(text)


def _extract_keywords(single_danmaku_df: pd.DataFrame, top_n: int = 20) -> List[Dict[str, Any]]:
    if single_danmaku_df.empty:
        return []
    words = _segment_text("".join(single_danmaku_df["text"].fillna("").astype(str).tolist()))
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
    return [{"word": word, "count": int(count)} for word, count in Counter(clean_words).most_common(top_n)]


def _sec_to_mmss(seconds: float) -> str:
    sec_int = int(max(seconds, 0))
    return f"{sec_int // 60:02d}:{sec_int % 60:02d}"


def _build_danmaku_trend(single_danmaku_df: pd.DataFrame, window_size: int = 10) -> Dict[str, List[Any]]:
    if single_danmaku_df.empty:
        return {"times": [], "counts": [], "maxDanmakus": []}

    work_df = single_danmaku_df.copy()
    work_df["progress_sec"] = to_numeric(work_df["progress_sec"])
    work_df = work_df.dropna(subset=["progress_sec"])
    if work_df.empty:
        return {"times": [], "counts": [], "maxDanmakus": []}

    work_df["time_window"] = (work_df["progress_sec"] // window_size) * window_size
    timeline = work_df.groupby("time_window").size().reset_index(name="count").sort_values("time_window")
    times = [_sec_to_mmss(x) for x in timeline["time_window"].tolist()]
    counts = [int(v) for v in timeline["count"].tolist()]
    peak_window = float(timeline.loc[timeline["count"].idxmax(), "time_window"])
    peak_df = work_df[work_df["time_window"] == peak_window]
    max_danmakus = [txt for txt in peak_df["text"].astype(str).str.strip().tolist() if txt][:50]
    return {"times": times, "counts": counts, "maxDanmakus": max_danmakus}


def _fallback_ai_payload() -> Dict[str, Any]:
    return {
        "summary": "该视频传播与互动数据已完成分析，可用于前端展示。",
        "insight": "当前观众讨论集中在高频词与高能弹幕对应片段，说明内容触发点较明确。",
        "advice": "建议持续强化高能片段传播，优化标题关键词并引导评论区互动。",
        "tags": ["戏曲传播", "弹幕热点", "运营优化"],
    }


def _normalize_ai_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    summary = str(payload.get("summary", "")).strip()
    insight = str(payload.get("insight", "")).strip()
    advice = str(payload.get("advice", "")).strip()
    tags_raw = payload.get("tags", [])
    tags: List[str] = []
    if isinstance(tags_raw, list):
        tags = [str(x).strip() for x in tags_raw if str(x).strip()]
    elif isinstance(tags_raw, str) and tags_raw.strip():
        tags = [x.strip() for x in re.split(r"[，,、\s]+", tags_raw) if x.strip()]

    if not summary:
        summary = "该视频传播表现稳定，核心数据已完成结构化分析。"
    if not insight:
        insight = "弹幕高频词与高能片段显示观众主要聚焦于表演张力和经典唱段。"
    if not advice:
        advice = "建议围绕高能片段做二创切条，并在标题与封面突出剧种特色。"
    if not tags:
        tags = ["戏曲传播", "弹幕互动", "内容优化"]
    return {"summary": summary[:60], "insight": insight[:120], "advice": advice[:120], "tags": tags[:3]}


def _build_qwen_prompt(
    province: str,
    opera: str,
    bvid: str,
    title: str,
    stats: Dict[str, int],
    spread_heat: float,
    interaction_quality: float,
    score: float,
    keywords: List[Dict[str, Any]],
    max_danmakus: List[str],
) -> str:
    keyword_text = "、".join([f"{item['word']}({item['count']})" for item in keywords[:20]]) if keywords else "无"
    peak_text = "；".join(max_danmakus[:20]) if max_danmakus else "无"
    return (
        "你是一名大数据分析师和传统戏曲文化传播顾问。\n\n"
        "以下是一个 B站地方戏曲视频的数据分析结果。\n\n"
        "【指标定义】\n"
        "传播热度指数用于衡量视频传播广度，计算公式为：\n"
        "传播热度指数 =\n"
        "0.35 × 播放量归一化\n"
        "+ 0.20 × 点赞数归一化\n"
        "+ 0.15 × 收藏数归一化\n"
        "+ 0.15 × 投币数归一化\n"
        "+ 0.10 × 评论数归一化\n"
        "+ 0.05 × 弹幕数归一化\n\n"
        "互动质量指数用于衡量观众观看后的主动参与程度，计算公式为：\n"
        "互动质量指数 =\n"
        "0.25 × 点赞率归一化\n"
        "+ 0.20 × 投币率归一化\n"
        "+ 0.20 × 收藏率归一化\n"
        "+ 0.20 × 评论率归一化\n"
        "+ 0.15 × 弹幕率归一化\n\n"
        "综合评分用于筛选代表视频，计算公式为：\n"
        "综合评分 = 0.6 × 传播热度指数 + 0.4 × 互动质量指数\n\n"
        "其中：\n"
        "点赞率 = 点赞数 / 播放量\n"
        "投币率 = 投币数 / 播放量\n"
        "收藏率 = 收藏数 / 播放量\n"
        "评论率 = 评论数 / 播放量\n"
        "弹幕率 = 弹幕数 / 播放量\n\n"
        "【视频信息】\n"
        f"剧种：{opera}\n"
        f"省份：{province}\n"
        f"标题：{title}\n"
        f"BV号：{bvid}\n\n"
        "【原始传播数据】\n"
        f"播放量：{stats['view']}\n"
        f"点赞数：{stats['like']}\n"
        f"投币数：{stats['coin']}\n"
        f"收藏数：{stats['favorite']}\n"
        f"评论数：{stats['comment']}\n"
        f"弹幕数：{stats['danmaku']}\n\n"
        "【计算结果】\n"
        f"传播热度指数：{spread_heat}\n"
        f"互动质量指数：{interaction_quality}\n"
        f"综合评分：{score}\n\n"
        "【弹幕高频词】\n"
        f"{keyword_text}\n\n"
        "【高能弹幕】\n"
        f"{peak_text}\n\n"
        "请严格基于以上数据进行分析，不要编造不存在的数据。\n\n"
        "请输出 JSON：\n"
        "{\n"
        '  "summary": "50字以内总结该视频传播表现",\n'
        '  "insight": "100字以内分析观众关注点与传播原因",\n'
        '  "advice": "100字以内给戏曲传播方的优化建议",\n'
        '  "tags": ["标签1", "标签2", "标签3"]\n'
        "}\n\n"
        "要求：\n"
        "1. 只输出 JSON；\n"
        "2. 不要输出 Markdown；\n"
        "3. 不要解释公式；\n"
        "4. 不要编造播放量、点赞量等未给出的信息；\n"
        "5. 分析必须结合传播热度指数、互动质量指数、综合评分、弹幕高频词和高能弹幕。"
    )


def _generate_ai_analysis(
    ask_qwen_func: Optional[Callable[..., Optional[str]]],
    qwen_state: Dict[str, bool],
    province: str,
    opera: str,
    bvid: str,
    title: str,
    stats: Dict[str, int],
    spread_heat: float,
    interaction_quality: float,
    score: float,
    keywords: List[Dict[str, Any]],
    max_danmakus: List[str],
) -> Dict[str, Any]:
    prompt = _build_qwen_prompt(
        province=province,
        opera=opera,
        bvid=bvid,
        title=title,
        stats=stats,
        spread_heat=spread_heat,
        interaction_quality=interaction_quality,
        score=score,
        keywords=keywords,
        max_danmakus=max_danmakus,
    )
    parsed = ask_qwen_json(ask_qwen_func, qwen_state, prompt, _fallback_ai_payload())
    return _normalize_ai_payload(parsed)


def _extract_opera_from_title(title: str) -> Optional[str]:
    title_text = str(title).strip()
    if not title_text:
        return None
    match = re.search(r"([\u4e00-\u9fa5]{1,8}(?:剧|戏|腔|调|曲))", title_text)
    if match:
        return match.group(1).strip()
    bracket_match = re.search(r"《([^》]{1,20})》", title_text)
    if bracket_match:
        return bracket_match.group(1).strip()
    return None


def _resolve_opera_name(video_obj: Dict[str, Any]) -> str:
    for key in ["opera", "operaName", "opera_name", "剧种"]:
        value = str(video_obj.get(key, "")).strip()
        if value.lower() in {"nan", "none"}:
            value = ""
        if value:
            return value
    return _extract_opera_from_title(str(video_obj.get("title", ""))) or "未知剧种"


def _build_representative_videos(videos: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    representative: Dict[str, Dict[str, Any]] = {}
    for video in videos:
        opera_name = _resolve_opera_name(video)
        current_score = float(video.get("indexes", {}).get("score", 0.0))
        existing = representative.get(opera_name)
        if existing is None or current_score > float(existing.get("indexes", {}).get("score", 0.0)):
            representative[opera_name] = video
    return representative


def build_video_analysis_data(
    video_info_path: Path,
    comments_path: Path,
    danmaku_path: Path,
    qwen_script_path: Optional[Path] = None,
) -> Dict[str, Any]:
    video_df = read_csv(video_info_path)
    comments_df = read_csv(comments_path)
    danmaku_df = read_csv(danmaku_path)

    canonical_video = _canonicalize_video_df(video_df)
    canonical_comments = _canonicalize_comments_df(comments_df)
    canonical_danmaku = _canonicalize_danmaku_df(danmaku_df)
    _fill_count_by_detail(canonical_video, canonical_comments, target_col="comment")
    _fill_count_by_detail(canonical_video, canonical_danmaku, target_col="danmaku")
    canonical_video = _calculate_indexes(canonical_video)

    if qwen_script_path is None:
        qwen_script_path = Path(__file__).resolve().parents[1] / "Qwen_Analysis.py"
    ask_qwen_func = load_qwen_ask_func(qwen_script_path, module_name="legacy_qwen_analysis_video")
    qwen_state = {"disabled": ask_qwen_func is None}

    videos: List[Dict[str, Any]] = []
    for _, row in canonical_video.iterrows():
        bvid = str(row["bvid"])
        single_danmaku = canonical_danmaku[canonical_danmaku["bvid"] == bvid]
        keywords = _extract_keywords(single_danmaku, top_n=20)
        trend = _build_danmaku_trend(single_danmaku, window_size=10)
        stats = {
            "view": int(round(float(row["view"]))),
            "like": int(round(float(row["like"]))),
            "coin": int(round(float(row["coin"]))),
            "favorite": int(round(float(row["favorite"]))),
            "comment": int(round(float(row["comment"]))),
            "danmaku": int(round(float(row["danmaku"]))),
        }
        spread_heat = float(row["spreadHeat"])
        interaction_quality = float(row["interactionQuality"])
        score = float(row["score"])
        ai_analysis = _generate_ai_analysis(
            ask_qwen_func=ask_qwen_func,
            qwen_state=qwen_state,
            province=str(row["province"]),
            opera=str(row["opera"]),
            bvid=bvid,
            title=str(row["title"]),
            stats=stats,
            spread_heat=spread_heat,
            interaction_quality=interaction_quality,
            score=score,
            keywords=keywords,
            max_danmakus=trend["maxDanmakus"],
        )
        videos.append(
            {
                "province": str(row["province"]),
                "opera": str(row["opera"]),
                "bvid": bvid,
                "title": str(row["title"]),
                "stats": stats,
                "indexes": {"spreadHeat": spread_heat, "interactionQuality": interaction_quality, "score": score},
                "keywords": keywords,
                "danmakuTrend": trend,
                "aiAnalysis": ai_analysis,
            }
        )
    return {"videos": videos, "representativeVideos": _build_representative_videos(videos)}
