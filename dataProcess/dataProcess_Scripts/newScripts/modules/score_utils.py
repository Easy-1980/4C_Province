from __future__ import annotations

import json
import math
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .common_utils import safe_float
from .qwen_utils import ask_qwen_json


def extract_video_rows(video_analysis_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    videos = video_analysis_data.get("videos", [])
    if not isinstance(videos, list):
        return []

    rows: List[Dict[str, Any]] = []
    for item in videos:
        if not isinstance(item, dict):
            continue

        province = str(item.get("province", "")).strip() or "未知省份"
        indexes = item.get("indexes", {}) if isinstance(item.get("indexes"), dict) else {}
        score = safe_float(indexes.get("score"), default=float("nan"))
        if math.isnan(score):
            spread = safe_float(indexes.get("spreadHeat"), default=0.0)
            interaction = safe_float(indexes.get("interactionQuality"), default=0.0)
            score = round(0.6 * spread + 0.4 * interaction, 1)
        rows.append({"province": province, "score": round(score, 1)})
    return rows


def compute_thresholds(values: List[float]) -> Tuple[float, float, bool]:
    clean_values = [float(v) for v in values if not math.isnan(float(v))]
    if len(clean_values) < 5:
        return 50.0, 70.0, True
    series = pd.Series(clean_values)
    low = float(series.quantile(0.33))
    high = float(series.quantile(0.66))
    if low >= high:
        return 50.0, 70.0, True
    return low, high, False


def score_level(value: float, low: float, high: float) -> str:
    if value >= high:
        return "强传播"
    if value >= low:
        return "中等传播"
    return "弱传播"


def resource_level(opera_count: int) -> str:
    if opera_count >= 25:
        return "多"
    if opera_count >= 10:
        return "中"
    return "少"


def spread_level(avg_score: float, low: float, high: float) -> str:
    if avg_score >= high:
        return "高"
    if avg_score >= low:
        return "中"
    return "低"


def structure_type(resource_lv: str, spread_lv: str, video_count: int) -> str:
    if video_count <= 0:
        return "样本不足"
    mapping = {
        ("多", "高"): "均衡发展型",
        ("多", "中"): "资源转化提升型",
        ("多", "低"): "资源待激活型",
        ("中", "高"): "潜力成长型",
        ("中", "中"): "稳步发展型",
        ("中", "低"): "传播提质型",
        ("少", "高"): "特色突破型",
        ("少", "中"): "基础培育型",
        ("少", "低"): "起步孵化型",
    }
    return mapping.get((resource_lv, spread_lv), "传播发展型")


def fallback_national_ai() -> Dict[str, str]:
    return {
        "analysis": "剧种数量高的省份整体评分更稳定，但并非剧种越多评分越高。",
        "examples": "部分省份剧种数量多但平均评分一般，也有剧种数量中等却传播效率更高的情况。",
        "advice": "建议按剧种建立分层传播策略，优先放大高分样本内容并优化弱势剧种运营。",
    }


def normalize_national_ai(payload: Dict[str, Any]) -> Dict[str, str]:
    fallback = fallback_national_ai()
    return {
        "analysis": str(payload.get("analysis", "")).strip() or fallback["analysis"],
        "examples": str(payload.get("examples", "")).strip() or fallback["examples"],
        "advice": str(payload.get("advice", "")).strip() or fallback["advice"],
    }


def fallback_spread_ai() -> Dict[str, str]:
    return {
        "analysis": "该省传播结构已完成分层识别，当前以样本内综合评分分布为依据。",
        "advice": "建议围绕高分内容扩散并修复弱传播段，形成稳定的省域传播梯度。",
    }


def normalize_spread_ai(payload: Dict[str, Any]) -> Dict[str, str]:
    fallback = fallback_spread_ai()
    return {
        "analysis": str(payload.get("analysis", "")).strip() or fallback["analysis"],
        "advice": str(payload.get("advice", "")).strip() or fallback["advice"],
    }


def build_province_score_stats(
    video_rows: List[Dict[str, Any]],
    opera_province_data: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    scores_by_province: Dict[str, List[float]] = defaultdict(list)
    for row in video_rows:
        scores_by_province[str(row["province"])].append(float(row["score"]))

    all_provinces = set(opera_province_data.keys()) | set(scores_by_province.keys())
    stats: Dict[str, Dict[str, Any]] = {}
    for province in sorted(all_provinces):
        scores = scores_by_province.get(province, [])
        video_count = len(scores)
        avg_score = round(sum(scores) / video_count, 1) if video_count else 0.0
        stats[province] = {"scores": scores, "videoCount": video_count, "avgScore": avg_score}
    return stats


def build_province_score_top10(
    province_score_stats: Dict[str, Dict[str, Any]],
    opera_province_data: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for province, stat in province_score_stats.items():
        video_count = int(stat["videoCount"])
        if video_count <= 0:
            continue
        opera_part = opera_province_data.get(province, {})
        items.append(
            {
                "province": province,
                "avgScore": round(float(stat["avgScore"]), 1),
                "videoCount": video_count,
                "operaCount": int(opera_part.get("operaCount", 0)),
                "operas": opera_part.get("operas", []),
            }
        )
    items.sort(key=lambda x: (x["avgScore"], x["videoCount"]), reverse=True)
    return items[:10]


def build_province_opera_count_score_compare(
    province_score_stats: Dict[str, Dict[str, Any]],
    opera_province_data: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for province, opera_part in opera_province_data.items():
        stat = province_score_stats.get(province, {"avgScore": 0.0, "videoCount": 0})
        items.append(
            {
                "province": province,
                "operaCount": int(opera_part.get("operaCount", 0)),
                "avgScore": round(float(stat.get("avgScore", 0.0)), 1),
                "videoCount": int(stat.get("videoCount", 0)),
            }
        )
    items.sort(key=lambda x: (x["operaCount"], x["avgScore"]), reverse=True)
    return items[:10]


def build_national_opera_count_score_ai(
    compare_data: List[Dict[str, Any]],
    ask_qwen_func: Optional[Callable[..., Optional[str]]],
    qwen_state: Dict[str, bool],
) -> Dict[str, str]:
    prompt = (
        "你是一名戏曲传播数据分析师。\n\n"
        "数据如下：\n"
        f"{json.dumps(compare_data, ensure_ascii=False)}\n\n"
        "说明：\n"
        "综合评分 = 0.6 × 传播热度 + 0.4 × 互动质量\n\n"
        "重要约束：\n"
        "1. 传播热度、互动质量、综合评分均为同批样本内相对指数，只用于横向比较。\n"
        "2. 禁止使用“及格线、满分100、低于标准”等绝对评价表述。\n"
        "3. 仅可使用“相对较高/较低、处于中等、样本内靠前/靠后”等相对表述。\n"
        "4. 评论数据为采样文本（最多60条），不代表真实评论总数，不可用于评论规模或评论率判断。\n"
        "5. 传播规模判断主要依据播放、点赞、收藏、投币、弹幕等元数据。\n\n"
        "请分析：\n"
        "1. 剧种数量TOP10省份的评分趋势\n"
        "2. 是否存在资源多但传播弱 / 资源少但传播强\n"
        "3. 选1-2个省举例\n"
        "4. 给出建议\n\n"
        "输出 JSON：\n"
        "{\n"
        '  "analysis": "...",\n'
        '  "examples": "...",\n'
        '  "advice": "..."\n'
        "}\n\n"
        "要求：\n"
        "- 不编造数据\n"
        "- 只输出 JSON\n"
        "- 简洁"
    )
    parsed = ask_qwen_json(ask_qwen_func, qwen_state, prompt, fallback_national_ai())
    return normalize_national_ai(parsed)


def build_province_spread_structure(
    province: str,
    scores: List[float],
    avg_score: float,
    opera_count: int,
    video_count: int,
    score_low: float,
    score_high: float,
    avg_low: float,
    avg_high: float,
    ask_qwen_func: Optional[Callable[..., Optional[str]]],
    qwen_state: Dict[str, bool],
) -> Dict[str, Any]:
    bars = [{"level": "强传播", "count": 0}, {"level": "中等传播", "count": 0}, {"level": "弱传播", "count": 0}]
    level_to_idx = {"强传播": 0, "中等传播": 1, "弱传播": 2}
    for score in scores:
        bars[level_to_idx[score_level(score, score_low, score_high)]]["count"] += 1

    resource_lv = resource_level(opera_count)
    spread_lv = spread_level(avg_score, avg_low, avg_high) if video_count > 0 else "低"
    type_value = structure_type(resource_lv, spread_lv, video_count)

    if video_count <= 0:
        ai_analysis = {
            "analysis": "该省当前缺少视频样本，暂无法形成稳定传播结构判断。",
            "advice": "建议先补充代表视频并完善基础互动数据，再进行结构优化分析。",
        }
    else:
        prompt = (
            "你是一名戏曲传播分析师。\n\n"

            "数据：\n"
            f"省份：{province}\n"
            f"剧种数：{opera_count}\n"
            f"视频数（样本数）：{video_count}\n"
            f"平均评分：{avg_score}\n"
            f"结构类型：{type_value}\n\n"

            f"强：{bars[0]['count']}\n"
            f"中：{bars[1]['count']}\n"
            f"弱：{bars[2]['count']}\n\n"

            "说明：\n"
            "综合评分 = 0.6×传播热度 + 0.4×互动质量\n\n"

            "重要约束：\n"
            "1. 以上指数均为同批样本内相对指数，不是绝对分数。\n"
            "2. 禁止出现“及格线、满分100、低于标准”等表达。\n"
            "3. 使用“相对较高/较低、处于中等、样本内靠前/靠后”等相对语言。\n"
            "4. 评论数据是采样文本，不代表真实评论总量，不能据此判断评论规模或传播规模。\n"
            "5. 本数据为采样数据，每个省份仅包含少量代表性视频（通常4-6条）。\n"
            "6. 视频数仅表示本次分析的样本数量，不代表该省真实视频产出规模。\n"
            "7. 严禁根据视频数推断“视频产出少”“内容供给不足”“活跃度低”等结论。\n"
            "8. 所有分析必须限定在“当前样本内”，不得外推至整体情况。\n\n"

            "分析要求：\n"
            "1. 重点分析强/中/弱传播样本的分布结构。\n"
            "2. 判断该省样本内传播结构是否均衡。\n"
            "3. 结合平均评分与结构类型解释传播特征。\n"
            "4. 给出针对内容传播的优化建议。\n\n"

            "请输出：\n"
            "{\n"
            '  "analysis": "...",\n'
            '  "advice": "..."\n'
            "}\n\n"

            "要求：\n"
            "- 必须使用“在当前样本中”“从样本表现看”等表述\n"
            "- 不得评价视频产出规模或整体活跃度\n"
            "- 简洁\n"
            "- 基于数据\n"
            "- 不编造"
        )
        parsed = ask_qwen_json(ask_qwen_func, qwen_state, prompt, fallback_spread_ai())
        ai_analysis = normalize_spread_ai(parsed)

    return {
        "bars": bars,
        "avgScore": round(avg_score, 1),
        "operaCount": int(opera_count),
        "videoCount": int(video_count),
        "structureType": type_value,
        "aiAnalysis": ai_analysis,
    }
