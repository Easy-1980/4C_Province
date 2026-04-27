from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

import pandas as pd


# 复用 radarScores.py 的 6 维度词典
DIMENSIONS = [
    {"name": "服化道审美", "keywords": ["衣服", "扮相", "妆容", "头饰", "绝美", "好看", "漂亮", "服饰", "美轮美奂", "审美"]},
    {"name": "二创与整活", "keywords": ["哈哈", "梗", "鬼畜", "整活", "搞笑", "离谱", "笑死", "绝了", "魔性", "二创", "联动", "出圈"]},
    {"name": "名场面打卡", "keywords": ["名场面", "打卡", "终于", "高能", "前方", "经典", "来了", "啊啊啊", "名段", "名篇"]},
    {"name": "传统文化底蕴", "keywords": ["国粹", "非遗", "传承", "老祖宗", "文化", "底蕴", "艺术", "致敬", "传统", "瑰宝"]},
    {"name": "剧情与价值观", "keywords": ["感人", "泪目", "剧情", "故事", "爱情", "三观", "感动", "因果", "封建"]},
    {"name": "唱腔与身段", "keywords": ["唱腔", "好听", "嗓音", "身段", "功底", "基本功", "台步", "动作", "眼神", "绝活", "转音"]},
]


def calculate_radar_scores(text_data: str) -> List[int]:
    raw_scores = [sum(text_data.count(kw) for kw in dim["keywords"]) for dim in DIMENSIONS]
    if sum(raw_scores) == 0:
        return [60, 60, 60, 60, 60, 60]

    log_scores = [math.log(score + 1) for score in raw_scores]
    max_log = max(log_scores) if log_scores else 1.0
    if max_log == 0:
        return [60, 60, 60, 60, 60, 60]
    return [int(round(55 + (value / max_log) * (98 - 55))) for value in log_scores]


def build_radar_sections(video_df: pd.DataFrame, comments_df: pd.DataFrame) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    dimensions = [item["name"] for item in DIMENSIONS]
    if comments_df.empty or video_df.empty:
        return {"dimensions": dimensions, "scores": [60, 60, 60, 60, 60, 60]}, {}

    merged = comments_df.merge(video_df[["bvid", "province"]], on="bvid", how="inner")
    if merged.empty:
        return {"dimensions": dimensions, "scores": [60, 60, 60, 60, 60, 60]}, {}

    national = {"dimensions": dimensions, "scores": calculate_radar_scores("".join(merged["content"].astype(str).tolist()))}
    province_radar: Dict[str, Dict[str, Any]] = {}
    for province_name, group in merged.groupby("province"):
        province_radar[str(province_name)] = {
            "dimensions": dimensions,
            "scores": calculate_radar_scores("".join(group["content"].astype(str).tolist())),
        }
    return national, province_radar
