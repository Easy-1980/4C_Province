from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import pandas as pd

from .common_utils import find_column


def parse_province_and_count(text: Any) -> Tuple[str, int]:
    if pd.isna(text):
        return "", 0
    text_str = str(text).strip()
    match = re.search(
        r"(.+?)(?:省|市|维吾尔自治区|壮族自治区|回族自治区|自治区|特别行政区)?\s*[（\(](\d+)[）\)]",
        text_str,
    )
    if match:
        return match.group(1).strip(), int(match.group(2))

    cleaned = (
        text_str.replace("省", "")
        .replace("市", "")
        .replace("壮族自治区", "")
        .replace("维吾尔自治区", "")
        .replace("回族自治区", "")
        .replace("自治区", "")
        .replace("特别行政区", "")
        .strip()
    )
    return cleaned, 0


def clean_opera_name(name: Any) -> str:
    text = str(name).strip()
    if not text or text == "nan":
        return ""
    match = re.search(r"^([^\(（]+?)\s*([\(（])([^\)）]+)([\)）])", text)
    if match:
        inside = match.group(3).strip()
        if any(inside.endswith(kw) for kw in ["戏", "剧", "腔", "调", "词", "歌", "落", "传", "梆", "曲", "子"]):
            return inside
    return text


def parse_heritage_level(text: Any) -> str:
    if pd.isna(text):
        return "未计入"
    t = str(text).strip()
    if not t or t == "nan":
        return "未计入"
    if "人类" in t or "世界" in t:
        return "世界级"
    if "国家" in t:
        return "国家级"
    if "省" in t:
        return "省级"
    if "市" in t:
        return "市级"
    return "未计入"


def clean_dynasty_text(text: Any) -> str:
    if pd.isna(text) or str(text).strip() in {"", "nan"}:
        return "未知"
    t = re.sub(r"[\(（\)）\s]", "", str(text).strip())
    t = re.sub(r"(唐|宋|金|元|明|清|汉)中叶", r"\1代中叶", t)
    cross_match = re.search(r"(唐宋|宋金|宋元|金元|元明|明清)", t)
    if cross_match:
        t = cross_match.group(1)[0]
    core = t.replace("时期", "").replace("之际", "")
    if core in {"唐", "宋", "金", "元", "明", "清", "汉"}:
        return core + "代"
    if core in {"唐代", "宋代", "金代", "元代", "明代", "清代", "汉代"}:
        return core
    return t


def map_dynasty_bucket(time_str: str) -> str:
    if not time_str or time_str == "未知":
        return "未知"
    if any(x in time_str for x in ["宋", "元", "金", "汉", "唐"]):
        return "元代"
    if "明" in time_str:
        return "明代"
    if "清" in time_str or "十九世纪" in time_str:
        return "清代"
    if any(x in time_str for x in ["民国", "19", "20", "现代", "近现代", "二十世纪"]):
        return "近现代"
    return "其他"


def build_opera_sections(df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    province_col = find_column(df, ["省份", "province"])
    if province_col is None:
        raise ValueError("allOperas_Unprocessed.xlsx 缺少省份字段。")

    opera_col = find_column(df, ["剧种", "opera"])
    heritage_col = find_column(df, ["级别", "非遗级别", "heritage_level"])
    dynasty_col = find_column(df, ["产生时间", "起源朝代", "朝代", "origin_dynasty"])

    work_df = df.copy()
    work_df[province_col] = work_df[province_col].ffill()

    province_temp: Dict[str, Dict[str, Any]] = {}
    for _, row in work_df.iterrows():
        province_name, hinted_count = parse_province_and_count(row[province_col])
        if not province_name:
            continue

        temp = province_temp.setdefault(
            province_name,
            {"hinted_count": 0, "operas": set(), "heritage": defaultdict(int), "dynasty": defaultdict(int)},
        )
        temp["hinted_count"] = max(int(temp["hinted_count"]), int(hinted_count))

        if opera_col is not None:
            opera_name = clean_opera_name(row[opera_col])
            if opera_name:
                temp["operas"].add(opera_name)

        level = parse_heritage_level(row[heritage_col]) if heritage_col is not None else "未计入"
        temp["heritage"][level] += 1

        dynasty_text = clean_dynasty_text(row[dynasty_col]) if dynasty_col is not None else "未知"
        bucket = map_dynasty_bucket(dynasty_text)
        temp["dynasty"][bucket] += 1

    province_output: Dict[str, Dict[str, Any]] = {}
    map_data: List[Dict[str, Any]] = []
    for province_name, info in province_temp.items():
        opera_count = max(int(info["hinted_count"]), len(info["operas"]))
        operas = sorted(list(info["operas"]))
        province_output[province_name] = {
            "operaCount": int(opera_count),
            "operas": operas,
            "heritageLevel": dict(sorted(info["heritage"].items(), key=lambda x: x[0])),
            "originDynasty": dict(sorted(info["dynasty"].items(), key=lambda x: x[0])),
        }
        map_data.append({"name": province_name, "value": int(opera_count)})
    map_data.sort(key=lambda x: x["value"], reverse=True)
    return map_data, province_output
