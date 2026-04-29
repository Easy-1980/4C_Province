from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .audience_utils import analyze_tgi, build_audience_sections
from .common_utils import find_column, read_csv, read_excel, read_json, write_json
from .opera_utils import build_opera_sections
from .qwen_utils import load_qwen_ask_func, qwen_cooldown
from .radar_utils import DIMENSIONS, build_radar_sections, normalize_radar_payload
from .score_utils import (
    build_national_opera_count_score_ai,
    build_province_opera_count_score_compare,
    build_province_score_stats,
    build_province_score_top10,
    build_province_spread_structure,
    compute_thresholds,
    extract_video_rows,
)
from .wordcloud_utils import build_province_word_clouds, build_word_cloud, merge_word_cloud_sentiment


VIDEO_ALIASES = {
    "province": ["省份", "province"],
    "opera": ["剧种", "opera"],
    "bvid": ["BV号", "bvid", "BVID", "bv"],
}

COMMENTS_ALIASES = {
    "bvid": ["BV号", "bvid", "BVID", "bv"],
    "content": ["评论内容", "comment", "content", "message", "text"],
}

DANMAKU_ALIASES = {
    "bvid": ["BV号", "bvid", "BVID", "bv"],
    "text": ["弹幕内容", "弹幕文本", "danmaku", "content", "text"],
}

INVALID_PROVINCE_NAMES = {"", "全国", "未知", "未知省份", "nan", "none", "None"}


def _is_valid_province_name(value: Any) -> bool:
    text = str(value).strip()
    return bool(text) and text not in INVALID_PROVINCE_NAMES


def _canonicalize_video_df(df: pd.DataFrame) -> pd.DataFrame:
    bvid_col = find_column(df, VIDEO_ALIASES["bvid"])
    if bvid_col is None:
        raise ValueError("video_info.csv 未识别到 BV 号字段（如 BV号/bvid/BVID）。")

    province_col = find_column(df, VIDEO_ALIASES["province"])
    opera_col = find_column(df, VIDEO_ALIASES["opera"])

    data = pd.DataFrame()
    data["bvid"] = df[bvid_col].astype(str).str.strip().str.upper()
    data["province"] = df[province_col].astype(str).str.strip() if province_col is not None else ""
    data["opera"] = df[opera_col].astype(str).str.strip() if opera_col is not None else ""
    data["province"] = data["province"].replace({"nan": "", "None": "", "none": "", "全国": ""})
    data["opera"] = data["opera"].replace({"nan": "", "None": "", "none": ""})
    data = data[data["bvid"].ne("")].copy()
    data = data.drop_duplicates(subset=["bvid"], keep="first").reset_index(drop=True)
    return data


def _canonicalize_comments_df(df: pd.DataFrame) -> pd.DataFrame:
    bvid_col = find_column(df, COMMENTS_ALIASES["bvid"])
    content_col = find_column(df, COMMENTS_ALIASES["content"])
    if bvid_col is None:
        return pd.DataFrame(columns=["bvid", "content"])

    data = pd.DataFrame()
    data["bvid"] = df[bvid_col].astype(str).str.strip().str.upper()
    data["content"] = df[content_col].astype(str).str.strip() if content_col is not None else ""
    data["content"] = data["content"].replace({"nan": "", "None": "", "none": ""})
    data = data[data["bvid"].ne("")].copy()
    return data


def _canonicalize_danmaku_df(df: pd.DataFrame) -> pd.DataFrame:
    bvid_col = find_column(df, DANMAKU_ALIASES["bvid"])
    text_col = find_column(df, DANMAKU_ALIASES["text"])
    if bvid_col is None:
        return pd.DataFrame(columns=["bvid", "text"])

    data = pd.DataFrame()
    data["bvid"] = df[bvid_col].astype(str).str.strip().str.upper()
    data["text"] = df[text_col].astype(str).str.strip() if text_col is not None else ""
    data = data[data["bvid"].ne("")].copy()
    return data


def build_dashboard_data(
    all_operas_path: Path,
    audience_portrait_path: Path,
    video_info_path: Path,
    comments_path: Path,
    danmaku_path: Path,
    video_analysis_path: Path,
    output_path: Path,
    qwen_script_path: Optional[Path] = None,
) -> Dict[str, Any]:
    print("[analyze_dashboard] 读取输入文件...")
    all_operas_df = read_excel(all_operas_path)
    audience_df = read_excel(audience_portrait_path)
    video_df_raw = read_csv(video_info_path)
    comments_df_raw = read_csv(comments_path)
    danmaku_df_raw = read_csv(danmaku_path)
    video_analysis_data = read_json(video_analysis_path)

    video_df = _canonicalize_video_df(video_df_raw)
    comments_df = _canonicalize_comments_df(comments_df_raw)
    danmaku_df = _canonicalize_danmaku_df(danmaku_df_raw)

    map_data, opera_province_data = build_opera_sections(all_operas_df)
    audience_province_data, national_audience, national_tgi = build_audience_sections(audience_df)
    national_radar, province_radar = build_radar_sections(video_df, comments_df)
    national_radar = normalize_radar_payload(national_radar)
    national_word_cloud = build_word_cloud(danmaku_df, top_n=100)
    province_word_clouds = build_province_word_clouds(danmaku_df, video_df, top_n=100)

    video_rows = extract_video_rows(video_analysis_data)
    province_score_stats = build_province_score_stats(video_rows, opera_province_data)
    province_score_top10 = build_province_score_top10(province_score_stats, opera_province_data)
    compare_top10 = build_province_opera_count_score_compare(province_score_stats, opera_province_data)

    if qwen_script_path is None:
        qwen_script_path = Path(__file__).resolve().parents[1] / "Qwen_Analysis.py"
    ask_qwen_func = load_qwen_ask_func(qwen_script_path, module_name="legacy_qwen_analysis_dashboard")
    qwen_state = {"disabled": ask_qwen_func is None}

    opera_count_score_ai = build_national_opera_count_score_ai(compare_top10, ask_qwen_func, qwen_state)
    qwen_cooldown(qwen_state)

    # 词云情感分析：全国一次 + 各省各一次（每个词云批量分析）
    national_word_cloud = merge_word_cloud_sentiment(
        national_word_cloud, ask_qwen_func, qwen_state, top_n=15
    )
    qwen_cooldown(qwen_state)
    for province_name in list(province_word_clouds.keys()):
        province_word_clouds[province_name] = merge_word_cloud_sentiment(
            province_word_clouds.get(province_name, []), ask_qwen_func, qwen_state, top_n=15
        )
        qwen_cooldown(qwen_state, seconds=0.25)

    # TGI 分析：全国一次 + 省份一次
    national_tgi_analysis = analyze_tgi("全国戏曲受众", national_tgi, ask_qwen_func, qwen_state)
    qwen_cooldown(qwen_state)

    all_video_scores = [float(row["score"]) for row in video_rows]
    score_low, score_high, _ = compute_thresholds(all_video_scores)
    province_avg_scores = [float(stat["avgScore"]) for stat in province_score_stats.values() if int(stat["videoCount"]) > 0]
    avg_low, avg_high, _ = compute_thresholds(province_avg_scores)

    all_provinces_raw = sorted(
        set(opera_province_data.keys())
        | set(audience_province_data.keys())
        | set(province_radar.keys())
        | set(province_word_clouds.keys())
        | set(province_score_stats.keys())
    )
    all_provinces = [str(p).strip() for p in all_provinces_raw if _is_valid_province_name(p)]

    province_output: Dict[str, Dict[str, Any]] = {}
    default_radar = {"dimensions": [item["name"] for item in DIMENSIONS], "scores": [60, 60, 60, 60, 60, 60]}
    for province in all_provinces:
        opera_part = opera_province_data.get(province, {"operaCount": 0, "operas": [], "heritageLevel": {}, "originDynasty": {}})
        audience_part = audience_province_data.get(province, {"audiencePortrait": {}, "tgi": []})
        radar_part = normalize_radar_payload(province_radar.get(province, default_radar))
        word_cloud_part = province_word_clouds.get(province, [])
        score_part = province_score_stats.get(province, {"scores": [], "avgScore": 0.0, "videoCount": 0})
        tgi_part = audience_part.get("tgi", [])

        if tgi_part:
            tgi_analysis = analyze_tgi(f"{province}戏曲受众", tgi_part, ask_qwen_func, qwen_state)
        else:
            tgi_analysis = {
                "analysis": "暂无可用TGI数据。",
                "insight": "当前样本不足，无法形成稳定受众偏好判断。",
                "advice": "建议补充该省份受众画像数据后再进行精细分析。",
            }
        qwen_cooldown(qwen_state, seconds=0.25)

        spread_structure = build_province_spread_structure(
            province=province,
            scores=[float(x) for x in score_part.get("scores", [])],
            avg_score=float(score_part.get("avgScore", 0.0)),
            opera_count=int(opera_part.get("operaCount", 0)),
            video_count=int(score_part.get("videoCount", 0)),
            score_low=score_low,
            score_high=score_high,
            avg_low=avg_low,
            avg_high=avg_high,
            ask_qwen_func=ask_qwen_func,
            qwen_state=qwen_state,
        )

        province_output[province] = {
            "operaCount": int(opera_part.get("operaCount", 0)),
            "operas": opera_part.get("operas", []),
            "heritageLevel": opera_part.get("heritageLevel", {}),
            "originDynasty": opera_part.get("originDynasty", {}),
            "radarScores": radar_part,
            "audiencePortrait": audience_part.get("audiencePortrait", {}),
            "tgi": tgi_part,
            "tgiAnalysis": tgi_analysis,
            "wordCloud": word_cloud_part,
            "spreadStructure": spread_structure,
        }

    for province_name, province_payload in province_output.items():
        radar_payload = province_payload.get("radarScores", default_radar)
        province_payload["radarScores"] = normalize_radar_payload(radar_payload)
        province_output[province_name] = province_payload

    national_payload = {
        "mapData": map_data,
        "provinceScoreTop10": province_score_top10,
        "provinceOperaCountScoreCompare": compare_top10,
        "operaCountScoreAI": opera_count_score_ai,
        "wordCloud": national_word_cloud,
        "radarScores": normalize_radar_payload(national_radar),
        "audiencePortrait": national_audience,
        "tgi": national_tgi,
        "tgiAnalysis": national_tgi_analysis,
    }

    output = {
        "national": national_payload,
        "provinces": province_output,
    }

    write_json(output_path, output)
    print(f"[analyze_dashboard] 输出完成: {output_path}")
    return output
