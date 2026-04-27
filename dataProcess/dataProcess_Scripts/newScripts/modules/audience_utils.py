from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .common_utils import find_column, resolve_column_map
from .qwen_utils import ask_qwen_json


def _row_numeric(row: pd.Series, col_name: Optional[str]) -> float:
    if col_name is None:
        return 0.0
    return float(pd.to_numeric(pd.Series([row[col_name]]), errors="coerce").fillna(0.0).iloc[0])


def build_audience_sections(df: pd.DataFrame) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    province_col = find_column(df, ["省份", "province"])
    if province_col is None:
        raise ValueError("audiencePortrait.xlsx 缺少省份字段。")

    opera_col = find_column(df, ["各省代表剧种", "剧种", "opera"])
    work_df = df.copy()
    work_df[province_col] = work_df[province_col].ffill()
    first_df = work_df.groupby(province_col).first().reset_index()

    col_map = resolve_column_map(
        first_df,
        {
            "male_percent": ["男性占比", "男占比", "male_ratio"],
            "female_percent": ["女性占比", "女占比", "female_ratio"],
            "male_tgi": ["男性TGI", "男TGI", "male_tgi"],
            "female_tgi": ["女性TGI", "女TGI", "female_tgi"],
            "age_19_percent": ["≤19岁占比", "<=19岁占比", "19岁及以下占比", "19岁以下占比"],
            "age_20_percent": ["20-29岁占比", "20-29占比"],
            "age_30_percent": ["30-39岁占比", "30-39占比"],
            "age_40_percent": ["40-49岁占比", "40-49占比"],
            "age_50_percent": ["≥50岁占比", ">=50岁占比", "50岁及以上占比"],
            "age_19_tgi": ["≤19岁TGI", "<=19岁TGI", "19岁及以下TGI", "19岁以下TGI"],
            "age_20_tgi": ["20-29岁TGI", "20-29TGI"],
            "age_30_tgi": ["30-39岁TGI", "30-39TGI"],
            "age_40_tgi": ["40-49岁TGI", "40-49TGI"],
            "age_50_tgi": ["≥50岁TGI", ">=50岁TGI", "50岁及以上TGI"],
        },
    )

    province_output: Dict[str, Dict[str, Any]] = {}
    national_rows: List[Dict[str, float]] = []
    age_categories = ["≤19岁", "20-29岁", "30-39岁", "40-49岁", "≥50岁"]
    reverse_age_categories = ["≥50岁", "40-49岁", "30-39岁", "20-29岁", "≤19岁"]

    for _, row in first_df.iterrows():
        province_name = str(row[province_col]).strip()
        if not province_name:
            continue
        opera_name = str(row[opera_col]).strip() if opera_col is not None else ""
        male_percent = _row_numeric(row, col_map["male_percent"])
        female_percent = _row_numeric(row, col_map["female_percent"])
        age_percents = [
            _row_numeric(row, col_map["age_19_percent"]),
            _row_numeric(row, col_map["age_20_percent"]),
            _row_numeric(row, col_map["age_30_percent"]),
            _row_numeric(row, col_map["age_40_percent"]),
            _row_numeric(row, col_map["age_50_percent"]),
        ]
        age_tgis = [
            _row_numeric(row, col_map["age_19_tgi"]),
            _row_numeric(row, col_map["age_20_tgi"]),
            _row_numeric(row, col_map["age_30_tgi"]),
            _row_numeric(row, col_map["age_40_tgi"]),
            _row_numeric(row, col_map["age_50_tgi"]),
        ]
        male_tgi = _row_numeric(row, col_map["male_tgi"])
        female_tgi = _row_numeric(row, col_map["female_tgi"])

        male_ratio = male_percent / 100 if male_percent else 0.0
        female_ratio = female_percent / 100 if female_percent else 0.0
        province_output[province_name] = {
            "audiencePortrait": {
                "representativeOpera": opera_name,
                "genderRatio": {"male": round(male_percent, 2), "female": round(female_percent, 2)},
                "ageDistribution": {"categories": age_categories, "values": [round(x, 2) for x in age_percents]},
                "ageGender": {
                    "categories": reverse_age_categories,
                    "male": [
                        round(age_percents[4] * male_ratio, 2),
                        round(age_percents[3] * male_ratio, 2),
                        round(age_percents[2] * male_ratio, 2),
                        round(age_percents[1] * male_ratio, 2),
                        round(age_percents[0] * male_ratio, 2),
                    ],
                    "female": [
                        round(age_percents[4] * female_ratio, 2),
                        round(age_percents[3] * female_ratio, 2),
                        round(age_percents[2] * female_ratio, 2),
                        round(age_percents[1] * female_ratio, 2),
                        round(age_percents[0] * female_ratio, 2),
                    ],
                },
            },
            "tgi": [{"group": "年龄", "category": c, "tgi": round(v, 2)} for c, v in zip(age_categories, age_tgis)]
            + [{"group": "性别", "category": "男性", "tgi": round(male_tgi, 2)}, {"group": "性别", "category": "女性", "tgi": round(female_tgi, 2)}],
        }
        national_rows.append(
            {
                "male_percent": male_percent,
                "female_percent": female_percent,
                "age_19_percent": age_percents[0],
                "age_20_percent": age_percents[1],
                "age_30_percent": age_percents[2],
                "age_40_percent": age_percents[3],
                "age_50_percent": age_percents[4],
                "age_19_tgi": age_tgis[0],
                "age_20_tgi": age_tgis[1],
                "age_30_tgi": age_tgis[2],
                "age_40_tgi": age_tgis[3],
                "age_50_tgi": age_tgis[4],
                "male_tgi": male_tgi,
                "female_tgi": female_tgi,
            }
        )

    if national_rows:
        national_df = pd.DataFrame(national_rows)
        male_percent = float(national_df["male_percent"].mean())
        female_percent = float(national_df["female_percent"].mean())
        age_percents = [float(national_df[col].mean()) for col in ["age_19_percent", "age_20_percent", "age_30_percent", "age_40_percent", "age_50_percent"]]
        age_tgis = [float(national_df[col].mean()) for col in ["age_19_tgi", "age_20_tgi", "age_30_tgi", "age_40_tgi", "age_50_tgi"]]
        male_tgi = float(national_df["male_tgi"].mean())
        female_tgi = float(national_df["female_tgi"].mean())
    else:
        male_percent = female_percent = 0.0
        age_percents = [0.0] * 5
        age_tgis = [0.0] * 5
        male_tgi = female_tgi = 0.0

    male_ratio = male_percent / 100 if male_percent else 0.0
    female_ratio = female_percent / 100 if female_percent else 0.0
    national_audience = {
        "genderRatio": {"male": round(male_percent, 2), "female": round(female_percent, 2)},
        "ageDistribution": {"categories": age_categories, "values": [round(x, 2) for x in age_percents]},
        "ageGender": {
            "categories": reverse_age_categories,
            "male": [
                round(age_percents[4] * male_ratio, 2),
                round(age_percents[3] * male_ratio, 2),
                round(age_percents[2] * male_ratio, 2),
                round(age_percents[1] * male_ratio, 2),
                round(age_percents[0] * male_ratio, 2),
            ],
            "female": [
                round(age_percents[4] * female_ratio, 2),
                round(age_percents[3] * female_ratio, 2),
                round(age_percents[2] * female_ratio, 2),
                round(age_percents[1] * female_ratio, 2),
                round(age_percents[0] * female_ratio, 2),
            ],
        },
    }
    national_tgi = [{"group": "年龄", "category": c, "tgi": round(v, 2)} for c, v in zip(age_categories, age_tgis)]
    national_tgi.append({"group": "性别", "category": "男性", "tgi": round(male_tgi, 2)})
    national_tgi.append({"group": "性别", "category": "女性", "tgi": round(female_tgi, 2)})
    return province_output, national_audience, national_tgi


def fallback_tgi_analysis() -> Dict[str, str]:
    return {
        "analysis": "当前TGI显示受众偏好存在结构性差异，建议继续分群运营。",
        "insight": "高于100的人群是当前核心受众，应优先匹配其内容偏好。",
        "advice": "建议按年龄和性别分层投放内容，强化高TGI人群的传播转化。",
    }


def normalize_tgi_analysis(payload: Dict[str, Any]) -> Dict[str, str]:
    fallback = fallback_tgi_analysis()
    return {
        "analysis": str(payload.get("analysis", "")).strip() or fallback["analysis"],
        "insight": str(payload.get("insight", "")).strip() or fallback["insight"],
        "advice": str(payload.get("advice", "")).strip() or fallback["advice"],
    }


def build_tgi_analysis_prompt(label: str, tgi_data: List[Dict[str, Any]]) -> str:
    return (
        "你是一名用户画像分析专家。\n\n"
        f"以下是{label}的受众 TGI 数据：\n\n"
        f"{json.dumps(tgi_data, ensure_ascii=False)}\n\n"
        "说明：\n"
        "TGI > 100 表示该人群偏好高于平均水平。\n\n"
        "请分析：\n"
        "1. 哪些人群对该剧种偏好明显\n"
        "2. 是否存在年轻化或性别偏向\n"
        "3. 该剧种的传播受众特征\n"
        "4. 给出传播建议\n\n"
        "输出 JSON：\n"
        "{\n"
        '  "analysis": "整体人群分析（100字以内）",\n'
        '  "insight": "核心受众特征（100字以内）",\n'
        '  "advice": "传播优化建议（100字以内）"\n'
        "}\n\n"
        "要求：\n"
        "- 只输出 JSON\n"
        "- 不编造数据\n"
        "- 必须结合 TGI 数值"
    )


def analyze_tgi(
    label: str,
    tgi_data: List[Dict[str, Any]],
    ask_qwen_func: Optional[Callable[..., Optional[str]]],
    qwen_state: Dict[str, bool],
) -> Dict[str, str]:
    prompt = build_tgi_analysis_prompt(label, tgi_data)
    parsed = ask_qwen_json(ask_qwen_func, qwen_state, prompt, fallback_tgi_analysis())
    return normalize_tgi_analysis(parsed)
