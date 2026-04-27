from __future__ import annotations

import argparse
import importlib.util
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import jieba
import pandas as pd


# 复用 nationalWordCloud.py 的停用词逻辑
STOP_WORDS = {
    "哈哈哈哈", "哈哈", "这个", "那个", "每个", "一个", "两个", "有些", "就是", "不是", "还是", "但是",
    "而且", "然后", "所以", "因为", "真的", "确实", "其实", "简直", "有点", "好像", "一样", "这样",
    "那样", "怎么", "什么", "为什么", "哪里", "现在", "刚才", "之后", "之前", "感觉", "觉得", "认为",
    "以为", "发现", "知道", "看到", "起来", "出来", "你们", "我们", "他们", "人家", "自己", "这里",
    "那里", "每周", "必看", "弹幕", "视频", "画质", "老师", "演员", "前面", "后面", "这段", "可以", "没有",
    "还有", "应该", "可能", "意思", "真是", "好好",
}

# 复用 radarScores.py 的 6 维度词典
DIMENSIONS = [
    {"name": "服化道审美", "keywords": ["衣服", "扮相", "妆容", "头饰", "绝美", "好看", "漂亮", "服饰", "美轮美奂", "审美"]},
    {"name": "二创与整活", "keywords": ["哈哈", "梗", "鬼畜", "整活", "搞笑", "离谱", "笑死", "绝了", "魔性", "二创", "联动", "出圈"]},
    {"name": "名场面打卡", "keywords": ["名场面", "打卡", "终于", "高能", "前方", "经典", "来了", "啊啊啊", "名段", "名篇"]},
    {"name": "传统文化底蕴", "keywords": ["国粹", "非遗", "传承", "老祖宗", "文化", "底蕴", "艺术", "致敬", "传统", "瑰宝"]},
    {"name": "剧情与价值观", "keywords": ["感人", "泪目", "剧情", "故事", "爱情", "三观", "感动", "因果", "封建"]},
    {"name": "唱腔与身段", "keywords": ["唱腔", "好听", "嗓音", "身段", "功底", "基本功", "台步", "动作", "眼神", "绝活", "转音"]},
]

VIDEO_ALIASES = {
    "province": ["省份", "province"],
    "opera": ["剧种", "opera"],
    "bvid": ["BV号", "bvid", "BVID", "bv"],
}

COMMENTS_ALIASES = {
    "bvid": ["BV号", "bvid", "BVID", "bv"],
    "content": ["评论内容", "comment", "content"],
}

DANMAKU_ALIASES = {
    "bvid": ["BV号", "bvid", "BVID", "bv"],
    "text": ["弹幕内容", "弹幕文本", "danmaku", "content", "text"],
}


def _normalize_col_name(name: str) -> str:
    return re.sub(r"[\s_]+", "", str(name)).lower()


def _find_column(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    normalized_map = {_normalize_col_name(col): col for col in df.columns}
    for alias in aliases:
        hit = normalized_map.get(_normalize_col_name(alias))
        if hit is not None:
            return hit
    return None


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8")


def _read_excel(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, engine="openpyxl")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"未找到 JSON 文件: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _canonicalize_video_df(df: pd.DataFrame) -> pd.DataFrame:
    bvid_col = _find_column(df, VIDEO_ALIASES["bvid"])
    if bvid_col is None:
        raise ValueError("video_info.csv 未识别到 BV 号字段（如 BV号/bvid/BVID）。")

    province_col = _find_column(df, VIDEO_ALIASES["province"])
    opera_col = _find_column(df, VIDEO_ALIASES["opera"])

    data = pd.DataFrame()
    data["bvid"] = df[bvid_col].astype(str).str.strip()
    data["province"] = df[province_col].astype(str).str.strip() if province_col is not None else ""
    data["opera"] = df[opera_col].astype(str).str.strip() if opera_col is not None else ""
    data["province"] = data["province"].replace({"nan": "", "None": "", "none": ""})
    data["opera"] = data["opera"].replace({"nan": "", "None": "", "none": ""})
    data = data[data["bvid"].ne("")].copy()
    data = data.drop_duplicates(subset=["bvid"], keep="first").reset_index(drop=True)
    return data


def _canonicalize_comments_df(df: pd.DataFrame) -> pd.DataFrame:
    bvid_col = _find_column(df, COMMENTS_ALIASES["bvid"])
    content_col = _find_column(df, COMMENTS_ALIASES["content"])
    if bvid_col is None:
        return pd.DataFrame(columns=["bvid", "content"])

    data = pd.DataFrame()
    data["bvid"] = df[bvid_col].astype(str).str.strip()
    data["content"] = df[content_col].astype(str).str.strip() if content_col is not None else ""
    data = data[data["bvid"].ne("")].copy()
    return data


def _canonicalize_danmaku_df(df: pd.DataFrame) -> pd.DataFrame:
    bvid_col = _find_column(df, DANMAKU_ALIASES["bvid"])
    text_col = _find_column(df, DANMAKU_ALIASES["text"])
    if bvid_col is None:
        return pd.DataFrame(columns=["bvid", "text"])

    data = pd.DataFrame()
    data["bvid"] = df[bvid_col].astype(str).str.strip()
    data["text"] = df[text_col].astype(str).str.strip() if text_col is not None else ""
    data = data[data["bvid"].ne("")].copy()
    return data


def _parse_province_and_count(text: Any) -> Tuple[str, int]:
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


def _clean_opera_name(name: Any) -> str:
    text = str(name).strip()
    if not text or text == "nan":
        return ""
    match = re.search(r"^([^\(（]+?)\s*([\(（])([^\)）]+)([\)）])", text)
    if match:
        inside = match.group(3).strip()
        if any(inside.endswith(kw) for kw in ["戏", "剧", "腔", "调", "词", "歌", "落", "传", "梆", "曲", "子"]):
            return inside
    return text


def _parse_heritage_level(text: Any) -> str:
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


def _clean_dynasty_text(text: Any) -> str:
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


def _map_dynasty_bucket(time_str: str) -> str:
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


def _build_opera_sections(df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    province_col = _find_column(df, ["省份", "province"])
    if province_col is None:
        raise ValueError("allOperas_Unprocessed.xlsx 缺少省份字段。")

    opera_col = _find_column(df, ["剧种", "opera"])
    heritage_col = _find_column(df, ["级别", "非遗级别", "heritage_level"])
    dynasty_col = _find_column(df, ["产生时间", "起源朝代", "朝代", "origin_dynasty"])

    work_df = df.copy()
    work_df[province_col] = work_df[province_col].ffill()

    province_temp: Dict[str, Dict[str, Any]] = {}
    for _, row in work_df.iterrows():
        province_name, hinted_count = _parse_province_and_count(row[province_col])
        if not province_name:
            continue

        temp = province_temp.setdefault(
            province_name,
            {"hinted_count": 0, "operas": set(), "heritage": defaultdict(int), "dynasty": defaultdict(int)},
        )
        temp["hinted_count"] = max(int(temp["hinted_count"]), int(hinted_count))

        if opera_col is not None:
            opera_name = _clean_opera_name(row[opera_col])
            if opera_name:
                temp["operas"].add(opera_name)

        level = _parse_heritage_level(row[heritage_col]) if heritage_col is not None else "未计入"
        temp["heritage"][level] += 1

        dynasty_text = _clean_dynasty_text(row[dynasty_col]) if dynasty_col is not None else "未知"
        bucket = _map_dynasty_bucket(dynasty_text)
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


def _resolve_column_map(df: pd.DataFrame, alias_map: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
    return {key: _find_column(df, aliases) for key, aliases in alias_map.items()}


def _row_numeric(row: pd.Series, col_name: Optional[str]) -> float:
    if col_name is None:
        return 0.0
    return float(pd.to_numeric(pd.Series([row[col_name]]), errors="coerce").fillna(0.0).iloc[0])


def _build_audience_sections(df: pd.DataFrame) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    province_col = _find_column(df, ["省份", "province"])
    if province_col is None:
        raise ValueError("audiencePortrait.xlsx 缺少省份字段。")

    opera_col = _find_column(df, ["各省代表剧种", "剧种", "opera"])
    work_df = df.copy()
    work_df[province_col] = work_df[province_col].ffill()
    first_df = work_df.groupby(province_col).first().reset_index()

    col_map = _resolve_column_map(
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


def _calculate_radar_scores(text_data: str) -> List[int]:
    raw_scores = [sum(text_data.count(kw) for kw in dim["keywords"]) for dim in DIMENSIONS]
    if sum(raw_scores) == 0:
        return [60, 60, 60, 60, 60, 60]
    log_scores = [math.log(score + 1) for score in raw_scores]
    max_log = max(log_scores) if log_scores else 1.0
    if max_log == 0:
        return [60, 60, 60, 60, 60, 60]
    return [int(round(55 + (value / max_log) * (98 - 55))) for value in log_scores]


def _build_radar_sections(video_df: pd.DataFrame, comments_df: pd.DataFrame) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    dimensions = [item["name"] for item in DIMENSIONS]
    if comments_df.empty or video_df.empty:
        return {"dimensions": dimensions, "scores": [60, 60, 60, 60, 60, 60]}, {}
    merged = comments_df.merge(video_df[["bvid", "province"]], on="bvid", how="inner")
    if merged.empty:
        return {"dimensions": dimensions, "scores": [60, 60, 60, 60, 60, 60]}, {}

    national = {"dimensions": dimensions, "scores": _calculate_radar_scores("".join(merged["content"].astype(str).tolist()))}
    province_radar: Dict[str, Dict[str, Any]] = {}
    for province_name, group in merged.groupby("province"):
        province_radar[str(province_name)] = {
            "dimensions": dimensions,
            "scores": _calculate_radar_scores("".join(group["content"].astype(str).tolist())),
        }
    return national, province_radar


def _segment_text(text: str) -> List[str]:
    if not text:
        return []
    return jieba.lcut(text)


def _build_word_cloud(danmaku_df: pd.DataFrame, top_n: int = 100) -> List[Dict[str, Any]]:
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


def _build_province_word_clouds(danmaku_df: pd.DataFrame, video_df: pd.DataFrame, top_n: int = 100) -> Dict[str, List[Dict[str, Any]]]:
    if danmaku_df.empty or video_df.empty:
        return {}
    merged = danmaku_df.merge(video_df[["bvid", "province"]], on="bvid", how="left")
    merged = merged[merged["province"].astype(str).str.strip().ne("")].copy()
    if merged.empty:
        return {}
    return {str(province): _build_word_cloud(group, top_n=top_n) for province, group in merged.groupby("province")}


def _load_qwen_ask_func(qwen_script_path: Path) -> Optional[Callable[..., Optional[str]]]:
    if not qwen_script_path.exists():
        return None
    spec = importlib.util.spec_from_file_location("legacy_qwen_analysis_dashboard", qwen_script_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception:
        return None
    ask_func = getattr(module, "ask_qwen", None)
    return ask_func if callable(ask_func) else None


def _try_parse_json_block(raw_text: str) -> Optional[Dict[str, Any]]:
    if not raw_text:
        return None
    cleaned = raw_text.strip().replace("```json", "").replace("```", "").strip()
    decoder = json.JSONDecoder()
    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    for idx, char in enumerate(cleaned):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(cleaned[idx:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return None


def _ask_qwen_json(
    ask_qwen_func: Optional[Callable[..., Optional[str]]],
    qwen_state: Dict[str, bool],
    prompt: str,
    fallback: Dict[str, Any],
) -> Dict[str, Any]:
    if ask_qwen_func is None or qwen_state.get("disabled", False):
        return fallback
    try:
        raw_text = ask_qwen_func(
            prompt,
            system_role="你是一个资深的大数据分析师与戏曲文化推广专家。",
        )
    except TypeError:
        try:
            raw_text = ask_qwen_func(prompt)
        except Exception:
            qwen_state["disabled"] = True
            return fallback
    except Exception:
        qwen_state["disabled"] = True
        return fallback

    if not raw_text:
        qwen_state["disabled"] = True
        return fallback
    parsed = _try_parse_json_block(raw_text)
    if parsed is None:
        return fallback
    return parsed


def _extract_video_rows(video_analysis_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    videos = video_analysis_data.get("videos", [])
    if not isinstance(videos, list):
        return []
    rows: List[Dict[str, Any]] = []
    for item in videos:
        if not isinstance(item, dict):
            continue
        province = str(item.get("province", "")).strip() or "未知省份"
        indexes = item.get("indexes", {}) if isinstance(item.get("indexes"), dict) else {}
        score = _safe_float(indexes.get("score"), default=float("nan"))
        if math.isnan(score):
            spread = _safe_float(indexes.get("spreadHeat"), default=0.0)
            interaction = _safe_float(indexes.get("interactionQuality"), default=0.0)
            score = round(0.6 * spread + 0.4 * interaction, 1)
        rows.append({"province": province, "score": round(score, 1)})
    return rows


def _compute_thresholds(values: List[float]) -> Tuple[float, float, bool]:
    clean_values = [float(v) for v in values if not math.isnan(float(v))]
    if len(clean_values) < 5:
        return 50.0, 70.0, True
    series = pd.Series(clean_values)
    low = float(series.quantile(0.33))
    high = float(series.quantile(0.66))
    if low >= high:
        return 50.0, 70.0, True
    return low, high, False


def _score_level(value: float, low: float, high: float) -> str:
    if value >= high:
        return "强传播"
    if value >= low:
        return "中等传播"
    return "弱传播"


def _resource_level(opera_count: int) -> str:
    if opera_count >= 25:
        return "多"
    if opera_count >= 10:
        return "中"
    return "少"


def _spread_level(avg_score: float, low: float, high: float) -> str:
    if avg_score >= high:
        return "高"
    if avg_score >= low:
        return "中"
    return "低"


def _structure_type(resource_lv: str, spread_lv: str, video_count: int) -> str:
    if video_count <= 0:
        return "样本不足"
    mapping = {
        ("多", "高"): "均衡发展型",
        ("多", "中"): "资源转化提升型",
        ("多", "低"): "资源待激活型",
        ("中", "高"): "潜力成长型",
        ("中", "中"): "稳步发展型",
        ("中", "低"): "传播提质型",
        ("少", "高"): "小而精传播型",
        ("少", "中"): "基础培育型",
        ("少", "低"): "起步孵化型",
    }
    return mapping.get((resource_lv, spread_lv), "传播发展型")


def _fallback_national_ai() -> Dict[str, str]:
    return {
        "analysis": "剧种数量高的省份整体评分更稳定，但并非剧种越多评分越高。",
        "examples": "部分省份剧种数量多但平均评分一般，也有剧种数量中等却传播效率更高的情况。",
        "advice": "建议按剧种建立分层传播策略，优先放大高分样本内容并优化弱势剧种运营。",
    }


def _normalize_national_ai(payload: Dict[str, Any]) -> Dict[str, str]:
    fallback = _fallback_national_ai()
    return {
        "analysis": str(payload.get("analysis", "")).strip() or fallback["analysis"],
        "examples": str(payload.get("examples", "")).strip() or fallback["examples"],
        "advice": str(payload.get("advice", "")).strip() or fallback["advice"],
    }


def _fallback_spread_ai() -> Dict[str, str]:
    return {
        "analysis": "该省传播结构已完成分层识别，当前以样本内综合评分分布为依据。",
        "advice": "建议围绕高分内容扩散并修复弱传播段，形成稳定的省域传播梯度。",
    }


def _normalize_spread_ai(payload: Dict[str, Any]) -> Dict[str, str]:
    fallback = _fallback_spread_ai()
    return {
        "analysis": str(payload.get("analysis", "")).strip() or fallback["analysis"],
        "advice": str(payload.get("advice", "")).strip() or fallback["advice"],
    }


def _build_province_score_stats(
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


def _build_province_score_top10(
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


def _build_province_opera_count_score_compare(
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


def _build_national_opera_count_score_ai(
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
    parsed = _ask_qwen_json(ask_qwen_func, qwen_state, prompt, _fallback_national_ai())
    return _normalize_national_ai(parsed)


def _build_province_spread_structure(
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
        bars[level_to_idx[_score_level(score, score_low, score_high)]]["count"] += 1

    resource_lv = _resource_level(opera_count)
    spread_lv = _spread_level(avg_score, avg_low, avg_high) if video_count > 0 else "低"
    structure_type = _structure_type(resource_lv, spread_lv, video_count)

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
            f"视频数：{video_count}\n"
            f"平均评分：{avg_score}\n"
            f"结构类型：{structure_type}\n\n"
            f"强：{bars[0]['count']}\n"
            f"中：{bars[1]['count']}\n"
            f"弱：{bars[2]['count']}\n\n"
            "说明：\n"
            "综合评分 = 0.6×传播热度 + 0.4×互动质量\n\n"
            "请输出：\n"
            "{\n"
            '  "analysis": "...",\n'
            '  "advice": "..."\n'
            "}\n\n"
            "要求：\n"
            "- 简洁\n"
            "- 基于数据\n"
            "- 不编造"
        )
        parsed = _ask_qwen_json(ask_qwen_func, qwen_state, prompt, _fallback_spread_ai())
        ai_analysis = _normalize_spread_ai(parsed)

    return {
        "bars": bars,
        "avgScore": round(avg_score, 1),
        "operaCount": int(opera_count),
        "videoCount": int(video_count),
        "structureType": structure_type,
        "aiAnalysis": ai_analysis,
    }


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
    all_operas_df = _read_excel(all_operas_path)
    audience_df = _read_excel(audience_portrait_path)
    video_df_raw = _read_csv(video_info_path)
    comments_df_raw = _read_csv(comments_path)
    danmaku_df_raw = _read_csv(danmaku_path)
    video_analysis_data = _read_json(video_analysis_path)

    video_df = _canonicalize_video_df(video_df_raw)
    comments_df = _canonicalize_comments_df(comments_df_raw)
    danmaku_df = _canonicalize_danmaku_df(danmaku_df_raw)

    map_data, opera_province_data = _build_opera_sections(all_operas_df)
    audience_province_data, national_audience, national_tgi = _build_audience_sections(audience_df)
    national_radar, province_radar = _build_radar_sections(video_df, comments_df)
    national_word_cloud = _build_word_cloud(danmaku_df, top_n=100)
    province_word_clouds = _build_province_word_clouds(danmaku_df, video_df, top_n=100)

    video_rows = _extract_video_rows(video_analysis_data)
    province_score_stats = _build_province_score_stats(video_rows, opera_province_data)
    province_score_top10 = _build_province_score_top10(province_score_stats, opera_province_data)
    compare_top10 = _build_province_opera_count_score_compare(province_score_stats, opera_province_data)

    if qwen_script_path is None:
        qwen_script_path = Path(__file__).resolve().parents[1] / "Qwen_Analysis.py"
    ask_qwen_func = _load_qwen_ask_func(qwen_script_path)
    qwen_state = {"disabled": ask_qwen_func is None}
    opera_count_score_ai = _build_national_opera_count_score_ai(compare_top10, ask_qwen_func, qwen_state)

    all_video_scores = [float(row["score"]) for row in video_rows]
    score_low, score_high, _ = _compute_thresholds(all_video_scores)
    province_avg_scores = [float(stat["avgScore"]) for stat in province_score_stats.values() if int(stat["videoCount"]) > 0]
    avg_low, avg_high, _ = _compute_thresholds(province_avg_scores)

    all_provinces = sorted(
        set(opera_province_data.keys())
        | set(audience_province_data.keys())
        | set(province_radar.keys())
        | set(province_word_clouds.keys())
        | set(province_score_stats.keys())
    )

    province_output: Dict[str, Dict[str, Any]] = {}
    default_radar = {"dimensions": [item["name"] for item in DIMENSIONS], "scores": [60, 60, 60, 60, 60, 60]}
    for province in all_provinces:
        opera_part = opera_province_data.get(province, {"operaCount": 0, "operas": [], "heritageLevel": {}, "originDynasty": {}})
        audience_part = audience_province_data.get(province, {"audiencePortrait": {}, "tgi": []})
        radar_part = province_radar.get(province, default_radar)
        word_cloud_part = province_word_clouds.get(province, [])
        score_part = province_score_stats.get(province, {"scores": [], "avgScore": 0.0, "videoCount": 0})

        spread_structure = _build_province_spread_structure(
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
            "tgi": audience_part.get("tgi", []),
            "wordCloud": word_cloud_part,
            "spreadStructure": spread_structure,
        }

    output = {
        "national": {
            "mapData": map_data,
            "provinceScoreTop10": province_score_top10,
            "provinceOperaCountScoreCompare": compare_top10,
            "operaCountScoreAI": opera_count_score_ai,
            "wordCloud": national_word_cloud,
            "radarScores": national_radar,
            "audiencePortrait": national_audience,
            "tgi": national_tgi,
        },
        "provinces": province_output,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"[analyze_dashboard] 输出完成: {output_path}")
    return output


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description="生成看板分析 JSON 数据。")
    parser.add_argument(
        "--all-operas",
        type=Path,
        default=project_root / "dataProcess" / "rawData" / "allOperas_Unprocessed.xlsx",
    )
    parser.add_argument(
        "--audience-portrait",
        type=Path,
        default=project_root / "dataProcess" / "rawData" / "audiencePortrait.xlsx",
    )
    parser.add_argument(
        "--video-info",
        type=Path,
        default=project_root / "dataProcess" / "rawData" / "getData_bilibili" / "video_info.csv",
    )
    parser.add_argument(
        "--comments",
        type=Path,
        default=project_root / "dataProcess" / "rawData" / "getData_bilibili" / "comments_data.csv",
    )
    parser.add_argument(
        "--danmaku",
        type=Path,
        default=project_root / "dataProcess" / "rawData" / "getData_bilibili" / "danmaku_data.csv",
    )
    parser.add_argument(
        "--video-analysis",
        type=Path,
        default=project_root / "dataProcess" / "output" / "video_analysis.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=project_root / "dataProcess" / "output" / "dashboard_data.json",
    )
    parser.add_argument(
        "--qwen-script",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "Qwen_Analysis.py",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_dashboard_data(
        all_operas_path=args.all_operas,
        audience_portrait_path=args.audience_portrait,
        video_info_path=args.video_info,
        comments_path=args.comments,
        danmaku_path=args.danmaku,
        video_analysis_path=args.video_analysis,
        output_path=args.output,
        qwen_script_path=args.qwen_script,
    )
