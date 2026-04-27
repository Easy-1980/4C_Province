from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def normalize_col_name(name: str) -> str:
    return re.sub(r"[\s_]+", "", str(name)).lower()


def find_column(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    normalized_map = {normalize_col_name(col): col for col in df.columns}
    for alias in aliases:
        hit = normalized_map.get(normalize_col_name(alias))
        if hit is not None:
            return hit
    return None


def read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8")


def read_excel(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, engine="openpyxl")


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"未找到 JSON 文件: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def resolve_column_map(df: pd.DataFrame, alias_map: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
    return {key: find_column(df, aliases) for key, aliases in alias_map.items()}
