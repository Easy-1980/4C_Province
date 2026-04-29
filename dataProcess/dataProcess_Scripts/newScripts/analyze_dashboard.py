from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Optional


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from modules.dashboard_builder import build_dashboard_data as _build_dashboard_data_impl


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
    return _build_dashboard_data_impl(
        all_operas_path=all_operas_path,
        audience_portrait_path=audience_portrait_path,
        video_info_path=video_info_path,
        comments_path=comments_path,
        danmaku_path=danmaku_path,
        video_analysis_path=video_analysis_path,
        output_path=output_path,
        qwen_script_path=qwen_script_path,
    )


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
        default=Path(__file__).resolve().parent / "Qwen_Analysis.py",
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
