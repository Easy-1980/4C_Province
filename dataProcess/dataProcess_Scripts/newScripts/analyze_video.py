from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Optional


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from modules.common_utils import write_json
from modules.video_utils import build_video_analysis_data


def build_video_analysis(
    video_info_path: Path,
    comments_path: Path,
    danmaku_path: Path,
    output_path: Path,
    qwen_script_path: Optional[Path] = None,
) -> Dict[str, Any]:
    print(f"[analyze_video] 读取视频数据: {video_info_path}")
    output = build_video_analysis_data(
        video_info_path=video_info_path,
        comments_path=comments_path,
        danmaku_path=danmaku_path,
        qwen_script_path=qwen_script_path,
    )
    write_json(output_path, output)
    print(f"[analyze_video] 输出完成: {output_path}")
    return output


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description="生成视频分析 JSON 数据。")
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
        "--output",
        type=Path,
        default=project_root / "dataProcess" / "output" / "video_analysis.json",
    )
    parser.add_argument(
        "--qwen-script",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "Qwen_Analysis.py",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_video_analysis(
        video_info_path=args.video_info,
        comments_path=args.comments,
        danmaku_path=args.danmaku,
        output_path=args.output,
        qwen_script_path=args.qwen_script,
    )
