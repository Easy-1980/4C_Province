from __future__ import annotations

import argparse
import importlib.util
from datetime import datetime
from pathlib import Path

from analyze_dashboard import build_dashboard_data
from analyze_video import build_video_analysis


def _log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")


def _check_required_files(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        missing_text = "\n".join(missing)
        raise FileNotFoundError(f"以下输入文件不存在，请检查路径：\n{missing_text}")


def _check_runtime_dependencies() -> None:
    required_modules = ["openpyxl", "jieba"]
    missing = [name for name in required_modules if importlib.util.find_spec(name) is None]
    if missing:
        missing_text = ", ".join(missing)
        raise ModuleNotFoundError(
            f"缺少运行依赖：{missing_text}。请先在你的虚拟环境中安装后再执行 build_all_data.py。"
        )


def build_all_data(
    all_operas_path: Path,
    audience_portrait_path: Path,
    video_info_path: Path,
    comments_path: Path,
    danmaku_path: Path,
    output_dir: Path,
    qwen_script_path: Path,
) -> None:
    _log("检查运行依赖...")
    _check_runtime_dependencies()

    _log("开始检查输入文件...")
    _check_required_files(
        [
            all_operas_path,
            audience_portrait_path,
            video_info_path,
            comments_path,
            danmaku_path,
        ]
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    video_output = output_dir / "video_analysis.json"
    dashboard_output = output_dir / "dashboard_data.json"

    _log("开始执行 analyze_video，生成 video_analysis.json ...")
    build_video_analysis(
        video_info_path=video_info_path,
        comments_path=comments_path,
        danmaku_path=danmaku_path,
        output_path=video_output,
        qwen_script_path=qwen_script_path,
    )

    _log("开始执行 analyze_dashboard，生成 dashboard_data.json ...")
    build_dashboard_data(
        all_operas_path=all_operas_path,
        audience_portrait_path=audience_portrait_path,
        video_info_path=video_info_path,
        comments_path=comments_path,
        danmaku_path=danmaku_path,
        video_analysis_path=video_output,
        output_path=dashboard_output,
        qwen_script_path=qwen_script_path,
    )

    _log(f"全部任务完成。输出目录：{output_dir}")
    _log(f" - {video_output}")
    _log(f" - {dashboard_output}")


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description="一键构建视频级与省份级 JSON 数据。")
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
        "--output-dir",
        type=Path,
        default=project_root / "dataProcess" / "output",
    )
    parser.add_argument(
        "--qwen-script",
        type=Path,
        default=Path(__file__).resolve().parent / "Qwen_Analysis.py",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_all_data(
        all_operas_path=args.all_operas,
        audience_portrait_path=args.audience_portrait,
        video_info_path=args.video_info,
        comments_path=args.comments,
        danmaku_path=args.danmaku,
        output_dir=args.output_dir,
        qwen_script_path=args.qwen_script,
    )
