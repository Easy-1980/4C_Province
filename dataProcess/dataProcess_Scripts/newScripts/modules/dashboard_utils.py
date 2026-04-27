from __future__ import annotations

# 兼容层：保留旧导入路径，避免外部脚本中断。
from .dashboard_builder import build_dashboard_data

__all__ = ["build_dashboard_data"]
