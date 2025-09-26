#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick test script to verify Tushare Pro API connectivity.
- Reads token from ./IBelive/config/config.yaml or ./config/config.yaml (YAML key: token)
- Or read token from env var TUSHARE_TOKEN if set
- Initializes pro client
- Calls a lightweight endpoint and prints the first few rows

Run (from project root):
  python IBelive/test_api.py
"""
import os
import sys
from typing import Optional
import argparse
from datetime import datetime, timedelta

CONFIG_REL_PATH = os.path.join("config", "config.yaml")


def _read_token_from_yaml(cfg_path: str) -> Optional[str]:
    """Read `token` from a YAML file. Falls back to simple parsing if PyYAML not installed."""
    if not os.path.exists(cfg_path):
        return None

    # Try PyYAML if available
    try:
        import yaml  # type: ignore
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            token = data.get("token")
            if isinstance(token, str):
                return token.strip()
            return None
    except Exception:
        # Fallback: naive parse "token: value" lines
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                for line in f:
                    line_stripped = line.strip()
                    if line_stripped.startswith("token:"):
                        value = line_stripped.split(":", 1)[1].strip()
                        # trim quotes if present
                        if (value.startswith("\"") and value.endswith("\"")) or (
                            value.startswith("'") and value.endswith("'")
                        ):
                            value = value[1:-1]
                        return value
        except Exception:
            return None
    return None


def resolve_tushare_token() -> tuple[Optional[str], Optional[str]]:
    """Resolve Tushare token with precedence: env var -> config files.

    Returns a tuple of (token, source), where source is one of:
      - "ENV:TUSHARE_TOKEN"
      - path to the config file used
      - None if not found
    """
    # env var takes precedence
    env_token = os.getenv("TUSHARE_TOKEN")
    if env_token and env_token.strip():
        return env_token.strip(), "ENV:TUSHARE_TOKEN"

    # search candidate config paths
    script_dir = os.path.dirname(os.path.abspath(__file__))  # .../StockIBelive/IBelive
    project_root = os.path.dirname(script_dir)               # .../StockIBelive
    candidate_cfg_paths = [
        os.path.join(script_dir, "config", "config.yaml"),
        os.path.join(project_root, "config", "config.yaml"),
    ]
    for p in candidate_cfg_paths:
        token = _read_token_from_yaml(p)
        if token:
            return token, p

    return None, None


def test_tushare() -> int:
    # Resolve paths (for error hints/printing only)
    script_dir = os.path.dirname(os.path.abspath(__file__))  # .../StockIBelive/IBelive
    project_root = os.path.dirname(script_dir)               # .../StockIBelive

    candidate_cfg_paths = [
        os.path.join(script_dir, "config", "config.yaml"),     # ./IBelive/config/config.yaml
        os.path.join(project_root, "config", "config.yaml"),   # ./config/config.yaml
    ]

    # Unified token resolution
    token, cfg_path_used = resolve_tushare_token()

    if not token or token in {"your_tushare_api_token_here", "", "TODO"}:
        print("[ERROR] 未找到有效的 token，请在以下任一位置进行配置后重试：\n")
        print("1) 环境变量: TUSHARE_TOKEN\n2) 配置文件键 token: \n   - ./IBelive/config/config.yaml\n   - ./config/config.yaml\n")
        print("配置示例：\n  token: \"你的Tushare Token\"\n")
        print("已尝试读取的路径(若使用环境变量则显示 ENV):")
        print("  - ENV:TUSHARE_TOKEN")
        for p in candidate_cfg_paths:
            print(f"  - {p}")
        return 2

    try:
        import tushare as ts  # type: ignore
    except ImportError:
        print("[ERROR] 未安装 tushare 包，请先安装：\n  pip install tushare\n")
        return 3

    # Initialize pro client directly with token (no need to persist token locally)
    try:
        pro = ts.pro_api(token)
    except Exception as e:
        print(f"[ERROR] 初始化 Tushare Pro 客户端失败: {e}")
        return 4

    # Make a lightweight test call: trade calendar for a short range
    try:
        df = pro.trade_cal(
            exchange="",  # all exchanges
            start_date="20240101",
            end_date="20240110",
            fields="exchange,cal_date,is_open,pretrade_date",
            is_open="1",  # only open days
        )
        print("Tushare API 调用成功，示例数据前5行：")
        try:
            # DataFrame head
            print(df.head())
        except Exception:
            # If not a DataFrame, print raw
            print(df)
        return 0
    except Exception as e:
        print("[ERROR] Tushare API 调用失败：")
        print(e)
        # 常见原因：token无效/权限不足/网络异常/时间段无交易日等
        return 5


def fetch_listed_companies(asof_date: Optional[str] = None, output_path: Optional[str] = None) -> int:
    """
    获取截至指定日期(默认昨天)的所有在市公司基本信息（股票代码、名称、上市日期等），并保存为 CSV。
    - asof_date: 字符串 YYYYMMDD，若为空则取昨天
    - output_path: 输出 CSV 路径，若为空则保存到 ./IBelive/core/listed_companies_YYYYMMDD.csv
    返回 0 表示成功，其它为错误码。
    """
    # 计算 asof_date（默认昨天）
    if not asof_date:
        asof_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    # 跨模块统一的凭据解析
    token, _ = resolve_tushare_token()
    if not token or token in {"your_tushare_api_token_here", "", "TODO"}:
        print("[ERROR] 未找到有效的 token，无法请求 Tushare。请设置 TUSHARE_TOKEN 或配置文件 token。")
        return 2

    try:
        import tushare as ts  # type: ignore
    except ImportError:
        print("[ERROR] 未安装 tushare 包，请先安装：\n  pip install tushare\n")
        return 3

    try:
        pro = ts.pro_api(token)
    except Exception as e:
        print(f"[ERROR] 初始化 Tushare Pro 客户端失败: {e}")
        return 4

    # 拉取在市公司（list_status='L'），并按上市日期过滤<= asof_date
    try:
        fields = "ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date"
        df = pro.stock_basic(exchange="", list_status="L", fields=fields)
        if df is None:
            print("[ERROR] 接口返回为空。")
            return 5
        # 过滤上市日期<= asof_date
        try:
            df = df.copy()
            df["list_date"] = df["list_date"].astype(str)
            df = df[df["list_date"] <= asof_date]
        except Exception:
            # 如果缺少 list_date 字段或转换失败，则忽略过滤
            pass

        # 输出路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        if not output_path:
            core_dir = os.path.join(script_dir, "core")
            os.makedirs(core_dir, exist_ok=True)
            output_path = os.path.join(core_dir, f"listed_companies_{asof_date}.csv")

        df.to_csv(output_path, index=False)
        print(f"成功获取 {len(df)} 条记录，并已保存至: {output_path}")
        # 展示前几行
        try:
            print(df.head())
        except Exception:
            pass
        return 0
    except Exception as e:
        print("[ERROR] 获取上市公司列表失败：")
        print(e)
        return 6


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tushare 测试与数据拉取工具")
    parser.add_argument("--list-companies", action="store_true", help="获取截至日期的在市公司基本信息并保存 CSV")
    parser.add_argument("--asof", type=str, default=None, help="截止日期 YYYYMMDD，默认昨天")
    parser.add_argument("--out", type=str, default=None, help="输出 CSV 文件路径")
    args = parser.parse_args()

    if args.list_companies:
        sys.exit(fetch_listed_companies(asof_date=args.asof, output_path=args.out))
    else:
        sys.exit(test_tushare())