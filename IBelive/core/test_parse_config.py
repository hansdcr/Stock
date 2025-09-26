#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试：验证 ParseConfig.get_token 是否能正确读取 IBelive/config/config.yaml 中的 token。
运行：
  python IBelive/core/test_parse_config.py
"""
import os
import sys

try:
    import yaml  # type: ignore
except Exception:
    print("[ERROR] 未安装 PyYAML，请先执行: pip install pyyaml")
    sys.exit(3)

# 便于从同目录导入 ParseConfig
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

from ParseConfig import ParseConfig  # noqa: E402


def main() -> int:
    cfg_path = os.path.join(os.path.dirname(THIS_DIR), "config", "config.yaml")
    if not os.path.exists(cfg_path):
        print(f"[ERROR] 配置文件不存在: {cfg_path}")
        return 1

    # 使用 ParseConfig 读取 token
    pc = ParseConfig(cfg_path)
    token_from_class = pc.get_token()

    # 使用 YAML 直接读取作为基准
    with open(cfg_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    token_from_yaml = data.get("token")

    if token_from_class and token_from_yaml and token_from_class == token_from_yaml:
        masked = (
            token_from_class[:6] + "..."
            if isinstance(token_from_class, str) and len(token_from_class) > 6
            else str(token_from_class)
        )
        print(f"[PASS] 成功获取 token，与配置一致。token(部分显示): {masked}")
        return 0
    else:
        print("[FAIL] 读取 token 不一致或为空：")
        print(f"  get_token(): {token_from_class!r}")
        print(f"  yaml token:  {token_from_yaml!r}")
        return 2


if __name__ == "__main__":
    sys.exit(main())