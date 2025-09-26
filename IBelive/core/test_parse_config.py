#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试用例：验证 ParseConfig 类是否能正确获取 token 和 MySQL 配置
运行：
  python IBelive/core/test_parse_config.py
"""
import os
import sys
import unittest
from unittest.mock import patch, mock_open

# 添加当前目录到 Python 路径以便导入 ParseConfig
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

from ParseConfig import ParseConfig  # noqa: E402


class TestParseConfig(unittest.TestCase):
    
    def setUp(self):
        """测试前的准备工作"""
        # 实际的配置文件路径
        self.config_file = os.path.join(os.path.dirname(THIS_DIR), "config", "config.yaml")
        
    def test_config_file_exists(self):
        """测试配置文件是否存在"""
        self.assertTrue(os.path.exists(self.config_file), 
                       f"配置文件不存在: {self.config_file}")
    
    def test_parse_config_initialization(self):
        """测试 ParseConfig 类能否正常初始化"""
        try:
            pc = ParseConfig()
            self.assertIsNotNone(pc)
            self.assertIsNotNone(pc.config)
            self.assertIsInstance(pc.config, dict)
        except Exception as e:
            self.fail(f"ParseConfig 初始化失败: {e}")
    
    def test_get_token(self):
        """测试获取 token 功能"""
        pc = ParseConfig()
        token = pc.get_token()
        
        # token 应该存在且不为空字符串
        self.assertIsNotNone(token, "token 不能为 None")
        if token is not None:
            self.assertIsInstance(token, str, "token 应该是字符串类型")
            self.assertNotEqual(token.strip(), "", "token 不能为空字符串")
            self.assertNotIn("your_tushare_api_token_here", token, 
                           "请替换默认的 token 占位符")
    
    def test_get_mysql_config(self):
        """测试获取 MySQL 配置功能"""
        pc = ParseConfig()
        mysql_config = pc.get_mysql_config()
        
        # MySQL 配置应该是一个字典
        self.assertIsInstance(mysql_config, dict, "MySQL 配置应该是字典类型")
        
    def test_token_in_config(self):
        """测试 token 确实存在于配置文件中"""
        import yaml
        with open(self.config_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f) or {}
        
        self.assertIn('token', config_data, "配置文件中应该包含 token 字段")
        
        token = config_data.get('token')
        if token:
            self.assertNotEqual(token.strip(), "", "配置文件中的 token 不能为空")


def main():
    """运行测试"""
    print("正在运行 ParseConfig 测试用例...")
    
    # 检查配置文件是否存在
    config_file = os.path.join(os.path.dirname(THIS_DIR), "config", "config.yaml")
    if not os.path.exists(config_file):
        print(f"❌ 配置文件不存在: {config_file}")
        print("请先创建配置文件: IBelive/config/config.yaml")
        print("内容示例:")
        print("token: \"你的Tushare Token\"")
        print("mysql:")
        print("  host: localhost")
        print("  port: 3306")
        print("  user: root")
        print("  password: password")
        print("  database: stock_db")
        return 1
    
    # 运行单元测试
    unittest.main(argv=[''], verbosity=2, exit=False)
    
    # 额外显示 token 信息（部分隐藏）
    try:
        pc = ParseConfig()
        token = pc.get_token()
        if token:
            masked_token = token[:6] + "*" * (len(token) - 6) if len(token) > 6 else "*" * 6
            print(f"\n🔐 当前 token (部分显示): {masked_token}")
        else:
            print("\n❌ 未获取到 token")
    except Exception as e:
        print(f"\n❌ 获取 token 失败: {e}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())