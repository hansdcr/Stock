#!/usr/bin/env python3
"""
测试动量选股策略
"""
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from IBelive.core.parse_config import ParseConfig
from IBelive.core.strategy.momentum_strategy import MomentumStrategy
import tushare as ts


def test_momentum_strategy():
    """测试动量选股策略"""
    print("🚀 开始测试动量选股策略...")
    
    # 初始化配置
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建策略实例
    strategy = MomentumStrategy(config, pro)
    
    # 运行策略
    results = strategy.run()
    
    if results:
        print(f"\n✅ 策略测试成功！共选出 {len(results)} 只动量股票")
        return True
    else:
        print("❌ 策略测试失败")
        return False


if __name__ == "__main__":
    success = test_momentum_strategy()
    sys.exit(0 if success else 1)