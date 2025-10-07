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


def test_momentum_strategy_default_dates():
    """测试动量选股策略（使用默认日期）"""
    print("🚀 开始测试动量选股策略（默认日期）...")
    
    # 初始化配置
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建策略实例（使用默认日期）
    strategy = MomentumStrategy(config, pro)
    
    # 运行策略
    results = strategy.run()
    
    if results:
        print(f"\n✅ 策略测试成功！共选出 {len(results)} 只动量股票")
        return True
    else:
        print("❌ 策略测试失败")
        return False


def test_momentum_strategy_with_dates(start_date, end_date):
    """测试动量选股策略（使用指定日期）"""
    print(f"🚀 开始测试动量选股策略（日期范围: {start_date} 到 {end_date}）...")
    
    # 初始化配置
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建策略实例（使用指定日期）
    strategy = MomentumStrategy(config, pro, start_date=start_date, end_date=end_date)
    
    # 运行策略
    results = strategy.run()
    
    if results:
        print(f"\n✅ 策略测试成功！共选出 {len(results)} 只动量股票")
        return True
    else:
        print("❌ 策略测试失败")
        return False


if __name__ == "__main__":
    # 测试默认日期
    print("=" * 60)
    print("测试1: 使用默认日期")
    print("=" * 60)
    success1 = test_momentum_strategy_default_dates()
    
    print("\n" + "=" * 60)
    print("测试2: 使用指定日期 (20240925 到 20240930)")
    print("=" * 60)
    success2 = test_momentum_strategy_with_dates("20240925", "20240930")
    
    # 两个测试都成功才算成功
    success = success1 and success2
    sys.exit(0 if success else 1)