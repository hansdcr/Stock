#!/usr/bin/env python3
"""
测试沪深300相对强度积分策略
"""
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from IBelive.core.strategy.csi300_above_ma_strategy import CSI300RelativeStrengthStrategy
from IBelive.core.parse_config import ParseConfig

def test_strategy():
    """测试策略"""
    print("🧪 开始测试沪深300相对强度积分策略...")
    
    # 加载配置
    config = ParseConfig()
    
    # 创建策略实例
    strategy = CSI300RelativeStrengthStrategy(config)
    
    print(f"📋 策略名称: {strategy.strategy_name}")
    print(f"📊 移动平均线周期: {strategy.ma_period}日")
    print(f"📈 最小成交量要求: {strategy.min_volume:,}股")
    
    # 准备数据
    print("\n📊 准备策略数据...")
    data_prepared = strategy.prepare_data()
    
    if not data_prepared:
        print("❌ 数据准备失败")
        return False
    
    # 执行策略
    print("\n🎯 执行选股策略...")
    results = strategy.execute()
    
    if not results:
        print("⚠️  未选出符合条件的股票")
        return True
    
    # 保存结果
    print("\n💾 保存选股结果...")
    strategy.save_results(results)
    
    print("\n✅ 策略测试完成!")
    return True

if __name__ == "__main__":
    try:
        success = test_strategy()
        if success:
            print("\n🎉 策略测试成功!")
        else:
            print("\n❌ 策略测试失败!")
            sys.exit(1)
    except Exception as e:
        print(f"\n💥 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)