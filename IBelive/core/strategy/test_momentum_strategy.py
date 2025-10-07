#!/usr/bin/env python3
"""
测试动量选股策略
包含：基础测试、价格计算验证、最小数据点数配置测试
"""
import sys
import os
import pandas as pd

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from IBelive.core.parse_config import ParseConfig
from IBelive.core.strategy.momentum_strategy import MomentumStrategy
import tushare as ts


def test_momentum_strategy_default_dates(min_data_points=None):
    """测试动量选股策略（使用默认日期）"""
    print(f"🚀 开始测试动量选股策略（默认日期，最小数据点数: {min_data_points or 20}）...")
    
    # 初始化配置
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建策略实例（使用默认日期）
    strategy = MomentumStrategy(config, pro, min_data_points=min_data_points)
    
    # 运行策略
    results = strategy.run()
    
    if results:
        print(f"\n✅ 策略测试成功！共选出 {len(results)} 只动量股票")
        return results  # 返回结果列表而不是布尔值
    else:
        print("❌ 策略测试失败")
        return []  # 返回空列表而不是False


def test_momentum_strategy_with_dates(start_date, end_date, min_data_points=None):
    """测试动量选股策略（使用指定日期）"""
    print(f"🚀 开始测试动量选股策略（日期范围: {start_date} 到 {end_date}，最小数据点数: {min_data_points or 20}）...")
    
    # 初始化配置
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建策略实例（使用指定日期）
    strategy = MomentumStrategy(config, pro, start_date=start_date, end_date=end_date, min_data_points=min_data_points)
    
    # 运行策略
    results = strategy.run()
    
    if results:
        print(f"\n✅ 策略测试成功！共选出 {len(results)} 只动量股票")
        return results  # 返回结果列表而不是布尔值
    else:
        print("❌ 策略测试失败")
        return []  # 返回空列表而不是False


def verify_price_calculation():
    """验证期初价格和期末价格的计算逻辑"""
    print("🔍 验证期初价格和期末价格计算...")
    
    # 模拟股票数据（假设从20250901到20250930的数据）
    sample_data = {
        'ts_code': ['000001.SZ'] * 20,  # 20个交易日
        'trade_date': [
            '20250901', '20250902', '20250903', '20250904', '20250905',
            '20250908', '20250909', '20250910', '20250911', '20250912',
            '20250915', '20250916', '20250917', '20250918', '20250919',
            '20250922', '20250923', '20250924', '20250925', '20250930'
        ],
        'close': [
            10.00, 10.20, 10.50, 10.30, 10.80,  # 第一周
            11.00, 11.20, 11.50, 11.30, 11.80,  # 第二周
            12.00, 12.20, 12.50, 12.30, 12.80,  # 第三周
            13.00, 13.20, 13.50, 13.30, 14.00   # 第四周
        ]
    }
    
    df = pd.DataFrame(sample_data)
    
    print("📊 模拟股票数据:")
    print(df.to_string(index=False))
    
    # 按交易日期排序（确保顺序正确）
    df = df.sort_values('trade_date')
    
    # 计算期初价格和期末价格
    start_close = df.iloc[0]['close']  # 第一条记录的收盘价
    end_close = df.iloc[-1]['close']   # 最后一条记录的收盘价
    
    print(f"\n📅 时间范围: {df.iloc[0]['trade_date']} 到 {df.iloc[-1]['trade_date']}")
    print(f"💰 期初价格 (20250901): {start_close:.2f}")
    print(f"💰 期末价格 (20250930): {end_close:.2f}")
    
    # 计算动量
    momentum = (end_close - start_close) / start_close * 100
    print(f"📈 动量值: {momentum:.2f}%")
    print(f"📊 绝对涨幅: {end_close - start_close:.2f}")
    
    # 验证具体日期
    print(f"\n✅ 验证:")
    print(f"   期初价格对应日期: {df.iloc[0]['trade_date']} -> {start_close:.2f}")
    print(f"   期末价格对应日期: {df.iloc[-1]['trade_date']} -> {end_close:.2f}")
    
    return True


def test_different_min_data_points():
    """测试不同的最小数据点数配置"""
    print("🧪 测试最小数据点数配置功能...")
    
    # 测试不同的最小数据点数要求
    test_cases = [
        ("20240901", "20240930", 5, "5天数据要求"),
        ("20240901", "20240930", 10, "10天数据要求"),
        ("20240901", "20240930", 15, "15天数据要求"),
        ("20240901", "20240930", 20, "20天数据要求（默认）"),
    ]
    
    all_success = True
    
    for start_date, end_date, min_data_points, description in test_cases:
        print(f"\n🔬 测试用例: {description}")
        print(f"   📅 日期范围: {start_date} 到 {end_date}")
        print(f"   📊 最小数据点数: {min_data_points}")
        
        try:
            results = test_momentum_strategy_with_dates(start_date, end_date, min_data_points)
            print(f"   ✅ 选股结果: {len(results)} 只股票")
            
            if results:
                for i, stock in enumerate(results[:3], 1):  # 显示前3只
                    print(f"      {i}. {stock['ts_code']} - 动量: {stock['momentum']:.2f}%")
            else:
                all_success = False
                print("   ⚠️  未选出股票")
            
        except Exception as e:
            print(f"   ❌ 测试失败: {e}")
            all_success = False
    
    return all_success


if __name__ == "__main__":
    # # 测试1: 价格计算验证
    # print("=" * 60)
    # print("测试1: 价格计算验证")
    # print("=" * 60)
    # verify_price_calculation()
    
    # # 测试2: 使用默认日期
    # print("\n" + "=" * 60)
    # print("测试2: 使用默认日期")
    # print("=" * 60)
    # results2 = test_momentum_strategy_default_dates()
    # success2 = len(results2) > 0  # 只要有结果就认为成功
    
    # 测试3: 使用指定日期
    print("\n" + "=" * 60)
    print("测试3: 使用指定日期 (20250901 到 20250930)")
    print("=" * 60)
    results3 = test_momentum_strategy_with_dates("20250901", "20250930", min_data_points=20)
    success3 = len(results3) > 0  # 只要有结果就认为成功
    
    # # 测试4: 最小数据点数配置测试
    # print("\n" + "=" * 60)
    # print("测试4: 最小数据点数配置测试")
    # print("=" * 60)
    # success4 = test_different_min_data_points()
    
    # # 所有测试都成功才算成功
    # success = success2 and success3 and success4
    # print(f"\n🎯 总体测试结果: {'✅ 成功' if success else '❌ 失败'}")
    # sys.exit(0 if success else 1)