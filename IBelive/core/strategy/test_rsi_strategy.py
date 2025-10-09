"""
RSI策略测试文件
测试RSI相对强弱指数策略的计算和功能
"""
import pandas as pd
import numpy as np
from IBelive.core.strategy.rsi_strategy import RSIStrategy
from IBelive.core.parse_config import ParseConfig


def test_rsi_calculation():
    """测试RSI计算方法"""
    print("🧪 测试RSI计算方法...")
    
    # 创建配置实例
    config = ParseConfig()
    
    # 创建RSI策略实例
    strategy = RSIStrategy(config, rsi_period=14, ma_period=6)
    
    # 创建测试数据
    test_prices = pd.Series([
        100.0, 102.0, 101.5, 103.0, 105.0, 104.0, 106.0, 108.0, 
        107.0, 109.0, 111.0, 110.0, 112.0, 115.0, 117.0, 116.5
    ])
    
    # 计算RSI
    rsi_values = strategy.calculate_rsi(test_prices, 14)
    
    print(f"📊 测试价格序列长度: {len(test_prices)}")
    print(f"📈 计算出的RSI值长度: {len(rsi_values)}")
    print(f"🔢 前5个RSI值: {rsi_values.head().tolist()}")
    print(f"🔢 后5个RSI值: {rsi_values.tail().tolist()}")
    
    # 验证RSI值在合理范围内
    valid_rsi = rsi_values.dropna()
    assert all((valid_rsi >= 0) & (valid_rsi <= 100)), "RSI值应该在0-100之间"
    print("✅ RSI值范围验证通过")
    
    return rsi_values


def test_rsi_status_determination():
    """测试RSI状态判断"""
    print("\n🧪 测试RSI状态判断...")
    
    config = ParseConfig()
    strategy = RSIStrategy(config)
    
    # 测试各种RSI值的状态判断
    test_cases = [
        (25.0, "超卖"),
        (29.9, "超卖"),
        (30.0, "正常"),
        (50.0, "正常"),
        (69.9, "正常"),
        (70.0, "正常"),
        (70.1, "超买"),
        (85.0, "超买")
    ]
    
    for rsi_value, expected_status in test_cases:
        actual_status = strategy.determine_rsi_status(rsi_value)
        assert actual_status == expected_status, f"RSI值{rsi_value}应该返回{expected_status}, 但得到{actual_status}"
        print(f"✅ RSI={rsi_value} -> {actual_status}")
    
    print("✅ 所有状态判断测试通过")


def test_rsi_ma_calculation():
    """测试RSI移动平均计算"""
    print("\n🧪 测试RSI移动平均计算...")
    
    config = ParseConfig()
    strategy = RSIStrategy(config)
    
    # 创建测试RSI值序列
    test_rsi_values = pd.Series([45.2, 52.8, 58.3, 62.1, 67.5, 71.2, 73.8, 69.4, 65.1, 62.7])
    
    # 计算6日移动平均
    rsi_ma_values = strategy.calculate_rsi_ma(test_rsi_values, 6)
    
    print(f"📊 测试RSI序列: {test_rsi_values.tolist()}")
    print(f"📈 计算出的RSI MA值: {rsi_ma_values.tolist()}")
    
    # 验证移动平均计算
    expected_ma_6 = test_rsi_values.rolling(window=6, min_periods=1).mean()
    assert rsi_ma_values.equals(expected_ma_6), "RSI MA计算应该与pandas rolling mean一致"
    print("✅ RSI移动平均计算验证通过")
    
    return rsi_ma_values


def test_single_stock_rsi():
    """测试单只股票的RSI计算"""
    print("\n🧪 测试单只股票的RSI计算...")
    
    try:
        # 使用内置的测试函数
        from IBelive.core.strategy.rsi_strategy import test_rsi_strategy_for_stock
        results = test_rsi_strategy_for_stock("000001.SZ", rsi_period=14, ma_period=6)
        
        if results:
            print(f"✅ 成功计算单只股票的RSI，得到 {len(results)} 条记录")
            
            # 显示前几条结果
            for i, result in enumerate(results[:3]):
                print(f"  第{i+1}条: {result['trade_date']} - RSI: {result['rsi_value']:.2f}, MA: {result['rsi_ma_value']:.2f}, 状态: {result['rsi_status']}")
            
            # 分析RSI状态分布
            status_counts = {}
            for result in results:
                status = result['rsi_status']
                status_counts[status] = status_counts.get(status, 0) + 1
            
            print("📊 单只股票RSI状态分布:")
            for status, count in status_counts.items():
                print(f"  {status}: {count} 条 ({count/len(results)*100:.1f}%)")
        else:
            print("⚠️  未找到000001.SZ的数据或计算失败")
        
        return results
    except Exception as e:
        print(f"❌ 单只股票测试失败: {e}")
        return []


def test_full_rsi_strategy():
    """测试完整的RSI策略（包括数据库保存）"""
    print("\n🧪 测试完整的RSI策略（包括数据库保存）...")
    
    try:
        # 使用内置的测试函数
        from IBelive.core.strategy.rsi_strategy import test_rsi_strategy
        success = test_rsi_strategy()
        
        if success:
            print("✅ 完整RSI策略测试成功")
        else:
            print("❌ 完整RSI策略测试失败")
        
        return success
    except Exception as e:
        print(f"❌ 完整RSI策略测试失败: {e}")
        return False


def test_custom_periods():
    """测试自定义RSI周期"""
    print("\n🧪 测试自定义RSI周期...")
    
    config = ParseConfig()
    
    # 测试不同的RSI周期
    test_periods = [7, 14, 21, 30]
    
    for period in test_periods:
        print(f"\n🔧 测试 {period} 天RSI周期...")
        
        strategy = RSIStrategy(config, rsi_period=period, ma_period=6)
        
        # 创建测试数据
        test_prices = pd.Series(np.random.uniform(90, 110, 50))
        
        # 计算RSI
        rsi_values = strategy.calculate_rsi(test_prices, period)
        
        valid_rsi = rsi_values.dropna()
        print(f"  RSI值范围: {valid_rsi.min():.1f} - {valid_rsi.max():.1f}")
        print(f"  有效RSI值数量: {len(valid_rsi)}")
        
        assert len(valid_rsi) >= 1, f"{period}天周期应该产生有效的RSI值"
    
    print("✅ 所有自定义周期测试通过")


def main():
    """主测试函数"""
    print("=" * 60)
    print("🚀 开始RSI策略测试")
    print("=" * 60)
    
    try:
        # 运行所有测试
        test_rsi_calculation()
        test_rsi_status_determination()
        test_rsi_ma_calculation()
        test_custom_periods()
        
        print("\n📋 跳过需要数据库连接的测试...")
        print("💡 要测试完整功能，请确保MySQL数据库配置正确")
        
        # 跳过需要数据库的测试
        test_single_stock_rsi()
        test_full_rsi_strategy()
        
        print("\n" + "=" * 60)
        print("🎉 核心RSI算法测试通过！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        raise


if __name__ == "__main__":
    main()