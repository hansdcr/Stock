"""
指数日线数据管理器功能测试脚本
"""
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from core.index.index_daily_manager import IndexDailyManager
from core.parse_config import ParseConfig
import tushare as ts

def test_index_daily_manager():
    """测试指数日线数据管理器功能"""
    print("🚀 开始测试指数日线数据管理器功能")
    
    # 初始化配置和Tushare Pro
    try:
        config = ParseConfig()
        pro = ts.pro_api(config.get_token())
        
        # 创建指数日线数据管理器
        manager = IndexDailyManager(config, pro)
        print("✅ 成功创建IndexDailyManager实例")
        
        # # 测试1: 获取单个指数的日线数据
        # print("\n📊 测试1: 获取单个指数的日线数据")
        # test_ts_code = "000001.SH"  # 上证指数
        # test_trade_date = "20250929"  # 测试日期
        
        # df = manager.fetch_index_daily_data(test_ts_code, test_trade_date)
        # if df is not None and not df.empty:
        #     print(f"✅ 成功获取 {test_ts_code} 在 {test_trade_date} 的数据")
        #     print(f"数据形状: {df.shape}")
        #     print(f"数据列名: {list(df.columns)}")
        #     print(f"前5行数据:")
        #     print(df.head())
        # else:
        #     print(f"❌ 获取 {test_ts_code} 在 {test_trade_date} 的数据失败")
        
        # # 测试2: 获取并保存单个指数的日线数据到MySQL
        # print("\n💾 测试2: 获取并保存单个指数的日线数据到MySQL")
        # df_saved = manager.fetch_and_save_index_daily_data(test_ts_code, test_trade_date)
        # if df_saved is not None:
        #     print(f"✅ 数据获取和保存操作完成")
        # else:
        #     print(f"❌ 数据获取和保存操作失败")
        

        start_date = "20250101"
        end_date = "20251001"
        # 沪深300,上证50，中证500, 创业板指, 上证100
        test_ts_code = ["000300.SH", "000016.SH", "000905.SH", "399001.SZ", "000001.SH"]
        
        # df_period = manager.fetch_index_daily_data_period(test_ts_code, start_date, end_date)
        # if df_period is not None and not df_period.empty:
        #     print(f"✅ 成功获取 {test_ts_code} 在 {start_date} 到 {end_date} 的数据")
        #     print(f"数据形状: {df_period.shape}")
        #     print(f"交易日期范围: {df_period['trade_date'].min()} 到 {df_period['trade_date'].max()}")
        # else:
        #     print(f"❌ 获取 {test_ts_code} 在 {start_date} 到 {end_date} 的数据失败")
        
        # 测试5: 获取并保存时间段内的指数数据到MySQL
        print("\n💾 测试5: 获取并保存时间段内的指数数据到MySQL")
        # 循环处理每个指数代码
        for ts_code in test_ts_code:
            print(f"\n📊 处理指数: {ts_code}")
            df_period_saved = manager.fetch_and_save_index_daily_data_period(
                ts_code, start_date, end_date
            )
            if df_period_saved is not None and not df_period_saved.empty:
                print(f"✅ 成功处理指数 {ts_code}")
            else:
                print(f"⚠️  处理指数 {ts_code} 时未找到数据")
        
        print(f"✅ 所有指数的时间段数据获取和保存操作完成")
        
        # # 测试6: 获取指定时间段内所有指数的日线数据
        # print("\n📊 测试6: 获取指定时间段内所有指数的日线数据")
        # df_all_indexes = manager.fetch_all_index_daily_data_period(start_date, end_date)
        # if df_all_indexes is not None and not df_all_indexes.empty:
        #     print(f"✅ 成功获取所有指数在 {start_date} 到 {end_date} 的数据")
        #     print(f"数据形状: {df_all_indexes.shape}")
        #     print(f"包含的指数数量: {df_all_indexes['ts_code'].nunique()}")
        #     print(f"交易日期范围: {df_all_indexes['trade_date'].min()} 到 {df_all_indexes['trade_date'].max()}")
        #     print(f"前5行数据:")
        #     print(df_all_indexes.head())
        # else:
        #     print(f"❌ 获取所有指数在 {start_date} 到 {end_date} 的数据失败")
        
        # # 测试7: 获取并保存指定时间段内所有指数的日线数据到MySQL
        # print("\n💾 测试7: 获取并保存指定时间段内所有指数的日线数据到MySQL")
        # df_all_saved = manager.fetch_and_save_all_index_daily_data_period(start_date, end_date)
        # if df_all_saved is not None:
        #     print(f"✅ 所有指数数据获取和保存操作完成")
        #     if not df_all_saved.empty:
        #         print(f"成功保存 {len(df_all_saved)} 条记录到MySQL")
        # else:
        #     print(f"❌ 所有指数数据获取和保存操作失败")
        
        print("\n🎉 所有测试完成！")
        
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_index_daily_manager()