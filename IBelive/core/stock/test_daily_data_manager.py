#!/usr/bin/env python3
"""
测试日线数据管理器 (DailyDataManager)
测试从2025-09-27到2025-09-30期间的所有股票日线数据获取功能
"""

import sys
import os

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

from IBelive.core.parse_config import ParseConfig
from IBelive.core.stock.daily_data_manager import DailyDataManager
import tushare as ts
import pandas as pd

def test_daily_data_manager():
    """测试日线数据管理器"""
    print("🚀 开始测试日线数据管理器...")
    
    # 初始化配置和Tushare Pro API
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建日线数据管理器
    daily_data_manager = DailyDataManager(config, pro)
    
    # 测试时间段 - 使用已知的真实交易日
    start_date = "2020101"
    end_date = "20231231"
    
    print(f"📅 测试时间段: {start_date} 到 {end_date}")
    
    try:
        # 测试1: 获取指定时间段内所有股票的日线数据
        print("\n=== 测试1: 获取所有股票日线数据 ===")
        
        # 获取所有股票的基本信息
        stock_basic = pro.stock_basic(exchange='', list_status='L', 
                                   fields='ts_code,symbol,name,area,industry,list_date')
        
        if stock_basic.empty:
            print("❌ 无法获取股票基本信息")
            return False
            
        print(f"📊 共找到 {len(stock_basic)} 只上市股票")
        
        # 为了测试，只取前10只股票（避免数据量过大）
        test_stocks = stock_basic.head(10)['ts_code'].tolist()
        print(f"🔍 测试股票代码: {test_stocks}")
        
        # 获取日线数据（使用fetch_all_stocks_daily_data_period方法并自动保存到MySQL）
        all_stocks_data = daily_data_manager.fetch_all_stocks_daily_data_period(
            start_date=start_date,
            end_date=end_date,
            save_to_mysql=True
        )
        
        if not all_stocks_data:
            print(f"❌ 未获取到 {start_date} 到 {end_date} 期间的日线数据")
            return False
            
        print(f"✅ 成功获取 {len(all_stocks_data)} 只股票的日线数据")
        
        # 提取测试股票的数据
        test_data = []
        for ts_code in test_stocks:
            if ts_code in all_stocks_data:
                test_data.append(all_stocks_data[ts_code])
        
        if not test_data:
            print(f"❌ 未获取到测试股票的数据")
            return False
            
        # 合并所有测试股票的数据
        daily_data = pd.concat(test_data, ignore_index=True)
        
        print(f"✅ 成功获取测试股票在 {start_date} 到 {end_date} 期间的日线数据，共 {len(daily_data)} 条记录")
        print(f"📈 数据预览:")
        print(daily_data.head())
        
        print(f"✅ 数据已通过fetch_all_stocks_daily_data_period方法自动保存到MySQL")
            
        # 测试3: 从MySQL查询数据
        print("\n=== 测试3: 从MySQL查询数据 ===")
        
        mysql_manager = daily_data_manager.mysql_manager
        
        # 查询整个时间段的数据记录数
        result_df = mysql_manager.query_data(
            table_name="daily_data",
            columns=["COUNT(*) as count"],
            conditions="DATE(trade_date) BETWEEN %s AND %s",
            params=[start_date, end_date]
        )
        
        if result_df is not None and not result_df.empty:
            # 使用正确的列名访问
            count = result_df.iloc[0]['COUNT(*) as count']
            print(f"✅ MySQL中 {start_date} 到 {end_date} 的数据记录数: {count}")
            
            if count > 0:
                # 查询具体数据（按日期分组显示）
                date_result_df = mysql_manager.query_data(
                    table_name="daily_data",
                    columns=["DATE(trade_date) as date", "COUNT(*) as daily_count"],
                    conditions="DATE(trade_date) BETWEEN %s AND %s GROUP BY DATE(trade_date) ORDER BY DATE(trade_date)",
                    params=[start_date, end_date]
                )
                
                if date_result_df is not None and not date_result_df.empty:
                    print(f"📊 每日数据统计:")
                    for _, row in date_result_df.iterrows():
                        print(f"   {row.iloc[0]}: {row.iloc[1]} 条记录")
                
                # 查询前5条数据详情
                detail_result_df = mysql_manager.query_data(
                    table_name="daily_data",
                    columns=["ts_code", "trade_date", "open", "high", "low", "close", "vol"],
                    conditions="DATE(trade_date) BETWEEN %s AND %s",
                    params=[start_date, end_date],
                    order_by="trade_date, ts_code",
                    limit=5
                )
                
                if detail_result_df is not None and not detail_result_df.empty:
                    print(f"📋 前5条数据详情:")
                    for i, (_, row) in enumerate(detail_result_df.iterrows()):
                        print(f"   {i+1}. {row['ts_code']} {row['trade_date']}: {row['close']} (成交量: {row['vol']})")
        
        print("\n🎉 日线数据管理器测试完成！")
        return True
        
    except Exception as e:
        print(f"❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_daily_data_manager()
    sys.exit(0 if success else 1)