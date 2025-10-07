"""
动量选股策略
基于过去20日涨幅选择排名前10%的股票
"""
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy
import tushare as ts
from IBelive.core.mysql_manager import MySQLManager


class MomentumStrategy(BaseStrategy):
    """动量选股策略"""
    
    def __init__(self, config, pro_api):
        """
        初始化动量策略
        
        :param config: 配置对象
        :param pro_api: Tushare Pro API对象
        """
        super().__init__(config)
        self.pro = pro_api
        self.mysql_manager = MySQLManager(config)
        self.lookback_period = 20  # 回看周期20天
        self.top_percentage = 0.1  # 选择前10%的股票
        self.stock_data = None
        self.stock_basic_df = None  # 存储股票基本信息
        
    def prepare_data(self) -> bool:
        """准备策略所需数据"""
        try:
            print("📊 准备动量策略数据...")
            
            # 1. 从MySQL数据库获取所有上市股票基本信息
            print("🔍 从MySQL数据库listed_companies表获取上市股票信息...")
            
            stock_basic_df = self.mysql_manager.query_data(
                table_name="listed_companies",
                columns=["ts_code", "symbol", "name", "area", "industry", "list_date"],
                conditions="list_status = 'L'"  # 只获取上市状态的股票
            )
            
            if stock_basic_df is None or stock_basic_df.empty:
                print("❌ 无法从MySQL获取股票基本信息")
                return False
                
            print(f"✅ 从MySQL获取到 {len(stock_basic_df)} 只上市股票")
            
            # 存储股票基本信息
            self.stock_basic_df = stock_basic_df
            
            # 2. 从MySQL数据库获取最近20个交易日的日线数据
            # 计算日期范围（这里需要根据实际情况调整日期）
            end_date = "20250930"  # 示例结束日期
            start_date = "20250901"  # 示例开始日期（大约20个交易日）
            
            print(f"📅 从MySQL获取 {start_date} 到 {end_date} 的日线数据...")
            
            # 获取测试股票列表（限制100只股票用于演示）
            test_stocks = stock_basic_df['ts_code'].head(100).tolist()
            
            # 从MySQL数据库查询日线数据
            daily_data_df = self.mysql_manager.query_data(
                table_name="daily_data",
                columns=["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
                conditions=f"ts_code IN ({','.join(['%s'] * len(test_stocks))}) AND trade_date >= %s AND trade_date <= %s",
                params=test_stocks + [start_date, end_date],
                order_by="ts_code, trade_date"
            )
            
            if daily_data_df is None or daily_data_df.empty:
                print("❌ 无法从MySQL获取日线数据")
                return False
                
            print(f"✅ 成功从MySQL获取 {len(daily_data_df['ts_code'].unique())} 只股票的日线数据，共 {len(daily_data_df)} 条记录")
            
            # 存储数据
            self.stock_data = daily_data_df
            
            return True
            
        except Exception as e:
            print(f"❌ 数据准备失败: {e}")
            return False
    
    def calculate_momentum(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """计算股票的动量值（过去20日涨幅）"""
        print("📈 计算股票动量值...")
        
        # 按股票代码分组计算
        momentum_results = []
        
        for ts_code, group in stock_data.groupby('ts_code'):
            # 按交易日期排序
            group = group.sort_values('trade_date')
            
            if len(group) >= self.lookback_period:
                # 计算过去20日涨幅
                start_close = group.iloc[0]['close']
                end_close = group.iloc[-1]['close']
                
                if start_close > 0:  # 避免除零错误
                    momentum = (end_close - start_close) / start_close * 100
                    momentum_results.append({
                        'ts_code': ts_code,
                        'momentum': momentum,
                        'start_date': group.iloc[0]['trade_date'],
                        'end_date': group.iloc[-1]['trade_date'],
                        'start_close': start_close,
                        'end_close': end_close,
                        'data_points': len(group)
                    })
        
        if not momentum_results:
            return pd.DataFrame()
            
        momentum_df = pd.DataFrame(momentum_results)
        
        # 按动量值排序
        momentum_df = momentum_df.sort_values('momentum', ascending=False)
        
        return momentum_df
    
    def filter_stocks(self, momentum_df: pd.DataFrame) -> pd.DataFrame:
        """过滤股票，选择排名前10%的股票"""
        if momentum_df.empty:
            return pd.DataFrame()
            
        # 计算前10%的股票数量
        top_n = max(1, int(len(momentum_df) * self.top_percentage))
        
        # 选择前10%的股票
        selected_stocks = momentum_df.head(top_n)
        
        print(f"✅ 从 {len(momentum_df)} 只股票中选出前 {top_n} 只动量最强的股票")
        
        return selected_stocks
    
    def execute(self) -> List[Dict[str, Any]]:
        """执行动量选股策略"""
        try:
            # 计算动量值
            momentum_df = self.calculate_momentum(self.stock_data)
            
            if momentum_df.empty:
                print("❌ 动量计算失败或数据不足")
                return []
            
            # 过滤股票
            selected_stocks = self.filter_stocks(momentum_df)
            
            if selected_stocks.empty:
                print("❌ 未筛选出符合条件的股票")
                return []
            
            # 转换为结果格式
            results = []
            for _, row in selected_stocks.iterrows():
                # 获取公司名称
                company_name = 'N/A'
                if self.stock_basic_df is not None:
                    company_info = self.stock_basic_df[self.stock_basic_df['ts_code'] == row['ts_code']]
                    if not company_info.empty:
                        company_name = company_info.iloc[0]['name']
                
                results.append({
                    'ts_code': row['ts_code'],
                    'name': company_name,
                    'momentum': round(row['momentum'], 2),
                    'start_date': row['start_date'],
                    'end_date': row['end_date'],
                    'start_close': round(row['start_close'], 2),
                    'end_close': round(row['end_close'], 2),
                    'data_points': row['data_points'],
                    'strategy': 'momentum_20d'
                })
            
            return results
            
        except Exception as e:
            print(f"❌ 策略执行失败: {e}")
            return []
    
    def save_results(self, results: List[Dict[str, Any]]) -> bool:
        """保存选股结果"""
        if not results:
            print("⚠️  无选股结果需要保存")
            return False
            
        print("\n🎯 动量选股结果:")
        print("=" * 100)
        for i, stock in enumerate(results, 1):
            print(f"{i:2d}. {stock['ts_code']} - {stock.get('name', 'N/A')} - "
                  f"动量: {stock['momentum']:6.2f}% - "
                  f"期初: {stock['start_close']:6.2f} - "
                  f"期末: {stock['end_close']:6.2f}")
        
        print("=" * 100)
        print(f"📊 共选出 {len(results)} 只动量最强的股票")
        
        # 这里可以添加保存到数据库或文件的逻辑
        return True


def test_momentum_strategy():
    """测试动量策略"""
    from IBelive.core.parse_config import ParseConfig
    
    print("🚀 开始测试动量选股策略...")
    
    # 初始化配置
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建策略实例
    strategy = MomentumStrategy(config, pro)
    
    # 运行策略
    results = strategy.run()
    
    return results


# if __name__ == "__main__":
#     test_momentum_strategy()