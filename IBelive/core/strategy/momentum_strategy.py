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
    
    def __init__(self, config, pro_api, start_date=None, end_date=None, min_data_points=None, 
                 volatility_threshold=None, trend_threshold=None):
        """
        初始化动量策略
        
        :param config: 配置对象
        :param pro_api: Tushare Pro API对象
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        :param min_data_points: 最小数据点数要求，默认为20
        :param volatility_threshold: 波动率阈值，默认为0.5（50%）
        :param trend_threshold: 趋势一致性阈值，默认为0.1
        """
        super().__init__(config)
        self.pro = pro_api
        self.mysql_manager = MySQLManager(config)
        self.lookback_period = 20  # 回看周期20天
        self.top_percentage = 0.1  # 选择前10%的股票
        self.min_data_points = min_data_points or 20  # 最小数据点数要求，可配置
        self.volatility_threshold = volatility_threshold or 0.5  # 波动率阈值，可配置
        self.trend_threshold = trend_threshold or 0.1  # 趋势一致性阈值，可配置
        self.stock_data = None
        self.stock_basic_df = None  # 存储股票基本信息
        self.start_date = start_date
        self.end_date = end_date
        
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
            
            # 2. 从MySQL数据库获取指定日期范围的日线数据
            # 使用传入的日期参数，如果没有传入则使用默认值
            end_date = self.end_date or "20250930"  # 默认结束日期
            start_date = self.start_date or "20250901"  # 默认开始日期
            
            print(f"📅 从MySQL获取 {start_date} 到 {end_date} 的日线数据...")
            
            # 获取所有上市股票的代码列表（不再限制前100只）
            all_stocks = stock_basic_df['ts_code'].tolist()
            
            # 从MySQL数据库查询日线数据
            # 由于股票数量可能很多，我们分批查询以避免SQL语句过长
            batch_size = 100  # 每批查询100只股票
            daily_data_dfs = []
            
            for i in range(0, len(all_stocks), batch_size):
                batch_stocks = all_stocks[i:i + batch_size]
                print(f"📦 查询第 {i//batch_size + 1} 批股票数据 ({len(batch_stocks)} 只)...")
                
                batch_df = self.mysql_manager.query_data(
                    table_name="daily_data",
                    columns=["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
                    conditions=f"ts_code IN ({','.join(['%s'] * len(batch_stocks))}) AND trade_date >= %s AND trade_date <= %s",
                    params=batch_stocks + [start_date, end_date],
                    order_by="ts_code, trade_date"
                )
                
                if batch_df is not None and not batch_df.empty:
                    daily_data_dfs.append(batch_df)
            
            # 合并所有批次的数据
            if daily_data_dfs:
                daily_data_df = pd.concat(daily_data_dfs, ignore_index=True)
            else:
                daily_data_df = pd.DataFrame()
            
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
        print(f"📈 计算股票动量值（最小数据点数要求: {self.min_data_points}）...")
        
        # 按股票代码分组计算
        momentum_results = []
        skipped_stocks = []  # 记录被跳过的股票
        
        for ts_code, group in stock_data.groupby('ts_code'):
            # 按交易日期排序
            group = group.sort_values('trade_date')
            
            # 使用实际可用的数据点计算动量
            if len(group) >= self.min_data_points:  # 至少需要指定数量的数据点才能计算涨幅
                # 使用移动平均价格减少极值影响
                # 取前25%数据点的平均作为期初价格，后25%数据点的平均作为期末价格
                window_size = max(3, len(group) // 4)  # 至少3个数据点
                start_close = group.head(window_size)['close'].mean()
                end_close = group.tail(window_size)['close'].mean()
                
                if start_close > 0:  # 避免除零错误
                    momentum = (end_close - start_close) / start_close * 100
                    
                    # 检查价格稳定性（排除异常波动）
                    price_std = group['close'].std()
                    price_mean = group['close'].mean()
                    volatility_ratio = price_std / price_mean if price_mean > 0 else 0
                    
                    # 检查趋势一致性（使用简单的线性回归计算斜率）
                    n = len(group)
                    x = list(range(n))
                    y = group['close'].values
                    
                    # 手动计算线性回归斜率
                    x_mean = sum(x) / n
                    y_mean = sum(y) / n
                    numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
                    denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
                    slope = numerator / denominator if denominator != 0 else 0
                    trend_consistency = abs(slope) / (price_std + 1e-10)  # 避免除零
                    
                    # 只有波动率在合理范围内且趋势一致才纳入结果
                    if volatility_ratio <= self.volatility_threshold and trend_consistency >= self.trend_threshold:
                        momentum_results.append({
                            'ts_code': ts_code,
                            'momentum': momentum,
                            'start_date': group.iloc[0]['trade_date'],
                            'end_date': group.iloc[-1]['trade_date'],
                            'start_close': start_close,
                            'end_close': end_close,
                            'data_points': len(group),
                            'volatility': volatility_ratio,
                            'trend_strength': trend_consistency
                        })
                    else:
                        # 记录被过滤的股票
                        reason = f'价格波动率过高（{volatility_ratio:.2%} > {self.volatility_threshold:.0%}）' if volatility_ratio > self.volatility_threshold else f'趋势不明显（强度: {trend_consistency:.3f} < {self.trend_threshold:.3f}）'
                        skipped_stocks.append({
                            'ts_code': ts_code,
                            'reason': reason,
                            'data_points': len(group)
                        })
                else:
                    # 记录除零错误的股票
                    skipped_stocks.append({
                        'ts_code': ts_code,
                        'reason': '起始价格为零或负数',
                        'data_points': len(group)
                    })
            else:
                # 记录数据点数不够的股票
                skipped_stocks.append({
                    'ts_code': ts_code,
                    'reason': f'数据点数不足（需要至少{self.min_data_points}个，实际{len(group)}个）',
                    'data_points': len(group)
                })
        
        # 打印被跳过的股票信息
        if skipped_stocks:
            print(f"\n⚠️  跳过 {len(skipped_stocks)} 只股票（数据不足或无效）:")
            for i, stock in enumerate(skipped_stocks[:10], 1):  # 只显示前10只
                print(f"   {i}. {stock['ts_code']} - {stock['reason']}")
            if len(skipped_stocks) > 10:
                print(f"   ... 还有 {len(skipped_stocks) - 10} 只股票被跳过")
        
        if not momentum_results:
            print("❌ 没有足够的数据计算动量（至少需要2个交易日数据）")
            return pd.DataFrame()
            
        momentum_df = pd.DataFrame(momentum_results)
        
        # 按动量值排序
        momentum_df = momentum_df.sort_values('momentum', ascending=False)
        
        print(f"✅ 成功计算了 {len(momentum_df)} 只股票的动量值")
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
        
        # 保存选股结果到数据库
        success = self._save_selected_stocks_to_db(results)
        
        # 保存选中股票的完整日线数据到新表
        if success:
            self._save_complete_daily_data_to_new_table(results)
        
        return success
    
    def _save_selected_stocks_to_db(self, results: List[Dict[str, Any]]) -> bool:
        """保存选中的股票信息到数据库"""
        if not results:
            return False
            
        try:
            # 转换为DataFrame
            results_df = pd.DataFrame(results)
            
            # 定义表名和字段
            table_name = "momentum_selected_stocks"
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS momentum_selected_stocks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ts_code VARCHAR(20) NOT NULL,
                name VARCHAR(100),
                momentum DECIMAL(10,2),
                start_date DATE,
                end_date DATE,
                start_close DECIMAL(10,2),
                end_close DECIMAL(10,2),
                data_points INT,
                strategy VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # 创建表
            if not self.mysql_manager.create_table_if_not_exists(table_name, create_table_sql):
                print("❌ 创建选股结果表失败")
                return False
            
            # 准备插入语句
            insert_query = """
            INSERT INTO momentum_selected_stocks 
            (ts_code, name, momentum, start_date, end_date, start_close, end_close, data_points, strategy)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # 准备数据
            data_tuples = []
            for stock in results:
                data_tuples.append((
                    stock['ts_code'],
                    stock.get('name', 'N/A'),
                    stock['momentum'],
                    stock['start_date'],
                    stock['end_date'],
                    stock['start_close'],
                    stock['end_close'],
                    stock['data_points'],
                    stock.get('strategy', 'momentum_20d')
                ))
            
            # 保存数据
            success = self.mysql_manager.execute_many(insert_query, data_tuples)
            
            if success:
                print(f"✅ 成功保存 {len(results)} 条选股结果到MySQL表 {table_name}")
            else:
                print(f"❌ 保存选股结果到MySQL表 {table_name} 失败")
            
            return success
            
        except Exception as e:
            print(f"❌ 保存选股结果失败: {e}")
            return False
    
    def _save_complete_daily_data_to_new_table(self, results: List[Dict[str, Any]]) -> bool:
        """保存选中股票的完整日线数据到新表strategy_20days_momentum"""
        if not results:
            return False
            
        try:
            # 获取选中股票的代码列表
            selected_ts_codes = [stock['ts_code'] for stock in results]
            
            # 从MySQL数据库重新查询选中股票的完整日线数据（所有历史数据）
            print(f"📊 查询 {len(selected_ts_codes)} 只选中股票的完整日线数据...")
            
            # 分批查询以避免SQL语句过长
            batch_size = 100
            selected_daily_data_dfs = []
            
            for i in range(0, len(selected_ts_codes), batch_size):
                batch_stocks = selected_ts_codes[i:i + batch_size]
                print(f"📦 查询第 {i//batch_size + 1} 批选中股票数据 ({len(batch_stocks)} 只)...")
                
                batch_df = self.mysql_manager.query_data(
                    table_name="daily_data",
                    columns=["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
                    conditions=f"ts_code IN ({','.join(['%s'] * len(batch_stocks))})",
                    params=batch_stocks,
                    order_by="ts_code, trade_date"
                )
                
                if batch_df is not None and not batch_df.empty:
                    selected_daily_data_dfs.append(batch_df)
            
            # 合并所有批次的数据
            if selected_daily_data_dfs:
                selected_daily_data = pd.concat(selected_daily_data_dfs, ignore_index=True)
            else:
                selected_daily_data = pd.DataFrame()
            
            if selected_daily_data.empty:
                print("⚠️  未找到选中股票的日线数据")
                return False
            
            print(f"📊 准备保存 {len(selected_daily_data)} 条选中股票的日线数据到新表...")
            
            # 定义新表名和字段（与daily_data表结构一致）
            new_table_name = "strategy_20days_momentum"
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS strategy_20days_momentum (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ts_code VARCHAR(20),
                trade_date DATETIME,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                pre_close FLOAT,
                `change` FLOAT,
                pct_chg FLOAT,
                vol FLOAT,
                amount FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_stock_date (ts_code, trade_date)
            )
            """
            
            # 创建新表
            if not self.mysql_manager.create_table_if_not_exists(new_table_name, create_table_sql):
                print("❌ 创建strategy_20days_momentum表失败")
                return False
            
            # 准备插入语句（与daily_data表结构一致）
            insert_query = """
            INSERT INTO strategy_20days_momentum (ts_code, trade_date, open, high, low, close, pre_close, 
                                                `change`, pct_chg, vol, amount, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                pre_close = VALUES(pre_close),
                `change` = VALUES(`change`),
                pct_chg = VALUES(pct_chg),
                vol = VALUES(vol),
                amount = VALUES(amount),
                updated_at = CURRENT_TIMESTAMP
            """
            
            # 准备数据
            data_tuples = []
            for _, row in selected_daily_data.iterrows():
                data_tuples.append((
                    row['ts_code'],
                    row['trade_date'],
                    row.get('open', 0.0),
                    row.get('high', 0.0),
                    row.get('low', 0.0),
                    row.get('close', 0.0),
                    row.get('pre_close', 0.0),
                    row.get('change', 0.0),
                    row.get('pct_chg', 0.0),
                    row.get('vol', 0.0),
                    row.get('amount', 0.0)
                ))
            
            # 保存数据
            success = self.mysql_manager.execute_many(insert_query, data_tuples)
            
            if success:
                print(f"✅ 成功保存 {len(data_tuples)} 条日线数据到MySQL表 '{new_table_name}'")
                print(f"📈 涉及 {len(selected_ts_codes)} 只选中股票")
            else:
                print(f"❌ 保存日线数据到MySQL表 '{new_table_name}' 失败")
            
            return success
            
        except Exception as e:
            print(f"❌ 保存日线数据失败: {e}")
            return False


def test_momentum_strategy(min_data_points=None, volatility_threshold=None, trend_threshold=None):
    """测试动量策略（使用默认日期）"""
    from IBelive.core.parse_config import ParseConfig
    
    print(f"🚀 开始测试动量选股策略（默认日期，最小数据点数: {min_data_points or 20}）...")
    if volatility_threshold is not None:
        print(f"📊 波动率阈值: {volatility_threshold}")
    if trend_threshold is not None:
        print(f"📈 趋势一致性阈值: {trend_threshold}")
    
    # 初始化配置
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建策略实例（使用默认日期）
    strategy = MomentumStrategy(config, pro, min_data_points=min_data_points, 
                              volatility_threshold=volatility_threshold, 
                              trend_threshold=trend_threshold)
    
    # 运行策略
    results = strategy.run()
    
    return results


def test_momentum_strategy_with_dates(start_date, end_date, min_data_points=None, 
                                      volatility_threshold=None, trend_threshold=None):
    """测试动量策略（使用指定日期）"""
    from IBelive.core.parse_config import ParseConfig
    
    print(f"🚀 开始测试动量选股策略（日期范围: {start_date} 到 {end_date}，最小数据点数: {min_data_points or 20}）...")
    if volatility_threshold is not None:
        print(f"📊 波动率阈值: {volatility_threshold}")
    if trend_threshold is not None:
        print(f"📈 趋势一致性阈值: {trend_threshold}")
    
    # 初始化配置
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建策略实例（使用指定日期）
    strategy = MomentumStrategy(config, pro, start_date=start_date, end_date=end_date, 
                              min_data_points=min_data_points,
                              volatility_threshold=volatility_threshold, 
                              trend_threshold=trend_threshold)
    
    # 运行策略
    results = strategy.run()
    
    return results