"""
RSI相对强弱指数策略
计算股票的RSI值并判断超买超卖状态
"""
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy
from IBelive.core.mysql_manager import MySQLManager


class RSIStrategy(BaseStrategy):
    """RSI相对强弱指数策略"""
    
    def __init__(self, config, rsi_period=14, ma_period=6):
        """
        初始化RSI策略
        
        :param config: 配置对象
        :param rsi_period: RSI计算周期，默认为14天
        :param ma_period: RSI移动平均周期，默认为6天
        """
        super().__init__(config)
        self.rsi_period = rsi_period
        self.ma_period = ma_period
        self.strategy_name = f"RSI_{rsi_period}天策略"
        
        # MySQL管理器
        self.mysql_manager = MySQLManager(config)
        
        # 数据缓存
        self.stock_data = None
        self.rsi_results = None
    
    def prepare_data(self) -> bool:
        """准备策略所需数据"""
        try:
            print(f"📊 准备RSI策略数据（周期: {self.rsi_period}天）...")
            
            # 从MySQL数据库获取所有股票的日线数据
            print("🔍 从MySQL数据库daily_data表获取日线数据...")
            
            # 查询所有股票的日线数据
            stock_data_df = self.mysql_manager.query_data(
                table_name="daily_data",
                columns=["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
                order_by="ts_code, trade_date"
            )
            
            if stock_data_df is None or stock_data_df.empty:
                print("❌ 无法从MySQL获取日线数据")
                return False
                
            print(f"✅ 从MySQL获取到 {len(stock_data_df['ts_code'].unique())} 只股票的日线数据，共 {len(stock_data_df)} 条记录")
            
            # 存储数据
            self.stock_data = stock_data_df
            
            return True
            
        except Exception as e:
            print(f"❌ 数据准备失败: {e}")
            return False
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """
        计算RSI值
        
        :param prices: 价格序列
        :param period: RSI计算周期
        :return: RSI值序列
        """
        # 计算价格变化
        delta = prices.diff()
        
        # 分离上涨和下跌
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        
        # 计算平均收益和平均损失
        # rolling 创建一个滑动窗口 窗口大小14天
        # mean 对窗口内所有的值计算算数平均值
        avg_gain = gain.rolling(window=period, min_periods=1).mean() # 计算14天内的平均收益
        avg_loss = loss.rolling(window=period, min_periods=1).mean() # 计算14天内的平均损失
        
        # 计算RS
        rs = avg_gain / avg_loss.replace(0, float('inf'))
        
        # 计算RSI
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def calculate_rsi_ma(self, rsi_values: pd.Series, period: int = 6) -> pd.Series:
        """
        计算RSI的移动平均值
        
        :param rsi_values: RSI值序列
        :param period: 移动平均周期
        :return: RSI移动平均值序列
        """
        return rsi_values.rolling(window=period, min_periods=1).mean()
    
    def determine_rsi_status(self, rsi_value: float) -> str:
        """
        根据RSI值判断超买超卖状态
        
        :param rsi_value: RSI值
        :return: 状态字符串（'超买', '超卖', '正常'）
        """
        if rsi_value > 70:
            return "超买"
        elif rsi_value < 30:
            return "超卖"
        else:
            return "正常"
    
    def execute(self) -> List[Dict[str, Any]]:
        """执行RSI计算策略"""
        try:
            if self.stock_data is None or self.stock_data.empty:
                print("❌ 数据未准备完成")
                return []
            
            print(f"📈 开始计算 {self.rsi_period} 天RSI值...")
            
            # 按股票代码分组计算RSI
            rsi_results = []
            
            for ts_code, group in self.stock_data.groupby('ts_code'):
                # 按交易日期排序
                group = group.sort_values('trade_date')
                
                # 计算RSI
                rsi_values = self.calculate_rsi(group['close'], self.rsi_period)
                
                # 计算RSI移动平均
                rsi_ma_values = self.calculate_rsi_ma(rsi_values, self.ma_period)
                
                # 为每条记录添加RSI相关信息
                for idx, row in group.iterrows():
                    trade_date = row['trade_date']
                    rsi_value = rsi_values.get(idx, float('nan'))
                    rsi_ma_value = rsi_ma_values.get(idx, float('nan'))
                    
                    # 跳过NaN值
                    if pd.isna(rsi_value) or pd.isna(rsi_ma_value):
                        continue
                    
                    # 判断状态
                    rsi_status = self.determine_rsi_status(rsi_value)
                    
                    rsi_results.append({
                        'ts_code': ts_code,
                        'trade_date': trade_date,
                        'close': row['close'],
                        'rsi_value': round(rsi_value, 4),
                        'rsi_ma_value': round(rsi_ma_value, 4),
                        'rsi_status': rsi_status
                    })
            
            # 存储结果
            self.rsi_results = pd.DataFrame(rsi_results)
            
            print(f"✅ 成功计算了 {len(self.rsi_results['ts_code'].unique())} 只股票的RSI值，共 {len(self.rsi_results)} 条记录")
            
            # 返回结果（这里返回所有计算结果，不仅仅是选股结果）
            return rsi_results
            
        except Exception as e:
            print(f"❌ RSI计算失败: {e}")
            return []
    
    def filter_stocks(self, stocks_data: pd.DataFrame) -> pd.DataFrame:
        """
        过滤股票数据（RSI策略不需要选股过滤，返回所有数据）
        
        :param stocks_data: 股票数据DataFrame
        :return: 过滤后的数据
        """
        # RSI策略计算所有股票的RSI，不需要额外过滤
        return stocks_data
    
    def save_results(self, results: List[Dict[str, Any]]) -> bool:
        """保存RSI计算结果到数据库"""
        if not results:
            return False
            
        try:
            # 定义表名（根据RSI周期动态生成）
            table_name = f"rsi_{self.rsi_period}days_data"
            
            # 创建表的SQL语句
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ts_code VARCHAR(20) NOT NULL,
                trade_date DATETIME NOT NULL,
                close FLOAT,
                rsi_value FLOAT,
                rsi_ma_value FLOAT,
                rsi_status VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_stock_date (ts_code, trade_date)
            )
            """
            
            # 创建表
            if not self.mysql_manager.create_table_if_not_exists(table_name, create_table_sql):
                print(f"❌ 创建{table_name}表失败")
                return False
            
            # 准备插入语句
            insert_query = f"""
            INSERT INTO {table_name} (ts_code, trade_date, close, rsi_value, rsi_ma_value, rsi_status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                close = VALUES(close),
                rsi_value = VALUES(rsi_value),
                rsi_ma_value = VALUES(rsi_ma_value),
                rsi_status = VALUES(rsi_status),
                updated_at = CURRENT_TIMESTAMP
            """
            
            # 准备数据
            data_tuples = []
            for result in results:
                data_tuples.append((
                    result['ts_code'],
                    result['trade_date'],
                    result.get('close', 0.0),
                    result.get('rsi_value', 0.0),
                    result.get('rsi_ma_value', 0.0),
                    result.get('rsi_status', '正常')
                ))
            
            # 保存数据（分批处理，避免max_allowed_packet错误）
            batch_size = 1000  # 每批处理1000条记录
            total_batches = (len(data_tuples) + batch_size - 1) // batch_size
            success_count = 0
            
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                success = self.mysql_manager.execute_many(insert_query, batch)
                
                if success:
                    success_count += len(batch)
                    print(f"✅ 已保存 {success_count}/{len(data_tuples)} 条记录到MySQL表 '{table_name}'")
                else:
                    print(f"❌ 批量保存失败: 第 {i//batch_size + 1}/{total_batches} 批")
                    # 继续尝试保存其他批次，不立即返回失败
            
            if success_count == len(data_tuples):
                print(f"✅ 成功保存所有 {len(data_tuples)} 条RSI数据到MySQL表 '{table_name}'")
                return True
            else:
                print(f"⚠️  部分保存成功: {success_count}/{len(data_tuples)} 条记录")
                return False
            
        except Exception as e:
            print(f"❌ 保存RSI结果失败: {e}")
            return False


def test_rsi_strategy(rsi_period=14, ma_period=6):
    """测试RSI策略"""
    from IBelive.core.parse_config import ParseConfig
    
    print(f"🚀 开始测试RSI策略（RSI周期: {rsi_period}天，MA周期: {ma_period}天）...")
    
    # 初始化配置
    config = ParseConfig()
    
    # 创建策略实例
    strategy = RSIStrategy(config, rsi_period=rsi_period, ma_period=ma_period)
    
    # 运行策略
    results = strategy.run()
    
    if results:
        print(f"✅ RSI策略执行成功！共计算 {len(results)} 条RSI记录")
    else:
        print("❌ RSI策略执行失败")
    
    return results


def test_rsi_strategy_for_stock(ts_code, rsi_period=14, ma_period=6):
    """测试特定股票的RSI策略"""
    from IBelive.core.parse_config import ParseConfig
    
    print(f"🚀 开始测试股票 {ts_code} 的RSI策略（RSI周期: {rsi_period}天，MA周期: {ma_period}天）...")
    
    # 初始化配置
    config = ParseConfig()
    
    # 创建策略实例
    strategy = RSIStrategy(config, rsi_period=rsi_period, ma_period=ma_period)
    
    # 准备数据
    if not strategy.prepare_data():
        print("❌ 数据准备失败")
        return []
    
    # 筛选特定股票的数据
    stock_data = strategy.stock_data[strategy.stock_data['ts_code'] == ts_code]
    if stock_data.empty:
        print(f"❌ 未找到股票 {ts_code} 的数据")
        return []
    
    # 计算RSI
    rsi_values = strategy.calculate_rsi(stock_data['close'], rsi_period)
    rsi_ma_values = strategy.calculate_rsi_ma(rsi_values, ma_period)
    
    # 生成结果
    results = []
    for idx, row in stock_data.iterrows():
        trade_date = row['trade_date']
        rsi_value = rsi_values.get(idx, float('nan'))
        rsi_ma_value = rsi_ma_values.get(idx, float('nan'))
        
        if pd.isna(rsi_value) or pd.isna(rsi_ma_value):
            continue
        
        rsi_status = strategy.determine_rsi_status(rsi_value)
        
        results.append({
            'ts_code': ts_code,
            'trade_date': trade_date,
            'close': row['close'],
            'rsi_value': round(rsi_value, 4),
            'rsi_ma_value': round(rsi_ma_value, 4),
            'rsi_status': rsi_status
        })
    
    # 保存结果
    strategy.save_results(results)
    
    print(f"✅ 成功计算股票 {ts_code} 的RSI值，共 {len(results)} 条记录")
    
    return results