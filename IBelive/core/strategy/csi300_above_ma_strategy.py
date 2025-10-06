"""
沪深300相对强度积分策略
基于120日移动平均线的相对强度积分比较，筛选长期跑赢指数的股票
"""
import pandas as pd
import numpy as np
import tushare as ts
from typing import List, Dict, Any
from datetime import datetime, timedelta

from .base_strategy import BaseStrategy


class CSI300RelativeStrengthStrategy(BaseStrategy):
    """沪深300相对强度积分策略"""
    
    def __init__(self, config):
        super().__init__(config)
        self.strategy_name = "沪深300相对强度积分策略"
        
        # 策略参数
        self.ma_period = 90  # 移动平均线周期（90日）
        self.min_volume = 1000000  # 最小成交量（股）
        self.min_outperformance_days = 54  # 最小跑赢天数（80%，90天*0.8）
        self.min_total_score = 0  # 最小总积分
        
        # Tushare Pro API
        self.pro = ts.pro_api(config.get_token())
        
        # 数据缓存
        self.csi300_data = None
        self.stocks_historical_data = None
        self.csi300_ma_series = None
    
    def prepare_data(self) -> bool:
        """准备策略所需数据"""
        print("📊 准备策略数据...")
        
        try:
            # 获取沪深300指数历史数据
            self._get_csi300_data()
            
            # 获取个股历史数据
            self._get_stocks_historical_data()
            
            # 准备沪深300移动平均线序列
            self._prepare_csi300_ma_series()
            
            print("✅ 数据准备完成")
            return True
            
        except Exception as e:
            print(f"❌ 数据准备失败: {e}")
            return False
    
    def _get_csi300_data(self):
        """获取沪深300指数历史数据（足够计算90日MA）"""
        print("  获取沪深300指数历史数据...")
        
        # 这里需要根据实际的数据源实现
        # 假设我们从MySQL获取数据
        from ..index.index_daily_manager import IndexDailyManager
        
        # 计算日期范围（需要足够的数据计算90日MA）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')  # 多取一些数据用于计算MA
        
        index_manager = IndexDailyManager(self.config, self.pro)
        # 从MySQL数据库获取沪深300指数数据
        self.csi300_data = index_manager.get_index_daily_data_from_mysql(
            ts_codes=['000300.SH'],  # 沪深300指数代码
            start_date=start_date,
            end_date=end_date,
            fields=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
        )
        
        if self.csi300_data is None or self.csi300_data.empty:
            raise Exception("获取沪深300指数数据失败")
        
        # 按日期排序并计算移动平均线
        self.csi300_data = self.csi300_data.sort_values('trade_date')
        self.csi300_data['ma'] = self.csi300_data['close'].rolling(window=self.ma_period).mean()
        
        # 只保留有完整MA计算的数据
        self.csi300_data = self.csi300_data.dropna(subset=['ma'])
        
        print(f"  获取到 {len(self.csi300_data)} 条沪深300指数数据（含MA计算）")
    
    def _get_stocks_historical_data(self):
        """获取个股历史数据（足够计算90日MA）"""
        print("  获取个股历史日线数据...")
        
        # 这里需要根据实际的数据源实现
        # 假设我们从MySQL获取数据
        from ..stock.daily_data_manager import DailyDataManager
        
        # 获取日期范围（需要足够的数据计算90日MA）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')  # 多取一些数据用于计算MA
        
        stock_manager = DailyDataManager(self.config, self.pro)
        
        # 从MySQL数据库获取所有股票在指定日期范围内的数据
        all_stocks_data = stock_manager.get_daily_data_from_mysql(
            start_date=start_date,
            end_date=end_date
        )
        
        if all_stocks_data is None or all_stocks_data.empty:
            raise Exception("从MySQL获取股票日线数据失败")
        
        # 按股票代码分组
        self.stocks_historical_data = {}
        for _, row in all_stocks_data.iterrows():
            ts_code = row['ts_code']
            if ts_code not in self.stocks_historical_data:
                self.stocks_historical_data[ts_code] = []
            self.stocks_historical_data[ts_code].append(row.to_dict())
        
        # 转换为DataFrame并计算移动平均线
        processed_data = []
        for ts_code, records in self.stocks_historical_data.items():
            if len(records) >= self.ma_period:  # 确保有足够数据计算MA
                df = pd.DataFrame(records).sort_values('trade_date')
                df['ma'] = df['close'].rolling(window=self.ma_period).mean()
                # 只保留有完整MA计算的数据
                df = df.dropna(subset=['ma'])
                if not df.empty:
                    processed_data.append(df)
        
        if processed_data:
            self.stocks_historical_data = pd.concat(processed_data, ignore_index=True)
            print(f"  获取到 {len(self.stocks_historical_data['ts_code'].unique())} 只股票的历史数据（含MA计算）")
        else:
            raise Exception("获取个股历史数据失败")
    
    def _prepare_csi300_ma_series(self):
        """准备沪深300移动平均线序列"""
        print("  准备沪深300移动平均线序列...")
        
        # 确保数据已排序
        csi300_sorted = self.csi300_data.sort_values('trade_date')
        
        # 创建日期到MA值的映射
        self.csi300_ma_series = dict(zip(
            csi300_sorted['trade_date'], 
            csi300_sorted['ma']
        ))
        
        print(f"  准备完成 {len(self.csi300_ma_series)} 个交易日的沪深300 MA数据")
    
    def execute(self) -> List[Dict[str, Any]]:
        """执行选股策略"""
        print("🎯 执行相对强度积分选股策略...")
        
        if self.csi300_ma_series is None or self.stocks_historical_data is None:
            print("❌ 数据未准备完成")
            return []
        
        # 计算每只股票的相对强度积分
        stock_scores = self._calculate_relative_strength_scores()
        
        # 过滤股票
        filtered_stocks = self.filter_stocks(stock_scores)
        
        # 转换为结果格式
        results = []
        for stock_info in filtered_stocks:
            result = {
                'ts_code': stock_info['ts_code'],
                'name': stock_info.get('name', ''),
                'total_score': stock_info['total_score'],
                'outperformance_days': stock_info['outperformance_days'],
                'outperformance_ratio': stock_info['outperformance_ratio'],
                'latest_close': stock_info['latest_close'],
                'latest_volume': stock_info['latest_volume'],
                'trade_date': stock_info['trade_date']
            }
            results.append(result)
        
        return results
    
    def _calculate_relative_strength_scores(self) -> List[Dict[str, Any]]:
        """计算每只股票的相对强度积分"""
        print("  计算相对强度积分...")
        
        stock_scores = []
        
        # 按股票代码分组处理
        grouped = self.stocks_historical_data.groupby('ts_code')
        
        for ts_code, stock_data in grouped:
            try:
                # 确保数据按日期排序
                stock_data = stock_data.sort_values('trade_date')
                
                # 获取股票名称（如果有）
                stock_name = stock_data['name'].iloc[0] if 'name' in stock_data.columns else ''
                
                # 计算每日相对强度得分
                daily_scores = []
                outperformance_days = 0
                
                # 从第2个数据点开始计算（需要前一日数据）
                for i in range(1, len(stock_data)):
                    current_row = stock_data.iloc[i]
                    prev_row = stock_data.iloc[i-1]
                    trade_date = current_row['trade_date']
                    
                    # 获取对应的沪深300 MA值
                    if trade_date in self.csi300_ma_series:
                        csi300_ma_current = self.csi300_ma_series[trade_date]
                        csi300_ma_prev = self.csi300_ma_series.get(prev_row['trade_date'], csi300_ma_current)
                        
                        # 计算MA涨幅（百分比）
                        if not pd.isna(current_row['ma']) and not pd.isna(prev_row['ma']) and prev_row['ma'] != 0:
                            stock_ma_pct = (current_row['ma'] / prev_row['ma'] - 1) * 100
                        else:
                            stock_ma_pct = 0
                        
                        if not pd.isna(csi300_ma_current) and not pd.isna(csi300_ma_prev) and csi300_ma_prev != 0:
                            csi300_ma_pct = (csi300_ma_current / csi300_ma_prev - 1) * 100
                        else:
                            csi300_ma_pct = 0
                        
                        # 计算当日得分（股票MA涨幅 - 指数MA涨幅）
                        daily_score = stock_ma_pct - csi300_ma_pct
                        daily_scores.append(daily_score)
                        
                        # 统计跑赢天数
                        if daily_score > 0:
                            outperformance_days += 1
                
                if daily_scores:
                    total_score = sum(daily_scores)
                    outperformance_ratio = outperformance_days / len(daily_scores)
                    
                    # 获取最新数据
                    latest_data = stock_data.iloc[-1]
                    
                    stock_scores.append({
                        'ts_code': ts_code,
                        'name': stock_name,
                        'total_score': total_score,
                        'outperformance_days': outperformance_days,
                        'outperformance_ratio': outperformance_ratio,
                        'latest_close': latest_data['close'],
                        'latest_volume': latest_data['vol'],
                        'trade_date': latest_data['trade_date']
                    })
                    
            except Exception as e:
                print(f"  计算股票 {ts_code} 相对强度时出错: {e}")
                continue
        
        print(f"  完成 {len(stock_scores)} 只股票的相对强度计算")
        return stock_scores
    
    def filter_stocks(self, stock_scores: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """过滤股票数据"""
        print("🔍 过滤股票数据...")
        
        filtered_stocks = []
        
        for stock in stock_scores:
            # 1. 总积分要求
            if stock['total_score'] <= self.min_total_score:
                continue
                
            # 2. 跑赢天数要求（80%以上）
            if stock['outperformance_ratio'] < (self.min_outperformance_days / self.ma_period):
                continue
                
            # 3. 成交量过滤（避免低流动性股票）
            if stock['latest_volume'] < self.min_volume:
                continue
                
            # 4. 排除ST股票
            if 'name' in stock and 'ST' in str(stock['name']):
                continue
                
            filtered_stocks.append(stock)
        
        print(f"   初步筛选后剩余股票: {len(filtered_stocks)} 只")
        
        # 按总积分排序
        filtered_stocks.sort(key=lambda x: x['total_score'], reverse=True)
        
        return filtered_stocks
    
    def save_results(self, results: List[Dict[str, Any]]) -> bool:
        """保存选股结果"""
        if not results:
            print("⚠️  无选股结果可保存")
            return False
        
        print(f"\n📈 选股结果汇总:")
        print(f"   策略名称: {self.strategy_name}")
        print(f"   筛选条件: 总积分 > {self.min_total_score}, 跑赢天数 ≥ {self.min_outperformance_days}/{self.ma_period} ({self.min_outperformance_days/self.ma_period:.1%})")
        print(f"   选出股票数量: {len(results)} 只")
        print(f"\n🏆 相对强度最佳的前10只股票:")
        
        # 打印前10只股票
        for i, stock in enumerate(results[:10], 1):
            print(f"   {i:2d}. {stock['ts_code']} - {stock.get('name', 'N/A')} "
                  f"总积分: {stock['total_score']:.2f} 跑赢天数: {stock['outperformance_days']}/{self.ma_period} ({stock['outperformance_ratio']:.1%})")
            print(f"      最新收盘价: {stock['latest_close']:.2f} 成交量: {stock['latest_volume']:,.0f}")
        
        # 这里可以添加保存到数据库或文件的逻辑
        # self._save_to_database(results)
        # self._save_to_csv(results)
        
        return True
    
    def _save_to_database(self, results: List[Dict[str, Any]]) -> bool:
        """保存结果到数据库"""
        # 实现数据库保存逻辑
        pass
    
    def _save_to_csv(self, results: List[Dict[str, Any]]) -> bool:
        """保存结果到CSV文件"""
        # 实现CSV保存逻辑
        pass


def create_strategy(config) -> CSI300RelativeStrengthStrategy:
    """创建策略实例"""
    return CSI300RelativeStrengthStrategy(config)