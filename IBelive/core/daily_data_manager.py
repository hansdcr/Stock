"""
日线数据管理类
负责处理股票日线数据的获取、保存和管理
"""
import os
import pandas as pd
import tushare
from typing import List, Optional
from models.daily_data import DailyData


class DailyDataManager:
    """日线数据管理类"""
    
    def __init__(self, config, pro):
        """
        初始化日线数据管理器
        
        :param config: 配置对象
        :param pro: Tushare Pro API对象
        """
        self.config = config
        self.pro = pro
        self.data_dir = config.get_data_dir()
    
    def fetch_daily_data(self, ts_code: str, trade_date: str, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取单只股票在指定日期的交易数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param trade_date: 交易日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含股票交易数据，如果无数据返回None
        """
        try:
            # 使用 DailyData 类的默认字段
            default_fields = DailyData.DEFAULT_FIELDS
            
            # 合并用户指定字段和默认字段
            if fields is None:
                fields = default_fields
            else:
                fields = list(set(fields + default_fields))
            
            # 构建查询参数
            params = {
                "ts_code": ts_code,
                "trade_date": trade_date,
            }
            
            # 执行查询
            df = self.pro.daily(**params)
            
            if df.empty:
                print(f"⚠️  未找到股票 {ts_code} 在 {trade_date} 的交易数据")
                return None
            
            print(f"✅ 成功获取股票 {ts_code} 在 {trade_date} 的交易数据")
            
            # 数据预处理
            df = self._preprocess_daily_data(df)
            
            return df
            
        except Exception as e:
            print(f"❌ 获取股票交易数据失败: {e}")
            return None
    
    def _preprocess_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理日线数据
        
        :param df: 原始日线数据DataFrame
        :return: 处理后的DataFrame
        """
        # 转换日期字段为datetime.date类型
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        
        # 转换其他数值字段为float类型
        for field in ['open', 'high', 'low', 'close', 'vol', 'amount']:
            if field in df.columns:
                # 避免链式赋值警告，直接赋值
                df[field] = pd.to_numeric(df[field], errors='coerce')
                # 处理NaN值
                df[field] = df[field].fillna(0.0)
                # 处理inf值
                df[field] = df[field].replace([float('inf'), float('-inf')], 0.0)
        
        return df
    
    def save_daily_data_to_csv(self, df: pd.DataFrame, ts_code: str, trade_date: str) -> None:
        """
        保存单只股票交易数据到本地CSV文件
        
        :param df: 包含交易数据的DataFrame
        :param ts_code: 股票代码
        :param trade_date: 交易日期
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return
        
        # 构建文件名
        filename = f"{self.data_dir}/daily_{ts_code}_{trade_date}.csv"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # 保存到CSV
        df.to_csv(filename, mode='a', index=False, encoding="utf-8-sig")
        
        print(f"✅ 股票交易数据已保存到 {filename}")
    
    def fetch_and_save_daily_data(self, ts_code: str, trade_date: str, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取并保存单只股票在指定日期的交易数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param trade_date: 交易日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含股票交易数据，如果无数据返回None
        """
        df = self.fetch_daily_data(ts_code, trade_date, fields)
        if df is not None:
            self.save_daily_data_to_csv(df, ts_code, trade_date)
        return df

if __name__ == "__main__":
    from parse_config import ParseConfig
    config = ParseConfig()
    daily_manager = DailyDataManager(config, tushare.pro_api(config.get_token()))
    # 获取并保存000001.SZ在20250926的交易数据
    df = daily_manager.fetch_and_save_daily_data("000001.SZ", "20250926")