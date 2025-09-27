"""
日线数据管理类
负责处理股票日线数据的获取、保存和管理
"""
import os
import pandas as pd
import tushare
from typing import List, Optional
from models.daily_data import DailyData
from mysql_manager import MySQLManager


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
        self.mysql_manager = MySQLManager(config)
    
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

    # 将每日数据保存到MySQL数据库
    def save_daily_data_to_mysql(self, df: pd.DataFrame, ts_code: str, trade_date: str) -> None:
        """
        保存单只股票交易数据到MySQL数据库
        
        :param df: 包含交易数据的DataFrame
        :param ts_code: 股票代码
        :param trade_date: 交易日期
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return
        
        # 构建表名
        table_name = f"daily_data"
        
        # 转换日期字段为字符串类型 (YYYYMMDD格式)
        df['trade_date'] = df['trade_date'].apply(lambda x: x.strftime('%Y%m%d') if hasattr(x, 'strftime') else str(x).replace('-', ''))
        
        # 保存到MySQL
        self._save_to_mysql(df, table_name)
        
        print(f"✅ 股票交易数据已保存到MySQL表 {table_name}")

    def _save_to_mysql(self, df: pd.DataFrame, table_name: str) -> None:
        """
        私有方法：将数据保存到MySQL数据库
        
        :param df: 包含数据的DataFrame
        :param table_name: 数据库表名
        """
        # 创建日线数据表（如果不存在）
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts_code VARCHAR(20),
            trade_date VARCHAR(8),
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
        
        # 准备插入数据
        insert_query = f"""
        INSERT INTO {table_name} (ts_code, trade_date, open, high, low, close, pre_close, `change`, pct_chg, vol, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        
        # 使用MySQL管理器保存数据
        success = self.mysql_manager.save_dataframe_to_table(
            df=df,
            table_name=table_name,
            insert_query=insert_query,
            expected_columns=DailyData.DEFAULT_FIELDS,
            fill_missing_defaults={
                'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0,
                'pre_close': 0.0, 'change': 0.0, 'pct_chg': 0.0,
                'vol': 0.0, 'amount': 0.0
            }
        )
        
        if success:
            # 确保表结构正确
            self.mysql_manager.create_table_if_not_exists(table_name, create_table_query)

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
            self.save_daily_data_to_mysql(df, ts_code, trade_date)
        return df

if __name__ == "__main__":
    from parse_config import ParseConfig
    config = ParseConfig()
    daily_manager = DailyDataManager(config, tushare.pro_api(config.get_token()))
    # 获取并保存000001.SZ在20250926的交易数据
    #df = daily_manager.fetch_and_save_daily_data("000001.SZ", "20250926")
    df = daily_manager.fetch_and_save_daily_data("000001.SZ", "20250924")
