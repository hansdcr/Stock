"""
股票基本面数据管理类
负责处理股票基本面数据的获取、保存和管理
参考Tushare daily_basic接口：https://tushare.pro/document/2?doc_id=32
"""
import os
import pandas as pd
import tushare
from typing import Dict, List, Optional
from ..models.daily_basic import DailyBasic
from ..mysql_manager import MySQLManager
from ..company_manager import CompanyManager


class DailyBasicManager:
    """股票基本面数据管理类"""
    
    def __init__(self, config, pro):
        """
        初始化基本面数据管理器
        
        :param config: 配置对象
        :param pro: Tushare Pro API对象
        """
        self.config = config
        self.pro = pro
        self.data_dir = config.get_data_dir()
        self.mysql_manager = MySQLManager(config)
        self.company_manager = CompanyManager(config)
    
    def fetch_daily_basic_data(self, ts_code: str = None, trade_date: str = None, start_date: str = None, 
                              end_date: str = None, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取股票基本面数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'（可选，为空表示所有股票）
        :param trade_date: 交易日期，格式YYYYMMDD（可选）
        :param start_date: 开始日期，格式YYYYMMDD（可选）
        :param end_date: 结束日期，格式YYYYMMDD（可选）
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含股票基本面数据，如果无数据返回None
        """
        try:
            # 使用 DailyBasic 类的默认字段
            default_fields = DailyBasic.DEFAULT_FIELDS
            
            # 合并用户指定字段和默认字段
            if fields is None:
                fields = default_fields
            else:
                fields = list(set(fields + default_fields))
            
            # 构建查询参数
            params = {
                "fields": ",".join(fields)
            }
            
            # 添加股票代码参数（如果提供）
            if ts_code:
                params["ts_code"] = ts_code
            
            # 添加日期参数
            if trade_date:
                params["trade_date"] = trade_date
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            # 执行查询（使用daily_basic接口）
            df = self.pro.daily_basic(**params)
            
            if df.empty:
                print(f"⚠️  未找到基本面数据")
                return None
            
            print(f"✅ 成功获取基本面数据，共 {len(df)} 条记录")
            
            # 数据预处理
            df = self._preprocess_daily_basic_data(df)
            
            return df
            
        except Exception as e:
            print(f"❌ 获取基本面数据失败: {e}")
            return None
    
    def _preprocess_daily_basic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理基本面数据
        
        :param df: 原始基本面数据DataFrame
        :return: 处理后的DataFrame
        """
        # 转换日期字段为datetime.date类型
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        
        # 转换其他数值字段为float类型
        numeric_fields = [
            'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio',
            'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm',
            'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv'
        ]
        
        for field in numeric_fields:
            if field in df.columns:
                # 避免链式赋值警告，直接赋值
                df[field] = pd.to_numeric(df[field], errors='coerce')
                # 处理NaN值
                df[field] = df[field].fillna(0.0)
                # 处理inf值
                df[field] = df[field].replace([float('inf'), float('-inf')], 0.0)
        
        return df
    
    def save_daily_basic_data_to_csv(self, df: pd.DataFrame, filename_suffix: str = "") -> None:
        """
        保存基本面数据到本地CSV文件
        
        :param df: 包含基本面数据的DataFrame
        :param filename_suffix: 文件名后缀
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return
        
        # 构建文件名
        if filename_suffix:
            filename = f"{self.data_dir}/daily_basic_{filename_suffix}.csv"
        else:
            filename = f"{self.data_dir}/daily_basic.csv"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # 保存到CSV
        df.to_csv(filename, mode='a', index=False, encoding="utf-8-sig")
        
        print(f"✅ 基本面数据已保存到 {filename}")

    def _get_daily_basic_table_queries(self, table_name: str, include_status_fields: bool = False) -> tuple:
        """
        公共方法：获取基本面数据表的创建和插入查询语句
        
        :param table_name: 数据库表名
        :param include_status_fields: 是否包含状态字段（data_status, status_reason）
        :return: (create_table_query, insert_query, expected_columns, fill_missing_defaults)
        """
        # 构建表创建语句
        status_fields_sql = ""
        if include_status_fields:
            status_fields_sql = """
            data_status VARCHAR(10),
            status_reason VARCHAR(100),
            """
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts_code VARCHAR(20),
            trade_date DATETIME,
            close FLOAT,
            turnover_rate FLOAT,
            turnover_rate_f FLOAT,
            volume_ratio FLOAT,
            pe FLOAT,
            pe_ttm FLOAT,
            pb FLOAT,
            ps FLOAT,
            ps_ttm FLOAT,
            dv_ratio FLOAT,
            dv_ttm FLOAT,
            total_share FLOAT,
            float_share FLOAT,
            free_share FLOAT,
            total_mv FLOAT,
            circ_mv FLOAT,
            {status_fields_sql}
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_stock_date (ts_code, trade_date)
        )
        """
        
        # 构建插入语句
        status_fields_insert = ""
        status_fields_values = ""
        status_fields_update = ""
        
        if include_status_fields:
            status_fields_insert = "data_status, status_reason,"
            status_fields_values = "%s, %s,"
            status_fields_update = """
            data_status = VALUES(data_status),
            status_reason = VALUES(status_reason),
            """
        
        insert_query = f"""
        INSERT INTO {table_name} (ts_code, trade_date, close, turnover_rate, turnover_rate_f, 
                                volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
                                total_share, float_share, free_share, total_mv, circ_mv, 
                                {status_fields_insert} created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                {status_fields_values} CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            close = VALUES(close),
            turnover_rate = VALUES(turnover_rate),
            turnover_rate_f = VALUES(turnover_rate_f),
            volume_ratio = VALUES(volume_ratio),
            pe = VALUES(pe),
            pe_ttm = VALUES(pe_ttm),
            pb = VALUES(pb),
            ps = VALUES(ps),
            ps_ttm = VALUES(ps_ttm),
            dv_ratio = VALUES(dv_ratio),
            dv_ttm = VALUES(dv_ttm),
            total_share = VALUES(total_share),
            float_share = VALUES(float_share),
            free_share = VALUES(free_share),
            total_mv = VALUES(total_mv),
            circ_mv = VALUES(circ_mv),
            {status_fields_update}
            updated_at = CURRENT_TIMESTAMP
        """
        
        # 构建期望列和默认值
        expected_columns = [
            'ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f',
            'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio',
            'dv_ttm', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv'
        ]
        
        fill_missing_defaults = {
            'close': 0.0, 'turnover_rate': 0.0, 'turnover_rate_f': 0.0, 'volume_ratio': 0.0,
            'pe': 0.0, 'pe_ttm': 0.0, 'pb': 0.0, 'ps': 0.0, 'ps_ttm': 0.0, 'dv_ratio': 0.0,
            'dv_ttm': 0.0, 'total_share': 0.0, 'float_share': 0.0, 'free_share': 0.0,
            'total_mv': 0.0, 'circ_mv': 0.0
        }
        
        if include_status_fields:
            expected_columns.extend(['data_status', 'status_reason'])
            fill_missing_defaults.update({
                'data_status': 'success',
                'status_reason': ''
            })
        
        return create_table_query, insert_query, expected_columns, fill_missing_defaults

    def _save_daily_basic_data_to_mysql(self, df: pd.DataFrame, table_name: str = "daily_basic", 
                                       batch_size: int = 50) -> bool:
        """
        保存基本面数据到MySQL数据库（批量处理）
        
        :param df: 包含基本面数据的DataFrame
        :param table_name: 数据库表名
        :param batch_size: 批量处理大小
        :return: 是否成功保存
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存到MySQL")
            return False
        
        try:
            # 获取表创建和插入查询
            create_table_query, insert_query, expected_columns, fill_missing_defaults = \
                self._get_daily_basic_table_queries(table_name)
            
            # 创建表
            self.mysql_manager.execute_query(create_table_query)
            
            # 准备数据
            data_to_insert = []
            for _, row in df.iterrows():
                row_data = {}
                for col in expected_columns:
                    if col in row:
                        row_data[col] = row[col]
                    else:
                        row_data[col] = fill_missing_defaults.get(col, None)
                
                # 转换trade_date为字符串格式
                if 'trade_date' in row_data and hasattr(row_data['trade_date'], 'strftime'):
                    row_data['trade_date'] = row_data['trade_date'].strftime('%Y-%m-%d')
                
                data_to_insert.append(tuple(row_data[col] for col in expected_columns))
            
            # 批量插入数据（每batch_size条保存一次）
            total_records = len(data_to_insert)
            success_count = 0
            
            for i in range(0, total_records, batch_size):
                batch = data_to_insert[i:i + batch_size]
                try:
                    self.mysql_manager.execute_many(insert_query, batch)
                    success_count += len(batch)
                    # print(f"✅ 已批量保存 {len(batch)} 条基本面数据到MySQL ({i + len(batch)}/{total_records})")
                except Exception as e:
                    print(f"❌ 批量保存失败: {e}")
            
            print(f"✅ 成功保存 {success_count}/{total_records} 条基本面数据到MySQL表 {table_name}")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 保存基本面数据到MySQL失败: {e}")
            return False

    def fetch_daily_basic_data_by_trade_date(self, trade_date: str, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取指定交易日的所有股票基本面数据
        
        :param trade_date: 交易日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含所有股票基本面数据，如果无数据返回None
        """
        try:
            # 使用 DailyBasic 类的默认字段
            default_fields = DailyBasic.DEFAULT_FIELDS
            
            # 合并用户指定字段和默认字段
            if fields is None:
                fields = default_fields
            else:
                fields = list(set(fields + default_fields))
            
            # 构建查询参数
            params = {
                "trade_date": trade_date,
                "fields": ",".join(fields)
            }
            
            # 执行查询（使用daily_basic接口）
            df = self.pro.daily_basic(**params)
            
            if df.empty:
                print(f"⚠️  未找到交易日 {trade_date} 的基本面数据")
                return None
            
            print(f"✅ 成功获取交易日 {trade_date} 的基本面数据，共 {len(df)} 条记录")
            
            # 数据预处理
            df = self._preprocess_daily_basic_data(df)
            
            return df
            
        except Exception as e:
            print(f"❌ 获取交易基本面数据失败: {e}")
            return None

    def fetch_all_stocks_daily_basic_period(self, start_date: str, end_date: str, 
                                           save_to_mysql: bool = True, table_name: str = "daily_basic",
                                           batch_size: int = 50) -> pd.DataFrame:
        """
        获取所有股票在指定日期范围内的基本面数据（按照Tushare推荐的方式：按日期循环）
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param save_to_mysql: 是否保存到MySQL数据库
        :param table_name: MySQL表名
        :param batch_size: 批量保存大小
        :return: 包含所有股票基本面数据的DataFrame
        """
        print(f"🚀 开始获取所有股票基本面数据 ({start_date} 到 {end_date})")
        
        # 获取交易日列表
        trade_dates = self._get_trade_dates(start_date, end_date)
        
        if not trade_dates:
            print("❌ 没有找到有效的交易日")
            return pd.DataFrame()
        
        all_data = []
        total_dates = len(trade_dates)
        
        # 按交易日循环获取数据
        for i, trade_date in enumerate(trade_dates, 1):
            print(f"📅 正在处理交易日 {i}/{total_dates}: {trade_date}")
            
            try:
                # 获取该交易日的所有股票基本面数据
                df = self.fetch_daily_basic_data_by_trade_date(trade_date)
                
                if df is not None and not df.empty:
                    all_data.append(df)
                    print(f"✅ 成功获取交易日 {trade_date} 的基本面数据，共 {len(df)} 条记录")
                    
                    # 如果启用了MySQL保存，立即保存这批数据
                    if save_to_mysql:
                        self._save_daily_basic_data_to_mysql(df, table_name, batch_size)
                    
                    # 保存到CSV文件（按日期）
                    # self.save_daily_basic_data_to_csv(df, trade_date)
                else:
                    print(f"⚠️  交易日 {trade_date} 无基本面数据")
                    
            except Exception as e:
                print(f"❌ 处理交易日 {trade_date} 时出错: {e}")
                continue
        
        # 合并所有数据
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"✅ 成功获取所有股票基本面数据，共 {len(combined_df)} 条记录")
            
            # 保存合并后的数据到CSV
            csv_filename = f"daily_basic_all_stocks_{start_date}_{end_date}"
            # self.save_daily_basic_data_to_csv(combined_df, csv_filename)
            
            return combined_df
        else:
            print("❌ 未获取到任何基本面数据")
            return pd.DataFrame()

    def _get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        获取指定日期范围内的交易日列表
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :return: 交易日列表（格式YYYYMMDD）
        """
        try:
            # 获取交易日历
            df = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, fields='cal_date,is_open')
            
            if df.empty:
                print(f"⚠️  未找到交易日历数据 ({start_date} 到 {end_date})")
                return []
            
            # 筛选开市日
            trade_dates = df[df['is_open'] == 1]['cal_date'].tolist()
            
            if not trade_dates:
                print(f"⚠️  在指定日期范围内没有开市日")
                return []
            
            print(f"✅ 找到 {len(trade_dates)} 个交易日")
            return trade_dates
            
        except Exception as e:
            print(f"❌ 获取交易日失败: {e}")
            return []


# # if __name__ == "__main__":
#     # 示例用法
#     import tushare as ts
#     from parse_config import ParseConfig
    
#     # 初始化配置和Tushare Pro API
#     config = ParseConfig()
#     pro = ts.pro_api(config.get_token())
    
#     # 创建基本面数据管理器
#     daily_basic_manager = DailyBasicManager(config, pro)
    
#     # # 示例1：获取单只股票的基本面数据
#     # print("=== 示例1：获取单只股票基本面数据 ===")
#     # df_single = daily_basic_manager.fetch_daily_basic_data("000001.SZ", start_date="20250927", end_date="20251001")
#     # if df_single is not None:
#     #     daily_basic_manager.save_daily_basic_data_to_csv(df_single, "000001.SZ")
    
#     # 示例2：获取所有股票的基本面数据（按日期循环）
#     print("\n=== 示例2：获取所有股票基本面数据 ===")
#     df_all = daily_basic_manager.fetch_all_stocks_daily_basic_period(
#         start_date="20250101", 
#         end_date="20251001",
#         save_to_mysql=True,
#         batch_size=50
#     )
    
#     print("✅ 基本面数据获取完成！")