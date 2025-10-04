"""
月线数据管理类
负责处理股票月线数据的获取、保存和管理
参考Tushare月线数据接口：https://tushare.pro/document/2?doc_id=145
"""
import os
import pandas as pd
import tushare
from typing import Dict, List, Optional
from models.monthly_data import MonthlyData
from mysql_manager import MySQLManager
from company_manager import CompanyManager


class MonthlyDataManager:
    """月线数据管理类"""
    
    def __init__(self, config, pro):
        """
        初始化月线数据管理器
        
        :param config: 配置对象
        :param pro: Tushare Pro API对象
        """
        self.config = config
        self.pro = pro
        self.data_dir = config.get_data_dir()
        self.mysql_manager = MySQLManager(config)
        self.company_manager = CompanyManager(config)
    
    def fetch_monthly_data(self, ts_code: str, trade_date: str = None, start_date: str = None, 
                          end_date: str = None, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取单只股票的月线交易数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param trade_date: 交易日期，格式YYYYMMDD（可选）
        :param start_date: 开始日期，格式YYYYMMDD（可选）
        :param end_date: 结束日期，格式YYYYMMDD（可选）
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含股票月线数据，如果无数据返回None
        """
        try:
            # 使用 MonthlyData 类的默认字段
            default_fields = MonthlyData.DEFAULT_FIELDS
            
            # 合并用户指定字段和默认字段
            if fields is None:
                fields = default_fields
            else:
                fields = list(set(fields + default_fields))
            
            # 构建查询参数
            params = {
                "ts_code": ts_code,
                "fields": ",".join(fields)
            }
            
            # 添加日期参数
            if trade_date:
                params["trade_date"] = trade_date
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            # 执行查询（使用monthly接口）
            df = self.pro.monthly(**params)
            
            if df.empty:
                print(f"⚠️  未找到股票 {ts_code} 的月线数据")
                return None
            
            print(f"✅ 成功获取股票 {ts_code} 的月线数据，共 {len(df)} 条记录")
            
            # 数据预处理
            df = self._preprocess_monthly_data(df)
            
            return df
            
        except Exception as e:
            print(f"❌ 获取股票月线数据失败: {e}")
            return None
    
    def _preprocess_monthly_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理月线数据
        
        :param df: 原始月线数据DataFrame
        :return: 处理后的DataFrame
        """
        # 转换日期字段为datetime.date类型
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        
        # 转换其他数值字段为float类型
        for field in ['open', 'high', 'low', 'close', 'vol', 'amount', 'pre_close', 'change', 'pct_chg']:
            if field in df.columns:
                # 避免链式赋值警告，直接赋值
                df[field] = pd.to_numeric(df[field], errors='coerce')
                # 处理NaN值
                df[field] = df[field].fillna(0.0)
                # 处理inf值
                df[field] = df[field].replace([float('inf'), float('-inf')], 0.0)
        
        return df
    
    def save_monthly_data_to_csv(self, df: pd.DataFrame, ts_code: str) -> None:
        """
        保存单只股票月线数据到本地CSV文件
        
        :param df: 包含月线数据的DataFrame
        :param ts_code: 股票代码
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return
        
        # 构建文件名
        filename = f"{self.data_dir}/monthly_{ts_code}.csv"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # 保存到CSV
        df.to_csv(filename, mode='a', index=False, encoding="utf-8-sig")
        
        print(f"✅ 股票月线数据已保存到 {filename}")

    def _get_monthly_data_table_queries(self, table_name: str, include_status_fields: bool = False) -> tuple:
        """
        公共方法：获取月线数据表的创建和插入查询语句
        
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
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            pre_close FLOAT,
            `change` FLOAT,
            pct_chg FLOAT,
            vol FLOAT,
            amount FLOAT,
            {status_fields_sql}
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_stock_month (ts_code, trade_date)
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
        INSERT INTO {table_name} (ts_code, trade_date, open, high, low, close, pre_close, 
                                `change`, pct_chg, vol, amount, {status_fields_insert}
                                created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {status_fields_values}
                CURRENT_TIMESTAMP)
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
            {status_fields_update}
            updated_at = CURRENT_TIMESTAMP
        """
        
        # 构建期望列和默认值
        expected_columns = [
            'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 
            'pre_close', 'change', 'pct_chg', 'vol', 'amount'
        ]
        
        fill_missing_defaults = {
            'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0,
            'pre_close': 0.0, 'change': 0.0, 'pct_chg': 0.0,
            'vol': 0.0, 'amount': 0.0
        }
        
        if include_status_fields:
            expected_columns.extend(['data_status', 'status_reason'])
            fill_missing_defaults.update({
                'data_status': 'success',
                'status_reason': ''
            })
        
        return create_table_query, insert_query, expected_columns, fill_missing_defaults

    def _save_monthly_data_to_mysql(self, df: pd.DataFrame, table_name: str = "monthly_data", 
                                   batch_size: int = 50) -> bool:
        """
        保存月线数据到MySQL数据库（批量处理）
        
        :param df: 包含月线数据的DataFrame
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
                self._get_monthly_data_table_queries(table_name)
            
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
                    # print(f"✅ 已批量保存 {len(batch)} 条月线数据到MySQL ({i + len(batch)}/{total_records})")
                except Exception as e:
                    print(f"❌ 批量保存失败: {e}")
            
            print(f"✅ 成功保存 {success_count}/{total_records} 条月线数据到MySQL表 {table_name}")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 保存月线数据到MySQL失败: {e}")
            return False

    def fetch_monthly_data_by_trade_date(self, trade_date: str, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取指定交易日的所有股票月线数据
        
        :param trade_date: 交易日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含所有股票月线数据，如果无数据返回None
        """
        try:
            # 使用 MonthlyData 类的默认字段
            default_fields = MonthlyData.DEFAULT_FIELDS
            
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
            
            # 执行查询（使用monthly接口）
            df = self.pro.monthly(**params)
            
            if df.empty:
                print(f"⚠️  未找到交易日 {trade_date} 的月线数据")
                return None
            
            print(f"✅ 成功获取交易日 {trade_date} 的月线数据，共 {len(df)} 条记录")
            
            # 数据预处理
            df = self._preprocess_monthly_data(df)
            
            return df
            
        except Exception as e:
            print(f"❌ 获取交易日月线数据失败: {e}")
            return None

    def _get_monthly_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        获取指定日期范围内的月度日期列表（每月最后一个交易日）
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :return: 月度日期列表（格式YYYYMMDD）
        """
        try:
            # 获取交易日历
            df = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, fields='cal_date,is_open')
            
            if df.empty:
                print(f"⚠️  未找到交易日历数据 ({start_date} 到 {end_date})")
                return []
            
            # 筛选开市日
            open_days = df[df['is_open'] == 1]['cal_date'].tolist()
            
            if not open_days:
                print(f"⚠️  在指定日期范围内没有开市日")
                return []
            
            # 按月份分组，获取每个月的最后一个交易日
            monthly_dates = []
            current_month = ""
            last_trade_date = ""
            
            for date_str in sorted(open_days):
                month_str = date_str[:6]  # YYYYMM
                
                if month_str != current_month:
                    if last_trade_date:
                        monthly_dates.append(last_trade_date)
                    current_month = month_str
                
                last_trade_date = date_str
            
            # 添加最后一个月的最后一个交易日
            if last_trade_date and last_trade_date not in monthly_dates:
                monthly_dates.append(last_trade_date)
            
            print(f"✅ 找到 {len(monthly_dates)} 个月度交易日: {monthly_dates}")
            return monthly_dates
            
        except Exception as e:
            print(f"❌ 获取月度日期失败: {e}")
            return []

    def fetch_all_stocks_monthly_data_period(self, start_date: str, end_date: str, 
                                           save_to_mysql: bool = True, table_name: str = "monthly_data",
                                           batch_size: int = 50) -> pd.DataFrame:
        """
        获取所有股票在指定日期范围内的月线数据（按照Tushare推荐的方式：按日期循环）
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param save_to_mysql: 是否保存到MySQL数据库
        :param table_name: MySQL表名
        :param batch_size: 批量保存大小
        :return: 包含所有股票月线数据的DataFrame
        """
        print(f"🚀 开始获取所有股票月线数据 ({start_date} 到 {end_date})")
        
        # 获取月度日期列表
        monthly_dates = self._get_monthly_dates(start_date, end_date)
        
        if not monthly_dates:
            print("❌ 没有找到有效的月度交易日")
            return pd.DataFrame()
        
        all_data = []
        total_dates = len(monthly_dates)
        
        # 按月度日期循环获取数据
        for i, trade_date in enumerate(monthly_dates, 1):
            print(f"📅 正在处理月度交易日 {i}/{total_dates}: {trade_date}")
            
            try:
                # 获取该交易日的所有股票月线数据
                df = self.fetch_monthly_data_by_trade_date(trade_date)
                
                if df is not None and not df.empty:
                    all_data.append(df)
                    print(f"✅ 成功获取交易日 {trade_date} 的月线数据，共 {len(df)} 条记录")
                    
                    # 如果启用了MySQL保存，立即保存这批数据
                    if save_to_mysql:
                        self._save_monthly_data_to_mysql(df, table_name, batch_size)
                else:
                    print(f"⚠️  交易日 {trade_date} 无月线数据")
                    
            except Exception as e:
                print(f"❌ 处理交易日 {trade_date} 时出错: {e}")
                continue
        
        # 合并所有数据
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"✅ 成功获取所有股票月线数据，共 {len(combined_df)} 条记录")
            
            # 保存合并后的数据到CSV
            csv_filename = f"{self.data_dir}/monthly_all_stocks_{start_date}_{end_date}.csv"
            combined_df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
            print(f"✅ 所有股票月线数据已保存到 {csv_filename}")
            
            return combined_df
        else:
            print("❌ 未获取到任何月线数据")
            return pd.DataFrame()


if __name__ == "__main__":
    # 示例用法
    import tushare as ts
    from parse_config import ParseConfig
    
    # 初始化配置和Tushare Pro API
    config = ParseConfig()
    pro = ts.pro_api(config.get_token())
    
    # 创建月线数据管理器
    monthly_manager = MonthlyDataManager(config, pro)
    
    # # 示例1：获取单只股票的月线数据
    # print("=== 示例1：获取单只股票月线数据 ===")
    # df_single = monthly_manager.fetch_monthly_data("000001.SZ", start_date="20250901", end_date="20251001")
    # if df_single is not None:
    #     monthly_manager.save_monthly_data_to_csv(df_single, "000001.SZ")
    
    # 示例2：获取所有股票的月线数据（按日期循环）
    print("\n=== 示例2：获取所有股票月线数据 ===")
    df_all = monthly_manager.fetch_all_stocks_monthly_data_period(
        start_date="20250101", 
        end_date="20251001",
        save_to_mysql=True,
        batch_size=50
    )
    
    print("✅ 月线数据获取完成！")