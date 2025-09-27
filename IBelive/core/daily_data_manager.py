"""
日线数据管理类
负责处理股票日线数据的获取、保存和管理
"""
import os
import pandas as pd
import tushare
from typing import Dict, List, Optional
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

    def _get_daily_data_table_queries(self, table_name: str, include_status_fields: bool = False) -> tuple:
        """
        公共方法：获取日线数据表的创建和插入查询语句
        
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
        expected_columns = DailyData.DEFAULT_FIELDS.copy()
        fill_missing_defaults = {
            'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0,
            'pre_close': 0.0, 'change': 0.0, 'pct_chg': 0.0,
            'vol': 0.0, 'amount': 0.0
        }
        
        if include_status_fields:
            expected_columns.extend(['data_status', 'status_reason'])
            fill_missing_defaults.update({
                'data_status': '',
                'status_reason': ''
            })
        
        return create_table_query, insert_query, expected_columns, fill_missing_defaults
    
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
            # 添加数据状态标记
            df['data_status'] = '正常'
            df['status_reason'] = ''
            
            # 构建表名
            table_name = "daily_data"
            
            # 转换日期字段为字符串类型 (YYYYMMDD格式)
            df['trade_date'] = df['trade_date'].apply(
                lambda x: x.strftime('%Y%m%d') if hasattr(x, 'strftime') else str(x).replace('-', '')
            )
            
            # 使用公共方法获取查询语句（包含状态字段）
            create_table_query, insert_query, expected_columns, fill_missing_defaults = \
                self._get_daily_data_table_queries(table_name, include_status_fields=True)
            
            # 首先确保表已创建
            table_created = self.mysql_manager.create_table_if_not_exists(table_name, create_table_query)
            
            if not table_created:
                print(f"❌ 创建表 {table_name} 失败")
                return df
            
            # 使用MySQL管理器保存数据
            success = self.mysql_manager.save_dataframe_to_table(
                df=df,
                table_name=table_name,
                insert_query=insert_query,
                expected_columns=expected_columns,
                fill_missing_defaults=fill_missing_defaults
            )

            if success:
                print(f"✅ 成功保存 {ts_code} 在 {trade_date} 的数据到MySQL表 {table_name}")
            else:
                print(f"⚠️  保存 {ts_code} 在 {trade_date} 的数据到MySQL表 {table_name} 失败")
        
        return df

    def fetch_daily_data_period(
        self, 
        ts_code: str, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取指定时间段内每个交易日的股票数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含期间所有日期的股票数据，非交易日或停盘的数据标记为空并注明原因
        """
        try:
            # 获取交易日历
            trade_cal = self.pro.trade_cal(
                exchange='', 
                start_date=start_date, 
                end_date=end_date,
                fields=['cal_date', 'is_open']
            )
            
            if trade_cal.empty:
                print(f"⚠️  未找到 {start_date} 到 {end_date} 的交易日历")
                return pd.DataFrame()
            
            # 筛选交易日
            trading_days = trade_cal[trade_cal['is_open'] == 1]['cal_date'].tolist()
            
            if not trading_days:
                print(f"⚠️  {start_date} 到 {end_date} 期间没有交易日")
                return pd.DataFrame()
            
            print(f"📅 找到 {len(trading_days)} 个交易日: {start_date} 到 {end_date}")
            
            # 获取所有交易日的股票数据
            all_data = []
            
            for trade_date in trading_days:
                try:
                    # 获取单日数据
                    daily_df = self.fetch_daily_data(ts_code, trade_date, fields)
                    
                    if daily_df is not None and not daily_df.empty:
                        # 添加数据状态标记
                        daily_df['data_status'] = '正常'
                        daily_df['status_reason'] = ''
                        all_data.append(daily_df)
                        print(f"✅ 成功获取 {ts_code} 在 {trade_date} 的数据")
                    else:
                        # 创建空数据行并标记为停盘
                        empty_row = self._create_empty_daily_data(ts_code, trade_date, fields)
                        empty_row['data_status'] = '停盘'
                        empty_row['status_reason'] = '当日停牌或无交易数据'
                        all_data.append(empty_row)
                        print(f"⚠️  {ts_code} 在 {trade_date} 停盘或无数据")
                        
                except Exception as e:
                    print(f"❌ 获取 {ts_code} 在 {trade_date} 的数据时出错: {e}")
                    # 创建空数据行并标记为错误
                    error_row = self._create_empty_daily_data(ts_code, trade_date, fields)
                    error_row['data_status'] = '错误'
                    error_row['status_reason'] = f'数据获取失败: {str(e)}'
                    all_data.append(error_row)
            
            # 合并所有数据
            if all_data:
                result_df = pd.concat(all_data, ignore_index=True)
                print(f"✅ 成功获取 {ts_code} 在 {start_date} 到 {end_date} 期间的 {len(result_df)} 条数据记录")
                return result_df
            else:
                print(f"⚠️  未获取到 {ts_code} 在 {start_date} 到 {end_date} 期间的任何数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ 获取期间股票数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_and_save_daily_data_period(
        self, 
        ts_code: str, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取并保存指定时间段内每个交易日的股票数据到MySQL
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含期间所有日期的股票数据
        """
        # 获取期间数据
        df = self.fetch_daily_data_period(ts_code, start_date, end_date, fields)
        
        if df is not None and not df.empty:
            # 保存到MySQL
            self._save_period_data_to_mysql(df, ts_code, start_date, end_date)
        
        return df

    def fetch_all_stocks_daily_data_period(
        self, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None,
        save_to_mysql: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间段内所有股票的日线数据
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :param save_to_mysql: 是否保存到MySQL数据库，默认False
        :return: 字典，键为股票代码，值为包含期间所有日期的股票数据的DataFrame
        """
        from company_manager import CompanyManager
        
        try:
            # 获取所有上市股票列表
            company_manager = CompanyManager(self.config)
            all_stocks_df = company_manager.fetch_listed_companies()
            
            if all_stocks_df.empty:
                print("⚠️  未获取到任何上市股票信息")
                return {}
            
            # 获取所有股票代码
            all_stocks = all_stocks_df['ts_code'].tolist()
            print(f"📊 找到 {len(all_stocks)} 只上市股票")
            
            # 存储所有股票的数据
            all_stocks_data = {}
            
            # 遍历所有股票，获取期间数据
            for i, ts_code in enumerate(all_stocks, 1):
                try:
                    print(f"\n🔍 正在处理第 {i}/{len(all_stocks)} 只股票: {ts_code}")
                    
                    if save_to_mysql:
                        # 获取并保存数据到MySQL
                        stock_df = self.fetch_and_save_daily_data_period(ts_code, start_date, end_date, fields)
                    else:
                        # 只获取数据，不保存到MySQL
                        stock_df = self.fetch_daily_data_period(ts_code, start_date, end_date, fields)
                    
                    if stock_df is not None and not stock_df.empty:
                        all_stocks_data[ts_code] = stock_df
                        print(f"✅ 成功获取 {ts_code} 在 {start_date} 到 {end_date} 期间的 {len(stock_df)} 条数据")
                    else:
                        print(f"⚠️  未获取到 {ts_code} 在 {start_date} 到 {end_date} 期间的任何数据")
                        
                except Exception as e:
                    print(f"❌ 处理股票 {ts_code} 时发生错误: {e}")
                    continue
            
            print(f"\n🎉 完成! 成功获取 {len(all_stocks_data)} 只股票的日线数据")
            return all_stocks_data
            
        except Exception as e:
            print(f"❌ 获取所有股票日线数据失败: {e}")
            return {}

    def fetch_stocks_list_daily_data_period(
        self, 
        stocks_list: List[str], 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None,
        save_to_mysql: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定股票列表中从开始日期到结束日期的日线数据
        
        :param stocks_list: 股票代码列表，格式如 ['000001.SZ', '600000.SH']
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :param save_to_mysql: 是否保存到MySQL数据库，默认False
        :return: 字典，键为股票代码，值为包含期间所有日期的股票数据的DataFrame
        """
        try:
            if not stocks_list:
                print("⚠️  股票列表为空")
                return {}
            
            print(f"📊 开始处理 {len(stocks_list)} 只指定股票的日线数据")
            
            # 存储所有股票的数据
            stocks_data = {}
            
            # 遍历指定股票列表，获取期间数据
            for i, ts_code in enumerate(stocks_list, 1):
                try:
                    print(f"\n🔍 正在处理第 {i}/{len(stocks_list)} 只股票: {ts_code}")
                    
                    if save_to_mysql:
                        # 获取并保存数据到MySQL
                        stock_df = self.fetch_and_save_daily_data_period(ts_code, start_date, end_date, fields)
                    else:
                        # 只获取数据，不保存到MySQL
                        stock_df = self.fetch_daily_data_period(ts_code, start_date, end_date, fields)
                    
                    if stock_df is not None and not stock_df.empty:
                        stocks_data[ts_code] = stock_df
                        print(f"✅ 成功获取 {ts_code} 在 {start_date} 到 {end_date} 期间的 {len(stock_df)} 条数据")
                    else:
                        print(f"⚠️  未获取到 {ts_code} 在 {start_date} 到 {end_date} 期间的任何数据")
                        
                except Exception as e:
                    print(f"❌ 处理股票 {ts_code} 时发生错误: {e}")
                    continue
            
            print(f"\n🎉 完成! 成功获取 {len(stocks_data)} 只指定股票的日线数据")
            return stocks_data
            
        except Exception as e:
            print(f"❌ 获取指定股票列表日线数据失败: {e}")
            return {}
    
    def _save_period_data_to_mysql(self, df: pd.DataFrame, ts_code: str, start_date: str, end_date: str) -> None:
        """
        私有方法：将期间数据保存到MySQL数据库
        
        :param df: 包含期间数据的DataFrame
        :param ts_code: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return
        
        # 构建表名
        table_name = "daily_data"
        
        # 转换日期字段为字符串类型 (YYYYMMDD格式)
        df['trade_date'] = df['trade_date'].apply(
            lambda x: x.strftime('%Y%m%d') if hasattr(x, 'strftime') else str(x).replace('-', '')
        )
        
        # 使用公共方法获取查询语句（包含状态字段）
        create_table_query, insert_query, expected_columns, fill_missing_defaults = \
            self._get_daily_data_table_queries(table_name, include_status_fields=True)
        
        # 首先确保表已创建
        table_created = self.mysql_manager.create_table_if_not_exists(table_name, create_table_query)
        
        if not table_created:
            print(f"❌ 创建表 {table_name} 失败")
            return
        
        # 使用MySQL管理器保存数据
        success = self.mysql_manager.save_dataframe_to_table(
            df=df,
            table_name=table_name,
            insert_query=insert_query,
            expected_columns=expected_columns,
            fill_missing_defaults=fill_missing_defaults
        )

        if success:
            print(f"✅ 成功保存 {ts_code} 在 {start_date} 到 {end_date} 期间的 {len(df)} 条数据到MySQL表 {table_name}")
        else:
            print(f"⚠️  保存 {ts_code} 在 {start_date} 到 {end_date} 期间的数据到MySQL表 {table_name} 失败")
    
    def _create_empty_daily_data(self, ts_code: str, trade_date: str, fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        创建空的日线数据行
        
        :param ts_code: 股票代码
        :param trade_date: 交易日期
        :param fields: 字段列表
        :return: 包含空数据的DataFrame
        """
        # 使用 DailyData 类的默认字段
        default_fields = DailyData.DEFAULT_FIELDS
        
        # 合并用户指定字段和默认字段
        if fields is None:
            target_fields = default_fields
        else:
            target_fields = list(set(fields + default_fields))
        
        # 创建空数据行
        empty_data = {}
        for field in target_fields:
            if field == 'ts_code':
                empty_data[field] = ts_code
            elif field == 'trade_date':
                empty_data[field] = pd.to_datetime(trade_date, format='%Y%m%d').date()
            elif field in ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']:
                empty_data[field] = 0.0
            else:
                empty_data[field] = None
        
        # 添加状态字段
        empty_data['data_status'] = ''
        empty_data['status_reason'] = ''
        
        return pd.DataFrame([empty_data])

if __name__ == "__main__":
    from parse_config import ParseConfig
    config = ParseConfig()
    daily_manager = DailyDataManager(config, tushare.pro_api(config.get_token()))
    # 获取并保存000001.SZ在20250926的交易数据
    #df = daily_manager.fetch_and_save_daily_data("000001.SZ", "20250926")
    # df = daily_manager.fetch_and_save_daily_data("000001.SZ", "20250917")

    # # 测试fetch_daily_data_period
    # df = daily_manager.fetch_daily_data_period("000001.SZ", "20250919", "20250926")
    # print(df)

    # # 测试fetch_and_save_daily_data_period
    # df = daily_manager.fetch_and_save_daily_data_period("000001.SZ", "20250919", "20250926")
    # print(df)

    # 测试fetch_all_stocks_daily_data_period
    # all_stocks_data = daily_manager.fetch_all_stocks_daily_data_period("20250925", "20250926", save_to_mysql=True)
    # print(all_stocks_data)

    # # 测试fetch_stocks_list_daily_data_period
    # stocks_list = ["000001.SZ", "000002.SZ"]
    # stocks_data = daily_manager.fetch_stocks_list_daily_data_period(stocks_list, "20250921", "20250926", save_to_mysql=True)
    # print(stocks_data)
