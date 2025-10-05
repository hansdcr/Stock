"""
周线数据管理类
负责处理股票周线数据的获取、保存和管理
参考Tushare周线数据接口：https://tushare.pro/document/2?doc_id=144
"""
import os
import pandas as pd
import tushare
from typing import Dict, List, Optional
from ..models.weekly_data import WeeklyData
from ..mysql_manager import MySQLManager
from ..company_manager import CompanyManager


class WeeklyDataManager:
    """周线数据管理类"""
    
    def __init__(self, config, pro):
        """
        初始化周线数据管理器
        
        :param config: 配置对象
        :param pro: Tushare Pro API对象
        """
        self.config = config
        self.pro = pro
        self.data_dir = config.get_data_dir()
        self.mysql_manager = MySQLManager(config)
        self.company_manager = CompanyManager(config)
    
    def fetch_weekly_data(self, ts_code: str, trade_date: str = None, start_date: str = None, 
                         end_date: str = None, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取单只股票的周线交易数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param trade_date: 交易日期，格式YYYYMMDD（可选）
        :param start_date: 开始日期，格式YYYYMMDD（可选）
        :param end_date: 结束日期，格式YYYYMMDD（可选）
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含股票周线数据，如果无数据返回None
        """
        try:
            # 使用 WeeklyData 类的默认字段
            default_fields = WeeklyData.DEFAULT_FIELDS
            
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
            
            # 执行查询（使用weekly接口）
            df = self.pro.weekly(**params)
            
            if df.empty:
                print(f"⚠️  未找到股票 {ts_code} 的周线数据")
                return None
            
            print(f"✅ 成功获取股票 {ts_code} 的周线数据，共 {len(df)} 条记录")
            
            # 数据预处理
            df = self._preprocess_weekly_data(df)
            
            return df
            
        except Exception as e:
            print(f"❌ 获取股票周线数据失败: {e}")
            return None
    
    def _preprocess_weekly_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理周线数据
        
        :param df: 原始周线数据DataFrame
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
    
    def save_weekly_data_to_csv(self, df: pd.DataFrame, ts_code: str) -> None:
        """
        保存单只股票周线数据到本地CSV文件
        
        :param df: 包含周线数据的DataFrame
        :param ts_code: 股票代码
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return
        
        # 构建文件名
        filename = f"{self.data_dir}/weekly_{ts_code}.csv"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # 保存到CSV
        df.to_csv(filename, mode='a', index=False, encoding="utf-8-sig")
        
        print(f"✅ 股票周线数据已保存到 {filename}")

    def _get_weekly_data_table_queries(self, table_name: str, include_status_fields: bool = False) -> tuple:
        """
        公共方法：获取周线数据表的创建和插入查询语句
        
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
            UNIQUE KEY unique_stock_week (ts_code, trade_date)
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
        expected_columns = WeeklyData.DEFAULT_FIELDS.copy()
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
    
    def fetch_and_save_weekly_data(self, ts_code: str, trade_date: str = None, 
                                 start_date: str = None, end_date: str = None, 
                                 fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取并保存单只股票的周线交易数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param trade_date: 交易日期，格式YYYYMMDD（可选）
        :param start_date: 开始日期，格式YYYYMMDD（可选）
        :param end_date: 结束日期，格式YYYYMMDD（可选）
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含股票周线数据，如果无数据返回None
        """
        df = self.fetch_weekly_data(ts_code, trade_date, start_date, end_date, fields)
        if df is not None:
            # 添加数据状态标记
            df['data_status'] = '正常'
            df['status_reason'] = ''
            
            # 构建表名
            table_name = "weekly_data"
            
            # 转换日期字段为datetime类型 (确保MySQL正确存储为datetime格式)
            if 'trade_date' in df.columns:
                df['trade_date'] = df['trade_date'].apply(
                    lambda x: pd.to_datetime(x) if not pd.api.types.is_datetime64_any_dtype(x) else x
                )
            
            # 使用公共方法获取查询语句（包含状态字段）
            create_table_query, insert_query, expected_columns, fill_missing_defaults = \
                self._get_weekly_data_table_queries(table_name, include_status_fields=True)
            
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
                print(f"✅ 成功保存 {ts_code} 的周线数据到MySQL表 {table_name}")
            else:
                print(f"⚠️  保存 {ts_code} 的周线数据到MySQL表 {table_name} 失败")
        
        return df

    def fetch_weekly_data_period(
        self, 
        ts_code: str, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取指定时间段内的股票周线数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含期间所有周线的股票数据
        """
        try:
            # 直接调用周线数据接口，Tushare周线接口支持日期范围查询
            df = self.fetch_weekly_data(ts_code, None, start_date, end_date, fields)
            
            if df is None or df.empty:
                print(f"⚠️  未找到股票 {ts_code} 在 {start_date} 到 {end_date} 期间的周线数据")
                return pd.DataFrame()
            
            print(f"✅ 成功获取股票 {ts_code} 在 {start_date} 到 {end_date} 期间的周线数据，共 {len(df)} 条记录")
            
            return df
            
        except Exception as e:
            print(f"❌ 获取股票周线数据失败: {e}")
            return pd.DataFrame()

    def fetch_and_save_weekly_data_period(
        self, 
        ts_code: str, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取并保存指定时间段内的股票周线数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含期间所有周线的股票数据
        """
        df = self.fetch_weekly_data_period(ts_code, start_date, end_date, fields)
        
        if not df.empty:
            # 保存到MySQL
            self.fetch_and_save_weekly_data(ts_code, None, start_date, end_date, fields)
            
            # 保存到CSV
            self.save_weekly_data_to_csv(df, ts_code)
        
        return df

    def fetch_weekly_data_by_trade_date(self, trade_date: str, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取指定交易日期所有股票的周线数据
        
        :param trade_date: 交易日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含所有股票的周线数据
        """
        try:
            # 使用 WeeklyData 类的默认字段
            default_fields = WeeklyData.DEFAULT_FIELDS
            
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
            
            # 执行查询（使用weekly接口）
            df = self.pro.weekly(**params)
            
            if df.empty:
                print(f"⚠️  未找到 {trade_date} 的周线数据")
                return None
            
            print(f"✅ 成功获取 {trade_date} 的周线数据，共 {len(df)} 条记录")
            
            # 数据预处理
            df = self._preprocess_weekly_data(df)
            
            return df
            
        except Exception as e:
            print(f"❌ 获取周线数据失败: {e}")
            return None

    def _save_weekly_data_to_mysql(self, df: pd.DataFrame, table_name: str = "weekly_data") -> bool:
        """
        保存周线数据到MySQL数据库
        
        :param df: 包含周线数据的DataFrame
        :param table_name: 数据库表名
        :return: 保存是否成功
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return False
        
        # 使用公共方法获取查询语句（包含状态字段）
        create_table_query, insert_query, expected_columns, fill_missing_defaults = \
            self._get_weekly_data_table_queries(table_name, include_status_fields=True)
        
        # 首先确保表已创建
        table_created = self.mysql_manager.create_table_if_not_exists(table_name, create_table_query)
        
        if not table_created:
            print(f"❌ 创建表 {table_name} 失败")
            return False
        
        # 使用MySQL管理器保存数据
        success = self.mysql_manager.save_dataframe_to_table(
            df=df,
            table_name=table_name,
            insert_query=insert_query,
            expected_columns=expected_columns,
            fill_missing_defaults=fill_missing_defaults
        )

        if success:
            print(f"✅ 成功保存周线数据到MySQL表 {table_name}")
        else:
            print(f"⚠️  保存周线数据到MySQL表 {table_name} 失败")
        
        return success

    def fetch_and_save_weekly_data_period_incremental(
        self, 
        ts_code: str, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        增量获取并保存指定时间段内的股票周线数据
        
        :param ts_code: 股票代码，格式如 '000001.SZ'
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含期间所有周线的股票数据
        """
        try:
            # 首先检查数据库中已有的数据
            existing_data_query = f"""
                SELECT MAX(trade_date) as last_date 
                FROM weekly_data 
                WHERE ts_code = '{ts_code}'
            """
            
            existing_data = self.mysql_manager.execute_query(existing_data_query)
            
            # 如果已有数据，从最后日期开始增量获取
            if existing_data and not existing_data.empty and existing_data.iloc[0]['last_date'] is not None:
                last_date = existing_data.iloc[0]['last_date'].strftime('%Y%m%d')
                print(f"📅 发现已有数据，最后日期: {last_date}")
                
                # 如果最后日期早于结束日期，增量获取
                if last_date < end_date:
                    incremental_start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime('%Y%m%d')
                    print(f"🔄 增量获取从 {incremental_start_date} 到 {end_date}")
                    
                    df = self.fetch_weekly_data_period(ts_code, incremental_start_date, end_date, fields)
                    
                    if not df.empty:
                        # 保存增量数据
                        self._save_weekly_data_to_mysql(df)
                        self.save_weekly_data_to_csv(df, ts_code)
                        
                    return df
                else:
                    print("✅ 数据已是最新，无需增量获取")
                    return pd.DataFrame()
            else:
                # 没有现有数据，全量获取
                print("🔄 无现有数据，进行全量获取")
                df = self.fetch_weekly_data_period(ts_code, start_date, end_date, fields)
                
                if not df.empty:
                    # 保存数据
                    self._save_weekly_data_to_mysql(df)
                    self.save_weekly_data_to_csv(df, ts_code)
                    
                return df
                
        except Exception as e:
            print(f"❌ 增量获取周线数据失败: {e}")
            return pd.DataFrame()

    def fetch_all_stocks_weekly_data_period(
        self, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None,
        save_to_mysql: bool = True,
        batch_size: int = 50
    ) -> Dict[str, pd.DataFrame]:
        """
        获取从开始日期到结束日期的所有股票的周线数据（Tushare官方推荐方式）
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :param save_to_mysql: 是否保存到MySQL数据库，默认True
        :param batch_size: 批次大小，默认50条记录
        :return: 字典，键为股票代码，值为包含期间所有周线的股票数据的DataFrame
        """
        try:
            print(f"🚀 开始获取所有股票从 {start_date} 到 {end_date} 的周线数据（官方推荐方式）...")
            
            # 获取周线日期（每周五为周线数据日期）
            weekly_dates = self._get_weekly_dates(start_date, end_date)
            
            if not weekly_dates:
                print(f"⚠️  未找到 {start_date} 到 {end_date} 期间的周线日期")
                return {}
            
            print(f"📅 找到 {len(weekly_dates)} 个周线日期: {weekly_dates[0]} 到 {weekly_dates[-1]}")
            
            # 存储所有股票的数据（按股票代码组织）
            all_stocks_data = {}
            
            # 按周线日期循环（Tushare官方推荐方式）
            for i, weekly_date in enumerate(weekly_dates, 1):
                try:
                    print(f"\n📊 正在处理第 {i}/{len(weekly_dates)} 个周线日期: {weekly_date}")
                    
                    # 获取当日所有股票周线数据
                    weekly_df = self.fetch_weekly_data_by_trade_date(weekly_date, fields)
                    
                    if weekly_df is not None and not weekly_df.empty:
                        # 添加数据状态标记
                        weekly_df['data_status'] = '正常'
                        weekly_df['status_reason'] = ''
                        
                        # 保存到MySQL（如果启用）
                        if save_to_mysql:
                            save_success = self._save_weekly_data_to_mysql(weekly_df)
                            if save_success:
                                print(f"✅ 成功保存 {weekly_date} 的周线数据到MySQL，共 {len(weekly_df)} 条记录")
                            else:
                                print(f"⚠️  保存 {weekly_date} 的周线数据到MySQL失败")
                        
                        # 按股票代码组织数据
                        for _, row in weekly_df.iterrows():
                            ts_code = row['ts_code']
                            if ts_code not in all_stocks_data:
                                all_stocks_data[ts_code] = []
                            all_stocks_data[ts_code].append(row)
                        
                        print(f"✅ 成功处理 {weekly_date} 的数据，共 {len(weekly_df)} 条记录")
                    else:
                        print(f"⚠️  未获取到 {weekly_date} 的周线数据")
                        
                except Exception as e:
                    print(f"❌ 处理周线日期 {weekly_date} 时发生错误: {e}")
                    continue
            
            # 将每个股票的数据列表转换为DataFrame
            result_data = {}
            for ts_code, data_list in all_stocks_data.items():
                if data_list:
                    result_data[ts_code] = pd.DataFrame(data_list)
            
            print(f"\n🎉 完成! 成功获取 {len(result_data)} 只股票的周线数据")
            
            # # 保存合并后的数据到CSV
            # if result_data:
            #     combined_df = pd.concat(result_data.values(), ignore_index=True)
            #     combined_filename = f"{self.data_dir}/weekly_all_stocks_{start_date}_{end_date}.csv"
            #     combined_df.to_csv(combined_filename, index=False, encoding="utf-8-sig")
            #     print(f"💾 合并数据已保存到 {combined_filename}")
            
            return result_data
            
        except Exception as e:
            print(f"❌ 获取所有股票周线数据失败: {e}")
            return {}
    
    def _get_weekly_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        获取指定时间段内的周线日期（每周五）
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :return: 周线日期列表（格式YYYYMMDD）
        """
        try:
            # 获取交易日历
            trade_cal = self.pro.trade_cal(
                exchange='', 
                start_date=start_date, 
                end_date=end_date,
                fields=['cal_date', 'is_open', 'pretrade_date']
            )
            
            if trade_cal.empty:
                print(f"⚠️  未找到 {start_date} 到 {end_date} 的交易日历")
                return []
            
            # 筛选交易日（只获取交易日）
            trade_dates = trade_cal[trade_cal['is_open'] == 1]['cal_date'].tolist()
            
            if not trade_dates:
                print(f"⚠️  未找到 {start_date} 到 {end_date} 的交易日")
                return []
            
            # 由于Tushare没有提供day_of_week字段，我们手动计算周五
            # 将日期转换为datetime对象，找出周五
            weekly_dates = []
            for date_str in trade_dates:
                try:
                    date_obj = pd.to_datetime(date_str, format='%Y%m%d')
                    if date_obj.weekday() == 4:  # 周五（0=周一, 4=周五）
                        weekly_dates.append(date_str)
                except:
                    continue
            
            # 如果没有找到周五数据，使用所有交易日作为周线日期（兼容性处理）
            if not weekly_dates:
                weekly_dates = trade_dates
                print(f"⚠️  未找到周五交易日，使用所有 {len(weekly_dates)} 个交易日作为周线日期")
            else:
                print(f"✅ 找到 {len(weekly_dates)} 个周五交易日")
            
            return weekly_dates
            
        except Exception as e:
            print(f"❌ 获取周线日期失败: {e}")
            return []


# 获取从20250101到20250930的所有股票周线数据并保存到数据库
if __name__ == "__main__":
    import tushare as ts
    from parse_config import ParseConfig
    config = ParseConfig()

    pro = ts.pro_api(config.get_token())
    weekly_manager = WeeklyDataManager(config, pro)
    
    # 示例：获取所有股票从20250919到20250930的周线数据
    weekly_manager.fetch_all_stocks_weekly_data_period("20250101", "20250930")


