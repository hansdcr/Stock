"""
指数日线数据管理类
负责处理指数日线数据的获取、保存和管理
"""
import os
import pandas as pd
import tushare
from typing import Dict, List, Optional
from ..models.index_daily import IndexDaily
from ..mysql_manager import MySQLManager


class IndexDailyManager:
    """指数日线数据管理类"""
    
    def __init__(self, config, pro):
        """
        初始化指数日线数据管理器
        
        :param config: 配置对象
        :param pro: Tushare Pro API对象
        """
        self.config = config
        self.pro = pro
        self.data_dir = config.get_data_dir()
        self.mysql_manager = MySQLManager(config)
    
    def fetch_index_daily_data(self, ts_code: str, trade_date: str, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取单个指数在指定日期的交易数据
        
        :param ts_code: 指数代码，格式如 '000001.SH'
        :param trade_date: 交易日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含指数交易数据，如果无数据返回None
        """
        try:
            # 使用 IndexDaily 类的默认字段
            default_fields = IndexDaily.DEFAULT_FIELDS
            
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
            
            # 执行查询 - 使用Tushare的index_daily接口
            df = self.pro.index_daily(**params)
            
            if df.empty:
                print(f"⚠️  未找到指数 {ts_code} 在 {trade_date} 的交易数据")
                return None
            
            print(f"✅ 成功获取指数 {ts_code} 在 {trade_date} 的交易数据")
            
            # 数据预处理
            df = self._preprocess_index_daily_data(df)
            
            return df
        except Exception as e:
            print(f"❌ 获取指数 {ts_code} 在 {trade_date} 的交易数据时发生错误: {e}")
            return None

    def get_index_daily_data_from_mysql(
        self, 
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        从MySQL数据库获取指数日线数据
        
        :param ts_codes: 指数代码列表，默认None表示所有指数
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含指数日线数据
        """
        try:
            # 构建条件字符串和参数
            conditions = []
            params = []
            
            # 添加指数代码过滤条件
            if ts_codes:
                placeholders = ",".join(["%s"] * len(ts_codes))
                conditions.append(f"ts_code IN ({placeholders})")
                params.extend(ts_codes)
            
            # 添加日期过滤条件
            if start_date:
                start_dt = pd.to_datetime(start_date, format='%Y%m%d')
                conditions.append("trade_date >= %s")
                params.append(start_dt.strftime('%Y-%m-%d'))
            
            if end_date:
                end_dt = pd.to_datetime(end_date, format='%Y%m%d')
                conditions.append("trade_date <= %s")
                params.append(end_dt.strftime('%Y-%m-%d'))
            
            # 构建条件字符串
            where_clause = " AND ".join(conditions) if conditions else None
            
            # 执行查询
            df = self.mysql_manager.query_data(
                table_name="index_daily_data",
                columns=fields,
                conditions=where_clause,
                params=params,
                order_by="ts_code, trade_date"
            )
            
            if df is None or df.empty:
                print("⚠️  从MySQL未找到符合条件的指数日线数据")
                return pd.DataFrame()
            
            # 如果返回的是默认列名，重命名为实际列名
            if fields and len(df.columns) == len(fields):
                df.columns = fields
            
            print(f"✅ 从MySQL成功获取 {len(df)} 条指数日线数据")
            return df
            
        except Exception as e:
            print(f"❌ 从MySQL获取指数日线数据失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def get_index_daily_data_by_trade_date_from_mysql(
        self, 
        trade_date: str,
        ts_codes: Optional[List[str]] = None,
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        从MySQL数据库获取指定交易日的指数日线数据
        
        :param trade_date: 交易日期，格式YYYYMMDD
        :param ts_codes: 指数代码列表，默认None表示所有指数
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含指定交易日的指数日线数据
        """
        try:
            # 转换日期格式
            trade_dt = pd.to_datetime(trade_date, format='%Y%m%d')
            
            # 构建条件字符串和参数
            conditions = ["trade_date = %s"]
            params = [trade_dt.strftime('%Y-%m-%d')]
            
            # 添加指数代码过滤条件
            if ts_codes:
                placeholders = ",".join(["%s"] * len(ts_codes))
                conditions.append(f"ts_code IN ({placeholders})")
                params.extend(ts_codes)
            
            # 构建条件字符串
            where_clause = " AND ".join(conditions) if conditions else None
            
            # 执行查询
            df = self.mysql_manager.query_data(
                table_name="index_daily_data",
                columns=fields,
                conditions=where_clause,
                params=params,
                order_by="ts_code"
            )
            
            if df is None or df.empty:
                print(f"⚠️  从MySQL未找到 {trade_date} 的指数日线数据")
                return pd.DataFrame()
            
            # 如果返回的是默认列名，重命名为实际列名
            if fields and len(df.columns) == len(fields):
                df.columns = fields
            
            print(f"✅ 从MySQL成功获取 {trade_date} 的 {len(df)} 条指数日线数据")
            return df
            
        except Exception as e:
            print(f"❌ 从MySQL获取 {trade_date} 的指数日线数据失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def fetch_all_index_daily_data_period(
        self, 
        start_date: str, 
        end_date: str, 
        ts_codes: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
        batch_size: int = 50
    ) -> pd.DataFrame:
        """
        获取指定时间段内所有指数的日线数据
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param ts_codes: 指数代码列表，默认None表示从MySQL获取所有指数代码
        :param fields: 要获取的字段列表，默认None表示所有字段
        :param batch_size: 批量处理大小
        :return: pandas DataFrame 包含期间所有指数的数据
        """
        try:
            # 如果没有提供指数代码列表，从MySQL获取所有指数代码
            if ts_codes is None:
                ts_codes = self._get_all_index_codes_from_mysql()
                if not ts_codes:
                    print("⚠️  未找到任何指数代码，无法获取数据")
                    return pd.DataFrame()
                print(f"📊 将从MySQL获取 {len(ts_codes)} 个指数的数据")
            
            all_data = []
            total_codes = len(ts_codes)
            
            for i, ts_code in enumerate(ts_codes, 1):
                print(f"📈 正在获取指数 {ts_code} 的数据 ({i}/{total_codes})...")
                
                # 获取单个指数的期间数据
                df_single = self.fetch_index_daily_data_period(ts_code, start_date, end_date, fields)
                
                if df_single is not None and not df_single.empty:
                    all_data.append(df_single)
                    print(f"✅ 成功获取指数 {ts_code} 的 {len(df_single)} 条数据")
                else:
                    print(f"⚠️  未找到指数 {ts_code} 在 {start_date} 到 {end_date} 的数据")
                
                # 批量处理控制
                if i % batch_size == 0:
                    print(f"🔄 已处理 {i}/{total_codes} 个指数")
            
            # 合并所有数据
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                print(f"✅ 总共获取 {len(combined_df)} 条指数日线数据，来自 {len(all_data)} 个指数")
                return combined_df
            else:
                print("⚠️  未获取到任何指数日线数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ 获取所有指数日线数据失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def _get_all_index_codes_from_mysql(self) -> List[str]:
        """
        从MySQL数据库获取所有指数代码
        
        :return: 指数代码列表
        """
        try:
            query = "SELECT DISTINCT ts_code FROM index_basic_data WHERE ts_code IS NOT NULL"
            result = self.mysql_manager.execute_query(query)
            
            if result:
                ts_codes = [row[0] for row in result]
                print(f"✅ 从MySQL获取到 {len(ts_codes)} 个指数代码")
                return ts_codes
            else:
                print("⚠️  MySQL中未找到指数代码，尝试从Tushare获取...")
                return self._get_all_index_codes_from_tushare()
                
        except Exception as e:
            print(f"❌ 从MySQL获取指数代码失败: {e}")
            return []

    def _get_all_index_codes_from_tushare(self) -> List[str]:
        """
        从Tushare获取所有指数代码
        
        :return: 指数代码列表
        """
        try:
            # 这里需要导入IndexBasicManager来获取指数代码
            # 由于循环导入问题，我们直接使用Tushare API
            df = self.pro.index_basic(fields="ts_code")
            
            if not df.empty:
                ts_codes = df['ts_code'].tolist()
                print(f"✅ 从Tushare获取到 {len(ts_codes)} 个指数代码")
                return ts_codes
            else:
                print("⚠️  Tushare中未找到指数代码")
                return []
                
        except Exception as e:
            print(f"❌ 从Tushare获取指数代码失败: {e}")
            return []
    
    def _preprocess_index_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理指数日线数据
        
        :param df: 原始指数日线数据DataFrame
        :return: 处理后的DataFrame
        """
        # 转换日期字段为datetime.date类型
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        
        # 转换其他数值字段为float类型
        for field in ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']:
            if field in df.columns:
                # 避免链式赋值警告，直接赋值
                df[field] = pd.to_numeric(df[field], errors='coerce')
                # 处理NaN值
                df[field] = df[field].fillna(0.0)
                # 处理inf值
                df[field] = df[field].replace([float('inf'), float('-inf')], 0.0)
        
        return df
    
    def save_index_daily_data_to_csv(self, df: pd.DataFrame, ts_code: str, trade_date: str) -> None:
        """
        保存单个指数交易数据到本地CSV文件
        
        :param df: 包含交易数据的DataFrame
        :param ts_code: 指数代码
        :param trade_date: 交易日期
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return
        
        # 构建文件名
        filename = f"{self.data_dir}/index_daily_{ts_code}_{trade_date}.csv"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # 保存到CSV
        df.to_csv(filename, mode='a', index=False, encoding="utf-8-sig")
        
        print(f"✅ 指数交易数据已保存到 {filename}")

    def _get_index_daily_table_queries(self, table_name: str, include_status_fields: bool = False) -> tuple:
        """
        公共方法：获取指数日线数据表的创建和插入查询语句
        
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
            UNIQUE KEY unique_index_date (ts_code, trade_date)
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
        expected_columns = IndexDaily.DEFAULT_FIELDS.copy()
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
    
    def fetch_and_save_index_daily_data(self, ts_code: str, trade_date: str, fields: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        获取并保存单个指数在指定日期的交易数据
        
        :param ts_code: 指数代码，格式如 '000001.SH'
        :param trade_date: 交易日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含指数交易数据，如果无数据返回None
        """
        df = self.fetch_index_daily_data(ts_code, trade_date, fields)
        if df is not None:
            # 添加数据状态标记
            df['data_status'] = '正常'
            df['status_reason'] = ''
            
            # 构建表名
            table_name = "index_daily_data"
            
            # 转换日期字段为datetime类型 (确保MySQL正确存储为datetime格式)
            df['trade_date'] = df['trade_date'].apply(
                lambda x: pd.to_datetime(x) if not pd.api.types.is_datetime64_any_dtype(x) else x
            )
            
            # 使用公共方法获取查询语句（包含状态字段）
            create_table_query, insert_query, expected_columns, fill_missing_defaults = \
                self._get_index_daily_table_queries(table_name, include_status_fields=True)
            
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

    def fetch_and_save_all_index_daily_data_period(
        self, 
        start_date: str, 
        end_date: str, 
        ts_codes: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
        batch_size: int = 50
    ) -> pd.DataFrame:
        """
        获取并保存指定时间段内所有指数的日线数据到MySQL
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param ts_codes: 指数代码列表，默认None表示从MySQL获取所有指数代码
        :param fields: 要获取的字段列表，默认None表示所有字段
        :param batch_size: 批量处理大小
        :return: pandas DataFrame 包含期间所有指数的数据
        """
        print(f"🚀 开始获取并保存 {start_date} 到 {end_date} 的所有指数日线数据...")
        
        # 获取所有指数数据
        df_all = self.fetch_all_index_daily_data_period(start_date, end_date, ts_codes, fields, batch_size)
        
        if df_all is not None and not df_all.empty:
            # 保存到MySQL
            success = self._save_index_daily_data_to_mysql(df_all, f"all_indexes_{start_date}_to_{end_date}")
            
            if success:
                print(f"✅ 成功保存所有指数在 {start_date} 到 {end_date} 的数据到MySQL，共 {len(df_all)} 条记录")
            else:
                print(f"⚠️  保存所有指数在 {start_date} 到 {end_date} 的数据到MySQL失败")
        else:
            print("⚠️  未获取到任何指数数据，无需保存")
        
        return df_all

    def fetch_index_daily_data_by_trade_date(
        self, 
        trade_date: str, 
        ts_codes: Optional[List[str]] = None,
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取指定交易日期所有指数的数据
        
        :param trade_date: 交易日期，格式YYYYMMDD
        :param ts_codes: 指数代码列表，默认None表示所有指数
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含指定交易日所有指数的数据
        """
        try:
            # 构建查询参数
            params = {
                "trade_date": trade_date,
                "fields": fields if fields else IndexDaily.DEFAULT_FIELDS
            }
            
            # 如果指定了指数代码列表，添加到参数中
            if ts_codes:
                params["ts_code"] = ",".join(ts_codes)
            
            # 执行查询
            df = self.pro.index_daily(**params)
            
            if df.empty:
                print(f"⚠️  未找到 {trade_date} 的指数交易数据")
                return pd.DataFrame()
            
            print(f"✅ 成功获取 {trade_date} 的指数交易数据，共 {len(df)} 条记录")
            
            # 数据预处理
            df = self._preprocess_index_daily_data(df)
            
            # 添加数据状态标记
            df['data_status'] = '正常'
            df['status_reason'] = ''
            
            return df
            
        except Exception as e:
            print(f"❌ 获取 {trade_date} 指数交易数据失败: {e}")
            return pd.DataFrame()

    def fetch_and_save_index_daily_data_by_trade_date(
        self, 
        trade_date: str, 
        ts_codes: Optional[List[str]] = None,
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取并保存指定交易日期所有指数的数据到MySQL
        
        :param trade_date: 交易日期，格式YYYYMMDD
        :param ts_codes: 指数代码列表，默认None表示所有指数
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含指定交易日所有指数的数据
        """
        # 获取当日数据
        df = self.fetch_index_daily_data_by_trade_date(trade_date, ts_codes, fields)
        
        if df is not None and not df.empty:
            # 保存到MySQL
            self._save_index_daily_data_to_mysql(df, trade_date)
        
        return df

    def _save_index_daily_data_to_mysql(self, df: pd.DataFrame, trade_date: str) -> bool:
        """
        保存单日指数数据到MySQL
        
        :param df: 包含指数数据的DataFrame
        :param trade_date: 交易日期
        :return: 是否保存成功
        """
        if df.empty:
            print(f"⚠️  无数据可保存到MySQL，交易日期: {trade_date}")
            return False
        
        # 构建表名
        table_name = "index_daily_data"
        
        # 使用公共方法获取查询语句（包含状态字段）
        create_table_query, insert_query, expected_columns, fill_missing_defaults = \
            self._get_index_daily_table_queries(table_name, include_status_fields=True)
        
        # 首先确保表已创建
        table_created = self.mysql_manager.create_table_if_not_exists(table_name, create_table_query)
        
        if not table_created:
            print(f"❌ 创建表 {table_name} 失败")
            return False
        
        # 转换日期字段为datetime类型 (确保MySQL正确存储为datetime格式)
        df['trade_date'] = df['trade_date'].apply(
            lambda x: pd.to_datetime(x) if not pd.api.types.is_datetime64_any_dtype(x) else x
        )
        
        # 使用MySQL管理器保存数据
        success = self.mysql_manager.save_dataframe_to_table(
            df=df,
            table_name=table_name,
            insert_query=insert_query,
            expected_columns=expected_columns,
            fill_missing_defaults=fill_missing_defaults
        )

        if success:
            print(f"✅ 成功保存 {trade_date} 的指数数据到MySQL表 {table_name}，共 {len(df)} 条记录")
        else:
            print(f"⚠️  保存 {trade_date} 的指数数据到MySQL表 {table_name} 失败")
        
        return success

    def fetch_index_daily_data_period(
        self, 
        ts_code: str, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取指定时间段内单个指数的日线数据
        
        :param ts_code: 指数代码，格式如 '000001.SH'
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含期间所有日期的指数数据
        """
        try:
            # 构建查询参数
            params = {
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date,
                "fields": fields if fields else IndexDaily.DEFAULT_FIELDS
            }
            
            # 执行查询
            df = self.pro.index_daily(**params)
            
            if df.empty:
                print(f"⚠️  未找到指数 {ts_code} 在 {start_date} 到 {end_date} 的交易数据")
                return pd.DataFrame()
            
            print(f"✅ 成功获取指数 {ts_code} 在 {start_date} 到 {end_date} 的交易数据，共 {len(df)} 条记录")
            
            # 数据预处理
            df = self._preprocess_index_daily_data(df)
            
            # 添加数据状态标记
            df['data_status'] = '正常'
            df['status_reason'] = ''
            
            return df
            
        except Exception as e:
            print(f"❌ 获取指数 {ts_code} 在 {start_date} 到 {end_date} 的交易数据失败: {e}")
            return pd.DataFrame()

    def fetch_and_save_index_daily_data_period(
        self, 
        ts_code: str, 
        start_date: str, 
        end_date: str, 
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取并保存指定时间段内单个指数的日线数据到MySQL
        
        :param ts_code: 指数代码，格式如 '000001.SH'
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        :param fields: 要获取的字段列表，默认None表示所有字段
        :return: pandas DataFrame 包含期间所有日期的指数数据
        """
        # 获取期间数据
        df = self.fetch_index_daily_data_period(ts_code, start_date, end_date, fields)
        
        if df is not None and not df.empty:
            # 保存到MySQL
            success = self._save_index_daily_data_to_mysql(df, f"{start_date}_to_{end_date}")
            
            if success:
                print(f"✅ 成功保存指数 {ts_code} 在 {start_date} 到 {end_date} 的数据到MySQL")
            else:
                print(f"⚠️  保存指数 {ts_code} 在 {start_date} 到 {end_date} 的数据到MySQL失败")
        
        return df
