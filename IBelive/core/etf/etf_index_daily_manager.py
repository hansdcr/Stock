"""
ETF指数日线数据管理类
- 使用Tushare index_daily接口，按交易日循环（官方推荐方式）获取数据
- 支持按开始/结束日期获取多个指数在区间内的所有交易日数据
- 每50条数据批量保存一次到MySQL，降低终端中断导致的数据丢失风险

注意：index_daily接口需要传入具体指数代码(ts_code)，若未提供指数代码列表，将尝试通过index_basic获取全部指数列表。
"""
import os
import sys
from typing import List, Optional, Tuple, Dict, Any

import pandas as pd
import tushare

# 添加必要的路径到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
core_dir = os.path.dirname(current_dir)  # /Users/gelin/Desktop/store/dev/python/20250926stock/StockIBelive/IBelive/core
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

# 将项目根目录添加到sys.path
project_root = os.path.dirname(os.path.dirname(core_dir))  # /Users/gelin/Desktop/store/dev/python/20250926stock/StockIBelive
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# 现在可以安全导入所有模块
from mysql_manager import MySQLManager
from models.etf_index_daily import ETFIndexDaily
from parse_config import ParseConfig
import tushare


class ETFIndexDailyManager:
    """ETF指数日线数据管理类"""

    DEFAULT_FIELDS = ETFIndexDaily.DEFAULT_FIELDS

    def __init__(self, config, pro: tushare.pro):
        """
        :param config: 配置对象，需提供get_data_dir()和get_token()/get_mysql_config()
        :param pro: Tushare Pro接口对象
        """
        self.config = config
        self.pro = pro
        self.data_dir = config.get_data_dir()
        self.mysql_manager = MySQLManager(config)

    def _dataframe_to_etf_index_daily_objects(self, df: pd.DataFrame) -> List[ETFIndexDaily]:
        """将DataFrame转换为ETFIndexDaily对象列表"""
        etf_objects = []
        for _, row in df.iterrows():
            # 将行数据转换为字典
            row_dict = row.to_dict()
            # 使用模型的from_dict方法创建对象
            etf_obj = ETFIndexDaily.from_dict(row_dict)
            etf_objects.append(etf_obj)
        return etf_objects

    def _save_etf_index_daily_objects_to_mysql(self, etf_objects: List[ETFIndexDaily], table_name: str = 'etf_index_daily') -> bool:
        """保存ETFIndexDaily对象列表到MySQL"""
        if not etf_objects:
            return False
        
        # 将对象列表转换为DataFrame
        data_dicts = []
        for obj in etf_objects:
            obj_dict = {
                'ts_code': obj.ts_code,
                'trade_date': obj.trade_date,
                'open': obj.open,
                'high': obj.high,
                'low': obj.low,
                'close': obj.close,
                'pre_close': obj.pre_close,
                'change': obj.change,
                'pct_chg': obj.pct_chg,
                'vol': obj.vol,
                'amount': obj.amount,
                'data_status': '正常',
                'status_reason': ''
            }
            data_dicts.append(obj_dict)
        
        df = pd.DataFrame(data_dicts)
        return self._save_index_daily_to_mysql(df, table_name)

    def _preprocess_index_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        """预处理index_daily数据"""
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
        for field in ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0.0)
                df[field] = df[field].replace([float('inf'), float('-inf')], 0.0)
        return df

    def _get_index_daily_table_queries(self, table_name: str, include_status_fields: bool = True) -> Tuple[str, str, List[str], Dict[str, Any]]:
        """构建表创建与插入SQL"""
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
            open FLOAT,
            high FLOAT,
            low FLOAT,
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
        status_fields_insert = status_fields_values = status_fields_update = ""
        if include_status_fields:
            status_fields_insert = "data_status, status_reason,"
            status_fields_values = "%s, %s,"
            status_fields_update = """
            data_status = VALUES(data_status),
            status_reason = VALUES(status_reason),
            """
        insert_query = f"""
        INSERT INTO {table_name} (ts_code, trade_date, close, open, high, low, pre_close,
                                  `change`, pct_chg, vol, amount, {status_fields_insert}
                                  created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {status_fields_values} CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            close = VALUES(close),
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            pre_close = VALUES(pre_close),
            `change` = VALUES(`change`),
            pct_chg = VALUES(pct_chg),
            vol = VALUES(vol),
            amount = VALUES(amount),
            {status_fields_update}
            updated_at = CURRENT_TIMESTAMP
        """
        expected_columns = self.DEFAULT_FIELDS.copy()
        fill_missing_defaults = {
            'close': 0.0, 'open': 0.0, 'high': 0.0, 'low': 0.0,
            'pre_close': 0.0, 'change': 0.0, 'pct_chg': 0.0, 'vol': 0.0, 'amount': 0.0,
            'data_status': '', 'status_reason': ''
        }
        if include_status_fields:
            expected_columns.extend(['data_status', 'status_reason'])
        return create_table_query, insert_query, expected_columns, fill_missing_defaults

    def get_all_index_codes(self) -> List[str]:
        """尝试通过index_basic获取全部指数ts_code列表"""
        try:
            df = self.pro.index_basic()  # 获取所有指数基础信息
            if df is not None and not df.empty and 'ts_code' in df.columns:
                return df['ts_code'].dropna().unique().tolist()
        except Exception as e:
            print(f"⚠️ 获取指数列表失败: {e}")
        return []

    def fetch_index_daily_by_trade_date(self, trade_date: str, index_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取指定交易日的指数日线数据（按指数代码循环）
        :param trade_date: 交易日 YYYYMMDD
        :param index_codes: 指数代码列表，若为空则尝试获取全部指数
        """
        if not index_codes:
            index_codes = self.get_all_index_codes()
            if not index_codes:
                print(f"⚠️ 未提供指数代码且无法自动获取指数列表，trade_date={trade_date}")
                return pd.DataFrame()
        all_rows: List[pd.DataFrame] = []
        for code in index_codes:
            try:
                df = self.pro.index_daily(ts_code=code, trade_date=trade_date)
                if df is not None and not df.empty:
                    df = self._preprocess_index_daily(df)
                    df['data_status'] = '正常'
                    df['status_reason'] = ''
                    all_rows.append(df)
                else:
                    # 生成占位行（无数据）
                    empty = pd.DataFrame([{**{k: None for k in self.DEFAULT_FIELDS}, 'ts_code': code, 'trade_date': trade_date}])
                    empty = self._preprocess_index_daily(empty)
                    empty['data_status'] = '停盘'
                    empty['status_reason'] = '当日无该指数数据'
                    all_rows.append(empty)
            except Exception as e:
                print(f"❌ 指数 {code} 在 {trade_date} 获取失败: {e}")
                error = pd.DataFrame([{**{k: None for k in self.DEFAULT_FIELDS}, 'ts_code': code, 'trade_date': trade_date}])
                error = self._preprocess_index_daily(error)
                error['data_status'] = '错误'
                error['status_reason'] = f'数据获取失败: {str(e)}'
                all_rows.append(error)
        if all_rows:
            return pd.concat(all_rows, ignore_index=True)
        return pd.DataFrame()

    def _save_index_daily_to_mysql(self, df: pd.DataFrame, table_name: str = 'etf_index_daily') -> bool:
        """保存指数数据到MySQL"""
        if df is None or df.empty:
            return False
        create_table_query, insert_query, expected_columns, fill_missing_defaults = self._get_index_daily_table_queries(table_name, include_status_fields=True)
        table_created = self.mysql_manager.create_table_if_not_exists(table_name, create_table_query)
        if not table_created:
            print(f"❌ 创建表 {table_name} 失败")
            return False
        # 确保trade_date为datetime
        df['trade_date'] = df['trade_date'].apply(lambda x: pd.to_datetime(x) if not pd.api.types.is_datetime64_any_dtype(x) else x)
        return self.mysql_manager.save_dataframe_to_table(
            df=df,
            table_name=table_name,
            insert_query=insert_query,
            expected_columns=expected_columns,
            fill_missing_defaults=fill_missing_defaults
        )

    def fetch_and_save_index_daily_by_trade_date(self, trade_date: str, index_codes: Optional[List[str]] = None, batch_size: int = 50) -> pd.DataFrame:
        """获取并按批次保存指定交易日的指数数据"""
        df = self.fetch_index_daily_by_trade_date(trade_date, index_codes)
        if df.empty:
            print(f"⚠️ {trade_date} 无可保存指数数据")
            return df
        # 批处理保存
        total = len(df)
        saved_total = 0
        for i in range(0, total, batch_size):
            batch_df = df.iloc[i:i + batch_size]
            ok = self._save_index_daily_to_mysql(batch_df)
            if ok:
                saved_total += len(batch_df)
                print(f"✅ {trade_date} 批量保存 {len(batch_df)}/{total}，累计保存 {saved_total}")
            else:
                print(f"⚠️ {trade_date} 批量保存失败：区间 {i}-{i + len(batch_df)}")
        return df

    def fetch_and_save_index_daily_period(self, start_date: str, end_date: str, index_codes: Optional[List[str]] = None, batch_size: int = 50) -> pd.DataFrame:
        """
        获取并保存区间内所有交易日的指数日线数据：
        - 按交易日循环（官方推荐）
        - 每50条数据保存一次
        - 支持指定指数列表，未指定时尝试自动获取全部指数
        """
        try:
            trade_cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, fields=['cal_date', 'is_open'])
            if trade_cal.empty:
                print(f"⚠️ 未找到 {start_date}-{end_date} 的交易日历")
                return pd.DataFrame()
            trading_days = trade_cal[trade_cal['is_open'] == 1]['cal_date'].tolist()
            if not trading_days:
                print(f"⚠️ 区间 {start_date}-{end_date} 无交易日")
                return pd.DataFrame()
            print(f"📅 区间交易日数量: {len(trading_days)} 从 {start_date} 到 {end_date}")
            buffer: List[pd.DataFrame] = []
            saved_total = 0
            all_data: List[pd.DataFrame] = []
            for trade_date in trading_days:
                daily_df = self.fetch_index_daily_by_trade_date(trade_date, index_codes)
                if daily_df is not None and not daily_df.empty:
                    buffer.append(daily_df)
                    all_data.append(daily_df)
                    # 够批次则保存
                    curr_count = sum(len(x) for x in buffer)
                    if curr_count >= batch_size:
                        to_save = pd.concat(buffer, ignore_index=True)
                        ok = self._save_index_daily_to_mysql(to_save)
                        if ok:
                            saved_total += len(to_save)
                            print(f"✅ 累计保存 {saved_total} 条记录")
                        else:
                            print("⚠️ 批量保存失败（period）")
                        buffer.clear()
                else:
                    print(f"⚠️ {trade_date} 无指数数据")
            # 保存剩余缓冲
            if buffer:
                to_save = pd.concat(buffer, ignore_index=True)
                ok = self._save_index_daily_to_mysql(to_save)
                if ok:
                    saved_total += len(to_save)
                    print(f"✅ 区间收尾保存 {len(to_save)} 条，累计保存 {saved_total}")
                else:
                    print("⚠️ 收尾批量保存失败")
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ 区间获取保存失败: {e}")
            return pd.DataFrame()


def main():
    """主测试函数"""
    try:
        print("🚀 开始测试ETF指数日线数据管理器...")
        
        # 初始化配置和Tushare
        config = ParseConfig()
        pro = tushare.pro_api(config.get_token())
        
        # 创建管理器实例
        manager = ETFIndexDailyManager(config, pro)
        
        print("✅ 管理器初始化成功")
        
        # 测试获取指数代码列表
        index_codes = manager.get_all_index_codes()
        print(f"📊 获取到 {len(index_codes)} 个指数代码")
        if index_codes:
            print(f"📋 前5个指数代码: {index_codes[:5]}")
        
        # 测试获取单日数据（使用少量指数代码进行测试）
        # test_codes = index_codes[:3] if index_codes else ['000001.SH', '399001.SZ']
        test_codes = ['510300.SH', '159949.SZ','588000.SH','513180.SH']
        print(f"🧪 测试指数代码: {test_codes}")
        
        # # 获取测试日期的数据
        # test_date = '20250927'
        # df = manager.fetch_index_daily_by_trade_date(test_date, test_codes)
        
        # if not df.empty:
        #     print(f"✅ 成功获取 {test_date} 的指数数据")
        #     print(f"📈 数据形状: {df.shape}")
        #     print(f"📊 数据预览:")
        #     print(df.head())
            
        #     # 测试保存到MySQL
        #     success = manager._save_index_daily_to_mysql(df)
        #     if success:
        #         print("✅ 数据成功保存到MySQL")
        #     else:
        #         print("⚠️  数据保存到MySQL失败")
        # else:
        #     print(f"⚠️  未获取到 {test_date} 的指数数据")
            
        # 测试区间数据获取
        print("\n🧪 测试区间数据获取...")
        result_df = manager.fetch_and_save_index_daily_period(
            start_date='20250101', 
            end_date='20251001', 
            index_codes=test_codes,
            batch_size=10
        )
        
        if not result_df.empty:
            print(f"✅ 区间数据获取成功，共 {len(result_df)} 条记录")
        else:
            print("⚠️  区间数据获取失败或没有数据")
            
    except Exception as e:
        print(f"❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()