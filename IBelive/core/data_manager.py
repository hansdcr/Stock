import tushare
import os
import sys
import pandas as pd
import mysql.connector
from mysql.connector import Error
from typing import List, Optional

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

# 导入模型
from models.companies import Company
from models.daily_data import DailyData
from daily_data_manager import DailyDataManager

data_dir = os.path.join(os.path.dirname(THIS_DIR), "data")

class StockDataManager:
    
    def __init__(self, config_parser):
        self.config = config_parser
        self.pro = tushare.pro_api(self.config.get_token())
        self.data_dir = os.path.join(os.path.dirname(THIS_DIR), "data")
        
    def fetch_listed_companies(self, asof_date=None, fields=None, save_to_mysql=False):
        """
        获取指定日期的所有上市股票信息
        
        :param asof_date: 查询日期，格式YYYYMMDD，默认None表示最新数据
        :param fields: 要获取的字段列表，默认None表示所有字段
        :param save_to_mysql: 是否保存到MySQL数据库，默认False
        :return: pandas DataFrame 包含股票信息
        """
        # 使用 Company 类的默认字段
        default_fields = Company.DEFAULT_FIELDS
        
        # 合并用户指定字段和默认字段
        if fields is None:
            fields = default_fields
        else:
            fields = list(set(fields + default_fields))
        
        # 构建查询参数
        params = {
            "list_status": "L",  # 上市股票
            "fields": ",".join(fields)
        }
        
        if asof_date:
            params["list_date"] = asof_date
        
        # 执行查询
        df = self.pro.stock_basic(**params)
        
        # 保存数据到本地文件
        self.save_listed_companies(df, asof_date)
        
        # 保存数据到MySQL（如果启用）
        if save_to_mysql:
            self.save_listed_companies_to_mysql(df, asof_date=asof_date)
        
        return df

    def save_listed_companies(self, df, asof_date=None):
        """
        保存上市股票数据到本地文件
        
        :param df: 包含股票信息的DataFrame
        :param asof_date: 查询日期，用于文件名
        """
        
        # 构建文件名
        if asof_date:
            filename = f"{self.data_dir}/listed_companies_{asof_date}.csv"
        else:
            filename = f"{self.data_dir}/listed_companies_latest.csv"

        if not os.path.exists(filename):
            os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # 保存到CSV
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        
        print(f"✅ 上市股票数据已保存到 {filename}")

    def save_listed_companies_to_mysql(self, df, table_name="listed_companies", asof_date=None):
        """
        保存上市股票数据到MySQL数据库
        
        :param df: 包含股票信息的DataFrame
        :param table_name: 数据库表名，默认为listed_companies
        :param asof_date: 查询日期，用于数据版本标识
        """
        try:
            # 获取MySQL配置
            mysql_config = self.config.get_mysql_config()
            
            # 建立数据库连接
            connection = mysql.connector.connect(
                host=mysql_config['host'],
                port=mysql_config['port'],
                user=mysql_config['user'],
                password=mysql_config['password'],
                database=mysql_config['db']
            )
            
            if connection.is_connected():
                cursor = connection.cursor()
                
                # 创建表（如果不存在）
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20),
                    symbol VARCHAR(20),
                    name VARCHAR(100),
                    area VARCHAR(50),
                    industry VARCHAR(100),
                    fullname VARCHAR(200),
                    enname VARCHAR(200),
                    cnspell VARCHAR(100),
                    market VARCHAR(20),
                    exchange VARCHAR(20),
                    list_status VARCHAR(10),
                    list_date VARCHAR(8),
                    delist_date VARCHAR(8),
                    is_hs BOOLEAN,
                    is_st BOOLEAN,
                    asof_date VARCHAR(8),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_stock_date (ts_code, asof_date)
                )
                """
                cursor.execute(create_table_query)
                
                # 添加asof_date列到数据中
                df_with_date = df.copy()
                df_with_date['asof_date'] = asof_date if asof_date else pd.Timestamp.now().strftime('%Y%m%d')
                
                # 检查并添加缺失的字段，设置默认值
                expected_columns = ['ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname', 'cnspell', 
                                  'market', 'exchange', 'list_status', 'list_date', 'delist_date', 'is_hs', 'is_st']
                
                for col in expected_columns:
                    if col not in df_with_date.columns:
                        if col == 'is_hs' or col == 'is_st':
                            df_with_date[col] = 'N'  # 布尔字段默认'N'
                        elif col in ['fullname', 'enname', 'cnspell']:
                            df_with_date[col] = ''  # 字符串字段默认空字符串
                        else:
                            df_with_date[col] = None  # 其他字段默认None
                
                # 转换Tushare的字符串布尔值为MySQL兼容的布尔值
                if 'is_hs' in df_with_date.columns:
                    df_with_date['is_hs'] = df_with_date['is_hs'].apply(lambda x: True if x == 'S' else False)
                if 'is_st' in df_with_date.columns:
                    df_with_date['is_st'] = df_with_date['is_st'].apply(lambda x: True if x == 'S' else False)
                
                # 准备插入数据
                insert_query = f"""
                INSERT INTO {table_name} (ts_code, symbol, name, area, industry, fullname, enname, cnspell, 
                                        market, exchange, list_status, list_date, delist_date, is_hs, is_st, asof_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    symbol = VALUES(symbol),
                    name = VALUES(name),
                    area = VALUES(area),
                    industry = VALUES(industry),
                    fullname = VALUES(fullname),
                    enname = VALUES(enname),
                    cnspell = VALUES(cnspell),
                    market = VALUES(market),
                    exchange = VALUES(exchange),
                    list_status = VALUES(list_status),
                    list_date = VALUES(list_date),
                    delist_date = VALUES(delist_date),
                    is_hs = VALUES(is_hs),
                    is_st = VALUES(is_st),
                    updated_at = CURRENT_TIMESTAMP
                """
                
                # 转换DataFrame为元组列表
                data_tuples = [tuple(x) for x in df_with_date[expected_columns + ['asof_date']].to_numpy()]
                
                # 批量插入数据
                cursor.executemany(insert_query, data_tuples)
                connection.commit()
                
                print(f"✅ 成功保存 {len(data_tuples)} 条记录到MySQL表 {table_name}")
                
        except Error as e:
            print(f"❌ MySQL保存失败: {e}")
            if 'connection' in locals() and connection.is_connected():
                connection.rollback()
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()


if __name__ == "__main__":
    from parse_config import ParseConfig
    config = ParseConfig()
    manager = StockDataManager(config)
    df = manager.fetch_listed_companies("20250925",save_to_mysql=True)