"""
MySQL数据库管理工具类
提供通用的MySQL数据库连接、表创建、数据插入等操作
可以被其他数据管理类复用
"""
import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Optional
import pandas as pd


class MySQLManager:
    """MySQL数据库管理工具类"""
    
    def __init__(self, config):
        """
        初始化MySQL管理器
        
        :param config: 配置对象，需要包含get_mysql_config()方法
        """
        self.config = config
        self.mysql_config = config.get_mysql_config()
        self.connection = None
    
    def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.connection = mysql.connector.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                database=self.mysql_config['db']
            )
            return self.connection.is_connected()
        except Error as e:
            print(f"❌ MySQL连接失败: {e}")
            return False
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
    
    def execute_query(self, query: str, params: Optional[list] = None) -> Optional[list]:
        """执行查询语句并返回结果"""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            result = cursor.fetchall()
            cursor.close()
            return result
            
        except Error as e:
            print(f"❌ 查询执行失败: {e}")
            return None
    
    def execute_many(self, query: str, data: list) -> bool:
        """批量执行插入/更新语句"""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            
            cursor = self.connection.cursor()
            cursor.executemany(query, data)
            self.connection.commit()
            cursor.close()
            return True
            
        except Error as e:
            print(f"❌ 批量执行失败: {e}")
            if self.connection and self.connection.is_connected():
                self.connection.rollback()
            return False
    
    def create_table_if_not_exists(self, table_name: str, create_table_sql: str) -> bool:
        """创建表（如果不存在）"""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            
            cursor = self.connection.cursor()
            cursor.execute(create_table_sql)
            cursor.close()
            return True
            
        except Error as e:
            print(f"❌ 创建表失败: {e}")
            return False
    
    def save_dataframe_to_table(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        insert_query: str, 
        expected_columns: List[str],
        fill_missing_defaults: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        通用方法：保存DataFrame数据到MySQL表
        
        :param df: 要保存的DataFrame
        :param table_name: 表名
        :param insert_query: 插入SQL语句
        :param expected_columns: 期望的列名列表
        :param fill_missing_defaults: 缺失列的默认值映射
        :return: 是否成功
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存")
            return False
        
        try:
            # 处理缺失列
            df_processed = df.copy()
            
            for col in expected_columns:
                if col not in df_processed.columns:
                    if fill_missing_defaults and col in fill_missing_defaults:
                        df_processed[col] = fill_missing_defaults[col]
                    else:
                        # 默认处理逻辑
                        if col in ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']:
                            df_processed[col] = 0.0  # 数值字段默认0.0
                        elif col in ['is_hs', 'is_st']:
                            df_processed[col] = False  # 布尔字段默认False
                        else:
                            df_processed[col] = None  # 其他字段默认None
            
            # 转换DataFrame为元组列表
            data_tuples = [tuple(x) for x in df_processed[expected_columns].to_numpy()]
            
            # 批量插入数据
            success = self.execute_many(insert_query, data_tuples)
            
            if success:
                print(f"✅ 成功保存 {len(data_tuples)} 条记录到MySQL表 {table_name}")
            else:
                print(f"⚠️  保存 {len(data_tuples)} 条记录到MySQL表 {table_name} 失败")
            
            return success
            
        except Exception as e:
            print(f"❌ 保存数据到MySQL失败: {e}")
            return False
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()

    def query_data(
        self, 
        table_name: str, 
        columns: Optional[List[str]] = None, 
        conditions: Optional[str] = None,
        params: Optional[list] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        通用查询方法：根据表名、字段、条件语句查询数据
        
        :param table_name: 表名
        :param columns: 要查询的字段列表，None表示查询所有字段
        :param conditions: WHERE条件语句（不包含WHERE关键字）
        :param params: 查询参数列表
        :param order_by: ORDER BY子句（不包含ORDER BY关键字）
        :param limit: 限制返回记录数
        :return: 查询结果的DataFrame，失败返回None
        """
        try:
            # 构建SELECT子句
            if columns:
                columns_str = ", ".join(columns)
            else:
                columns_str = "*"
            
            # 构建基础查询语句
            query = f"SELECT {columns_str} FROM {table_name}"
            
            # 添加WHERE条件
            if conditions:
                query += f" WHERE {conditions}"
            
            # 添加ORDER BY
            if order_by:
                query += f" ORDER BY {order_by}"
            
            # 添加LIMIT
            if limit:
                query += f" LIMIT {limit}"
            
            # 执行查询
            result = self.execute_query(query, params)
            
            if result is None:
                return None
            
            # 获取列名
            if not self.connection or not self.connection.is_connected():
                self.connect()
            
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            column_names = [desc[0] for desc in cursor.description]
            cursor.close()
            
            # 如果指定了列，使用指定的列名
            if columns:
                # 确保查询结果列数与指定列数匹配
                if len(result[0]) == len(columns):
                    column_names = columns
                else:
                    print(f"⚠️  查询列数({len(result[0])})与指定列数({len(columns)})不匹配，使用实际列名")
            
            # 转换为DataFrame
            df = pd.DataFrame(result, columns=column_names)
            print(f"✅ 成功查询到 {len(df)} 条记录")
            return df
            
        except Error as e:
            print(f"❌ 查询数据失败: {e}")
            return None
        except Exception as e:
            print(f"❌ 查询过程中发生异常: {e}")
            return None


# 示例用法
if __name__ == "__main__":
    from parse_config import ParseConfig
    
    config = ParseConfig()
    mysql_manager = MySQLManager(config)
    
    # 使用上下文管理器
    with mysql_manager:
        # 执行查询
        result = mysql_manager.execute_query("SELECT COUNT(*) FROM listed_companies")
        if result:
            print(f"当前上市公司数量: {result[0][0]}")