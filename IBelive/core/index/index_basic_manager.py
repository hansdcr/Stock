"""
指数基本信息管理类
基于Tushare index_basic接口: https://tushare.pro/document/2?doc_id=94
"""
import os
import sys
import pandas as pd
import tushare
from typing import List, Optional, Dict, Any

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
from models.index_basic import IndexBasic, create_index_basic_from_dataframe    



class IndexBasicManager:
    """指数基本信息管理类"""
    
    DEFAULT_FIELDS = IndexBasic.DEFAULT_FIELDS

    def __init__(self, config, pro: tushare.pro):
        """
        初始化指数基本信息管理器
        
        :param config: 配置对象，需提供get_data_dir()和get_token()/get_mysql_config()
        :param pro: Tushare Pro接口对象
        """
        self.config = config
        self.pro = pro
        self.data_dir = config.get_data_dir()
        self.mysql_manager = MySQLManager(config)
        
    def create_table_if_not_exists(self) -> bool:
        """创建表（如果不存在）"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {IndexBasic.TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {IndexBasic.FIELD_TS_CODE} VARCHAR(20) NOT NULL,
            {IndexBasic.FIELD_NAME} VARCHAR(100) NOT NULL,
            {IndexBasic.FIELD_FULLNAME} VARCHAR(200),
            {IndexBasic.FIELD_MARKET} VARCHAR(20),
            {IndexBasic.FIELD_PUBLISHER} VARCHAR(50),
            {IndexBasic.FIELD_INDEX_TYPE} VARCHAR(50),
            {IndexBasic.FIELD_CATEGORY} VARCHAR(50),
            {IndexBasic.FIELD_BASE_DATE} VARCHAR(8),
            {IndexBasic.FIELD_BASE_POINT} DECIMAL(20, 4),
            {IndexBasic.FIELD_LIST_DATE} VARCHAR(8),
            {IndexBasic.FIELD_WEIGHT_RULE} VARCHAR(100),
            `{IndexBasic.FIELD_DESC}` TEXT,
            {IndexBasic.FIELD_EXP_DATE} VARCHAR(8),
            {IndexBasic.FIELD_DATA_STATUS} VARCHAR(20) DEFAULT '正常',
            {IndexBasic.FIELD_STATUS_REASON} VARCHAR(255) DEFAULT '',
            {IndexBasic.FIELD_CREATED_AT} TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            {IndexBasic.FIELD_UPDATED_AT} TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_ts_code ({IndexBasic.FIELD_TS_CODE}),
            INDEX idx_market ({IndexBasic.FIELD_MARKET}),
            INDEX idx_publisher ({IndexBasic.FIELD_PUBLISHER}),
            INDEX idx_category ({IndexBasic.FIELD_CATEGORY})   
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        return self.mysql_manager.create_table_if_not_exists(IndexBasic.TABLE_NAME, create_table_sql)
    
    def fetch_index_basic_data(self, 
                             market: Optional[str] = None,
                             publisher: Optional[str] = None,
                             category: Optional[str] = None,
                             fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取指数基本信息
        
        :param market: 市场代码，如'SSE'（上交所）、'SZSE'（深交所）、'CSI'（中证）等
        :param publisher: 发布方
        :param category: 指数类别
        :param fields: 要获取的字段列表，默认None表示所有默认字段
        :return: pandas DataFrame 包含指数基本信息
        """
        try:
            # 构建查询参数
            params = {}
            
            if market:
                params["market"] = market
            if publisher:
                params["publisher"] = publisher
            if category:
                params["category"] = category
            
            # 使用默认字段或指定字段
            if fields is None:
                params["fields"] = ",".join(self.DEFAULT_FIELDS)
            else:
                params["fields"] = ",".join(fields)
            
            # 执行查询
            df = self.pro.index_basic(**params)
            
            if df.empty:
                print("⚠️  未找到指数基本信息")
                return pd.DataFrame()
            
            print(f"✅ 成功获取 {len(df)} 条指数基本信息")
            return df
            
        except Exception as e:
            print(f"❌ 获取指数基本信息失败: {e}")
            return pd.DataFrame()
    
    def fetch_all_index_basic_data(self, 
                                 markets: Optional[List[str]] = None,
                                 batch_size: int = 50) -> pd.DataFrame:
        """
        获取所有指数基本信息（按市场循环获取）
        
        :param markets: 市场代码列表，默认None表示所有主要市场
        :param batch_size: 批量处理大小
        :return: 合并后的DataFrame
        """
        # 默认市场列表
        if markets is None:
            markets = ['SSE', 'SZSE', 'CSI', 'MSCI', 'CICC', 'SW', 'OTH']
        
        all_data = []
        
        for market in markets:
            print(f"📊 正在获取 {market} 市场的指数基本信息...")
            df_market = self.fetch_index_basic_data(market=market)
            
            if not df_market.empty:
                all_data.append(df_market)
                print(f"✅ 成功获取 {market} 市场的 {len(df_market)} 条指数信息")
            else:
                print(f"⚠️  {market} 市场未找到指数信息")
        
        # 合并所有数据
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"✅ 总共获取 {len(combined_df)} 条指数基本信息")
            return combined_df
        else:
            print("⚠️  未获取到任何指数基本信息")
            return pd.DataFrame()
    
    def _save_index_basic_to_mysql(self, df: pd.DataFrame, batch_size: int = 50) -> bool:
        """
        保存指数基本信息到MySQL数据库（批量处理）
        
        :param df: 包含指数基本信息的DataFrame
        :param batch_size: 批量处理大小
        :return: 是否成功保存
        """
        if df is None or df.empty:
            print("⚠️  无数据可保存到MySQL")
            return False
        
        try:
            # 首先确保表已创建
            self.create_table_if_not_exists()
            
            # 获取插入查询语句
            insert_query = IndexBasic.get_mysql_insert_query()
            
            # 转换DataFrame为IndexBasic对象列表
            index_objects = create_index_basic_from_dataframe(df)
            
            if not index_objects:
                print("⚠️  无有效的指数基本信息对象可保存") 
                return False
            
            # 准备数据元组
            data_to_insert = []
            for obj in index_objects:
                data_dict = obj.to_mysql_dict()
                data_tuple = (
                    data_dict.get('ts_code', ''),
                    data_dict.get('name', ''),
                    data_dict.get('fullname', ''),
                    data_dict.get('market', ''),
                    data_dict.get('publisher', ''),
                    data_dict.get('index_type', ''),
                    data_dict.get('category', ''),
                    data_dict.get('base_date', ''),
                    data_dict.get('base_point', 0.0),
                    data_dict.get('list_date', ''),
                    data_dict.get('weight_rule', ''),
                    data_dict.get('desc', ''),
                    data_dict.get('exp_date', ''),
                    data_dict.get('data_status', '正常'),
                    data_dict.get('status_reason', ''),
                    data_dict.get('created_at'),
                    data_dict.get('updated_at')
                )
                data_to_insert.append(data_tuple)
            
            # 批量插入数据（每batch_size条保存一次）
            total_records = len(data_to_insert)
            success_count = 0
            
            for i in range(0, total_records, batch_size):
                batch = data_to_insert[i:i + batch_size]
                try:
                    success = self.mysql_manager.execute_many(insert_query, batch)
                    if success:
                        success_count += len(batch)
                        print(f"✅ 已批量保存 {len(batch)} 条指数基本信息到MySQL ({i + len(batch)}/{total_records})")
                    else:
                        print(f"❌ 批量保存失败: 第 {i//batch_size + 1} 批")
                except Exception as e:
                    print(f"❌ 批量保存失败: {e}")
            
            print(f"✅ 成功保存 {success_count}/{total_records} 条指数基本信息到MySQL")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 保存指数基本信息到MySQL失败: {e}")
            return False
    
    def fetch_and_save_all_index_basic_data(self, 
                                          markets: Optional[List[str]] = None,
                                          batch_size: int = 50) -> bool:
        """
        获取并保存所有指数基本信息到MySQL
        
        :param markets: 市场代码列表
        :param batch_size: 批量处理大小
        :return: 是否成功
        """
        print("🚀 开始获取并保存所有指数基本信息...")
        
        # 获取所有指数基本信息
        df_all = self.fetch_all_index_basic_data(markets=markets)
        
        if df_all.empty:
            print("⚠️  未获取到指数基本信息，无法保存")
            return False
        
        # 保存到MySQL
        success = self._save_index_basic_to_mysql(df_all, batch_size=batch_size)
        
        if success:
            print("✅ 指数基本信息获取并保存完成！")
        else:
            print("❌ 指数基本信息保存失败")
        
        return success

def test_index_basic_manager():
    """测试指数基本信息管理器"""
    from parse_config import ParseConfig

    print("🚀 开始测试指数基本信息管理器...")
    
    
    try:
        # 初始化配置和Tushare Pro接口
        config = ParseConfig()
        pro = tushare.pro_api(config.get_token())
        
        # 创建管理器实例
        manager = IndexBasicManager(config, pro)
        
        print("✅ IndexBasicManager初始化成功")
        
        # 测试1: 创建表
        print("\n📊 测试1: 创建表...")
        success = manager.create_table_if_not_exists()
        if success:
            print("✅ 表创建成功")
        else:
            print("❌ 表创建失败")
            return False
        
        # 测试2: 获取单个市场数据
        print("\n📊 测试2: 获取SSE市场指数基本信息...")
        df_sse = manager.fetch_index_basic_data(market='SSE')
        if not df_sse.empty:
            print(f"✅ 成功获取 {len(df_sse)} 条SSE市场指数信息")
            print("📋 数据预览:")
            print(df_sse.head(3))
        else:
            print("⚠️  未获取到SSE市场数据")
        
        # 测试3: 获取所有市场数据
        print("\n📊 测试3: 获取所有市场指数基本信息...")
        df_all = manager.fetch_all_index_basic_data()
        if not df_all.empty:
            print(f"✅ 成功获取 {len(df_all)} 条指数基本信息")
            print("📋 数据预览:")
            print(df_all.head(5))
            
            # 显示市场分布
            if 'market' in df_all.columns:
                market_counts = df_all['market'].value_counts()
                print("\n🏢 市场分布:")
                print(market_counts)
        else:
            print("⚠️  未获取到任何指数基本信息")
        
        # 测试4: 保存数据到MySQL
        print("\n📊 测试4: 保存数据到MySQL...")
        if not df_all.empty:
            success = manager.fetch_and_save_all_index_basic_data(batch_size=50)
            if success:
                print("✅ 数据保存到MySQL成功")
            else:
                print("❌ 数据保存到MySQL失败")
        else:
            print("⚠️  无数据可保存，跳过MySQL保存测试")
        
        print("\n🎉 指数基本信息管理器测试完成！")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    