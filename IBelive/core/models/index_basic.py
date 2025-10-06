"""
ETF指数基本信息数据模型
基于Tushare index_basic接口: https://tushare.pro/document/2?doc_id=94
"""
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from datetime import datetime
import pandas as pd


@dataclass
class IndexBasic:
    """ETF指数基本信息数据类"""
    
    # 表名和字段常量
    TABLE_NAME = 'index_basic'
    
    # 字段常量
    FIELD_TS_CODE = 'ts_code'          # TS代码
    FIELD_NAME = 'name'               # 简称
    FIELD_FULLNAME = 'fullname'       # 指数全称
    FIELD_MARKET = 'market'           # 市场
    FIELD_PUBLISHER = 'publisher'     # 发布方
    FIELD_INDEX_TYPE = 'index_type'   # 指数风格
    FIELD_CATEGORY = 'category'        # 指数类别
    FIELD_BASE_DATE = 'base_date'     # 基期
    FIELD_BASE_POINT = 'base_point'   # 基点
    FIELD_LIST_DATE = 'list_date'     # 发布日期
    FIELD_WEIGHT_RULE = 'weight_rule' # 加权方式
    FIELD_DESC = 'desc'               # 描述
    FIELD_EXP_DATE = 'exp_date'       # 终止日期
    FIELD_DATA_STATUS = 'data_status'  # 数据状态
    FIELD_STATUS_REASON = 'status_reason'  # 状态原因
    FIELD_CREATED_AT = 'created_at'   # 创建时间
    FIELD_UPDATED_AT = 'updated_at'   # 更新时间
    
    # Tushare index_basic接口的默认字段
    DEFAULT_FIELDS = [
        FIELD_TS_CODE,      # TS代码
        FIELD_NAME,         # 简称
        FIELD_FULLNAME,     # 指数全称
        FIELD_MARKET,       # 市场
        FIELD_PUBLISHER,    # 发布方
        FIELD_INDEX_TYPE,   # 指数风格
        FIELD_CATEGORY,     # 指数类别
        FIELD_BASE_DATE,    # 基期
        FIELD_BASE_POINT,   # 基点
        FIELD_LIST_DATE,    # 发布日期
        FIELD_WEIGHT_RULE,  # 加权方式
        FIELD_DESC,         # 描述
        FIELD_EXP_DATE      # 终止日期
    ]
    
    # 实例字段
    ts_code: str                    # TS代码
    name: str                       # 简称
    fullname: str                   # 指数全称
    market: str                     # 市场
    publisher: str                  # 发布方
    index_type: str                 # 指数风格
    category: str                   # 指数类别
    base_date: str                  # 基期
    base_point: float               # 基点
    list_date: str                  # 发布日期
    weight_rule: str                # 加权方式
    desc: str                       # 描述
    exp_date: Optional[str] = None  # 终止日期
    
    # 状态字段（用于数据质量控制）
    data_status: str = '正常'        # 数据状态：正常、错误、停盘等
    status_reason: str = ''         # 状态原因
    created_at: Optional[datetime] = None  # 创建时间
    updated_at: Optional[datetime] = None   # 更新时间
    
    @classmethod
    def get_default_fields(cls) -> List[str]:
        """获取默认字段列表"""
        return cls.DEFAULT_FIELDS.copy()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ETFIndexBasic':
        """从字典创建ETFIndexBasic实例"""
        # 处理数值字段
        numeric_fields = ['base_point']
        for field in numeric_fields:
            if field in data and data[field] is not None:
                try:
                    data[field] = float(data[field])
                except (ValueError, TypeError):
                    data[field] = 0.0
        
        # 处理可选字段
        optional_fields = ['exp_date', 'desc', 'weight_rule']
        for field in optional_fields:
            if field not in data or data[field] is None:
                data[field] = ''
        
        # 创建实例
        return cls(**data)
    
    def _preprocess_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """统一预处理数据，处理所有字段的None和nan值"""
        # 处理字符串字段，将None和nan转换为空字符串
        string_fields = [
            'ts_code', 'name', 'fullname', 'market', 'publisher', 
            'index_type', 'category', 'base_date', 'list_date',
            'weight_rule', 'desc', 'exp_date', 'data_status', 'status_reason'
        ]
        
        for field in string_fields:
            if field in data and (data[field] is None or pd.isna(data[field])):
                data[field] = ''
        
        # 处理数值字段
        numeric_fields = ['base_point']
        for field in numeric_fields:
            if field in data and (data[field] is None or pd.isna(data[field])):
                data[field] = 0.0
        
        # 处理datetime字段
        datetime_fields = ['created_at', 'updated_at']
        for field in datetime_fields:
            if field in data and (data[field] is None or pd.isna(data[field])):
                data[field] = None
        
        return data
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        return self._preprocess_data(data)
    
    def to_mysql_dict(self) -> Dict[str, Any]:
        """转换为MySQL兼容的字典"""
        data = self.to_dict()
        # 处理datetime字段
        if self.created_at:
            data['created_at'] = self.created_at.strftime('%Y-%m-%d %H:%M:%S')
        if self.updated_at:
            data['updated_at'] = self.updated_at.strftime('%Y-%m-%d %H:%M:%S')
        return data
    
    @classmethod
    def get_mysql_table_definition(cls) -> str:
        """获取MySQL表定义SQL"""
        return """
        CREATE TABLE IF NOT EXISTS index_basic (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts_code VARCHAR(20) NOT NULL,
            name VARCHAR(100) NOT NULL,
            fullname VARCHAR(200),
            market VARCHAR(20),
            publisher VARCHAR(50),
            index_type VARCHAR(50),
            category VARCHAR(50),
            base_date VARCHAR(8),
            base_point DECIMAL(20, 4),
            list_date VARCHAR(8),
            weight_rule VARCHAR(100),
            `desc` TEXT,
            exp_date VARCHAR(8),
            data_status VARCHAR(20) DEFAULT '正常',
            status_reason VARCHAR(255) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_ts_code (ts_code),
            INDEX idx_market (market),
            INDEX idx_publisher (publisher),
            INDEX idx_category (category)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    
    @classmethod
    def get_mysql_insert_query(cls) -> str:
        """获取MySQL插入语句"""
        return """
        INSERT INTO index_basic 
        (ts_code, name, fullname, market, publisher, index_type, category, 
         base_date, base_point, list_date, weight_rule, `desc`, exp_date, 
         data_status, status_reason, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            fullname = VALUES(fullname),
            market = VALUES(market),
            publisher = VALUES(publisher),
            index_type = VALUES(index_type),
            category = VALUES(category),
            base_date = VALUES(base_date),
            base_point = VALUES(base_point),
            list_date = VALUES(list_date),
            weight_rule = VALUES(weight_rule),
            `desc` = VALUES(`desc`),
            exp_date = VALUES(exp_date),
            data_status = VALUES(data_status),
            status_reason = VALUES(status_reason),
            updated_at = VALUES(updated_at)
        """


def create_index_basic_from_dataframe(df: pd.DataFrame) -> List[IndexBasic]:
    """从DataFrame创建IndexBasic对象列表"""
    objects = []
    for _, row in df.iterrows():
        try:
            obj = IndexBasic.from_dict(row.to_dict())
            objects.append(obj)
        except Exception as e:
            print(f"❌ 创建IndexBasic对象失败: {e}")
    return objects


def index_basic_list_to_dataframe(objects: List[IndexBasic]) -> pd.DataFrame:
    """将IndexBasic对象列表转换为DataFrame"""   
    if not objects:
        return pd.DataFrame()
    
    data = [obj.to_dict() for obj in objects]
    df = pd.DataFrame(data)
    
    # 处理nan值，将其转换为None或空字符串
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
    
    return df