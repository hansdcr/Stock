"""
股票基本面数据模型类
基于Tushare daily_basic接口：https://tushare.pro/document/2?doc_id=32
"""
from dataclasses import dataclass, field
from typing import ClassVar, List
from datetime import date

@dataclass
class DailyBasic:
    """股票每日基本面数据模型"""

    # 类变量：默认字段列表（用于API查询）
    DEFAULT_FIELDS: ClassVar[List[str]] = [
        "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f",
        "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio",
        "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv"
    ]
    
    # 实例字段
    ts_code: str = ""                    # 股票代码
    trade_date: date = None              # 交易日期（datetime.date类型）
    close: float = 0.0                   # 当日收盘价
    turnover_rate: float = 0.0           # 换手率（%）
    turnover_rate_f: float = 0.0          # 换手率（自由流通股）
    volume_ratio: float = 0.0            # 量比
    pe: float = 0.0                      # 市盈率（总市值/净利润，亏损的PE为空）
    pe_ttm: float = 0.0                  # 市盈率（TTM，亏损的PE为空）
    pb: float = 0.0                      # 市净率（总市值/净资产）
    ps: float = 0.0                      # 市销率
    ps_ttm: float = 0.0                  # 市销率（TTM）
    dv_ratio: float = 0.0                # 股息率（%）
    dv_ttm: float = 0.0                  # 股息率（TTM）（%）
    total_share: float = 0.0             # 总股本（万股）
    float_share: float = 0.0             # 流通股本（万股）
    free_share: float = 0.0              # 自由流通股本（万）
    total_mv: float = 0.0                # 总市值（万元）
    circ_mv: float = 0.0                 # 流通市值（万元）

    @classmethod
    def get_default_fields(cls) -> str:
        """获取默认字段的逗号分隔字符串"""
        return ",".join(cls.DEFAULT_FIELDS)
    
    @classmethod
    def from_dict(cls, data_dict):
        """从字典创建DailyBasic实例"""
        # 处理trade_date字段转换
        if 'trade_date' in data_dict and isinstance(data_dict['trade_date'], str):
            # 将YYYYMMDD格式的字符串转换为date对象
            date_str = data_dict['trade_date']
            if len(date_str) == 8 and date_str.isdigit():
                data_dict['trade_date'] = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        
        # 处理数值字段的空值
        numeric_fields = [
            'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio',
            'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm',
            'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv'
        ]
        
        for field in numeric_fields:
            if field in data_dict and data_dict[field] is None:
                data_dict[field] = 0.0
        
        return cls(**data_dict)