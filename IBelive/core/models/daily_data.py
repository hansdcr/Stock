from dataclasses import dataclass, field
from typing import ClassVar, List
from datetime import date

@dataclass
class DailyData:
    """股票每日数据模型"""

    # 类变量：默认字段列表（用于API查询）
    DEFAULT_FIELDS: ClassVar[List[str]] = [
        "ts_code", "trade_date", "open", "high", "low", "close", 
        "pre_close", "change", "pct_chg", "vol", "amount"
    ]
    
    # 实例字段
    ts_code: str = ""           # 股票代码
    trade_date: str = ""        # 交易日期（YYYYMMDD格式）
    open: float = 0.0           # 开盘价
    high: float = 0.0           # 最高价
    low: float = 0.0            # 最低价
    close: float = 0.0          # 收盘价
    pre_close: float = 0.0      # 昨收价
    change: float = 0.0         # 涨跌额
    pct_chg: float = 0.0        # 涨跌幅（%）
    vol: float = 0.0            # 成交量（手）
    amount: float = 0.0         # 成交额（千元）

    @classmethod
    def get_default_fields(cls) -> str:
        """获取默认字段的逗号分隔字符串"""
        return ",".join(cls.DEFAULT_FIELDS)
    
    @classmethod
    def from_dict(cls, data_dict):
        """从字典创建DailyData实例"""
        return cls(**data_dict)
