from dataclasses import dataclass, field
from datetime import date
from typing import ClassVar, List

@dataclass
class Company:
    """上市公司数据模型"""
    
    # 类变量：默认字段列表（用于API查询）
    DEFAULT_FIELDS: ClassVar[List[str]] = [
        "ts_code", "symbol", "name", "area", "industry", "fullname",
        "enname", "cnspell", "market", "exchange", "list_status", "list_date",
        "delist_date", "is_hs", "is_st"
    ]
    
    # 实例字段
    ts_code: str = ""           # 股票代码
    symbol: str = ""            # 股票符号
    name: str = ""              # 股票名称
    area: str = ""              # 股票所属区域
    industry: str = ""          # 股票所属行业
    fullname: str = ""          # 股票全称
    enname: str = ""            # 股票英文名称
    cnspell: str = ""           # 股票中文拼写
    market: str = ""            # 股票所属市场
    exchange: str = ""          # 股票所属交易所
    list_status: str = ""       # 上市状态
    list_date: date = None      # 上市日期
    delist_date: date = None    # 退市日期
    is_hs: bool = False         # 是否沪深股票
    is_st: bool = False         # 是否ST股票
    
    @classmethod
    def get_default_fields(cls) -> str:
        """获取默认字段的逗号分隔字符串"""
        return ",".join(cls.DEFAULT_FIELDS)