from dataclasses import dataclass, field
from typing import ClassVar, List
from datetime import date


@dataclass
class ETFIndexDaily:
    """ETF指数日线数据模型"""

    # 类变量：默认字段列表（用于API查询）
    DEFAULT_FIELDS: ClassVar[List[str]] = [
        "ts_code", "trade_date", "open", "high", "low", "close",
        "pre_close", "change", "pct_chg", "vol", "amount"
    ]

    # 实例字段
    ts_code: str = ""  # 指数代码
    trade_date: date = None  # 交易日期（datetime.date类型）
    open: float = 0.0  # 开盘点位
    high: float = 0.0  # 最高点位
    low: float = 0.0  # 最低点位
    close: float = 0.0  # 收盘点位
    pre_close: float = 0.0  # 昨收点位
    change: float = 0.0  # 涨跌点
    pct_chg: float = 0.0  # 涨跌幅（%）
    vol: float = 0.0  # 成交量（手）
    amount: float = 0.0  # 成交额（千元）

    @classmethod
    def get_default_fields(cls) -> str:
        """获取默认字段的逗号分隔字符串"""
        return ",".join(cls.DEFAULT_FIELDS)

    @classmethod
    def from_dict(cls, data_dict):
        """从字典创建ETFIndexDaily实例"""
        # 处理trade_date字段转换
        if 'trade_date' in data_dict and isinstance(data_dict['trade_date'], str):
            # 将YYYYMMDD格式的字符串转换为date对象
            date_str = data_dict['trade_date']
            if len(date_str) == 8 and date_str.isdigit():
                data_dict['trade_date'] = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))

        # 处理数值字段的空值或异常值
        numeric_fields = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
        for field_name in numeric_fields:
            if field_name in data_dict:
                value = data_dict[field_name]
                if value is None or (isinstance(value, str) and value.strip() == ''):
                    data_dict[field_name] = 0.0
                elif isinstance(value, str):
                    try:
                        data_dict[field_name] = float(value)
                    except ValueError:
                        data_dict[field_name] = 0.0

        return cls(**data_dict)