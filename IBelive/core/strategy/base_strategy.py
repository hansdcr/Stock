"""
选股策略基类
定义策略的通用接口和基础功能
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd


class BaseStrategy(ABC):
    """选股策略抽象基类"""
    
    def __init__(self, config):
        """
        初始化策略
        
        :param config: 配置对象
        """
        self.config = config
        self.strategy_name = self.__class__.__name__
    
    @abstractmethod
    def prepare_data(self) -> bool:
        """准备策略所需数据"""
        pass
    
    @abstractmethod
    def execute(self) -> List[Dict[str, Any]]:
        """执行选股策略"""
        pass
    
    @abstractmethod
    def filter_stocks(self, stocks_data: pd.DataFrame) -> pd.DataFrame:
        """过滤股票数据"""
        pass
    
    def save_results(self, results: List[Dict[str, Any]]) -> bool:
        """保存选股结果"""
        # 默认实现，可以被子类重写
        print(f"策略 {self.strategy_name} 选出了 {len(results)} 只股票")
        return True
    
    def run(self) -> List[Dict[str, Any]]:
        """运行完整策略流程"""
        print(f"🚀 开始执行策略: {self.strategy_name}")
        
        # 准备数据
        if not self.prepare_data():
            print("❌ 数据准备失败")
            return []
        
        # 执行策略
        results = self.execute()
        
        # 保存结果
        self.save_results(results)
        
        print(f"✅ 策略执行完成，选出 {len(results)} 只股票")
        return results