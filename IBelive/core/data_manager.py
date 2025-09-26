import tushare
import os
import sys

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

# 导入模型
from models.companies import Company

data_dir = os.path.join(os.path.dirname(THIS_DIR), "data")

class StockDataManager:
    
    def __init__(self, config_parser):
        self.config = config_parser
        self.pro = tushare.pro_api(self.config.get_token())
        self.data_dir = os.path.join(os.path.dirname(THIS_DIR), "data")
        
    def fetch_listed_companies(self,asof_date=None,fields=None):
        """
        获取指定日期的所有上市股票信息
        
        :param asof_date: 查询日期，格式YYYYMMDD，默认None表示最新数据
        :param fields: 要获取的字段列表，默认None表示所有字段
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
        # 保存数据
        self.save_listed_companies(df, asof_date)
        
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


if __name__ == "__main__":
    from parse_config import ParseConfig
    config = ParseConfig()
    manager = StockDataManager(config)
    df = manager.fetch_listed_companies("20250925")