"""
RSI可视化工具
生成股票价格、RSI和RSI移动平均的关系图表
帮助分析买入/卖出时机
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import random
from IBelive.core.parse_config import ParseConfig
from IBelive.core.mysql_manager import MySQLManager

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['Hiragino Sans GB', 'SimHei', 'Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

class RSIVisualization:
    """RSI可视化工具类"""
    
    def __init__(self):
        """初始化可视化工具"""
        self.config = ParseConfig()
        self.mysql_manager = MySQLManager(self.config)
    
    def get_random_stocks(self, count=4, rsi_table="rsi_14days_data"):
        """从RSI表中随机选择股票"""
        try:
            # 查询所有不同的股票代码
            query = f"SELECT DISTINCT ts_code FROM {rsi_table} ORDER BY RAND() LIMIT %s"
            result = self.mysql_manager.execute_query(query, [count])
            
            if result:
                return [row[0] for row in result]
            else:
                print(f"❌ 无法从表 {rsi_table} 获取股票数据")
                return []
                
        except Exception as e:
            print(f"❌ 获取随机股票失败: {e}")
            return []
    
    def get_stock_rsi_data(self, ts_code, rsi_table="rsi_14days_data"):
        """获取特定股票的RSI数据"""
        try:
            query = f"""
                SELECT trade_date, close, rsi_value, rsi_ma_value, rsi_status 
                FROM {rsi_table} 
                WHERE ts_code = %s 
                ORDER BY trade_date
            """
            
            result = self.mysql_manager.execute_query(query, [ts_code])
            
            if result:
                # 转换为DataFrame
                df = pd.DataFrame(result, columns=['trade_date', 'close', 'rsi_value', 'rsi_ma_value', 'rsi_status'])
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                return df
            else:
                print(f"❌ 未找到股票 {ts_code} 的RSI数据")
                return None
                
        except Exception as e:
            print(f"❌ 获取股票 {ts_code} 的RSI数据失败: {e}")
            return None
    
    def generate_stock_chart(self, ts_code, rsi_data, output_path=None):
        """生成单个股票的RSI分析图表"""
        if rsi_data is None or rsi_data.empty:
            print(f"⚠️  股票 {ts_code} 无有效数据，跳过图表生成")
            return False
        
        try:
            # 创建图表
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
            fig.suptitle(f'股票 {ts_code} - RSI技术分析', fontsize=16, fontweight='bold')
            
            # 价格图表
            ax1.plot(rsi_data['trade_date'], rsi_data['close'], 
                    label='收盘价', color='blue', linewidth=2)
            ax1.set_ylabel('价格', fontsize=12)
            ax1.set_title('价格走势', fontsize=14)
            ax1.grid(True, alpha=0.3)
            ax1.legend()
            
            # 格式化x轴日期显示
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            
            # RSI图表
            ax2.plot(rsi_data['trade_date'], rsi_data['rsi_value'], 
                    label='RSI(14)', color='red', linewidth=2)
            ax2.plot(rsi_data['trade_date'], rsi_data['rsi_ma_value'], 
                    label='RSI_MA(6)', color='orange', linewidth=2)
            
            # 添加超买超卖区域
            ax2.axhline(y=70, color='red', linestyle='--', alpha=0.7, label='超买线(70)')
            ax2.axhline(y=30, color='green', linestyle='--', alpha=0.7, label='超卖线(30)')
            ax2.fill_between(rsi_data['trade_date'], 70, 100, color='red', alpha=0.1, label='超买区域')
            ax2.fill_between(rsi_data['trade_date'], 0, 30, color='green', alpha=0.1, label='超卖区域')
            
            ax2.set_ylabel('RSI值', fontsize=12)
            ax2.set_title('RSI指标分析', fontsize=14)
            ax2.set_ylim(0, 100)
            ax2.grid(True, alpha=0.3)
            ax2.legend()
            
            # 格式化x轴日期显示
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            
            # 旋转x轴标签
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            plt.tight_layout()
            
            # 保存或显示图表
            if output_path:
                filename = f"{output_path}/rsi_analysis_{ts_code}.png"
                plt.savefig(filename, dpi=300, bbox_inches='tight')
                print(f"✅ 已保存图表: {filename}")
            else:
                plt.show()
            
            plt.close()
            return True
            
        except Exception as e:
            print(f"❌ 生成股票 {ts_code} 图表失败: {e}")
            return False
    
    def generate_trading_recommendation(self, rsi_data):
        """生成交易建议"""
        if rsi_data is None or rsi_data.empty:
            return "无足够数据"
        
        # 获取最新数据
        latest_data = rsi_data.iloc[-1]
        rsi_value = latest_data['rsi_value']
        rsi_ma_value = latest_data['rsi_ma_value']
        
        # 分析RSI和RSI_MA的关系
        if rsi_value > 70:
            if rsi_value > rsi_ma_value:
                return "强烈卖出 - RSI超买且高于移动平均"
            else:
                return "考虑卖出 - RSI超买但低于移动平均"
        elif rsi_value < 30:
            if rsi_value < rsi_ma_value:
                return "强烈买入 - RSI超卖且低于移动平均"
            else:
                return "考虑买入 - RSI超卖但高于移动平均"
        else:
            if rsi_value > rsi_ma_value:
                return "持有/观望 - RSI正常但高于移动平均"
            else:
                return "持有/观望 - RSI正常但低于移动平均"
    
    def generate_comprehensive_report(self, stocks_data, output_path=None):
        """生成综合报告"""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle('4只随机股票的RSI技术分析报告', fontsize=18, fontweight='bold')
            
            axes = axes.flatten()
            
            for i, (ts_code, rsi_data) in enumerate(stocks_data.items()):
                if i >= 4:  # 最多显示4只股票
                    break
                    
                ax = axes[i]
                
                # 价格图表
                ax.plot(rsi_data['trade_date'], rsi_data['close'], 
                       label='收盘价', color='blue', linewidth=1.5)
                ax.set_title(f'{ts_code} - 价格走势', fontsize=12)
                ax.grid(True, alpha=0.3)
                ax.tick_params(axis='x', rotation=45)
                
                # 添加RSI信息
                latest_rsi = rsi_data['rsi_value'].iloc[-1]
                latest_ma = rsi_data['rsi_ma_value'].iloc[-1]
                recommendation = self.generate_trading_recommendation(rsi_data)
                
                ax.text(0.02, 0.98, f"RSI: {latest_rsi:.1f}\nMA: {latest_ma:.1f}\n建议: {recommendation}", 
                       transform=ax.transAxes, verticalalignment='top', 
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                       fontsize=9)
            
            plt.tight_layout()
            
            # 保存或显示图表
            if output_path:
                filename = f"{output_path}/rsi_comprehensive_report.png"
                plt.savefig(filename, dpi=300, bbox_inches='tight')
                print(f"✅ 已保存综合报告: {filename}")
            else:
                plt.show()
            
            plt.close()
            return True
            
        except Exception as e:
            print(f"❌ 生成综合报告失败: {e}")
            return False

def main():
    """主函数"""
    print("🚀 开始生成RSI技术分析图表...")
    
    # 创建可视化实例
    visualizer = RSIVisualization()
    
    # 自定义股票选择 - 可以修改这里的股票代码列表
    custom_stocks = [
        "000001.SZ",  # 平安银行
        "600036.SH",  # 招商银行
        "000333.SZ",  # 美的集团
        "600519.SH"   # 贵州茅台
    ]
    
    # 或者使用随机选择（取消注释下面这行，注释掉上面的custom_stocks）
    # custom_stocks = visualizer.get_random_stocks(4)
    
    if not custom_stocks:
        print("❌ 无法获取股票数据，请确保RSI数据表存在并包含数据")
        return
    
    print(f"📊 选择的股票: {custom_stocks}")
    
    # 获取每只股票的RSI数据
    stocks_data = {}
    for ts_code in custom_stocks:
        print(f"🔍 获取股票 {ts_code} 的RSI数据...")
        rsi_data = visualizer.get_stock_rsi_data(ts_code)
        if rsi_data is not None:
            stocks_data[ts_code] = rsi_data
            print(f"✅ 成功获取 {ts_code} 的 {len(rsi_data)} 条数据")
        
        # 生成单个股票图表
        visualizer.generate_stock_chart(ts_code, rsi_data, "rsi_charts")
    
    # 生成综合报告
    if stocks_data:
        visualizer.generate_comprehensive_report(stocks_data, "rsi_charts")
        
        # 显示每只股票的交易建议
        print("\n📋 交易建议汇总:")
        print("-" * 60)
        for ts_code, rsi_data in stocks_data.items():
            recommendation = visualizer.generate_trading_recommendation(rsi_data)
            latest_rsi = rsi_data['rsi_value'].iloc[-1]
            latest_ma = rsi_data['rsi_ma_value'].iloc[-1]
            print(f"{ts_code}: RSI={latest_rsi:.1f}, MA={latest_ma:.1f} -> {recommendation}")
    
    print("\n🎉 RSI技术分析图表生成完成！")
    print("💡 查看生成的PNG图表文件进行分析")

if __name__ == "__main__":
    main()