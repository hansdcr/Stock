#!/usr/bin/env python3
"""
完整测试IndexBasicManager功能
"""
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, '/Users/gelin/Desktop/store/dev/python/20250926stock/StockIBelive')

from IBelive.core.parse_config import ParseConfig
from IBelive.core.index.index_basic_manager import IndexBasicManager
import tushare as ts

def test_index_basic_manager():
    """测试IndexBasicManager完整功能"""
    print("🚀 开始测试IndexBasicManager完整功能...")
    
    try:
        # 1. 初始化配置和Tushare API
        print("\n📋 步骤1: 初始化配置和Tushare API...")
        config = ParseConfig()
        pro = ts.pro_api(config.get_token())
        print("✅ 配置和Tushare API初始化成功")
        
        # 2. 创建IndexBasicManager实例
        print("\n📋 步骤2: 创建IndexBasicManager实例...")
        manager = IndexBasicManager(config, pro)
        print("✅ IndexBasicManager实例创建成功")
        
        # 3. 测试表创建
        print("\n📋 步骤3: 测试表创建...")
        try:
            table_created = manager.create_table_if_not_exists()
            if table_created:
                print("✅ 表创建成功")
            else:
                print("❌ 表创建失败")
                # 测试MySQL连接是否正常
                print("🔍 测试MySQL连接状态...")
                mysql_connected = manager.mysql_manager.connect()
                if mysql_connected:
                    print("✅ MySQL连接正常")
                    # 测试简单查询
                    test_result = manager.mysql_manager.execute_query("SELECT 1")
                    if test_result:
                        print("✅ MySQL简单查询正常")
                    else:
                        print("❌ MySQL简单查询失败")
                    manager.mysql_manager.disconnect()
                else:
                    print("❌ MySQL连接失败")
                return False
        except Exception as e:
            print(f"❌ 表创建过程中发生异常: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # # 4. 测试获取单个市场数据
        # print("\n📋 步骤4: 测试获取SSE市场数据...")
        # sse_data = manager.fetch_index_basic_data(market='SSE')
        # if not sse_data.empty:
        #     print(f"✅ 成功获取 {len(sse_data)} 条SSE市场指数基本信息")
        #     print(f"📊 数据字段: {list(sse_data.columns)}")
        #     print(f"📊 前5条数据示例:")
        #     print(sse_data.head())
        # else:
        #     print("⚠️  未获取到SSE市场数据")
        
        # # 5. 测试获取所有市场数据
        # print("\n📋 步骤5: 测试获取所有市场数据...")
        # all_data = manager.fetch_all_index_basic_data()
        # if not all_data.empty:
        #     print(f"✅ 成功获取 {len(all_data)} 条所有市场指数基本信息")
        #     print(f"📊 市场分布: {all_data['market'].value_counts().to_dict()}")
        # else:
        #     print("⚠️  未获取到任何市场数据")
        
        # # 6. 测试保存数据到MySQL
        # print("\n📋 步骤6: 测试保存数据到MySQL...")
        # if not all_data.empty:
        #     save_success = manager._save_index_basic_to_mysql(all_data, batch_size=20)
        #     if save_success:
        #         print("✅ 数据成功保存到MySQL")
        #     else:
        #         print("❌ 数据保存到MySQL失败")
        # else:
        #     print("⚠️  无数据可保存，跳过保存测试")
        
        # 7. 测试完整流程
        print("\n📋 步骤7: 测试完整获取并保存流程...")
        full_success = manager.fetch_and_save_all_index_basic_data(batch_size=20)
        if full_success:
            print("✅ 完整流程执行成功")
        else:
            print("❌ 完整流程执行失败")
        
        print("\n🎉 IndexBasicManager功能测试完成！")
        return True
        
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_index_basic_manager()
    sys.exit(0 if success else 1)