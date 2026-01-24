"""
简化测试脚本 - 直接在 Python 中运行，不使用 Jupyter
"""
import sys
sys.path.append('..')

import pandas as pd
import akshare as ak
from models import Base, BalanceSheet, IncomeStatement, CashFlowStatement
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

print("="*60)
print("测试 akshare API 获取数据")
print("="*60)

# 获取数据
stock_code = "SH600519"
print(f"\n正在获取 {stock_code} 的资产负债表...")

try:
    df_balance = ak.stock_balance_sheet_by_report_em(symbol=stock_code)
    
    if df_balance is not None and not df_balance.empty:
        print(f"✓ 成功！共 {len(df_balance)} 条记录")
        print(f"\n列名示例（前10个）:")
        for i, col in enumerate(df_balance.columns[:10]):
            print(f"  {i+1}. {col}")
        
        print(f"\n数据示例（第1行）:")
        print(df_balance.iloc[0][['SECURITY_CODE', 'SECURITY_NAME_ABBR', 'REPORT_DATE', 'TOTAL_ASSETS', 'TOTAL_LIABILITIES']])
        
        # 加载列名映射
        print("\n" + "="*60)
        print("测试列名映射")
        print("="*60)
        
        with open('../config/column_mapping.yaml', 'r', encoding='utf-8') as f:
            column_mapping = yaml.safe_load(f)
        
        mapping = column_mapping.get('balance_sheet', {})
        common_mapping = column_mapping.get('common', {})
        all_mapping = {**mapping, **common_mapping}
        
        # 检查哪些列可以映射
        mapped_count = 0
        unmapped_count = 0
        
        print(f"\n检查 {len(df_balance.columns)} 个列名...")
        print("\n已映射的列（前10个）:")
        for col in df_balance.columns:
            if col in all_mapping:
                mapped_count += 1
                if mapped_count <= 10:
                    print(f"  {col} -> {all_mapping[col]}")
            else:
                unmapped_count += 1
        
        print(f"\n映射统计:")
        print(f"  已映射: {mapped_count}")
        print(f"  未映射: {unmapped_count}")
        
        # 测试数据库存储
        print("\n" + "="*60)
        print("测试数据库存储")
        print("="*60)
        
        database_url = "sqlite:///../database_test.sqlite"
        engine = create_engine(database_url, echo=False)
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 重命名列
        df_renamed = df_balance.rename(columns=all_mapping)
        
        # 只保存有映射的列
        valid_cols = [col for col in df_renamed.columns if hasattr(BalanceSheet, col)]
        df_to_save = df_renamed[valid_cols].head(3)  # 只保存前3条测试
        
        print(f"\n准备保存 {len(df_to_save)} 条记录...")
        print(f"可用字段: {len(valid_cols)} 个")
        
        saved_count = 0
        for _, row in df_to_save.iterrows():
            data_dict = {}
            for col in valid_cols:
                value = row[col]
                # 处理可能的 Series 对象
                if isinstance(value, pd.Series):
                    value = value.iloc[0] if len(value) > 0 else None
                # 检查是否为 NaN
                try:
                    if pd.isna(value):
                        data_dict[col] = None
                    else:
                        # 特殊处理日期字段
                        if col == 'report_date' or col == 'announcement_date':
                            if isinstance(value, str):
                                # 转换字符串日期为 date 对象
                                data_dict[col] = pd.to_datetime(value).date()
                            elif hasattr(value, 'date'):
                                data_dict[col] = value.date()
                            else:
                                data_dict[col] = value
                        else:
                            data_dict[col] = value
                except (ValueError, TypeError):
                    data_dict[col] = value
            
            try:
                record = BalanceSheet(**data_dict)
                session.add(record)
                saved_count += 1
            except Exception as e:
                print(f"保存失败: {e}")
        
        session.commit()
        print(f"✓ 成功保存 {saved_count} 条记录到数据库")
        
        # 读取验证
        query = f"SELECT * FROM balance_sheets WHERE stock_code = '{stock_code}'"
        df_from_db = pd.read_sql(query, engine)
        print(f"✓ 从数据库读取到 {len(df_from_db)} 条记录")
        
        session.close()
        
        print("\n" + "="*60)
        print("✓ 所有测试通过！")
        print("="*60)
        
    else:
        print("✗ API 返回空数据")
        
except Exception as e:
    print(f"✗ 错误: {e}")
    import traceback
    traceback.print_exc()
