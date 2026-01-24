"""
快速测试 akshare API 是否正常工作
"""
import akshare as ak
import pandas as pd

print(f"akshare 版本: {ak.__version__}")
print("\n" + "="*60)

# 测试贵州茅台
stock_code = "SH600519"
print(f"正在获取 {stock_code} 的资产负债表...")

try:
    df_balance = ak.stock_balance_sheet_by_report_em(symbol=stock_code)
    
    if df_balance is not None and not df_balance.empty:
        print(f"✓ 成功获取数据！共 {len(df_balance)} 条记录")
        print(f"\n数据列名（前20个）:")
        print(df_balance.columns.tolist()[:20])
        print(f"\n数据预览（前3行）:")
        print(df_balance.head(3))
    else:
        print("✗ API返回空数据")
        
except Exception as e:
    print(f"✗ 获取数据失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print(f"正在获取 {stock_code} 的利润表...")

try:
    df_income = ak.stock_profit_sheet_by_report_em(symbol=stock_code)
    
    if df_income is not None and not df_income.empty:
        print(f"✓ 成功获取数据！共 {len(df_income)} 条记录")
        print(f"\n数据列名（前20个）:")
        print(df_income.columns.tolist()[:20])
    else:
        print("✗ API返回空数据")
        
except Exception as e:
    print(f"✗ 获取数据失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print(f"正在获取 {stock_code} 的现金流量表...")

try:
    df_cashflow = ak.stock_cash_flow_sheet_by_report_em(symbol=stock_code)
    
    if df_cashflow is not None and not df_cashflow.empty:
        print(f"✓ 成功获取数据！共 {len(df_cashflow)} 条记录")
        print(f"\n数据列名（前20个）:")
        print(df_cashflow.columns.tolist()[:20])
    else:
        print("✗ API返回空数据")
        
except Exception as e:
    print(f"✗ 获取数据失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("测试完成！")
