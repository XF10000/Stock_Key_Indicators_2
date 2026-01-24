"""
数据更新器测试脚本

使用固定的股票列表测试数据更新器功能
"""
import pandas as pd
from datetime import datetime

from data_provider.akshare_client import AkshareClient
from data_provider.repository import Repository
from utils.config_loader import ConfigLoader
from utils.logger import Logger, create_data_quality_report


def main():
    """测试数据更新器"""
    
    # 加载配置
    config = ConfigLoader()
    
    # 设置日志
    logger_manager = Logger()
    logger = logger_manager.setup_data_update_logger()
    unmapped_logger = logger_manager.setup_unmapped_logger()
    
    logger.info("="*60)
    logger.info("数据更新器测试")
    logger.info("="*60)
    
    start_time = datetime.now()
    
    # 初始化客户端和仓储
    client = AkshareClient(
        request_delay=config.api_request_delay,
        retry_times=config.api_retry_times,
        retry_delay=config.api_retry_delay
    )
    
    database_url = f"sqlite:///{config.database_path}"
    repository = Repository(database_url)
    
    # 使用固定的测试股票列表
    test_stocks = pd.DataFrame({
        'stock_code': ['600519', '300750', '000725'],
        'stock_name': ['贵州茅台', '宁德时代', '京东方A'],
        'market_code': ['SH600519', 'SZ300750', 'SZ000725']
    })
    
    total_stocks = len(test_stocks)
    logger.info(f"测试股票数: {total_stocks}")
    
    # 加载列名映射
    balance_mapping = config.get_column_mapping('balance_sheet')
    income_mapping = config.get_column_mapping('income_statement')
    cashflow_mapping = config.get_column_mapping('cash_flow_statement')
    
    # 统计信息
    success_count = 0
    failed_stocks = []
    unmapped_columns = {
        'balance_sheet': set(),
        'income_statement': set(),
        'cash_flow_statement': set()
    }
    
    # 遍历股票列表
    logger.info("开始更新数据...")
    
    for idx, row in test_stocks.iterrows():
        stock_code = row['market_code']
        stock_name = row['stock_name']
        
        logger.info(f"\n处理 {stock_code} ({stock_name})...")
        
        try:
            # 获取财务数据
            data = client.get_all_financial_data(stock_code)
            
            # 保存资产负债表
            if data['balance_sheet'] is not None:
                # 记录未映射的列名
                for col in data['balance_sheet'].columns:
                    if col not in balance_mapping:
                        unmapped_columns['balance_sheet'].add(col)
                
                added, skipped = repository.save_balance_sheets(
                    data['balance_sheet'],
                    balance_mapping
                )
                logger.info(f"  资产负债表: 新增 {added}, 跳过 {skipped}")
            
            # 保存利润表
            if data['income_statement'] is not None:
                # 记录未映射的列名
                for col in data['income_statement'].columns:
                    if col not in income_mapping:
                        unmapped_columns['income_statement'].add(col)
                
                added, skipped = repository.save_income_statements(
                    data['income_statement'],
                    income_mapping
                )
                logger.info(f"  利润表: 新增 {added}, 跳过 {skipped}")
            
            # 保存现金流量表
            if data['cash_flow_statement'] is not None:
                # 记录未映射的列名
                for col in data['cash_flow_statement'].columns:
                    if col not in cashflow_mapping:
                        unmapped_columns['cash_flow_statement'].add(col)
                
                added, skipped = repository.save_cash_flow_statements(
                    data['cash_flow_statement'],
                    cashflow_mapping
                )
                logger.info(f"  现金流量表: 新增 {added}, 跳过 {skipped}")
            
            success_count += 1
            logger.info(f"✓ {stock_code} ({stock_name}) 更新成功")
            
        except Exception as e:
            failed_stocks.append(f"{stock_code} ({stock_name}): {str(e)}")
            logger.error(f"✗ {stock_code} ({stock_name}) 更新失败: {e}")
    
    end_time = datetime.now()
    
    # 记录未映射的列名
    logger.info("\n" + "="*60)
    logger.info("未映射列名统计")
    logger.info("="*60)
    for report_type, columns in unmapped_columns.items():
        if columns:
            logger.info(f"\n{report_type}: {len(columns)} 个未映射列名")
            unmapped_logger.warning(f"\n{report_type} 未映射的列名 ({len(columns)} 个):")
            for col in sorted(columns)[:10]:  # 只显示前10个
                logger.info(f"  - {col}")
                unmapped_logger.warning(f"  - {col}")
            if len(columns) > 10:
                logger.info(f"  ... 还有 {len(columns) - 10} 个")
    
    # 生成数据质量报告
    report_file = create_data_quality_report(
        output_dir='output/reports',
        total_stocks=total_stocks,
        success_count=success_count,
        failed_stocks=failed_stocks,
        start_time=start_time,
        end_time=end_time,
        unmapped_columns=unmapped_columns
    )
    
    # 输出总结
    logger.info("\n" + "="*60)
    logger.info("测试完成")
    logger.info("="*60)
    logger.info(f"总股票数: {total_stocks}")
    logger.info(f"成功: {success_count}")
    logger.info(f"失败: {len(failed_stocks)}")
    logger.info(f"成功率: {success_count/total_stocks*100:.2f}%")
    logger.info(f"耗时: {(end_time - start_time).total_seconds():.2f} 秒")
    logger.info(f"数据质量报告: {report_file}")
    logger.info("="*60)


if __name__ == "__main__":
    main()
