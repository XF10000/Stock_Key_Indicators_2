"""
数据更新器主程序

负责从 akshare 获取全部 A 股财务数据并存入数据库
支持断点续传、进度显示和数据质量报告生成
"""
import argparse
from datetime import datetime
from tqdm import tqdm

from data_provider.akshare_client import AkshareClient
from data_provider.repository import Repository
from utils.config_loader import ConfigLoader
from utils.logger import Logger, create_data_quality_report


def main(limit: int = None):
    """
    主函数
    
    Args:
        limit: 限制更新的股票数量（用于测试）
    """
    # 加载配置
    config = ConfigLoader()
    
    # 设置日志
    logger_manager = Logger()
    logger = logger_manager.setup_data_update_logger(
        config.get('logging.data_update_log', 'logs/data_update.log')
    )
    unmapped_logger = logger_manager.setup_unmapped_logger(
        config.get('logging.unmapped_columns_log', 'logs/unmapped_columns.log')
    )
    
    logger.info("="*60)
    logger.info("数据更新器启动")
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
    
    # 生成缓存版本号
    cache_version = start_time.strftime('%Y%m%d_%H%M%S')
    logger.info(f"缓存版本号: {cache_version}")
    
    # 获取股票列表
    logger.info("正在获取股票列表...")
    try:
        stock_list = client.get_stock_list()
        total_stocks = len(stock_list)
        logger.info(f"获取到 {total_stocks} 只股票")
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return
    
    # 限制数量（用于测试）
    if limit:
        stock_list = stock_list.head(limit)
        total_stocks = len(stock_list)
        logger.info(f"测试模式：只更新前 {total_stocks} 只股票")
    
    # 获取已处理的股票（用于断点续传）
    processed_stocks = repository.get_processed_stocks()
    logger.info(f"数据库中已有 {len(processed_stocks)} 只股票的数据")
    
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
    
    with tqdm(total=total_stocks, desc="更新进度") as pbar:
        for idx, row in stock_list.iterrows():
            stock_code = row['market_code']
            stock_name = row['stock_name']
            
            pbar.set_description(f"处理 {stock_code} ({stock_name})")
            
            # 断点续传：跳过已处理的股票
            if stock_code in processed_stocks:
                logger.debug(f"跳过已处理的股票: {stock_code}")
                pbar.update(1)
                continue
            
            try:
                # 保存股票信息（股票代码和公司名称的映射）
                try:
                    repository.save_stock_info(stock_code, stock_name)
                except Exception as e:
                    logger.warning(f"保存股票信息失败 {stock_code}: {e}")
                
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
                    logger.debug(f"{stock_code} 资产负债表: 新增 {added}, 跳过 {skipped}")
                
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
                    logger.debug(f"{stock_code} 利润表: 新增 {added}, 跳过 {skipped}")
                
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
                    logger.debug(f"{stock_code} 现金流量表: 新增 {added}, 跳过 {skipped}")
                
                success_count += 1
                logger.info(f"✓ {stock_code} ({stock_name}) 更新成功")
                
            except Exception as e:
                failed_stocks.append(f"{stock_code} ({stock_name}): {str(e)}")
                logger.error(f"✗ {stock_code} ({stock_name}) 更新失败: {e}")
            
            pbar.update(1)
    
    end_time = datetime.now()
    
    # 记录未映射的列名
    for report_type, columns in unmapped_columns.items():
        if columns:
            unmapped_logger.warning(f"\n{report_type} 未映射的列名 ({len(columns)} 个):")
            for col in sorted(columns):
                unmapped_logger.warning(f"  - {col}")
    
    # 生成数据质量报告
    report_file = create_data_quality_report(
        output_dir=config.get('output.report_dir', 'output/reports'),
        total_stocks=total_stocks,
        success_count=success_count,
        failed_stocks=failed_stocks,
        start_time=start_time,
        end_time=end_time,
        unmapped_columns=unmapped_columns
    )
    
    # 输出总结
    logger.info("="*60)
    logger.info("数据更新完成")
    logger.info("="*60)
    logger.info(f"总股票数: {total_stocks}")
    logger.info(f"成功: {success_count}")
    logger.info(f"失败: {len(failed_stocks)}")
    logger.info(f"成功率: {success_count/total_stocks*100:.2f}%")
    logger.info(f"耗时: {(end_time - start_time).total_seconds():.2f} 秒")
    logger.info(f"数据质量报告: {report_file}")
    logger.info("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A股财务数据更新器')
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='限制更新的股票数量（用于测试）'
    )
    
    args = parser.parse_args()
    main(limit=args.limit)
