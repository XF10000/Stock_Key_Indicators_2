"""
并发数据更新器

使用线程池并发获取数据，提升更新速度
"""
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm

from data_provider.akshare_client import AkshareClient
from data_provider.repository import Repository
from utils.config_loader import ConfigLoader
from utils.logger import Logger, create_data_quality_report


# 全局锁，用于数据库写入
db_lock = Lock()


def process_single_stock(
    stock_info: dict,
    client: AkshareClient,
    repository: Repository,
    balance_mapping: dict,
    income_mapping: dict,
    cashflow_mapping: dict,
    logger,
    unmapped_logger
) -> dict:
    """
    处理单只股票
    
    Args:
        stock_info: 股票信息字典
        client: API客户端
        repository: 数据库仓储
        balance_mapping: 资产负债表映射
        income_mapping: 利润表映射
        cashflow_mapping: 现金流量表映射
        logger: 日志记录器
        unmapped_logger: 未映射列名日志记录器
        
    Returns:
        处理结果字典
    """
    stock_code = stock_info['market_code']
    stock_name = stock_info['stock_name']
    
    result = {
        'stock_code': stock_code,
        'stock_name': stock_name,
        'success': False,
        'error': None,
        'unmapped_columns': {
            'balance_sheet': set(),
            'income_statement': set(),
            'cash_flow_statement': set()
        }
    }
    
    try:
        # 获取财务数据
        data = client.get_all_financial_data(stock_code)
        
        # 使用锁保护数据库写入
        with db_lock:
            # 保存资产负债表
            if data['balance_sheet'] is not None:
                for col in data['balance_sheet'].columns:
                    if col not in balance_mapping:
                        result['unmapped_columns']['balance_sheet'].add(col)
                
                added, skipped = repository.save_balance_sheets(
                    data['balance_sheet'],
                    balance_mapping
                )
                logger.debug(f"{stock_code} 资产负债表: 新增 {added}, 跳过 {skipped}")
            
            # 保存利润表
            if data['income_statement'] is not None:
                for col in data['income_statement'].columns:
                    if col not in income_mapping:
                        result['unmapped_columns']['income_statement'].add(col)
                
                added, skipped = repository.save_income_statements(
                    data['income_statement'],
                    income_mapping
                )
                logger.debug(f"{stock_code} 利润表: 新增 {added}, 跳过 {skipped}")
            
            # 保存现金流量表
            if data['cash_flow_statement'] is not None:
                for col in data['cash_flow_statement'].columns:
                    if col not in cashflow_mapping:
                        result['unmapped_columns']['cash_flow_statement'].add(col)
                
                added, skipped = repository.save_cash_flow_statements(
                    data['cash_flow_statement'],
                    cashflow_mapping
                )
                logger.debug(f"{stock_code} 现金流量表: 新增 {added}, 跳过 {skipped}")
        
        result['success'] = True
        logger.info(f"✓ {stock_code} ({stock_name}) 更新成功")
        
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"✗ {stock_code} ({stock_name}) 更新失败: {e}")
    
    return result


def main(limit: int = None, max_workers: int = 10):
    """
    主函数
    
    Args:
        limit: 限制更新的股票数量（用于测试）
        max_workers: 最大并发线程数
    """
    # 加载配置
    config = ConfigLoader()
    
    # 设置日志
    logger_manager = Logger()
    logger = logger_manager.setup_data_update_logger(
        config.get('logging.data_update_log', 'logs/data_update_concurrent.log')
    )
    unmapped_logger = logger_manager.setup_unmapped_logger(
        config.get('logging.unmapped_columns_log', 'logs/unmapped_columns.log')
    )
    
    logger.info("="*60)
    logger.info(f"并发数据更新器启动 (并发数: {max_workers})")
    logger.info("="*60)
    
    start_time = datetime.now()
    
    # 初始化客户端和仓储
    client = AkshareClient(
        request_delay=config.api_request_delay / max_workers,  # 调整延迟
        retry_times=config.api_retry_times,
        retry_delay=config.api_retry_delay
    )
    
    database_url = f"sqlite:///{config.database_path}"
    repository = Repository(database_url)
    
    # 使用固定的测试股票列表（避免网络问题）
    import pandas as pd
    test_stocks = pd.DataFrame({
        'stock_code': ['600519', '300750', '000725', '000001', '600036'],
        'stock_name': ['贵州茅台', '宁德时代', '京东方A', '平安银行', '招商银行'],
        'market_code': ['SH600519', 'SZ300750', 'SZ000725', 'SZ000001', 'SH600036']
    })
    
    stock_list = test_stocks
    total_stocks = len(stock_list)
    
    if limit:
        stock_list = stock_list.head(limit)
        total_stocks = len(stock_list)
    
    logger.info(f"测试股票数: {total_stocks}")
    logger.info(f"并发线程数: {max_workers}")
    
    # 获取已处理的股票（用于断点续传）
    processed_stocks = repository.get_processed_stocks()
    logger.info(f"数据库中已有 {len(processed_stocks)} 只股票的数据")
    
    # 过滤已处理的股票
    stock_list = stock_list[~stock_list['market_code'].isin(processed_stocks)]
    logger.info(f"待处理股票数: {len(stock_list)}")
    
    if len(stock_list) == 0:
        logger.info("所有股票已处理完成")
        return
    
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
    
    # 准备股票信息列表
    stock_infos = []
    for idx, row in stock_list.iterrows():
        stock_infos.append({
            'market_code': row['market_code'],
            'stock_name': row['stock_name']
        })
    
    # 使用线程池并发处理
    logger.info("开始并发更新数据...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = {
            executor.submit(
                process_single_stock,
                stock_info,
                client,
                repository,
                balance_mapping,
                income_mapping,
                cashflow_mapping,
                logger,
                unmapped_logger
            ): stock_info for stock_info in stock_infos
        }
        
        # 使用进度条显示进度
        with tqdm(total=len(futures), desc="更新进度") as pbar:
            for future in as_completed(futures):
                result = future.result()
                
                if result['success']:
                    success_count += 1
                    # 合并未映射列名
                    for report_type in unmapped_columns:
                        unmapped_columns[report_type].update(
                            result['unmapped_columns'][report_type]
                        )
                else:
                    failed_stocks.append(
                        f"{result['stock_code']} ({result['stock_name']}): {result['error']}"
                    )
                
                pbar.update(1)
                pbar.set_description(
                    f"成功: {success_count}/{len(futures)}"
                )
    
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
    duration = (end_time - start_time).total_seconds()
    logger.info("="*60)
    logger.info("并发数据更新完成")
    logger.info("="*60)
    logger.info(f"总股票数: {total_stocks}")
    logger.info(f"成功: {success_count}")
    logger.info(f"失败: {len(failed_stocks)}")
    logger.info(f"成功率: {success_count/total_stocks*100:.2f}%")
    logger.info(f"耗时: {duration:.2f} 秒 ({duration/60:.2f} 分钟)")
    logger.info(f"平均每只股票: {duration/total_stocks:.2f} 秒")
    logger.info(f"并发线程数: {max_workers}")
    logger.info(f"数据质量报告: {report_file}")
    logger.info("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A股财务数据并发更新器')
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='限制更新的股票数量（用于测试）'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='并发线程数（默认10）'
    )
    
    args = parser.parse_args()
    main(limit=args.limit, max_workers=args.workers)
