"""
健壮的并发数据更新器

特性：
1. 超时机制：API调用设置超时时间，避免长时间挂起
2. 批次暂停：每处理N只股票后暂停，避免API限流
3. 断点续传：自动跳过已处理的股票，支持中断后继续
4. 进度持久化：实时保存处理进度到文件
"""
import argparse
import json
import signal
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from threading import Lock
from pathlib import Path
from tqdm import tqdm
import time

from data_provider.akshare_client import AkshareClient
from data_provider.repository import Repository
from utils.config_loader import ConfigLoader
from utils.logger import Logger, create_data_quality_report


# 全局锁
db_lock = Lock()
progress_lock = Lock()

# 全局变量用于优雅退出
should_stop = False


def signal_handler(signum, frame):
    """处理中断信号（Ctrl+C）"""
    global should_stop
    print("\n\n收到中断信号，正在优雅退出...")
    print("当前进度已保存，下次运行将从断点继续")
    should_stop = True


# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class ProgressTracker:
    """进度跟踪器"""
    
    def __init__(self, progress_file: str = "progress.json"):
        self.progress_file = Path(progress_file)
        self.processed_stocks = set()
        self.failed_stocks = {}
        self.load_progress()
    
    def load_progress(self):
        """加载进度"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_stocks = set(data.get('processed', []))
                    self.failed_stocks = data.get('failed', {})
            except Exception:
                pass
    
    def save_progress(self):
        """保存进度"""
        with progress_lock:
            data = {
                'processed': list(self.processed_stocks),
                'failed': self.failed_stocks,
                'last_update': datetime.now().isoformat()
            }
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    
    def mark_processed(self, stock_code: str):
        """标记为已处理"""
        with progress_lock:
            self.processed_stocks.add(stock_code)
            if stock_code in self.failed_stocks:
                del self.failed_stocks[stock_code]
            self.save_progress()
    
    def mark_failed(self, stock_code: str, error: str):
        """标记为失败"""
        with progress_lock:
            self.failed_stocks[stock_code] = error
            self.save_progress()
    
    def is_processed(self, stock_code: str) -> bool:
        """检查是否已处理"""
        return stock_code in self.processed_stocks


def process_single_stock(
    stock_info: dict,
    client: AkshareClient,
    repository: Repository,
    balance_mapping: dict,
    income_mapping: dict,
    cashflow_mapping: dict,
    logger,
    timeout: float = 60.0
) -> dict:
    """
    处理单只股票（带超时）
    
    Args:
        stock_info: 股票信息
        client: API客户端
        repository: 数据库仓储
        balance_mapping: 资产负债表映射
        income_mapping: 利润表映射
        cashflow_mapping: 现金流量表映射
        logger: 日志记录器
        timeout: 超时时间（秒）
        
    Returns:
        处理结果
    """
    stock_code = stock_info['market_code']
    stock_name = stock_info['stock_name']
    
    result = {
        'stock_code': stock_code,
        'stock_name': stock_name,
        'success': False,
        'error': None,
        'records_saved': 0
    }
    
    try:
        # 获取财务数据（这里已经有重试机制）
        logger.debug(f"开始获取 {stock_code} ({stock_name}) 的财务数据...")
        start_time = time.time()
        
        data = client.get_all_financial_data(stock_code)
        
        fetch_time = time.time() - start_time
        logger.debug(f"{stock_code} 数据获取完成，耗时 {fetch_time:.2f}秒")
        
        records_saved = 0
        
        # 使用锁保护数据库写入
        logger.debug(f"{stock_code} 等待数据库锁...")
        with db_lock:
            logger.debug(f"{stock_code} 获得数据库锁，开始保存数据...")
            save_start = time.time()
            
            # 保存资产负债表
            if data['balance_sheet'] is not None:
                logger.debug(f"{stock_code} 保存资产负债表...")
                added, skipped = repository.save_balance_sheets(
                    data['balance_sheet'],
                    balance_mapping
                )
                records_saved += added
                logger.debug(f"{stock_code} 资产负债表: 新增{added}条, 跳过{skipped}条")
            
            # 保存利润表
            if data['income_statement'] is not None:
                logger.debug(f"{stock_code} 保存利润表...")
                added, skipped = repository.save_income_statements(
                    data['income_statement'],
                    income_mapping
                )
                records_saved += added
                logger.debug(f"{stock_code} 利润表: 新增{added}条, 跳过{skipped}条")
            
            # 保存现金流量表
            if data['cash_flow_statement'] is not None:
                logger.debug(f"{stock_code} 保存现金流量表...")
                added, skipped = repository.save_cash_flow_statements(
                    data['cash_flow_statement'],
                    cashflow_mapping
                )
                records_saved += added
                logger.debug(f"{stock_code} 现金流量表: 新增{added}条, 跳过{skipped}条")
            
            save_time = time.time() - save_start
            logger.debug(f"{stock_code} 数据保存完成，耗时 {save_time:.2f}秒")
        
        total_time = time.time() - start_time
        result['success'] = True
        result['records_saved'] = records_saved
        logger.info(f"✓ {stock_code} ({stock_name}) 保存 {records_saved} 条记录 (总耗时: {total_time:.2f}秒)")
        
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"✗ {stock_code} ({stock_name}) 失败: {e}")
        import traceback
        logger.debug(f"{stock_code} 错误堆栈:\n{traceback.format_exc()}")
    
    return result


def main(
    limit: int = None,
    max_workers: int = 10,
    batch_size: int = 50,
    batch_pause: float = 30.0,
    resume: bool = True
):
    """
    主函数
    
    Args:
        limit: 限制更新的股票数量
        max_workers: 最大并发线程数
        batch_size: 批次大小（每处理N只股票后暂停）
        batch_pause: 批次暂停时间（秒）
        resume: 是否从断点续传
    """
    # 加载配置
    config = ConfigLoader()
    
    # 设置日志（DEBUG级别以查看详细信息）
    log_manager = Logger()
    logger = log_manager.get_logger(
        "data_update",
        "logs/data_update_robust.log",
        "DEBUG"
    )
    logger.info("="*60)
    logger.info("健壮的并发数据更新器启动")
    logger.info("="*60)
    logger.info(f"配置: 并发数={max_workers}, 批次大小={batch_size}, 批次暂停={batch_pause}秒")
    
    start_time = datetime.now()
    
    # 初始化进度跟踪器
    progress_tracker = ProgressTracker('progress_robust.json')
    logger.info(f"已处理股票数: {len(progress_tracker.processed_stocks)}")
    logger.info(f"失败股票数: {len(progress_tracker.failed_stocks)}")
    
    # 初始化客户端和仓储
    client = AkshareClient(
        request_delay=config.api_request_delay / max_workers,
        retry_times=config.api_retry_times,
        retry_delay=config.api_retry_delay,
        timeout=30.0  # 30秒超时
    )
    
    database_url = f"sqlite:///{config.database_path}"
    repository = Repository(database_url)
    
    # 获取全A股股票列表（带重试机制）
    logger.info("正在获取A股股票列表...")
    max_retries = 5
    retry_delay = 10
    
    for retry in range(max_retries):
        try:
            all_stocks_df = client.get_stock_list()
            logger.info(f"获取到 {len(all_stocks_df)} 只股票")
            
            # 过滤掉9字头的北交所企业（数据通常缺失）
            all_stocks_df['stock_code_prefix'] = all_stocks_df['stock_code'].str[0]
            before_filter = len(all_stocks_df)
            all_stocks_df = all_stocks_df[all_stocks_df['stock_code_prefix'] != '9'].copy()
            filtered_count = before_filter - len(all_stocks_df)
            if filtered_count > 0:
                logger.info(f"过滤掉 {filtered_count} 只北交所企业（9字头）")
            
            # 删除临时列
            all_stocks_df = all_stocks_df.drop(columns=['stock_code_prefix'])
            
            stock_list = all_stocks_df
            logger.info(f"最终股票数: {len(stock_list)}")
            break
        except Exception as e:
            if retry < max_retries - 1:
                logger.warning(f"获取股票列表失败（尝试 {retry+1}/{max_retries}）: {e}")
                logger.info(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                logger.error(f"获取股票列表失败，已重试 {max_retries} 次: {e}")
                return
    
    if limit:
        stock_list = stock_list.head(limit)
    
    total_stocks = len(stock_list)
    logger.info(f"股票总数: {total_stocks}")
    
    # 过滤已处理的股票（断点续传）
    if resume:
        stock_list = stock_list[~stock_list['market_code'].isin(progress_tracker.processed_stocks)]
        logger.info(f"断点续传: 跳过 {total_stocks - len(stock_list)} 只已处理股票")
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
    failed_count = 0
    total_records = 0
    
    # 准备股票信息列表
    stock_infos = []
    for idx, row in stock_list.iterrows():
        stock_infos.append({
            'market_code': row['market_code'],
            'stock_name': row['stock_name']
        })
    
    # 分批处理
    logger.info("开始分批并发更新...")
    
    for batch_start in range(0, len(stock_infos), batch_size):
        if should_stop:
            logger.info("收到停止信号，退出处理循环")
            break
        
        batch_end = min(batch_start + batch_size, len(stock_infos))
        batch = stock_infos[batch_start:batch_end]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(stock_infos) + batch_size - 1) // batch_size
        
        logger.info(f"\n{'='*60}")
        logger.info(f"处理批次 {batch_num}/{total_batches} ({len(batch)} 只股票)")
        logger.info(f"{'='*60}")
        
        # 使用线程池并发处理当前批次
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                    60.0  # 60秒超时
                ): stock_info for stock_info in batch
            }
            
            # 处理完成的任务
            with tqdm(total=len(futures), desc=f"批次 {batch_num}") as pbar:
                for future in as_completed(futures):
                    if should_stop:
                        break
                    
                    stock_info = futures[future]
                    
                    try:
                        result = future.result(timeout=70.0)  # 比内部超时多10秒
                        
                        if result['success']:
                            success_count += 1
                            total_records += result['records_saved']
                            progress_tracker.mark_processed(result['stock_code'])
                        else:
                            failed_count += 1
                            progress_tracker.mark_failed(
                                result['stock_code'],
                                result['error']
                            )
                        
                    except TimeoutError:
                        failed_count += 1
                        error_msg = "处理超时"
                        logger.error(f"✗ {stock_info['market_code']} 超时")
                        progress_tracker.mark_failed(
                            stock_info['market_code'],
                            error_msg
                        )
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"✗ {stock_info['market_code']} 异常: {e}")
                        progress_tracker.mark_failed(
                            stock_info['market_code'],
                            str(e)
                        )
                    
                    pbar.update(1)
                    pbar.set_description(
                        f"批次 {batch_num} [成功:{success_count} 失败:{failed_count}]"
                    )
        
        # 批次完成日志
        logger.info(f"批次 {batch_num} 完成: 成功 {success_count - (batch_start // batch_size) * batch_size} 只")
        
        # 批次间暂停（避免API限流）
        if batch_end < len(stock_infos) and not should_stop:
            logger.info(f"批次完成，暂停 {batch_pause} 秒以避免API限流...")
            pause_start = time.time()
            time.sleep(batch_pause)
            pause_duration = time.time() - pause_start
            logger.info(f"暂停完成，实际暂停 {pause_duration:.2f} 秒")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # 输出总结
    logger.info("\n" + "="*60)
    logger.info("更新完成")
    logger.info("="*60)
    logger.info(f"处理股票数: {success_count + failed_count}")
    logger.info(f"成功: {success_count}")
    logger.info(f"失败: {failed_count}")
    logger.info(f"成功率: {success_count/(success_count+failed_count)*100:.2f}%")
    logger.info(f"保存记录数: {total_records}")
    logger.info(f"耗时: {duration:.2f} 秒 ({duration/60:.2f} 分钟)")
    if success_count > 0:
        logger.info(f"平均每只股票: {duration/success_count:.2f} 秒")
    logger.info(f"进度文件: progress_robust.json")
    logger.info("="*60)
    
    # 如果有失败的股票，提示可以重试
    if failed_count > 0:
        logger.info(f"\n提示: 有 {failed_count} 只股票处理失败")
        logger.info("可以重新运行程序，将自动重试失败的股票")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='健壮的A股财务数据更新器')
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='限制更新的股票数量'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='并发线程数（默认10）'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='批次大小，每处理N只股票后暂停（默认50）'
    )
    parser.add_argument(
        '--batch-pause',
        type=float,
        default=30.0,
        help='批次暂停时间（秒，默认30）'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='不使用断点续传，从头开始'
    )
    
    args = parser.parse_args()
    
    main(
        limit=args.limit,
        max_workers=args.workers,
        batch_size=args.batch_size,
        batch_pause=args.batch_pause,
        resume=not args.no_resume
    )
