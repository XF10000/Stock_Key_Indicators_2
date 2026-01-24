"""
日志记录模块

提供统一的日志记录功能，支持多个日志文件
"""
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime


class Logger:
    """日志记录器"""
    
    def __init__(self, name: str = "stock_analyzer"):
        """
        初始化日志记录器
        
        Args:
            name: 日志记录器名称
        """
        self.name = name
        self._loggers = {}
    
    def get_logger(
        self, 
        log_type: str = "main",
        log_file: Optional[str] = None,
        level: str = "INFO"
    ) -> logging.Logger:
        """
        获取或创建日志记录器
        
        Args:
            log_type: 日志类型（main, data_update, calculation, unmapped_columns）
            log_file: 日志文件路径
            level: 日志级别
            
        Returns:
            日志记录器
        """
        if log_type in self._loggers:
            return self._loggers[log_type]
        
        logger = logging.getLogger(f"{self.name}.{log_type}")
        logger.setLevel(getattr(logging, level.upper()))
        
        # 避免重复添加处理器
        if logger.handlers:
            return logger
        
        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 添加控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 添加文件处理器
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(getattr(logging, level.upper()))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        self._loggers[log_type] = logger
        return logger
    
    def setup_data_update_logger(self, log_file: str = "logs/data_update.log") -> logging.Logger:
        """设置数据更新日志记录器"""
        return self.get_logger("data_update", log_file, "INFO")
    
    def setup_calculation_logger(self, log_file: str = "logs/calculation.log") -> logging.Logger:
        """设置计算过程日志记录器"""
        return self.get_logger("calculation", log_file, "INFO")
    
    def setup_unmapped_logger(self, log_file: str = "logs/unmapped_columns.log") -> logging.Logger:
        """设置未映射列名日志记录器"""
        return self.get_logger("unmapped_columns", log_file, "WARNING")


def create_data_quality_report(
    output_dir: str,
    total_stocks: int,
    success_count: int,
    failed_stocks: list,
    start_time: datetime,
    end_time: datetime,
    unmapped_columns: dict
) -> str:
    """
    创建数据质量报告
    
    Args:
        output_dir: 输出目录
        total_stocks: 总股票数
        success_count: 成功数量
        failed_stocks: 失败的股票列表
        start_time: 开始时间
        end_time: 结束时间
        unmapped_columns: 未映射的列名统计
        
    Returns:
        报告文件路径
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / f"data_quality_report_{start_time.strftime('%Y%m%d_%H%M%S')}.txt"
    
    duration = (end_time - start_time).total_seconds()
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("数据质量报告\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("更新概况\n")
        f.write("-" * 60 + "\n")
        f.write(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"总耗时: {duration:.2f} 秒 ({duration/60:.2f} 分钟)\n")
        f.write(f"尝试更新股票总数: {total_stocks}\n\n")
        
        f.write("成功/失败统计\n")
        f.write("-" * 60 + "\n")
        f.write(f"成功: {success_count}\n")
        f.write(f"失败: {len(failed_stocks)}\n")
        f.write(f"成功率: {success_count/total_stocks*100:.2f}%\n\n")
        
        if failed_stocks:
            f.write("失败的股票列表\n")
            f.write("-" * 60 + "\n")
            for stock in failed_stocks[:50]:  # 只显示前50个
                f.write(f"  - {stock}\n")
            if len(failed_stocks) > 50:
                f.write(f"  ... 还有 {len(failed_stocks) - 50} 只股票\n")
            f.write("\n")
        
        if unmapped_columns:
            f.write("未映射列名统计\n")
            f.write("-" * 60 + "\n")
            for report_type, columns in unmapped_columns.items():
                f.write(f"\n{report_type}:\n")
                for col in sorted(columns)[:20]:  # 只显示前20个
                    f.write(f"  - {col}\n")
                if len(columns) > 20:
                    f.write(f"  ... 还有 {len(columns) - 20} 个列名\n")
            f.write("\n")
        
        f.write("=" * 60 + "\n")
    
    return str(report_file)
