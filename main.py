"""
主程序入口

处理命令行参数，调用Orchestrator执行分析流程
"""
import argparse
import sys
from pathlib import Path

from orchestrator import Orchestrator
from visualization.plotter import Plotter
from utils.logger import Logger


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description='A股上市公司财务指标分析工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py --code SH600519              # 分析贵州茅台
  python main.py --code SZ000858 --years 5   # 分析五粮液，分析近5年数据
  python main.py --code SH600519 --no-excel  # 不生成Excel文件
        """
    )
    
    parser.add_argument(
        '--code',
        type=str,
        required=True,
        help='股票代码（如SH600519、SZ000858）'
    )
    
    parser.add_argument(
        '--years',
        type=int,
        default=10,
        help='分析年限（默认10年）'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='output',
        help='输出目录（默认output）'
    )
    
    parser.add_argument(
        '--no-excel',
        action='store_true',
        help='不生成Excel文件'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='配置文件路径（默认config.yaml）'
    )
    
    args = parser.parse_args()
    
    # 初始化日志
    logger = Logger()
    
    try:
        # 验证股票代码格式
        stock_code = args.code.upper()
        if not (stock_code.startswith('SH') or stock_code.startswith('SZ')):
            logger.log_error(f"股票代码格式错误: {stock_code}")
            logger.log_error("股票代码应以SH或SZ开头，如SH600519、SZ000858")
            sys.exit(1)
        
        logger.log_info("=" * 60)
        logger.log_info(f"开始分析股票: {stock_code}")
        logger.log_info(f"分析年限: {args.years}年")
        logger.log_info("=" * 60)
        
        # 初始化流程调度器
        orchestrator = Orchestrator(config_path=args.config)
        
        # 检查数据库是否已初始化
        if not orchestrator.check_database_ready():
            logger.log_error("数据库为空！")
            logger.log_error("请先运行以下命令更新数据:")
            logger.log_error("  python data_updater_robust.py --workers 10 --batch-size 50 --batch-pause 30")
            logger.log_error("")
            logger.log_error("或者测试模式（仅更新10只股票）:")
            logger.log_error("  python data_updater_robust.py --limit 10 --workers 3 --batch-size 3 --batch-pause 5")
            sys.exit(1)
        
        # 执行分析
        logger.log_info("正在分析...")
        analysis_result = orchestrator.analyze_company(
            stock_code=stock_code,
            years=args.years
        )
        
        if analysis_result is None:
            logger.log_error(f"分析失败: 未找到股票 {stock_code} 的数据")
            logger.log_error("请确认:")
            logger.log_error("  1. 股票代码是否正确")
            logger.log_error("  2. 数据库中是否包含该股票的数据")
            logger.log_error("  3. 是否已运行 data_updater_robust.py 更新数据")
            sys.exit(1)
        
        logger.log_info("分析完成，正在生成报告...")
        
        # 初始化可视化模块
        plotter = Plotter(output_dir=args.output_dir)
        
        # 生成HTML报告
        html_path = plotter.generate_html_report(analysis_result)
        logger.log_info(f"HTML报告已生成: {html_path}")
        
        # 生成Excel文件（如果需要）
        if not args.no_excel:
            excel_path = plotter.export_to_excel(analysis_result)
            logger.log_info(f"Excel文件已生成: {excel_path}")
        
        logger.log_info("=" * 60)
        logger.log_info("分析完成！")
        logger.log_info("=" * 60)
        
        # 打印结果摘要
        indicators = analysis_result['indicators']
        if len(indicators) > 0:
            latest = indicators.iloc[-1]
            logger.log_info(f"\n最新指标（{latest['report_date']}）:")
            
            if 'ar_turnover' in latest and latest['ar_turnover'] is not None:
                logger.log_info(f"  应收账款周转率: {latest['ar_turnover']:.2f} 次")
            
            if 'gross_margin' in latest and latest['gross_margin'] is not None:
                logger.log_info(f"  毛利率: {latest['gross_margin']*100:.2f}%")
            
            if 'lt_asset_turnover' in latest and latest['lt_asset_turnover'] is not None:
                logger.log_info(f"  长期资产周转率: {latest['lt_asset_turnover']:.2f} 次")
            
            if 'working_capital_ratio' in latest and latest['working_capital_ratio'] is not None:
                logger.log_info(f"  营运净资本比率: {latest['working_capital_ratio']*100:.2f}%")
            
            if 'operating_cashflow_ratio' in latest and latest['operating_cashflow_ratio'] is not None:
                logger.log_info(f"  经营现金流比率: {latest['operating_cashflow_ratio']*100:.2f}%")
        
        logger.log_info(f"\n输出文件:")
        logger.log_info(f"  HTML报告: {html_path}")
        if not args.no_excel:
            logger.log_info(f"  Excel数据: {excel_path}")
        
    except KeyboardInterrupt:
        logger.log_info("\n用户中断")
        sys.exit(0)
    except Exception as e:
        logger.log_error(f"发生错误: {str(e)}")
        import traceback
        logger.log_error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
