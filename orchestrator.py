"""
流程调度器模块

协调各模块按顺序工作，控制整体分析流程
"""
from typing import Optional, Dict, List
from datetime import datetime, date
import pandas as pd

from data_provider.repository import Repository
from analysis.calculator import FinancialCalculator
from analysis.analyzer import MarketAnalyzer
from utils.config_loader import ConfigLoader
from utils.logger import Logger


class Orchestrator:
    """流程调度器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化流程调度器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = ConfigLoader(config_path)
        self.logger = Logger()
        
        # 构建数据库URL
        db_path = self.config.database_path
        if not db_path.startswith('sqlite:///'):
            db_url = f"sqlite:///{db_path}"
        else:
            db_url = db_path
        
        self.repository = Repository(db_url)
        self.calculator = FinancialCalculator()
        
        # 生成缓存版本号（时间戳）
        self.cache_version = datetime.now().strftime("%Y%m%d%H%M%S")
        self.analyzer = MarketAnalyzer(self.repository, self.cache_version)
    
    def check_database_ready(self) -> bool:
        """
        检查数据库是否已初始化
        
        Returns:
            True表示数据库已有数据，False表示数据库为空
        """
        session = self.repository.get_session()
        try:
            from models import BalanceSheet
            count = session.query(BalanceSheet).count()
            return count > 0
        finally:
            session.close()
    
    def analyze_company(
        self,
        stock_code: str,
        years: int = 10
    ) -> Dict[str, any]:
        """
        分析目标公司
        
        Args:
            stock_code: 股票代码（如SH600519）
            years: 分析年限（默认10年）
            
        Returns:
            分析结果字典，包含：
            - company_info: 公司基本信息
            - indicators: 各指标的计算结果
            - market_comparison: 与市场的对比数据
        """
        self.logger.log_info(f"开始分析股票: {stock_code}")
        
        # 1. 检查数据库是否为空
        if not self.check_database_ready():
            self.logger.log_error("数据库为空，请先运行 data_updater_robust.py 更新数据")
            return None
        
        # 2. 获取目标公司数据
        company_data = self._get_company_data(stock_code, years)
        if not company_data:
            self.logger.log_error(f"未找到股票 {stock_code} 的数据")
            return None
        
        # 3. 计算指标
        indicators = self._calculate_indicators(company_data)
        
        # 4. 获取市场数据并进行对比
        market_comparison = self._compare_with_market(stock_code, indicators)
        
        # 5. 组装结果
        result = {
            'stock_code': stock_code,
            'company_info': {
                'stock_code': stock_code,
                'analysis_date': datetime.now().strftime("%Y-%m-%d"),
                'data_years': years
            },
            'indicators': indicators,
            'market_comparison': market_comparison
        }
        
        self.logger.log_info(f"分析完成: {stock_code}")
        return result
    
    def _get_company_data(
        self,
        stock_code: str,
        years: int
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        获取目标公司的财务数据
        
        Args:
            stock_code: 股票代码
            years: 年限
            
        Returns:
            包含资产负债表、利润表、现金流量表的字典
        """
        session = self.repository.get_session()
        try:
            from models import BalanceSheet, IncomeStatement, CashFlowStatement
            from datetime import timedelta
            
            # 计算起始日期
            end_date = date.today()
            start_date = date(end_date.year - years, 1, 1)
            
            # 查询资产负债表
            balance_sheets = session.query(BalanceSheet).filter(
                BalanceSheet.stock_code == stock_code,
                BalanceSheet.report_date >= start_date,
                BalanceSheet.report_date <= end_date
            ).order_by(BalanceSheet.report_date).all()
            
            # 查询利润表
            income_statements = session.query(IncomeStatement).filter(
                IncomeStatement.stock_code == stock_code,
                IncomeStatement.report_date >= start_date,
                IncomeStatement.report_date <= end_date
            ).order_by(IncomeStatement.report_date).all()
            
            # 查询现金流量表
            cashflow_statements = session.query(CashFlowStatement).filter(
                CashFlowStatement.stock_code == stock_code,
                CashFlowStatement.report_date >= start_date,
                CashFlowStatement.report_date <= end_date
            ).order_by(CashFlowStatement.report_date).all()
            
            if not balance_sheets or not income_statements or not cashflow_statements:
                return None
            
            # 转换为DataFrame
            balance_df = pd.DataFrame([{
                'report_date': bs.report_date,
                'total_assets': bs.total_assets,
                'non_current_assets': bs.non_current_assets,
                'accounts_receivable': bs.accounts_receivable,
                'notes_receivable': bs.notes_receivable,
                'receivables_financing': bs.receivables_financing,
                'contract_assets': bs.contract_assets,
                'accounts_payable': bs.accounts_payable,
                'notes_payable': bs.notes_payable,
                'contract_liabilities': bs.contract_liabilities,
                'total_equity': bs.total_equity,
                'current_liabilities': bs.current_liabilities
            } for bs in balance_sheets])
            
            income_df = pd.DataFrame([{
                'report_date': inc.report_date,
                'operating_revenue': inc.operating_revenue,
                'operating_cost': inc.operating_cost,
                'net_profit': inc.net_profit
            } for inc in income_statements])
            
            cashflow_df = pd.DataFrame([{
                'report_date': cf.report_date,
                'operating_cashflow': cf.operating_cashflow
            } for cf in cashflow_statements])
            
            return {
                'balance_sheet': balance_df,
                'income_statement': income_df,
                'cashflow_statement': cashflow_df
            }
        finally:
            session.close()
    
    def _calculate_indicators(
        self,
        company_data: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """
        计算所有财务指标
        
        Args:
            company_data: 公司财务数据
            
        Returns:
            各指标的计算结果
        """
        balance_df = company_data['balance_sheet']
        income_df = company_data['income_statement']
        cashflow_df = company_data['cashflow_statement']
        
        # 合并数据
        merged_df = balance_df.merge(income_df, on='report_date', how='outer')
        merged_df = merged_df.merge(cashflow_df, on='report_date', how='outer')
        merged_df = merged_df.sort_values('report_date')
        
        results = []
        
        for i in range(len(merged_df)):
            row = merged_df.iloc[i]
            report_date = row['report_date']
            
            # 获取期初值（上一期的期末值）
            if i > 0:
                prev_row = merged_df.iloc[i - 1]
            else:
                prev_row = None
            
            # 计算TTM收入（需要最近4个季度的单季度收入）
            ttm_revenue = self._calculate_ttm_revenue_for_period(income_df, i)
            
            # 计算各指标
            indicator_row = {
                'report_date': report_date,
            }
            
            # 1. 应收账款周转率
            if ttm_revenue and prev_row is not None:
                ar_turnover = self.calculator.calculate_accounts_receivable_turnover(
                    revenue_ttm=ttm_revenue,
                    ar_begin=prev_row.get('accounts_receivable', 0) or 0,
                    ar_end=row.get('accounts_receivable', 0) or 0
                )
                indicator_row['ar_turnover'] = ar_turnover
            
            # 2. 毛利率
            gross_margin = self.calculator.calculate_gross_profit_margin(
                revenue=row.get('operating_revenue', 0) or 0,
                cost=row.get('operating_cost', 0) or 0
            )
            indicator_row['gross_margin'] = gross_margin
            
            # 3. 长期资产周转率
            if ttm_revenue and prev_row is not None:
                lt_turnover = self.calculator.calculate_long_term_asset_turnover(
                    revenue_ttm=ttm_revenue,
                    noncurrent_assets_begin=prev_row.get('non_current_assets', 0) or 0,
                    noncurrent_assets_end=row.get('non_current_assets', 0) or 0
                )
                indicator_row['lt_asset_turnover'] = lt_turnover
            
            # 4. 营运净资本比率
            wc_ratio = self.calculator.calculate_working_capital_ratio(
                accounts_receivable=row.get('accounts_receivable', 0),
                notes_receivable=row.get('notes_receivable', 0),
                receivables_financing=row.get('receivables_financing', 0),
                contract_assets=row.get('contract_assets', 0),
                accounts_payable=row.get('accounts_payable', 0),
                notes_payable=row.get('notes_payable', 0),
                contract_liabilities=row.get('contract_liabilities', 0),
                total_assets=row.get('total_assets', 0) or 0
            )
            indicator_row['working_capital_ratio'] = wc_ratio
            
            # 5. 经营现金流比率
            ocf_ratio = self.calculator.calculate_operating_cashflow_ratio(
                operating_cashflow=row.get('operating_cashflow', 0) or 0,
                total_assets=row.get('total_assets', 0) or 0
            )
            indicator_row['operating_cashflow_ratio'] = ocf_ratio
            
            results.append(indicator_row)
        
        return pd.DataFrame(results)
    
    def _calculate_ttm_revenue_for_period(
        self,
        income_df: pd.DataFrame,
        current_index: int
    ) -> Optional[float]:
        """
        计算指定期间的TTM收入
        
        Args:
            income_df: 利润表数据
            current_index: 当前期间索引
            
        Returns:
            TTM收入
        """
        if current_index < 3:
            return None
        
        # 获取最近4个季度的累计收入
        quarterly_revenues = []
        for i in range(current_index - 3, current_index + 1):
            cumulative = income_df.iloc[i]['operating_revenue']
            
            # 转换为单季度值
            if i > 0 and income_df.iloc[i]['report_date'].month != 3:
                # 非Q1，需要减去上一季度
                prev_cumulative = income_df.iloc[i - 1]['operating_revenue']
                quarterly = self.calculator.convert_cumulative_to_quarterly(
                    cumulative, prev_cumulative
                )
            else:
                # Q1，累计值就是单季度值
                quarterly = cumulative
            
            quarterly_revenues.append(quarterly)
        
        return self.calculator.calculate_ttm_revenue(quarterly_revenues[::-1])
    
    def _compare_with_market(
        self,
        stock_code: str,
        indicators: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        """
        与市场进行对比分析
        
        Args:
            stock_code: 股票代码
            indicators: 公司指标数据
            
        Returns:
            市场对比数据
        """
        # TODO: 实现市场对比逻辑
        # 这需要获取全市场数据并计算中位数、分位数等
        # 暂时返回空字典
        return {}
