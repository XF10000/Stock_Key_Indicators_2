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
        logger_manager = Logger()
        self.logger = logger_manager.get_logger("orchestrator", "logs/orchestrator.log")
        
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
        self.logger.info(f"开始分析股票: {stock_code}")
        
        # 1. 检查数据库是否为空
        if not self.check_database_ready():
            self.logger.error("数据库为空，请先运行 data_updater_robust.py 更新数据")
            return None
        
        # 2. 获取目标公司数据
        company_data = self._get_company_data(stock_code, years)
        if not company_data:
            self.logger.error(f"未找到股票 {stock_code} 的数据")
            return None
        
        # 3. 计算指标
        indicators = self._calculate_indicators(company_data)
        
        # 4. 获取市场数据并进行对比
        market_comparison = self._compare_with_market(stock_code, indicators)
        
        # 获取公司名称
        stock_name = self._get_stock_name(stock_code)
        
        # 5. 组装结果
        result = {
            'stock_code': stock_code,
            'company_info': {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'analysis_date': datetime.now().strftime("%Y-%m-%d"),
                'data_years': years
            },
            'indicators': indicators,
            'market_comparison': market_comparison
        }
        
        self.logger.info(f"分析完成: {stock_code}")
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
                'total_equity': bs.total_owners_equity,
                'current_liabilities': bs.current_liabilities,
                # 长期经营资产组成字段
                'fixed_assets_net': bs.fixed_assets_net,
                'construction_in_progress': bs.construction_in_progress,
                'productive_biological_assets': bs.productive_biological_assets,
                'consumptive_biological_assets': bs.consumptive_biological_assets,
                'oil_and_gas_assets': bs.oil_and_gas_assets,
                'right_of_use_assets': bs.right_of_use_assets,
                'intangible_assets': bs.intangible_assets,
                'development_expenditure': bs.development_expenditure,
                'goodwill': bs.goodwill,
                'long_term_deferred_expenses': bs.long_term_deferred_expenses,
                'other_non_current_assets': bs.other_non_current_assets
            } for bs in balance_sheets])
            
            income_df = pd.DataFrame([{
                'report_date': inc.report_date,
                'operating_revenue': inc.total_operating_revenue,
                'operating_cost': inc.total_operating_costs,
                'net_profit': inc.net_profit
            } for inc in income_statements])
            
            cashflow_df = pd.DataFrame([{
                'report_date': cf.report_date,
                'operating_cashflow': cf.net_cash_flows_from_operating_activities
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
            
            # 1. 应收账款周转率（取对数）
            ar_turnover = None
            if ttm_revenue and prev_row is not None:
                ar_turnover = self.calculator.calculate_accounts_receivable_turnover(
                    revenue_ttm=ttm_revenue,
                    ar_begin=prev_row.get('accounts_receivable', 0) or 0,
                    ar_end=row.get('accounts_receivable', 0) or 0
                )
                # 对周转率取对数
                if ar_turnover is not None and ar_turnover > 0:
                    import numpy as np
                    ar_turnover = np.log(ar_turnover)
            indicator_row['ar_turnover'] = ar_turnover
            
            # 2. 毛利率
            gross_margin = self.calculator.calculate_gross_profit_margin(
                revenue=row.get('operating_revenue', 0) or 0,
                cost=row.get('operating_cost', 0) or 0
            )
            indicator_row['gross_margin'] = gross_margin
            
            # 3. 长期资产周转率（取对数）
            lt_turnover = None
            if ttm_revenue and prev_row is not None:
                # 计算长期经营资产 = 固定资产 + 在建工程 + 生产性生物资产 + 公益性生物资产 + 
                #                   油气资产 + 使用权资产 + 无形资产 + 开发支出 + 
                #                   商誉 + 长期待摊费用 + 其他非流动资产
                import pandas as pd
                def calc_long_term_operating_assets(row_data):
                    def safe_get(key):
                        val = row_data.get(key, 0) if isinstance(row_data, dict) else row_data[key] if key in row_data.index else 0
                        return 0 if pd.isna(val) else val
                    
                    return (
                        safe_get('fixed_assets_net') +
                        safe_get('construction_in_progress') +
                        safe_get('productive_biological_assets') +
                        safe_get('consumptive_biological_assets') +
                        safe_get('oil_and_gas_assets') +
                        safe_get('right_of_use_assets') +
                        safe_get('intangible_assets') +
                        safe_get('development_expenditure') +
                        safe_get('goodwill') +
                        safe_get('long_term_deferred_expenses') +
                        safe_get('other_non_current_assets')
                    )
                
                lt_assets_begin = calc_long_term_operating_assets(prev_row)
                lt_assets_end = calc_long_term_operating_assets(row)
                
                lt_turnover = self.calculator.calculate_long_term_asset_turnover(
                    revenue_ttm=ttm_revenue,
                    long_term_operating_assets_begin=lt_assets_begin,
                    long_term_operating_assets_end=lt_assets_end
                )
                # 对周转率取对数
                if lt_turnover is not None and lt_turnover > 0:
                    import numpy as np
                    lt_turnover = np.log(lt_turnover)
            indicator_row['lt_asset_turnover'] = lt_turnover
            
            # 4. 营运净资本比率
            # 使用pd.isna()检查NaN值，将NaN转换为0
            import pandas as pd
            def get_value_or_zero(series_row, key):
                val = series_row.get(key, 0)
                return 0 if pd.isna(val) else val
            
            wc_ratio = self.calculator.calculate_working_capital_ratio(
                accounts_receivable=get_value_or_zero(row, 'accounts_receivable'),
                notes_receivable=get_value_or_zero(row, 'notes_receivable'),
                receivables_financing=get_value_or_zero(row, 'receivables_financing'),
                contract_assets=get_value_or_zero(row, 'contract_assets'),
                accounts_payable=get_value_or_zero(row, 'accounts_payable'),
                notes_payable=get_value_or_zero(row, 'notes_payable'),
                contract_liabilities=get_value_or_zero(row, 'contract_liabilities'),
                total_assets=get_value_or_zero(row, 'total_assets')
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
        
        # 获取最近4个季度的单季度收入
        quarterly_revenues = []
        
        for i in range(current_index - 3, current_index + 1):
            current_date = income_df.iloc[i]['report_date']
            cumulative = income_df.iloc[i]['operating_revenue']
            
            if cumulative is None:
                return None
            
            # 判断是否为Q1（3月31日）
            import pandas as pd
            current_month = pd.to_datetime(current_date).month
            
            if current_month == 3:
                # Q1，累计值就是单季度值
                quarterly = cumulative
            else:
                # Q2/Q3/Q4，需要减去同年上一季度的累计值
                # 找到同年上一季度的数据
                prev_quarter_found = False
                for j in range(i - 1, -1, -1):
                    prev_date = income_df.iloc[j]['report_date']
                    prev_year = pd.to_datetime(prev_date).year
                    current_year = pd.to_datetime(current_date).year
                    
                    # 必须是同一年的上一季度
                    if prev_year == current_year:
                        prev_cumulative = income_df.iloc[j]['operating_revenue']
                        if prev_cumulative is not None:
                            quarterly = cumulative - prev_cumulative
                            prev_quarter_found = True
                            break
                
                if not prev_quarter_found:
                    # 如果找不到同年上一季度，可能是年度第一个报告期，当作Q1处理
                    quarterly = cumulative
            
            if quarterly is None or quarterly < 0:
                return None
                
            quarterly_revenues.append(quarterly)
        
        # 计算TTM（最近4个季度的单季度收入之和）
        if len(quarterly_revenues) == 4 and all(q is not None for q in quarterly_revenues):
            return sum(quarterly_revenues)
        
        return None
    
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
            市场对比数据，包含各指标的中位数、分位数和分布信息
        """
        self.logger.info("开始计算市场对比数据...")
        
        # 指标列表
        indicator_columns = [
            'ar_turnover',
            'gross_margin', 
            'lt_asset_turnover',
            'working_capital_ratio',
            'operating_cashflow_ratio'
        ]
        
        market_comparison = {}
        
        for indicator_col in indicator_columns:
            self.logger.info(f"处理指标: {indicator_col}")
            
            # 获取目标公司该指标的历史数据
            company_data = indicators[['report_date', indicator_col]].copy()
            company_data = company_data[company_data[indicator_col].notna()]
            
            if len(company_data) == 0:
                continue
            
            # 为每个报告期计算市场数据
            comparison_results = []
            
            for _, row in company_data.iterrows():
                report_date = row['report_date']
                company_value = row[indicator_col]
                
                # 获取全市场该报告期的数据
                market_values = self._get_market_indicator_values(
                    indicator_col,
                    report_date
                )
                
                if market_values is None or len(market_values) == 0:
                    continue
                
                # 计算市场中位数
                market_median = self.analyzer.calculate_market_median(
                    indicator_col,
                    report_date,
                    market_values
                )
                
                # 计算分位数
                percentile = self.analyzer.calculate_percentile(
                    company_value,
                    market_values
                )
                
                comparison_results.append({
                    'report_date': report_date,
                    'company_value': company_value,
                    'market_median': market_median,
                    'percentile': percentile
                })
            
            if comparison_results:
                market_comparison[indicator_col] = pd.DataFrame(comparison_results)
                
                # 计算最新一期的市场分布
                latest_date = company_data['report_date'].max()
                latest_market_values = self._get_market_indicator_values(
                    indicator_col,
                    latest_date
                )
                
                if latest_market_values:
                    distribution = self.analyzer.calculate_distribution(latest_market_values)
                    market_comparison[f'{indicator_col}_distribution'] = distribution
        
        self.logger.info("市场对比数据计算完成")
        return market_comparison
    
    def _get_stock_name(self, stock_code: str) -> str:
        """
        从数据库获取股票名称
        
        Args:
            stock_code: 股票代码
            
        Returns:
            股票名称
        """
        session = self.repository.get_session()
        try:
            from models import StockInfo
            
            # 从数据库中查询股票名称
            result = session.query(StockInfo.stock_name).filter(
                StockInfo.stock_code == stock_code
            ).first()
            
            if result and result.stock_name:
                return result.stock_name
                
        except Exception as e:
            self.logger.warning(f"从数据库获取股票名称失败: {e}")
        finally:
            session.close()
        
        # 如果数据库中没有，返回股票代码
        return stock_code
    
    def _get_market_indicator_values(
        self,
        indicator_col: str,
        report_date: date
    ) -> List[float]:
        """
        获取全市场某个指标在指定报告期的值
        
        Args:
            indicator_col: 指标列名
            report_date: 报告日期
            
        Returns:
            全市场指标值列表
        """
        session = self.repository.get_session()
        try:
            from models import BalanceSheet, IncomeStatement, CashFlowStatement
            
            # 根据指标类型查询不同的表
            if indicator_col == 'ar_turnover':
                # 应收账款周转率 = 营业收入 / 平均应收账款
                # 简化处理：使用当期营业收入 / 当期应收账款
                balance_data = session.query(
                    BalanceSheet.stock_code,
                    BalanceSheet.accounts_receivable
                ).filter(
                    BalanceSheet.report_date == report_date
                ).all()
                
                income_data = session.query(
                    IncomeStatement.stock_code,
                    IncomeStatement.operating_revenue
                ).filter(
                    IncomeStatement.report_date == report_date
                ).all()
                
                # 合并数据
                balance_dict = {row.stock_code: row.accounts_receivable for row in balance_data}
                
                values = []
                for row in income_data:
                    ar = balance_dict.get(row.stock_code)
                    if ar and ar > 0 and row.operating_revenue and row.operating_revenue > 0:
                        turnover = row.operating_revenue / ar
                        # 对周转率取对数
                        if turnover > 0:
                            import numpy as np
                            turnover = np.log(turnover)
                            values.append(turnover)
                
                return values
                
            elif indicator_col == 'lt_asset_turnover':
                # 长期资产周转率 = 营业收入 / 长期经营资产
                # 长期经营资产 = 固定资产 + 在建工程 + 生产性生物资产 + 公益性生物资产 + 
                #               油气资产 + 使用权资产 + 无形资产 + 开发支出 + 
                #               商誉 + 长期待摊费用 + 其他非流动资产
                balance_data = session.query(
                    BalanceSheet.stock_code,
                    BalanceSheet.fixed_assets_net,
                    BalanceSheet.construction_in_progress,
                    BalanceSheet.productive_biological_assets,
                    BalanceSheet.consumptive_biological_assets,
                    BalanceSheet.oil_and_gas_assets,
                    BalanceSheet.right_of_use_assets,
                    BalanceSheet.intangible_assets,
                    BalanceSheet.development_expenditure,
                    BalanceSheet.goodwill,
                    BalanceSheet.long_term_deferred_expenses,
                    BalanceSheet.other_non_current_assets
                ).filter(
                    BalanceSheet.report_date == report_date
                ).all()
                
                income_data = session.query(
                    IncomeStatement.stock_code,
                    IncomeStatement.operating_revenue
                ).filter(
                    IncomeStatement.report_date == report_date
                ).all()
                
                # 计算长期经营资产
                balance_dict = {}
                for row in balance_data:
                    lt_operating_assets = (
                        (row.fixed_assets_net or 0) +
                        (row.construction_in_progress or 0) +
                        (row.productive_biological_assets or 0) +
                        (row.consumptive_biological_assets or 0) +
                        (row.oil_and_gas_assets or 0) +
                        (row.right_of_use_assets or 0) +
                        (row.intangible_assets or 0) +
                        (row.development_expenditure or 0) +
                        (row.goodwill or 0) +
                        (row.long_term_deferred_expenses or 0) +
                        (row.other_non_current_assets or 0)
                    )
                    balance_dict[row.stock_code] = lt_operating_assets
                
                values = []
                for row in income_data:
                    lt_assets = balance_dict.get(row.stock_code)
                    if lt_assets and lt_assets > 0 and row.operating_revenue and row.operating_revenue > 0:
                        turnover = row.operating_revenue / lt_assets
                        # 对周转率取对数
                        if turnover > 0:
                            import numpy as np
                            turnover = np.log(turnover)
                            values.append(turnover)
                
                return values
                
            elif indicator_col == 'gross_margin':
                # 只需要利润表数据
                income_data = session.query(
                    IncomeStatement.stock_code,
                    IncomeStatement.total_operating_revenue,
                    IncomeStatement.total_operating_costs
                ).filter(
                    IncomeStatement.report_date == report_date
                ).all()
                
                values = []
                for row in income_data:
                    if row.total_operating_revenue and row.total_operating_costs and row.total_operating_revenue > 0:
                        margin = (row.total_operating_revenue - row.total_operating_costs) / row.total_operating_revenue
                        values.append(margin)
                
                return values
                
            elif indicator_col == 'working_capital_ratio':
                # 需要资产负债表数据
                balance_data = session.query(
                    BalanceSheet.stock_code,
                    BalanceSheet.accounts_receivable,
                    BalanceSheet.notes_receivable,
                    BalanceSheet.receivables_financing,
                    BalanceSheet.contract_assets,
                    BalanceSheet.accounts_payable,
                    BalanceSheet.notes_payable,
                    BalanceSheet.contract_liabilities,
                    BalanceSheet.total_assets
                ).filter(
                    BalanceSheet.report_date == report_date
                ).all()
                
                values = []
                for row in balance_data:
                    if row.total_assets and row.total_assets > 0:
                        ratio = self.calculator.calculate_working_capital_ratio(
                            accounts_receivable=row.accounts_receivable,
                            notes_receivable=row.notes_receivable,
                            receivables_financing=row.receivables_financing,
                            contract_assets=row.contract_assets,
                            accounts_payable=row.accounts_payable,
                            notes_payable=row.notes_payable,
                            contract_liabilities=row.contract_liabilities,
                            total_assets=row.total_assets
                        )
                        if ratio is not None:
                            values.append(ratio)
                
                return values
                
            elif indicator_col == 'operating_cashflow_ratio':
                # 需要现金流量表和资产负债表数据
                cashflow_data = session.query(
                    CashFlowStatement.stock_code,
                    CashFlowStatement.net_cash_flows_from_operating_activities
                ).filter(
                    CashFlowStatement.report_date == report_date
                ).all()
                
                balance_data = session.query(
                    BalanceSheet.stock_code,
                    BalanceSheet.total_assets
                ).filter(
                    BalanceSheet.report_date == report_date
                ).all()
                
                # 合并数据
                balance_dict = {row.stock_code: row.total_assets for row in balance_data}
                
                values = []
                for row in cashflow_data:
                    total_assets = balance_dict.get(row.stock_code)
                    if total_assets and total_assets > 0 and row.net_cash_flows_from_operating_activities is not None:
                        ratio = row.net_cash_flows_from_operating_activities / total_assets
                        values.append(ratio)
                
                return values
            
            return []
            
        finally:
            session.close()
