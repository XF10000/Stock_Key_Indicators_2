"""
财务指标计算器单元测试
"""
import pytest
from analysis.calculator import FinancialCalculator


class TestFinancialCalculator:
    """测试财务指标计算器"""
    
    def setup_method(self):
        """每个测试方法前执行"""
        self.calculator = FinancialCalculator()
    
    # ==================== ROE 测试 ====================
    
    def test_calculate_roe_normal(self):
        """测试ROE正常计算"""
        roe = self.calculator.calculate_roe(
            net_profit=1000,
            equity_begin=8000,
            equity_end=10000
        )
        assert roe == pytest.approx(1000 / 9000, rel=1e-6)
    
    def test_calculate_roe_zero_equity(self):
        """测试ROE除数为零的情况"""
        roe = self.calculator.calculate_roe(
            net_profit=1000,
            equity_begin=0,
            equity_end=0
        )
        assert roe is None
    
    def test_calculate_roe_negative_profit(self):
        """测试ROE负利润的情况"""
        roe = self.calculator.calculate_roe(
            net_profit=-500,
            equity_begin=8000,
            equity_end=10000
        )
        assert roe == pytest.approx(-500 / 9000, rel=1e-6)
    
    def test_calculate_roe_none_input(self):
        """测试ROE输入为None的情况"""
        roe = self.calculator.calculate_roe(
            net_profit=None,
            equity_begin=8000,
            equity_end=10000
        )
        assert roe is None
    
    # ==================== 应收账款周转率测试 ====================
    
    def test_calculate_ar_turnover_normal(self):
        """测试应收账款周转率正常计算"""
        turnover = self.calculator.calculate_accounts_receivable_turnover(
            revenue_ttm=10000,
            ar_begin=1000,
            ar_end=1200
        )
        assert turnover == pytest.approx(10000 / 1100, rel=1e-6)
    
    def test_calculate_ar_turnover_zero_ar(self):
        """测试应收账款为零的情况"""
        turnover = self.calculator.calculate_accounts_receivable_turnover(
            revenue_ttm=10000,
            ar_begin=0,
            ar_end=0
        )
        assert turnover is None
    
    def test_calculate_ar_turnover_high_value(self):
        """测试高周转率的情况"""
        turnover = self.calculator.calculate_accounts_receivable_turnover(
            revenue_ttm=100000,
            ar_begin=100,
            ar_end=100
        )
        assert turnover == pytest.approx(1000, rel=1e-6)
    
    # ==================== 毛利率测试 ====================
    
    def test_calculate_gross_margin_normal(self):
        """测试毛利率正常计算"""
        margin = self.calculator.calculate_gross_profit_margin(
            revenue=10000,
            cost=7000
        )
        assert margin == pytest.approx(0.3, rel=1e-6)
    
    def test_calculate_gross_margin_zero_revenue(self):
        """测试收入为零的情况"""
        margin = self.calculator.calculate_gross_profit_margin(
            revenue=0,
            cost=7000
        )
        assert margin is None
    
    def test_calculate_gross_margin_negative(self):
        """测试负毛利率的情况（成本高于收入）"""
        margin = self.calculator.calculate_gross_profit_margin(
            revenue=10000,
            cost=12000
        )
        assert margin == pytest.approx(-0.2, rel=1e-6)
    
    def test_calculate_gross_margin_100_percent(self):
        """测试100%毛利率的情况"""
        margin = self.calculator.calculate_gross_profit_margin(
            revenue=10000,
            cost=0
        )
        assert margin == pytest.approx(1.0, rel=1e-6)
    
    # ==================== 长期资产周转率测试 ====================
    
    def test_calculate_long_term_turnover_normal(self):
        """测试长期资产周转率正常计算"""
        turnover = self.calculator.calculate_long_term_asset_turnover(
            revenue_ttm=50000,
            noncurrent_assets_begin=20000,
            noncurrent_assets_end=30000
        )
        assert turnover == pytest.approx(50000 / 25000, rel=1e-6)
    
    def test_calculate_long_term_turnover_zero_assets(self):
        """测试非流动资产为零的情况"""
        turnover = self.calculator.calculate_long_term_asset_turnover(
            revenue_ttm=50000,
            noncurrent_assets_begin=0,
            noncurrent_assets_end=0
        )
        assert turnover is None
    
    # ==================== 营运净资本比率测试 ====================
    
    def test_calculate_working_capital_ratio_normal(self):
        """测试营运净资本比率正常计算"""
        ratio = self.calculator.calculate_working_capital_ratio(
            accounts_receivable=1000,
            notes_receivable=500,
            receivables_financing=200,
            contract_assets=300,
            accounts_payable=800,
            notes_payable=400,
            contract_liabilities=300,
            total_assets=10000
        )
        # 营运净资本 = 1000 + 500 + 200 + 300 - 800 - 400 - 300 = 500
        # 比率 = 500 / 10000 = 0.05
        assert ratio == pytest.approx(0.05, rel=1e-6)
    
    def test_calculate_working_capital_ratio_with_none(self):
        """测试包含None值的情况"""
        ratio = self.calculator.calculate_working_capital_ratio(
            accounts_receivable=1000,
            notes_receivable=None,
            receivables_financing=None,
            contract_assets=None,
            accounts_payable=800,
            notes_payable=None,
            contract_liabilities=None,
            total_assets=10000
        )
        # 营运净资本 = 1000 + 0 + 0 + 0 - 800 - 0 - 0 = 200
        # 比率 = 200 / 10000 = 0.02
        assert ratio == pytest.approx(0.02, rel=1e-6)
    
    def test_calculate_working_capital_ratio_negative(self):
        """测试负营运净资本的情况"""
        ratio = self.calculator.calculate_working_capital_ratio(
            accounts_receivable=500,
            notes_receivable=0,
            receivables_financing=0,
            contract_assets=0,
            accounts_payable=1000,
            notes_payable=500,
            contract_liabilities=0,
            total_assets=10000
        )
        # 营运净资本 = 500 - 1500 = -1000
        # 比率 = -1000 / 10000 = -0.1
        assert ratio == pytest.approx(-0.1, rel=1e-6)
    
    # ==================== 经营现金流比率测试 ====================
    
    def test_calculate_operating_cashflow_ratio_normal(self):
        """测试经营现金流比率正常计算"""
        ratio = self.calculator.calculate_operating_cashflow_ratio(
            operating_cashflow=2000,
            total_assets=10000
        )
        assert ratio == pytest.approx(0.2, rel=1e-6)
    
    def test_calculate_operating_cashflow_ratio_negative(self):
        """测试负现金流的情况"""
        ratio = self.calculator.calculate_operating_cashflow_ratio(
            operating_cashflow=-500,
            total_assets=10000
        )
        assert ratio == pytest.approx(-0.05, rel=1e-6)
    
    def test_calculate_operating_cashflow_ratio_zero_assets(self):
        """测试总资产为零的情况"""
        ratio = self.calculator.calculate_operating_cashflow_ratio(
            operating_cashflow=2000,
            total_assets=0
        )
        assert ratio is None
    
    # ==================== TTM收入计算测试 ====================
    
    def test_calculate_ttm_revenue_normal(self):
        """测试TTM收入正常计算"""
        ttm = self.calculator.calculate_ttm_revenue([1000, 1200, 1100, 1300])
        assert ttm == pytest.approx(4600, rel=1e-6)
    
    def test_calculate_ttm_revenue_insufficient_data(self):
        """测试数据不足4个季度的情况"""
        ttm = self.calculator.calculate_ttm_revenue([1000, 1200, 1100])
        assert ttm is None
    
    def test_calculate_ttm_revenue_with_none(self):
        """测试包含None值的情况"""
        ttm = self.calculator.calculate_ttm_revenue([1000, None, 1100, 1300])
        assert ttm is None
    
    def test_calculate_ttm_revenue_more_than_four(self):
        """测试超过4个季度的情况（只取前4个）"""
        ttm = self.calculator.calculate_ttm_revenue([1000, 1200, 1100, 1300, 900, 800])
        assert ttm == pytest.approx(4600, rel=1e-6)
    
    # ==================== 累计值转换测试 ====================
    
    def test_convert_cumulative_q1(self):
        """测试Q1累计值转换（无上季度）"""
        quarterly = self.calculator.convert_cumulative_to_quarterly(
            current_cumulative=1000,
            previous_cumulative=None
        )
        assert quarterly == pytest.approx(1000, rel=1e-6)
    
    def test_convert_cumulative_q2_to_q4(self):
        """测试Q2-Q4累计值转换"""
        quarterly = self.calculator.convert_cumulative_to_quarterly(
            current_cumulative=5000,
            previous_cumulative=3000
        )
        assert quarterly == pytest.approx(2000, rel=1e-6)
    
    def test_convert_cumulative_negative_result(self):
        """测试转换结果为负的情况（可能是数据错误）"""
        quarterly = self.calculator.convert_cumulative_to_quarterly(
            current_cumulative=3000,
            previous_cumulative=5000
        )
        assert quarterly == pytest.approx(-2000, rel=1e-6)
