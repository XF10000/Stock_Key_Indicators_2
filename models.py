"""
数据库模型定义

使用 SQLAlchemy ORM 定义四张核心数据表：
1. BalanceSheet - 资产负债表
2. IncomeStatement - 利润表
3. CashFlowStatement - 现金流量表
4. IndicatorMedian - 指标中位数缓存表
"""

from datetime import date
from typing import Any

from sqlalchemy import Column, Date, Float, Index, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class BalanceSheet(Base):
    """资产负债表模型
    
    存储所有A股公司的资产负债表数据。采用宽表设计，包含约250个财务科目字段。
    在 stock_code 和 report_date 上建立复合唯一索引以提高查询性能。
    """

    __tablename__ = "balance_sheets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True, comment="股票代码")
    report_date = Column(Date, nullable=False, index=True, comment="报告日期")

    # 资产类科目
    monetary_capital = Column(Float, comment="货币资金")
    trading_assets = Column(Float, comment="交易性金融资产")
    notes_receivable = Column(Float, comment="应收票据")
    accounts_receivable = Column(Float, comment="应收账款")
    receivables_financing = Column(Float, comment="应收款项融资")
    prepayments = Column(Float, comment="预付款项")
    other_receivables = Column(Float, comment="其他应收款")
    inventories = Column(Float, comment="存货")
    contract_assets = Column(Float, comment="合同资产")
    current_assets = Column(Float, comment="流动资产合计")
    
    long_term_equity_investment = Column(Float, comment="长期股权投资")
    investment_property = Column(Float, comment="投资性房地产")
    fixed_assets = Column(Float, comment="固定资产")
    construction_in_progress = Column(Float, comment="在建工程")
    intangible_assets = Column(Float, comment="无形资产")
    goodwill = Column(Float, comment="商誉")
    long_term_deferred_expenses = Column(Float, comment="长期待摊费用")
    deferred_tax_assets = Column(Float, comment="递延所得税资产")
    non_current_assets = Column(Float, comment="非流动资产合计")
    
    total_assets = Column(Float, comment="资产总计")

    # 负债类科目
    short_term_borrowings = Column(Float, comment="短期借款")
    trading_liabilities = Column(Float, comment="交易性金融负债")
    notes_payable = Column(Float, comment="应付票据")
    accounts_payable = Column(Float, comment="应付账款")
    contract_liabilities = Column(Float, comment="合同负债")
    employee_benefits_payable = Column(Float, comment="应付职工薪酬")
    taxes_payable = Column(Float, comment="应交税费")
    other_payables = Column(Float, comment="其他应付款")
    current_liabilities = Column(Float, comment="流动负债合计")
    
    long_term_borrowings = Column(Float, comment="长期借款")
    bonds_payable = Column(Float, comment="应付债券")
    long_term_payables = Column(Float, comment="长期应付款")
    deferred_tax_liabilities = Column(Float, comment="递延所得税负债")
    non_current_liabilities = Column(Float, comment="非流动负债合计")
    
    total_liabilities = Column(Float, comment="负债合计")

    # 所有者权益类科目
    share_capital = Column(Float, comment="股本")
    capital_reserve = Column(Float, comment="资本公积")
    treasury_stock = Column(Float, comment="库存股")
    surplus_reserve = Column(Float, comment="盈余公积")
    retained_earnings = Column(Float, comment="未分配利润")
    total_equity = Column(Float, comment="所有者权益合计")
    
    total_liabilities_and_equity = Column(Float, comment="负债和所有者权益总计")

    __table_args__ = (
        UniqueConstraint("stock_code", "report_date", name="uix_balance_stock_date"),
        Index("ix_balance_stock_date", "stock_code", "report_date"),
    )

    def __repr__(self) -> str:
        return f"<BalanceSheet(stock_code={self.stock_code}, report_date={self.report_date})>"


class IncomeStatement(Base):
    """利润表模型
    
    存储所有A股公司的利润表数据。采用宽表设计，包含约200个财务科目字段。
    在 stock_code 和 report_date 上建立复合唯一索引以提高查询性能。
    """

    __tablename__ = "income_statements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True, comment="股票代码")
    report_date = Column(Date, nullable=False, index=True, comment="报告日期")

    # 收入类科目
    total_operating_revenue = Column(Float, comment="营业总收入")
    operating_revenue = Column(Float, comment="营业收入")
    
    # 成本费用类科目
    total_operating_cost = Column(Float, comment="营业总成本")
    operating_cost = Column(Float, comment="营业成本")
    taxes_and_surcharges = Column(Float, comment="税金及附加")
    selling_expenses = Column(Float, comment="销售费用")
    administrative_expenses = Column(Float, comment="管理费用")
    rd_expenses = Column(Float, comment="研发费用")
    financial_expenses = Column(Float, comment="财务费用")
    interest_expense = Column(Float, comment="利息费用")
    interest_income = Column(Float, comment="利息收入")
    
    # 损益类科目
    investment_income = Column(Float, comment="投资收益")
    fair_value_change_income = Column(Float, comment="公允价值变动收益")
    asset_disposal_income = Column(Float, comment="资产处置收益")
    other_income = Column(Float, comment="其他收益")
    operating_profit = Column(Float, comment="营业利润")
    
    non_operating_income = Column(Float, comment="营业外收入")
    non_operating_expenses = Column(Float, comment="营业外支出")
    
    total_profit = Column(Float, comment="利润总额")
    income_tax_expense = Column(Float, comment="所得税费用")
    net_profit = Column(Float, comment="净利润")
    
    # 每股收益
    basic_eps = Column(Float, comment="基本每股收益")
    diluted_eps = Column(Float, comment="稀释每股收益")

    __table_args__ = (
        UniqueConstraint("stock_code", "report_date", name="uix_income_stock_date"),
        Index("ix_income_stock_date", "stock_code", "report_date"),
    )

    def __repr__(self) -> str:
        return f"<IncomeStatement(stock_code={self.stock_code}, report_date={self.report_date})>"


class CashFlowStatement(Base):
    """现金流量表模型
    
    存储所有A股公司的现金流量表数据。采用宽表设计，包含约150个财务科目字段。
    在 stock_code 和 report_date 上建立复合唯一索引以提高查询性能。
    """

    __tablename__ = "cash_flow_statements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True, comment="股票代码")
    report_date = Column(Date, nullable=False, index=True, comment="报告日期")

    # 经营活动现金流
    goods_sale_cash_received = Column(Float, comment="销售商品、提供劳务收到的现金")
    tax_refund_received = Column(Float, comment="收到的税费返还")
    other_operating_cash_received = Column(Float, comment="收到其他与经营活动有关的现金")
    operating_cash_inflow = Column(Float, comment="经营活动现金流入小计")
    
    goods_purchase_cash_paid = Column(Float, comment="购买商品、接受劳务支付的现金")
    employee_cash_paid = Column(Float, comment="支付给职工以及为职工支付的现金")
    taxes_paid = Column(Float, comment="支付的各项税费")
    other_operating_cash_paid = Column(Float, comment="支付其他与经营活动有关的现金")
    operating_cash_outflow = Column(Float, comment="经营活动现金流出小计")
    
    net_operating_cash_flow = Column(Float, comment="经营活动产生的现金流量净额")

    # 投资活动现金流
    investment_withdrawal_cash = Column(Float, comment="收回投资收到的现金")
    investment_income_cash = Column(Float, comment="取得投资收益收到的现金")
    fixed_assets_disposal_cash = Column(Float, comment="处置固定资产、无形资产和其他长期资产收回的现金净额")
    other_investing_cash_received = Column(Float, comment="收到其他与投资活动有关的现金")
    investing_cash_inflow = Column(Float, comment="投资活动现金流入小计")
    
    fixed_assets_purchase_cash = Column(Float, comment="购建固定资产、无形资产和其他长期资产支付的现金")
    investment_cash_paid = Column(Float, comment="投资支付的现金")
    other_investing_cash_paid = Column(Float, comment="支付其他与投资活动有关的现金")
    investing_cash_outflow = Column(Float, comment="投资活动现金流出小计")
    
    net_investing_cash_flow = Column(Float, comment="投资活动产生的现金流量净额")

    # 筹资活动现金流
    capital_received = Column(Float, comment="吸收投资收到的现金")
    borrowing_cash_received = Column(Float, comment="取得借款收到的现金")
    other_financing_cash_received = Column(Float, comment="收到其他与筹资活动有关的现金")
    financing_cash_inflow = Column(Float, comment="筹资活动现金流入小计")
    
    debt_repayment_cash = Column(Float, comment="偿还债务支付的现金")
    dividend_cash_paid = Column(Float, comment="分配股利、利润或偿付利息支付的现金")
    other_financing_cash_paid = Column(Float, comment="支付其他与筹资活动有关的现金")
    financing_cash_outflow = Column(Float, comment="筹资活动现金流出小计")
    
    net_financing_cash_flow = Column(Float, comment="筹资活动产生的现金流量净额")

    # 汇率变动影响
    exchange_rate_effect = Column(Float, comment="汇率变动对现金及现金等价物的影响")
    
    # 现金净增加额
    net_cash_increase = Column(Float, comment="现金及现金等价物净增加额")
    cash_beginning = Column(Float, comment="期初现金及现金等价物余额")
    cash_ending = Column(Float, comment="期末现金及现金等价物余额")

    __table_args__ = (
        UniqueConstraint("stock_code", "report_date", name="uix_cashflow_stock_date"),
        Index("ix_cashflow_stock_date", "stock_code", "report_date"),
    )

    def __repr__(self) -> str:
        return f"<CashFlowStatement(stock_code={self.stock_code}, report_date={self.report_date})>"


class IndicatorMedian(Base):
    """指标中位数缓存表
    
    存储全市场各指标在每个报告期的中位数计算结果，用于加速分析性能。
    实现缓存版本控制机制，支持增量更新。
    """

    __tablename__ = "indicator_medians"

    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_name = Column(String(50), nullable=False, index=True, comment="指标名称")
    report_date = Column(Date, nullable=False, index=True, comment="报告日期")
    median_value = Column(Float, comment="中位数值")
    cache_version = Column(String(50), nullable=False, index=True, comment="缓存版本号")

    __table_args__ = (
        UniqueConstraint(
            "indicator_name", "report_date", "cache_version", name="uix_indicator_date_version"
        ),
        Index("ix_indicator_date_version", "indicator_name", "report_date", "cache_version"),
    )

    def __repr__(self) -> str:
        return (
            f"<IndicatorMedian(indicator={self.indicator_name}, "
            f"date={self.report_date}, version={self.cache_version})>"
        )


def create_tables(database_url: str = "sqlite:///database.sqlite") -> None:
    """创建所有数据库表
    
    Args:
        database_url: 数据库连接URL，默认为本地SQLite数据库
    """
    engine = create_engine(database_url, echo=False)
    Base.metadata.create_all(engine)
    print(f"数据库表创建成功: {database_url}")


if __name__ == "__main__":
    # 测试：创建数据库表
    create_tables()
