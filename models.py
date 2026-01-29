"""
数据库模型定义（完整版）

使用 SQLAlchemy ORM 定义四张核心数据表：
1. BalanceSheet - 资产负债表（140个字段）
2. IncomeStatement - 利润表（76个字段）
3. CashFlowStatement - 现金流量表（64个字段）
4. IndicatorMedian - 指标中位数缓存表

更新日期: 2026-01-28
说明: 包含akshare接口返回的所有财务报表字段
"""

from datetime import date
from typing import Any

from sqlalchemy import Column, Date, Float, Index, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StockInfo(Base):
    """股票基本信息表
    
    存储股票代码和公司名称的映射关系
    """
    
    __tablename__ = "stock_info"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, unique=True, index=True, comment="股票代码")
    stock_name = Column(String(100), nullable=False, comment="公司名称")
    
    __table_args__ = (
        Index('idx_stock_code', 'stock_code'),
    )


class BalanceSheet(Base):
    """资产负债表模型（完整版）
    
    存储所有A股公司的资产负债表数据。包含akshare接口返回的所有139个字段。
    在 stock_code 和 report_date 上建立复合唯一索引以提高查询性能。
    """

    __tablename__ = "balance_sheets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True, comment="股票代码")
    report_date = Column(Date, nullable=False, index=True, comment="报告日期")

    # 资产负债表字段（139个）
    monetary_capital = Column(Float, comment="货币资金")
    settlement_provisions = Column(Float, comment="结算备付金")
    loans_to_other_banks = Column(Float, comment="拆出资金")
    trading_financial_assets = Column(Float, comment="交易性金融资产")
    financial_assets_purchased_for_resale = Column(Float, comment="买入返售金融资产")
    derivative_financial_assets = Column(Float, comment="衍生金融资产")
    notes_and_accounts_receivable = Column(Float, comment="应收票据及应收账款")
    notes_receivable = Column(Float, comment="应收票据")
    accounts_receivable = Column(Float, comment="应收账款")
    receivables_financing = Column(Float, comment="应收款项融资")
    prepayments = Column(Float, comment="预付款项")
    dividends_receivable = Column(Float, comment="应收股利")
    interest_receivable = Column(Float, comment="应收利息")
    insurance_premiums_receivable = Column(Float, comment="应收保费")
    reinsurance_receivables = Column(Float, comment="应收分保账款")
    reinsurance_contract_reserves_receivable = Column(Float, comment="应收分保合同准备金")
    export_tax_rebates_receivable = Column(Float, comment="应收出口退税")
    subsidies_receivable = Column(Float, comment="应收补贴款")
    deposits_receivable = Column(Float, comment="应收保证金")
    internal_receivables = Column(Float, comment="内部应收款")
    other_receivables = Column(Float, comment="其他应收款")
    other_receivables_total = Column(Float, comment="其他应收款(合计)")
    inventories = Column(Float, comment="存货")
    assets_held_for_sale = Column(Float, comment="划分为持有待售的资产")
    prepaid_expenses = Column(Float, comment="待摊费用")
    current_assets_pending_disposal = Column(Float, comment="待处理流动资产损益")
    non_current_assets_due_within_one_year = Column(Float, comment="一年内到期的非流动资产")
    other_current_assets = Column(Float, comment="其他流动资产")
    total_current_assets = Column(Float, comment="流动资产合计")
    non_current_assets = Column(Float, comment="非流动资产")
    loans_and_advances = Column(Float, comment="发放贷款及垫款")
    debt_investments = Column(Float, comment="债权投资")
    other_debt_investments = Column(Float, comment="其他债权投资")
    financial_assets_at_fvoci = Column(Float, comment="以公允价值计量且其变动计入其他综合收益的金融资产")
    financial_assets_at_amortized_cost = Column(Float, comment="以摊余成本计量的金融资产")
    available_for_sale_financial_assets = Column(Float, comment="可供出售金融资产")
    long_term_equity_investments = Column(Float, comment="长期股权投资")
    investment_property = Column(Float, comment="投资性房地产")
    long_term_receivables = Column(Float, comment="长期应收款")
    other_equity_instrument_investments = Column(Float, comment="其他权益工具投资")
    other_non_current_financial_assets = Column(Float, comment="其他非流动金融资产")
    other_long_term_investments = Column(Float, comment="其他长期投资")
    fixed_assets_original_value = Column(Float, comment="固定资产原值")
    accumulated_depreciation = Column(Float, comment="累计折旧")
    fixed_assets_net_value = Column(Float, comment="固定资产净值")
    fixed_assets_impairment_provision = Column(Float, comment="固定资产减值准备")
    construction_in_progress_total = Column(Float, comment="在建工程合计")
    construction_in_progress = Column(Float, comment="在建工程")
    construction_materials = Column(Float, comment="工程物资")
    fixed_assets_net = Column(Float, comment="固定资产净额")
    fixed_assets_disposal = Column(Float, comment="固定资产清理")
    fixed_assets_and_disposal_total = Column(Float, comment="固定资产及清理合计")
    productive_biological_assets = Column(Float, comment="生产性生物资产")
    consumptive_biological_assets = Column(Float, comment="消耗性生物资产")
    oil_and_gas_assets = Column(Float, comment="油气资产")
    contract_assets = Column(Float, comment="合同资产")
    right_of_use_assets = Column(Float, comment="使用权资产")
    intangible_assets = Column(Float, comment="无形资产")
    development_expenditure = Column(Float, comment="开发支出")
    goodwill = Column(Float, comment="商誉")
    long_term_deferred_expenses = Column(Float, comment="长期待摊费用")
    split_share_structure_circulation_rights = Column(Float, comment="股权分置流通权")
    deferred_tax_assets = Column(Float, comment="递延所得税资产")
    other_non_current_assets = Column(Float, comment="其他非流动资产")
    total_non_current_assets = Column(Float, comment="非流动资产合计")
    total_assets = Column(Float, comment="资产总计")
    current_liabilities = Column(Float, comment="流动负债")
    short_term_borrowings = Column(Float, comment="短期借款")
    borrowings_from_central_bank = Column(Float, comment="向中央银行借款")
    deposits_from_customers_and_banks = Column(Float, comment="吸收存款及同业存放")
    borrowings_from_other_banks = Column(Float, comment="拆入资金")
    trading_financial_liabilities = Column(Float, comment="交易性金融负债")
    derivative_financial_liabilities = Column(Float, comment="衍生金融负债")
    notes_and_accounts_payable = Column(Float, comment="应付票据及应付账款")
    notes_payable = Column(Float, comment="应付票据")
    accounts_payable = Column(Float, comment="应付账款")
    advances_from_customers = Column(Float, comment="预收款项")
    contract_liabilities = Column(Float, comment="合同负债")
    financial_assets_sold_for_repurchase = Column(Float, comment="卖出回购金融资产款")
    fees_and_commissions_payable = Column(Float, comment="应付手续费及佣金")
    employee_benefits_payable = Column(Float, comment="应付职工薪酬")
    taxes_payable = Column(Float, comment="应交税费")
    interest_payable = Column(Float, comment="应付利息")
    dividends_payable = Column(Float, comment="应付股利")
    deposits_payable = Column(Float, comment="应付保证金")
    internal_payables = Column(Float, comment="内部应付款")
    other_payables = Column(Float, comment="其他应付款")
    other_payables_total = Column(Float, comment="其他应付款合计")
    other_taxes_payable = Column(Float, comment="其他应交款")
    guarantee_liability_reserves = Column(Float, comment="担保责任赔偿准备金")
    reinsurance_payables = Column(Float, comment="应付分保账款")
    insurance_contract_reserves = Column(Float, comment="保险合同准备金")
    securities_trading_agency_payables = Column(Float, comment="代理买卖证券款")
    securities_underwriting_agency_payables = Column(Float, comment="代理承销证券款")
    international_settlement = Column(Float, comment="国际票证结算")
    domestic_settlement = Column(Float, comment="国内票证结算")
    accrued_expenses = Column(Float, comment="预提费用")
    estimated_current_liabilities = Column(Float, comment="预计流动负债")
    short_term_bonds_payable = Column(Float, comment="应付短期债券")
    liabilities_held_for_sale = Column(Float, comment="划分为持有待售的负债")
    deferred_revenue_due_within_one_year = Column(Float, comment="一年内的递延收益")
    non_current_liabilities_due_within_one_year = Column(Float, comment="一年内到期的非流动负债")
    other_current_liabilities = Column(Float, comment="其他流动负债")
    total_current_liabilities = Column(Float, comment="流动负债合计")
    non_current_liabilities = Column(Float, comment="非流动负债")
    long_term_borrowings = Column(Float, comment="长期借款")
    bonds_payable = Column(Float, comment="应付债券")
    bonds_payable_preferred_stock = Column(Float, comment="应付债券：优先股")
    bonds_payable_perpetual_bonds = Column(Float, comment="应付债券：永续债")
    lease_liabilities = Column(Float, comment="租赁负债")
    long_term_employee_benefits_payable = Column(Float, comment="长期应付职工薪酬")
    long_term_payables = Column(Float, comment="长期应付款")
    long_term_payables_total = Column(Float, comment="长期应付款合计")
    special_payables = Column(Float, comment="专项应付款")
    estimated_non_current_liabilities = Column(Float, comment="预计非流动负债")
    long_term_deferred_revenue = Column(Float, comment="长期递延收益")
    deferred_tax_liabilities = Column(Float, comment="递延所得税负债")
    other_non_current_liabilities = Column(Float, comment="其他非流动负债")
    total_non_current_liabilities = Column(Float, comment="非流动负债合计")
    total_liabilities = Column(Float, comment="负债合计")
    owners_equity = Column(Float, comment="所有者权益")
    paid_in_capital = Column(Float, comment="实收资本(或股本)")
    other_equity_instruments = Column(Float, comment="其他权益工具")
    preferred_stock = Column(Float, comment="优先股")
    perpetual_bonds = Column(Float, comment="永续债")
    capital_reserve = Column(Float, comment="资本公积")
    less_treasury_stock = Column(Float, comment="减:库存股")
    other_comprehensive_income = Column(Float, comment="其他综合收益")
    special_reserve = Column(Float, comment="专项储备")
    surplus_reserve = Column(Float, comment="盈余公积")
    general_risk_reserve = Column(Float, comment="一般风险准备")
    unrecognized_investment_losses = Column(Float, comment="未确定的投资损失")
    retained_earnings = Column(Float, comment="未分配利润")
    proposed_cash_dividends = Column(Float, comment="拟分配现金股利")
    foreign_currency_translation_difference = Column(Float, comment="外币报表折算差额")
    equity_attributable_to_parent_company = Column(Float, comment="归属于母公司股东权益合计")
    minority_interests = Column(Float, comment="少数股东权益")
    total_owners_equity = Column(Float, comment="所有者权益(或股东权益)合计")
    total_liabilities_and_owners_equity = Column(Float, comment="负债和所有者权益(或股东权益)总计")

    __table_args__ = (
        UniqueConstraint("stock_code", "report_date", name="uix_balance_stock_date"),
        Index("ix_balance_stock_date", "stock_code", "report_date"),
    )

    def __repr__(self) -> str:
        return f"<BalanceSheet(stock_code={self.stock_code}, report_date={self.report_date})>"


class IncomeStatement(Base):
    """利润表模型（完整版）
    
    存储所有A股公司的利润表数据。包含akshare接口返回的所有76个字段。
    在 stock_code 和 report_date 上建立复合唯一索引以提高查询性能。
    """

    __tablename__ = "income_statements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True, comment="股票代码")
    report_date = Column(Date, nullable=False, index=True, comment="报告日期")

    # 利润表字段（76个）
    total_operating_revenue = Column(Float, comment="营业总收入")
    operating_revenue = Column(Float, comment="营业收入")
    interest_income = Column(Float, comment="利息收入")
    earned_premiums = Column(Float, comment="已赚保费")
    fees_and_commissions_income = Column(Float, comment="手续费及佣金收入")
    real_estate_sales_revenue = Column(Float, comment="房地产销售收入")
    other_business_revenue = Column(Float, comment="其他业务收入")
    total_operating_costs = Column(Float, comment="营业总成本")
    operating_costs = Column(Float, comment="营业成本")
    fees_and_commissions_expenses = Column(Float, comment="手续费及佣金支出")
    real_estate_sales_costs = Column(Float, comment="房地产销售成本")
    surrender_value = Column(Float, comment="退保金")
    net_claims_paid = Column(Float, comment="赔付支出净额")
    net_insurance_contract_reserves = Column(Float, comment="提取保险合同准备金净额")
    policy_dividend_expenses = Column(Float, comment="保单红利支出")
    reinsurance_expenses = Column(Float, comment="分保费用")
    other_business_costs = Column(Float, comment="其他业务成本")
    taxes_and_surcharges = Column(Float, comment="营业税金及附加")
    rd_expenses = Column(Float, comment="研发费用")
    selling_expenses = Column(Float, comment="销售费用")
    administrative_expenses = Column(Float, comment="管理费用")
    financial_expenses = Column(Float, comment="财务费用")
    interest_expenses = Column(Float, comment="利息费用")
    interest_expenditure = Column(Float, comment="利息支出")
    investment_income = Column(Float, comment="投资收益")
    investment_income_from_associates_and_joint_ventures = Column(Float, comment="对联营企业和合营企业的投资收益")
    gain_on_derecognition_of_financial_assets_at_amortized_cost = Column(Float, comment="以摊余成本计量的金融资产终止确认产生的收益")
    foreign_exchange_gains = Column(Float, comment="汇兑收益")
    net_open_hedge_gains = Column(Float, comment="净敞口套期收益")
    fair_value_change_gains = Column(Float, comment="公允价值变动收益")
    futures_gains_losses = Column(Float, comment="期货损益")
    custody_income = Column(Float, comment="托管收益")
    subsidy_income = Column(Float, comment="补贴收入")
    other_gains = Column(Float, comment="其他收益")
    asset_impairment_losses = Column(Float, comment="资产减值损失")
    credit_impairment_losses = Column(Float, comment="信用减值损失")
    other_business_profits = Column(Float, comment="其他业务利润")
    asset_disposal_gains = Column(Float, comment="资产处置收益")
    operating_profit = Column(Float, comment="营业利润")
    non_operating_income = Column(Float, comment="营业外收入")
    non_current_asset_disposal_gains = Column(Float, comment="非流动资产处置利得")
    non_operating_expenses = Column(Float, comment="营业外支出")
    non_current_asset_disposal_losses = Column(Float, comment="非流动资产处置损失")
    total_profit = Column(Float, comment="利润总额")
    income_tax_expense = Column(Float, comment="所得税费用")
    unrecognized_investment_losses = Column(Float, comment="未确认投资损失")
    net_profit = Column(Float, comment="净利润")
    net_profit_from_continuing_operations = Column(Float, comment="持续经营净利润")
    net_profit_from_discontinued_operations = Column(Float, comment="终止经营净利润")
    net_profit_attributable_to_parent_company = Column(Float, comment="归属于母公司所有者的净利润")
    net_profit_of_acquiree_before_merger = Column(Float, comment="被合并方在合并前实现净利润")
    minority_interests_profit_loss = Column(Float, comment="少数股东损益")
    other_comprehensive_income = Column(Float, comment="其他综合收益")
    other_comprehensive_income_attributable_to_parent = Column(Float, comment="归属于母公司所有者的其他综合收益")
    oci_not_reclassified_to_profit_loss = Column(Float, comment="（一）以后不能重分类进损益的其他综合收益")
    remeasurement_of_defined_benefit_plans = Column(Float, comment="重新计量设定受益计划变动额")
    oci_under_equity_method_not_reclassified = Column(Float, comment="权益法下不能转损益的其他综合收益")
    fair_value_change_of_other_equity_instruments = Column(Float, comment="其他权益工具投资公允价值变动")
    fair_value_change_of_own_credit_risk = Column(Float, comment="企业自身信用风险公允价值变动")
    oci_reclassified_to_profit_loss = Column(Float, comment="（二）以后将重分类进损益的其他综合收益")
    oci_under_equity_method_reclassified = Column(Float, comment="权益法下可转损益的其他综合收益")
    fair_value_change_of_afs_financial_assets = Column(Float, comment="可供出售金融资产公允价值变动损益")
    fair_value_change_of_other_debt_investments = Column(Float, comment="其他债权投资公允价值变动")
    financial_assets_reclassified_to_oci = Column(Float, comment="金融资产重分类计入其他综合收益的金额")
    credit_impairment_of_other_debt_investments = Column(Float, comment="其他债权投资信用减值准备")
    htm_reclassified_to_afs_gains_losses = Column(Float, comment="持有至到期投资重分类为可供出售金融资产损益")
    cash_flow_hedge_reserve = Column(Float, comment="现金流量套期储备")
    effective_portion_of_cash_flow_hedge = Column(Float, comment="现金流量套期损益的有效部分")
    foreign_currency_translation_difference = Column(Float, comment="外币财务报表折算差额")
    other = Column(Float, comment="其他")
    other_comprehensive_income_attributable_to_minority = Column(Float, comment="归属于少数股东的其他综合收益")
    total_comprehensive_income = Column(Float, comment="综合收益总额")
    total_comprehensive_income_attributable_to_parent = Column(Float, comment="归属于母公司所有者的综合收益总额")
    total_comprehensive_income_attributable_to_minority = Column(Float, comment="归属于少数股东的综合收益总额")
    basic_earnings_per_share = Column(Float, comment="基本每股收益")
    diluted_earnings_per_share = Column(Float, comment="稀释每股收益")

    __table_args__ = (
        UniqueConstraint("stock_code", "report_date", name="uix_income_stock_date"),
        Index("ix_income_stock_date", "stock_code", "report_date"),
    )

    def __repr__(self) -> str:
        return f"<IncomeStatement(stock_code={self.stock_code}, report_date={self.report_date})>"


class CashFlowStatement(Base):
    """现金流量表模型（完整版）
    
    存储所有A股公司的现金流量表数据。包含akshare接口返回的所有64个字段。
    在 stock_code 和 report_date 上建立复合唯一索引以提高查询性能。
    """

    __tablename__ = "cash_flow_statements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True, comment="股票代码")
    report_date = Column(Date, nullable=False, index=True, comment="报告日期")

    # 现金流量表字段（64个）
    cash_flows_from_operating_activities = Column(Float, comment="经营活动产生的现金流量")
    cash_received_from_sale_of_goods_and_services = Column(Float, comment="销售商品、提供劳务收到的现金")
    net_increase_in_customer_deposits_and_interbank_deposits = Column(Float, comment="客户存款和同业存放款项净增加额")
    net_increase_in_borrowings_from_central_bank = Column(Float, comment="向中央银行借款净增加额")
    net_increase_in_borrowings_from_other_financial_institutions = Column(Float, comment="向其他金融机构拆入资金净增加额")
    cash_received_from_original_insurance_premiums = Column(Float, comment="收到原保险合同保费取得的现金")
    net_cash_received_from_reinsurance = Column(Float, comment="收到再保险业务现金净额")
    net_increase_in_policyholder_deposits_and_investments = Column(Float, comment="保户储金及投资款净增加额")
    net_increase_from_disposal_of_trading_financial_assets = Column(Float, comment="处置交易性金融资产净增加额")
    cash_received_from_interest_fees_and_commissions = Column(Float, comment="收取利息、手续费及佣金的现金")
    net_increase_in_borrowed_funds = Column(Float, comment="拆入资金净增加额")
    net_increase_in_repurchase_funds = Column(Float, comment="回购业务资金净增加额")
    tax_refunds_received = Column(Float, comment="收到的税费返还")
    other_cash_received_from_operating_activities = Column(Float, comment="收到的其他与经营活动有关的现金")
    subtotal_of_cash_inflows_from_operating_activities = Column(Float, comment="经营活动现金流入小计")
    cash_paid_for_goods_and_services = Column(Float, comment="购买商品、接受劳务支付的现金")
    net_increase_in_customer_loans_and_advances = Column(Float, comment="客户贷款及垫款净增加额")
    net_increase_in_deposits_with_central_bank_and_interbank = Column(Float, comment="存放中央银行和同业款项净增加额")
    cash_paid_for_original_insurance_claims = Column(Float, comment="支付原保险合同赔付款项的现金")
    cash_paid_for_interest_fees_and_commissions = Column(Float, comment="支付利息、手续费及佣金的现金")
    cash_paid_for_policy_dividends = Column(Float, comment="支付保单红利的现金")
    cash_paid_to_and_for_employees = Column(Float, comment="支付给职工以及为职工支付的现金")
    taxes_and_fees_paid = Column(Float, comment="支付的各项税费")
    other_cash_paid_for_operating_activities = Column(Float, comment="支付的其他与经营活动有关的现金")
    subtotal_of_cash_outflows_from_operating_activities = Column(Float, comment="经营活动现金流出小计")
    net_cash_flows_from_operating_activities = Column(Float, comment="经营活动产生的现金流量净额")
    cash_flows_from_investing_activities = Column(Float, comment="投资活动产生的现金流量")
    cash_received_from_disposal_of_investments = Column(Float, comment="收回投资所收到的现金")
    cash_received_from_investment_income = Column(Float, comment="取得投资收益收到的现金")
    net_cash_received_from_disposal_of_fixed_intangible_and_other_long_term_assets = Column(Float, comment="处置固定资产、无形资产和其他长期资产所收回的现金净额")
    net_cash_received_from_disposal_of_subsidiaries_and_other_business_units = Column(Float, comment="处置子公司及其他营业单位收到的现金净额")
    other_cash_received_from_investing_activities = Column(Float, comment="收到的其他与投资活动有关的现金")
    cash_received_from_decrease_in_pledged_and_time_deposits = Column(Float, comment="减少质押和定期存款所收到的现金")
    net_increase_from_disposal_of_afs_financial_assets = Column(Float, comment="处置可供出售金融资产净增加额")
    subtotal_of_cash_inflows_from_investing_activities = Column(Float, comment="投资活动现金流入小计")
    cash_paid_for_fixed_intangible_and_other_long_term_assets = Column(Float, comment="购建固定资产、无形资产和其他长期资产所支付的现金")
    cash_paid_for_investments = Column(Float, comment="投资所支付的现金")
    net_increase_in_pledged_loans = Column(Float, comment="质押贷款净增加额")
    net_cash_paid_for_acquisition_of_subsidiaries_and_other_business_units = Column(Float, comment="取得子公司及其他营业单位支付的现金净额")
    cash_paid_for_increase_in_pledged_and_time_deposits = Column(Float, comment="增加质押和定期存款所支付的现金")
    other_cash_paid_for_investing_activities = Column(Float, comment="支付的其他与投资活动有关的现金")
    subtotal_of_cash_outflows_from_investing_activities = Column(Float, comment="投资活动现金流出小计")
    net_cash_flows_from_investing_activities = Column(Float, comment="投资活动产生的现金流量净额")
    cash_flows_from_financing_activities = Column(Float, comment="筹资活动产生的现金流量")
    cash_received_from_capital_contributions = Column(Float, comment="吸收投资收到的现金")
    cash_received_from_minority_shareholders_of_subsidiaries = Column(Float, comment="子公司吸收少数股东投资收到的现金")
    cash_received_from_borrowings = Column(Float, comment="取得借款收到的现金")
    cash_received_from_issuing_bonds = Column(Float, comment="发行债券收到的现金")
    other_cash_received_from_financing_activities = Column(Float, comment="收到其他与筹资活动有关的现金")
    subtotal_of_cash_inflows_from_financing_activities = Column(Float, comment="筹资活动现金流入小计")
    cash_paid_for_debt_repayment = Column(Float, comment="偿还债务支付的现金")
    cash_paid_for_dividends_profits_and_interest = Column(Float, comment="分配股利、利润或偿付利息所支付的现金")
    dividends_and_profits_paid_to_minority_shareholders_of_subsidiaries = Column(Float, comment="子公司支付给少数股东的股利、利润")
    other_cash_paid_for_financing_activities = Column(Float, comment="支付其他与筹资活动有关的现金")
    subtotal_of_cash_outflows_from_financing_activities = Column(Float, comment="筹资活动现金流出小计")
    net_cash_flows_from_financing_activities = Column(Float, comment="筹资活动产生的现金流量净额")
    effect_of_exchange_rate_changes_on_cash = Column(Float, comment="汇率变动对现金及现金等价物的影响")
    net_increase_in_cash_and_cash_equivalents = Column(Float, comment="现金及现金等价物净增加额")
    cash_and_cash_equivalents_at_beginning_of_period = Column(Float, comment="期初现金及现金等价物余额")
    cash_at_end_of_period = Column(Float, comment="现金的期末余额")
    cash_at_beginning_of_period = Column(Float, comment="现金的期初余额")
    cash_equivalents_at_end_of_period = Column(Float, comment="现金等价物的期末余额")
    cash_equivalents_at_beginning_of_period = Column(Float, comment="现金等价物的期初余额")
    cash_and_cash_equivalents_at_end_of_period = Column(Float, comment="期末现金及现金等价物余额")

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
