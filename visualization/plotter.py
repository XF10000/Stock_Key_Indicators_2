"""
可视化模块

生成HTML报告和图表
"""
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Optional
from datetime import datetime
import os


class Plotter:
    """图表生成器"""
    
    def __init__(self, output_dir: str = "output"):
        """
        初始化图表生成器
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_html_report(
        self,
        analysis_result: Dict,
        output_filename: Optional[str] = None
    ) -> str:
        """
        生成HTML报告
        
        Args:
            analysis_result: 分析结果
            output_filename: 输出文件名（不指定则自动生成）
            
        Returns:
            生成的HTML文件路径
        """
        stock_code = analysis_result['stock_code']
        
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"{stock_code}_分析报告_{timestamp}.html"
        
        output_path = os.path.join(self.output_dir, output_filename)
        
        # 生成HTML内容
        html_content = self._build_html_content(analysis_result)
        
        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return output_path
    
    def _build_html_content(self, analysis_result: Dict) -> str:
        """
        构建HTML内容
        
        Args:
            analysis_result: 分析结果
            
        Returns:
            HTML字符串
        """
        stock_code = analysis_result['stock_code']
        company_info = analysis_result['company_info']
        company_name = company_info.get('stock_name', stock_code)
        indicators = analysis_result['indicators'].copy()
        market_comparison = analysis_result.get('market_comparison', {})
        
        # 保留年度数据（12月31日）+ 最新季度数据（用于TTM）
        indicators['report_date'] = pd.to_datetime(indicators['report_date'])
        
        # 获取最新数据日期
        latest_date = indicators['report_date'].max()
        
        # 保留所有12月31日的数据 + 最新的非12月31日数据（如果存在）
        is_year_end = (indicators['report_date'].dt.month == 12) & (indicators['report_date'].dt.day == 31)
        is_latest = indicators['report_date'] == latest_date
        
        indicators = indicators[is_year_end | is_latest].copy()
        
        # 为最新的非年度数据添加TTM标记
        if latest_date.month != 12 or latest_date.day != 31:
            indicators.loc[indicators['report_date'] == latest_date, 'is_ttm'] = True
        else:
            indicators['is_ttm'] = False
        
        # 找到所有指标中最早的起始年份，确保所有图表使用相同的年份范围
        # 注意：不过滤NaN值，保持数据的完整性，NaN在图表中会自动断开连线
        indicator_columns = ['ar_turnover', 'gross_margin', 'lt_asset_turnover', 
                            'working_capital_ratio', 'operating_cashflow_ratio']
        
        # 对每个指标，找到第一个非NaN值的日期
        earliest_valid_dates = []
        for col in indicator_columns:
            valid_data = indicators[indicators[col].notna()]
            if len(valid_data) > 0:
                earliest_valid_dates.append(valid_data['report_date'].min())
        
        # 取最晚的起始日期作为统一的起始日期（确保所有图表从所有指标都开始有数据的年份开始）
        if earliest_valid_dates:
            unified_start_date = max(earliest_valid_dates)
            # 过滤数据，只保留统一起始日期之后的数据
            # 这样所有图表都会有相同的X轴范围，中间年份的NaN会在图表中自动断开连线
            indicators = indicators[indicators['report_date'] >= unified_start_date].copy()
            
            # 同时过滤市场对比数据，确保市场中位数图表也从相同年份开始
            for key in list(market_comparison.keys()):
                if market_comparison[key] is not None and isinstance(market_comparison[key], pd.DataFrame):
                    if len(market_comparison[key]) > 0 and 'report_date' in market_comparison[key].columns:
                        market_comparison[key] = market_comparison[key][
                            pd.to_datetime(market_comparison[key]['report_date']) >= unified_start_date
                        ].copy()
        
        # HTML头部
        html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{company_name} 财务指标分析报告</title>
    <script src="https://cdn.jsdelivr.net/npm/plotly.js@2.27.0/dist/plotly.min.js"></script>
    <style>
        body {{
            font-family: "Microsoft YaHei", "SimHei", Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 40px;
            border-left: 4px solid #4CAF50;
            padding-left: 10px;
        }}
        .info-box {{
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        .info-item {{
            display: inline-block;
            margin-right: 30px;
            margin-bottom: 10px;
        }}
        .info-label {{
            font-weight: bold;
            color: #666;
        }}
        .chart-container {{
            margin: 30px 0;
        }}
        .footer {{
            margin-top: 50px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            text-align: center;
            color: #999;
            font-size: 14px;
        }}
        details {{
            background-color: #f8f9fa;
            border-left: 4px solid #4CAF50;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }}
        summary {{
            cursor: pointer;
            font-weight: bold;
            color: #4CAF50;
            padding: 5px 0;
            user-select: none;
        }}
        summary:hover {{
            color: #45a049;
        }}
        summary::marker {{
            color: #4CAF50;
        }}
        .analysis-content {{
            margin-top: 15px;
            padding-top: 15px;
            border-top: 1px solid #ddd;
            line-height: 1.8;
        }}
        .analysis-content ol {{
            padding-left: 20px;
        }}
        .analysis-content li {{
            margin-bottom: 12px;
            color: #444;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{company_name} 财务指标分析报告</h1>
        
        <div class="info-box">
            <div class="info-item">
                <span class="info-label">股票代码：</span>{stock_code}
            </div>
            <div class="info-item">
                <span class="info-label">分析日期：</span>{company_info['analysis_date']}
            </div>
            <div class="info-item">
                <span class="info-label">分析年限：</span>{company_info['data_years']}年
            </div>
        </div>
"""
        
        # 指标1：应收账款周转率对数 vs 毛利率（特殊处理，双Y轴）
        html += '<h2 style="color: #C41E3A; border-bottom: 2px solid #C41E3A; padding-bottom: 10px;">指标1：回款能力 - 应收账款周转率对数 vs 毛利率</h2>'
        html += '''<p style="color: #666; margin-bottom: 20px;">
            衡量公司应收账款管理效率、回款能力与产品盈利能力的综合表现（周转率已取对数）<br>
            <strong>计算方法：</strong><br>
            • 应收账款周转率 = TTM营业收入 / 平均应收账款<br>
            • 平均应收账款 = (期初应收账款 + 期末应收账款) / 2<br>
            • TTM营业收入 = 最近四个季度的单季度营业收入之和<br>
            • 报告中显示的是对数值：ln(应收账款周转率)
        </p>'''
        html += '''
        <details>
            <summary>📊 点击展开：指标1深度分析说明</summary>
            <div class="analysis-content">
                <ol>
                    <li><strong>10年数据透视：</strong>应收账款周转率在全A样本中呈对数正态分布，真实性具备保障</li>
                    <li><strong>虚增收入检验逻辑：</strong>应收账款周转率=营业收入/应收账款，通常大于1，因此如果通过虚增应收账款来虚增营业收入，分子分母同时增加相同的值，应收账款周转率大概率下降。应收账款周转率下降意味着企业在产业链上的竞争力减弱</li>
                    <li><strong>毛利率交叉验证：</strong>但是营业成本很难随营业收入等比例虚增（折旧源于历史成本，员工工资需要和社保数据对应），如果通过虚增应收账款来虚增营业收入，毛利率可能上升，这又意味着企业议价权提高，与应收账款周转率指向不一致</li>
                    <li><strong>一致性检验：</strong>因此，检验应收账款周转率和毛利率走势一致性，是重要的报表质量验证方法，不一致不一定有问题，但是需要给出合理解释</li>
                </ol>
            </div>
        </details>
        '''
        html += self._create_indicator1_charts(indicators, market_comparison, company_name)
        
        # 指标2-4：标准单指标展示
        standard_indicators = [
            {
                'name': '长期资产周转率对数',
                'column': 'lt_asset_turnover',
                'unit': 'ln(次)',
                'description': '''衡量公司长期资产的使用效率和再投资质量（周转率已取对数）<br>
                    <strong>计算方法：</strong><br>
                    • 长期资产周转率 = TTM营业收入 / 平均长期经营资产<br>
                    • 平均长期经营资产 = (期初长期经营资产 + 期末长期经营资产) / 2<br>
                    • <strong>长期经营资产 =</strong> 非流动资产合计 - 长期股权投资 - 投资性房地产 - 递延所得税资产<br>
                    • 即：长期经营资产 = 固定资产 + 在建工程 + 生产性生物资产 + 公益性生物资产 + 油气资产 + 使用权资产 + 无形资产 + 开发支出 + 商誉 + 长期待摊费用 + 其他非流动资产<br>
                    • TTM营业收入 = 最近四个季度的单季度营业收入之和<br>
                    • 报告中显示的是对数值：ln(长期资产周转率)''',
                'title': '指标2：再投资质量',
                'analysis_note': '''
                    <ol>
                        <li><strong>10年数据透视：</strong>营业收入/(固定资产+无形资产)在全A样本中呈对数正态分布</li>
                        <li><strong>影响因素：</strong>影响固定资产周转率的因素包括单位产能造价、产能利用率、产品单价，一方面反映再投资质量，同时可以反映跑冒滴漏程度</li>
                        <li><strong>三步循环法检验：</strong>如果上市公司采用了完整的"三步循环法"一般会将虚增的利润(或者跑冒滴漏)变成了固定资产、无形资产等长期资产，再通过未来折旧或者减值消化，由于资产负债表是累积式的，周转率指标会发生趋势性下降</li>
                        <li><strong>分析要点：</strong>无论是哪种情况，固定资产+无形资产周转率下降，尤其是单个公司在全A样本中的分位数下降，都代表存量资产以及再投资质量下降，是重大的负面指标；反之则意味着资产利用效率、产业竞争力实打实改善</li>
                    </ol>
                '''
            },
            {
                'name': '营运净资本比率',
                'column': 'working_capital_ratio',
                'unit': '%',
                'description': '''衡量公司在产业链中的地位和议价能力<br>
                    <strong>计算方法：</strong><br>
                    • 营运净资本 = 应收账款 + 应收票据 + 应收款项融资 + 合同资产 - 应付账款 - 应付票据 - 合同负债<br>
                    • 营运净资本比率 = 营运净资本 / 总资产 × 100%<br>
                    • 负值表示公司占用上下游资金，正值表示被上下游占用资金''',
                'title': '指标3：产业链地位',
                'analysis_note': '''
                    <ol>
                        <li><strong>10年数据透视：</strong>营运净资本占总资产的比例在全A样本呈正态分布，真实性具备保障</li>
                        <li><strong>双重含义：</strong>营运净资本(应收账款+应收票据+应收款项融资+合同资产-应付账款-应付票据-合同负债)占比一方面体现上市公司资金运用效率，即不能创造收益的在途资金占比，另一方面反映公司在上下游产业链中的地位</li>
                        <li><strong>分布特征：</strong>该指标是所有指标中，全A样本分布"最正态"的一个，且全A样本中位数非常接近零</li>
                        <li><strong>龙头验证：</strong>尤其注意单个公司的该指标在全A样本中的分位数的边际变化。如果该公司在估值中的叙事是"龙头优势明显、强者恒强"，营运净资本占比在全A样本中的分位数就应该持续下降，或者绝对分位数很低，否则就是重大不一致，需要找到充足的理由解释</li>
                    </ol>
                '''
            },
            {
                'name': '经营现金流比率',
                'column': 'operating_cashflow_ratio',
                'unit': '%',
                'description': '''衡量公司真实的盈利能力和现金创造能力<br>
                    <strong>计算方法：</strong><br>
                    • 经营现金流比率 = 经营活动产生的现金流量净额 / 总资产 × 100%<br>
                    • 反映每单位资产创造的经营现金流<br>
                    • 数值越高，说明公司盈利质量越好，现金回收能力越强''',
                'title': '指标4：真实盈利能力',
                'analysis_note': '''
                    <ol>
                        <li><strong>10年数据透视：</strong>经营性现金流量净额/总资产在全A样本呈正态分布，真实性具备保障</li>
                        <li><strong>等价ROA：</strong>经营性现金流量净额中包含财务费用，因此分母用总资产，该指标相当于ROA。如之前所述，全A样本ROE存在调节的可能性，该指标更能体现资产的现金流创造能力</li>
                        <li><strong>市场基准：</strong>2024年全A样本该指标的中位数只有4.3%，反映了A股市场加杠杆之前的"平均盈利水平"；而2025Q1分布则呈现明显的左侧厚尾(历年一季度都有这个特点)，中位数接近零，即大部分公司一季度回款一般，如果单个公司一季度回款较好，则尤为不易</li>
                        <li><strong>叙事一致性：</strong>该指标的绝对值高低本身无谓多空，而是要对比财报中的画像与估值中隐含的叙事的一致性，包括历史趋势与全A样本分位数走势</li>
                    </ol>
                '''
            }
        ]
        
        for config in standard_indicators:
            html += f'<h2 style="color: #C41E3A; border-bottom: 2px solid #C41E3A; padding-bottom: 10px;">{config["title"]} - {config["name"]}</h2>'
            html += f'<p style="color: #666; margin-bottom: 20px;">{config["description"]}</p>'
            # 添加深度分析说明（可展开/隐藏）
            html += f'''
            <details>
                <summary>📊 点击展开：{config["title"]}深度分析说明</summary>
                <div class="analysis-content">
                    {config["analysis_note"]}
                </div>
            </details>
            '''
            html += self._create_standard_indicator_charts(
                indicators,
                market_comparison,
                config['name'],
                config['column'],
                config['unit'],
                company_name
            )
        
        # HTML尾部
        html += f"""
        <div class="footer">
            <p>报告生成时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            <p>数据来源：东方财富 (akshare)</p>
            <p style="color: #999; font-size: 12px; margin-top: 10px;">
                <strong>使用提示：</strong>所有图表的X轴（时间轴）已联动，在任意图表上缩放X轴时，其他图表会自动同步。
                您也可以独立调整每个图表的Y轴范围。双击图表可恢复初始视图。
            </p>
        </div>
    </div>
    
    <script>
    // X轴联动缩放功能
    document.addEventListener('DOMContentLoaded', function() {{
        const allDivs = document.querySelectorAll('.plotly-graph-div');
        let isUpdating = false;
        
        // 为每个图表添加relayout事件监听
        allDivs.forEach(function(div) {{
            div.on('plotly_relayout', function(eventData) {{
                if (isUpdating) return;
                
                // 检查是否是X轴范围变化
                if (eventData['xaxis.range[0]'] !== undefined && eventData['xaxis.range[1]'] !== undefined) {{
                    isUpdating = true;
                    const xRange = [eventData['xaxis.range[0]'], eventData['xaxis.range[1]']];
                    
                    // 同步到所有其他图表
                    allDivs.forEach(function(otherDiv) {{
                        if (otherDiv !== div) {{
                            Plotly.relayout(otherDiv, {{
                                'xaxis.range': xRange
                            }});
                        }}
                    }});
                    
                    setTimeout(function() {{ isUpdating = false; }}, 100);
                }} else if (eventData['xaxis.autorange'] === true) {{
                    // 同步自动缩放
                    isUpdating = true;
                    allDivs.forEach(function(otherDiv) {{
                        if (otherDiv !== div) {{
                            Plotly.relayout(otherDiv, {{
                                'xaxis.autorange': true
                            }});
                        }}
                    }});
                    setTimeout(function() {{ isUpdating = false; }}, 100);
                }}
            }});
        }});
    }});
    </script>
</body>
</html>
"""
        
        return html
    
    def _create_indicator1_charts(
        self,
        indicators: pd.DataFrame,
        market_comparison: Dict,
        company_name: str
    ) -> str:
        """
        创建指标1的3张图表（应收账款周转率 vs 毛利率）
        """
        html = ''
        
        # 准备数据（已经是年度数据）
        ar_data = indicators[['report_date', 'ar_turnover', 'gross_margin']].copy()
        # 不过滤NaN值，保留所有年份以确保X轴一致，NaN会在图表中自动断开连线
        
        if len(ar_data) == 0:
            return '<p style="color: #999; font-style: italic;">暂无数据</p>'
        
        ar_data['gross_margin'] = ar_data['gross_margin'] * 100
        
        # 获取市场对比数据
        ar_comparison = market_comparison.get('ar_turnover')
        gm_comparison = market_comparison.get('gross_margin')
        
        # 图1：目标公司的应收账款周转率对数 + 毛利率
        html += f'<h3>图1：{company_name} - 应收账款周转率对数 vs 毛利率</h3>'
        chart1 = self._create_dual_indicator_chart(
            ar_data, 'ar_turnover', 'gross_margin',
            '应收账款周转率对数', '毛利率', 'ln(次)', '%',
            company_name
        )
        html += f'<div class="chart-container">{chart1}</div>'
        
        # 图2：全A股中位数的应收账款周转率对数 + 毛利率
        html += '<h3>图2：全A股中位数 - 应收账款周转率对数 vs 毛利率</h3>'
        if ar_comparison is not None and gm_comparison is not None and len(ar_comparison) > 0 and len(gm_comparison) > 0:
            # 保留年度数据 + 最新季度数据
            ar_comp_annual = ar_comparison.copy()
            ar_comp_annual['report_date'] = pd.to_datetime(ar_comp_annual['report_date'])
            latest_ar_date = ar_comp_annual['report_date'].max()
            is_year_end_ar = (ar_comp_annual['report_date'].dt.month == 12) & (ar_comp_annual['report_date'].dt.day == 31)
            is_latest_ar = ar_comp_annual['report_date'] == latest_ar_date
            ar_comp_annual = ar_comp_annual[is_year_end_ar | is_latest_ar]
            
            gm_comp_annual = gm_comparison.copy()
            gm_comp_annual['report_date'] = pd.to_datetime(gm_comp_annual['report_date'])
            latest_gm_date = gm_comp_annual['report_date'].max()
            is_year_end_gm = (gm_comp_annual['report_date'].dt.month == 12) & (gm_comp_annual['report_date'].dt.day == 31)
            is_latest_gm = gm_comp_annual['report_date'] == latest_gm_date
            gm_comp_annual = gm_comp_annual[is_year_end_gm | is_latest_gm]
            
            if len(ar_comp_annual) > 0 and len(gm_comp_annual) > 0:
                # 合并两个指标的中位数数据
                median_data = ar_comp_annual[['report_date', 'market_median']].copy()
                median_data = median_data.rename(columns={'market_median': 'ar_median'})
                
                gm_median = gm_comp_annual[['report_date', 'market_median']].copy()
                gm_median = gm_median.rename(columns={'market_median': 'gm_median'})
                gm_median['gm_median'] = gm_median['gm_median'] * 100
                
                median_data = median_data.merge(gm_median, on='report_date', how='inner')
                
                # 对齐到公司数据的完整日期列表，为缺失年份填充None
                full_dates = ar_data[['report_date']].copy()
                median_data = full_dates.merge(median_data, on='report_date', how='left')
                
                if len(median_data) > 0:
                    chart2 = self._create_dual_indicator_chart(
                        median_data, 'ar_median', 'gm_median',
                        '应收账款周转率对数', '毛利率', 'ln(次)', '%',
                        '全A股中位数'
                    )
                    html += f'<div class="chart-container">{chart2}</div>'
                else:
                    html += '<p style="color: #999;">暂无年度数据</p>'
            else:
                html += '<p style="color: #999;">暂无年度数据</p>'
        else:
            html += '<p style="color: #999;">暂无市场对比数据</p>'
        
        # 图3：目标公司应收账款周转率对数的分位数走势
        html += f'<h3>图3：{company_name}应收账款周转率对数在全A股中的分位数走势</h3>'
        if ar_comparison is not None and len(ar_comparison) > 0:
            # 保留年度数据 + 最新季度数据
            ar_comp_annual = ar_comparison.copy()
            ar_comp_annual['report_date'] = pd.to_datetime(ar_comp_annual['report_date'])
            latest_date = ar_comp_annual['report_date'].max()
            is_year_end = (ar_comp_annual['report_date'].dt.month == 12) & (ar_comp_annual['report_date'].dt.day == 31)
            is_latest = ar_comp_annual['report_date'] == latest_date
            ar_comp_annual = ar_comp_annual[is_year_end | is_latest]
            
            if len(ar_comp_annual) > 0:
                # 对齐到公司数据的完整日期列表，为缺失年份填充None
                full_dates = ar_data[['report_date']].copy()
                ar_comp_aligned = full_dates.merge(ar_comp_annual, on='report_date', how='left')
                
                chart3 = self._create_percentile_chart(
                    ar_comp_aligned, '应收账款周转率对数', 'ar_turnover'
                )
                html += f'<div class="chart-container">{chart3}</div>'
            else:
                html += '<p style="color: #999;">暂无年度数据</p>'
        else:
            html += '<p style="color: #999;">暂无市场对比数据</p>'
        
        return html
    
    def _create_standard_indicator_charts(
        self,
        indicators: pd.DataFrame,
        market_comparison: Dict,
        indicator_name: str,
        column_name: str,
        unit: str,
        company_name: str
    ) -> str:
        """
        创建标准指标的2张图表（图1合并了公司和市场中位数，图2是分位数）
        """
        html = ''
        
        # 准备数据（已经是年度数据）
        valid_data = indicators[['report_date', column_name]].copy()
        # 不过滤NaN值，保留所有年份以确保X轴一致，NaN会在图表中自动断开连线
        
        if len(valid_data) == 0:
            return '<p style="color: #999; font-style: italic;">暂无数据</p>'
        
        if unit == '%':
            valid_data[column_name] = valid_data[column_name] * 100
        
        # 获取市场对比数据
        comparison_df = market_comparison.get(column_name)
        
        # 图1：目标公司 vs 全A股中位数（合并在一张图中）
        html += f'<h3>图1：{company_name} vs 全A股中位数 - {indicator_name}历史走势对比</h3>'
        
        if comparison_df is not None and len(comparison_df) > 0:
            # 保留年度数据 + 最新季度数据
            median_data = comparison_df[['report_date', 'market_median']].copy()
            median_data['report_date'] = pd.to_datetime(median_data['report_date'])
            latest_date = median_data['report_date'].max()
            is_year_end = (median_data['report_date'].dt.month == 12) & (median_data['report_date'].dt.day == 31)
            is_latest = median_data['report_date'] == latest_date
            median_data = median_data[is_year_end | is_latest]
            
            if len(median_data) > 0:
                if unit == '%':
                    median_data['market_median'] = median_data['market_median'] * 100
                
                # 对齐到公司数据的完整日期列表，为缺失年份填充None
                merged_data = valid_data.merge(median_data, on='report_date', how='left')
                merged_data = merged_data.sort_values('report_date')
                
                # 创建双线图表
                chart1 = self._create_comparison_chart(
                    merged_data, column_name, 'market_median',
                    company_name, '全A股中位数', indicator_name, unit
                )
                html += f'<div class="chart-container">{chart1}</div>'
            else:
                # 如果没有市场数据，只显示公司数据
                chart1 = self._create_single_line_chart(
                    valid_data, column_name, indicator_name, unit, company_name, '#C41E3A'
                )
                html += f'<div class="chart-container">{chart1}</div>'
        else:
            # 如果没有市场数据，只显示公司数据
            chart1 = self._create_single_line_chart(
                valid_data, column_name, indicator_name, unit, company_name, '#C41E3A'
            )
            html += f'<div class="chart-container">{chart1}</div>'
        
        # 图2：目标公司该指标的分位数走势
        html += f'<h3>图2：{company_name}{indicator_name}在全A股中的分位数走势</h3>'
        if comparison_df is not None and len(comparison_df) > 0:
            # 保留年度数据 + 最新季度数据
            percentile_data = comparison_df.copy()
            percentile_data['report_date'] = pd.to_datetime(percentile_data['report_date'])
            latest_date = percentile_data['report_date'].max()
            is_year_end = (percentile_data['report_date'].dt.month == 12) & (percentile_data['report_date'].dt.day == 31)
            is_latest = percentile_data['report_date'] == latest_date
            percentile_data = percentile_data[is_year_end | is_latest]
            
            if len(percentile_data) > 0:
                # 对齐到公司数据的完整日期列表，为缺失年份填充None
                full_dates = valid_data[['report_date']].copy()
                percentile_aligned = full_dates.merge(percentile_data, on='report_date', how='left')
                
                chart3 = self._create_percentile_chart(
                    percentile_aligned, indicator_name, column_name
                )
                html += f'<div class="chart-container">{chart3}</div>'
            else:
                html += '<p style="color: #999;">暂无年度数据</p>'
        else:
            html += '<p style="color: #999;">暂无市场对比数据</p>'
        
        return html
    
    def _create_dual_indicator_chart(
        self,
        data: pd.DataFrame,
        col1: str,
        col2: str,
        name1: str,
        name2: str,
        unit1: str,
        unit2: str,
        title_prefix: str
    ) -> str:
        """
        创建双Y轴图表（用于应收账款周转率 vs 毛利率）
        """
        data = data.copy()
        data['report_date'] = pd.to_datetime(data['report_date'])
        
        # 格式化为"YYYY年"，对于非12月31日的数据添加季度TTM标记
        def format_date_label(row):
            year = row['report_date'].year
            month = row['report_date'].month
            if month == 12 and row['report_date'].day == 31:
                return f'{year}年'
            else:
                # 根据月份确定季度
                quarter = (month - 1) // 3 + 1
                return f'{year}Q{quarter}-TTM'
        
        data['date_label'] = data.apply(format_date_label, axis=1)
        
        # 创建双Y轴图表
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # 第一个指标（左Y轴）
        fig.add_trace(
            go.Scatter(
                x=data['date_label'].tolist(),
                y=data[col1].tolist(),
                mode='lines',
                name=f'{name1}（左轴）',
                line=dict(color='#C41E3A', width=3),
                hovertemplate=f'{name1}: ' + '%{y:.2f}' + unit1
            ),
            secondary_y=False
        )
        
        # 第二个指标（右Y轴）
        fig.add_trace(
            go.Scatter(
                x=data['date_label'].tolist(),
                y=data[col2].tolist(),
                mode='lines',
                name=f'{name2}（右轴）',
                line=dict(color='#F5A623', width=3),
                hovertemplate=f'{name2}: ' + '%{y:.2f}' + unit2
            ),
            secondary_y=True
        )
        
        # 更新布局
        fig.update_layout(
            hovermode='x unified',
            plot_bgcolor='white',
            paper_bgcolor='white',
            height=400,
            font=dict(family="Microsoft YaHei, SimHei, Arial", size=12),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.25,
                xanchor="center",
                x=0.5,
                font=dict(size=11)
            ),
            margin=dict(l=60, r=60, t=40, b=80),
            dragmode='zoom',  # 启用拖拽缩放
            modebar=dict(
                orientation='v',
                bgcolor='rgba(255,255,255,0.7)',
                activecolor='#C41E3A'
            )
        )
        
        # 设置X轴
        fig.update_xaxes(
            title_text="",
            showgrid=False,
            showline=True,
            linewidth=1,
            linecolor='#E0E0E0',
            tickangle=0,
            tickfont=dict(size=10),
            type='category'
        )
        
        # 设置左Y轴
        fig.update_yaxes(
            title_text=f'{name1} ({unit1})',
            secondary_y=False,
            showgrid=True,
            gridwidth=1,
            gridcolor='#F0F0F0',
            showline=True,
            linewidth=1,
            linecolor='#E0E0E0',
            title_font=dict(color='#C41E3A', size=11),
            tickfont=dict(color='#C41E3A', size=10),
            fixedrange=False  # 允许用户调整Y轴范围
        )
        
        # 设置右Y轴
        fig.update_yaxes(
            title_text=f'{name2} ({unit2})',
            secondary_y=True,
            showgrid=False,
            showline=True,
            linewidth=1,
            linecolor='#E0E0E0',
            title_font=dict(color='#F5A623', size=11),
            tickfont=dict(color='#F5A623', size=10),
            fixedrange=False  # 允许用户调整Y轴范围
        )
        
        return fig.to_html(full_html=False, include_plotlyjs=False)
    
    def _create_single_line_chart(
        self,
        data: pd.DataFrame,
        column: str,
        indicator_name: str,
        unit: str,
        line_name: str,
        color: str
    ) -> str:
        """
        创建单条折线图
        """
        data = data.copy()
        data['report_date'] = pd.to_datetime(data['report_date'])
        
        # 格式化为"YYYY年"，对于非12月31日的数据添加季度TTM标记
        def format_date_label(row):
            year = row['report_date'].year
            month = row['report_date'].month
            if month == 12 and row['report_date'].day == 31:
                return f'{year}年'
            else:
                # 根据月份确定季度
                quarter = (month - 1) // 3 + 1
                return f'{year}Q{quarter}-TTM'
        
        data['date_label'] = data.apply(format_date_label, axis=1)
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=data['date_label'].tolist(),
            y=data[column].tolist(),
            mode='lines',
            name=line_name,
            line=dict(color=color, width=3),
            hovertemplate='%{y:.2f}' + unit
        ))
        
        fig.update_layout(
            hovermode='x unified',
            plot_bgcolor='white',
            paper_bgcolor='white',
            height=400,
            font=dict(family="Microsoft YaHei, SimHei, Arial", size=12),
            margin=dict(l=60, r=60, t=40, b=80),
            showlegend=False,
            dragmode='zoom',  # 启用拖拽缩放
            modebar=dict(
                orientation='v',
                bgcolor='rgba(255,255,255,0.7)',
                activecolor='#C41E3A'
            )
        )
        
        fig.update_xaxes(
            title_text="",
            showgrid=False,
            showline=True,
            linewidth=1,
            linecolor='#E0E0E0',
            tickangle=0,
            tickfont=dict(size=10),
            type='category'
        )
        
        fig.update_yaxes(
            title_text=f'{indicator_name} ({unit})',
            showgrid=True,
            gridwidth=1,
            gridcolor='#F0F0F0',
            showline=True,
            linewidth=1,
            linecolor='#E0E0E0',
            tickfont=dict(size=10)
        )
        
        return fig.to_html(full_html=False, include_plotlyjs=False)
    
    def _create_comparison_chart(
        self,
        data: pd.DataFrame,
        col1: str,
        col2: str,
        name1: str,
        name2: str,
        indicator_name: str,
        unit: str
    ) -> str:
        """
        创建双线对比图表（用于目标公司 vs 全A股中位数）
        """
        data = data.copy()
        data['report_date'] = pd.to_datetime(data['report_date'])
        
        # 格式化为"YYYY年"，对于非12月31日的数据添加季度TTM标记
        def format_date_label(row):
            year = row['report_date'].year
            month = row['report_date'].month
            if month == 12 and row['report_date'].day == 31:
                return f'{year}年'
            else:
                # 根据月份确定季度
                quarter = (month - 1) // 3 + 1
                return f'{year}Q{quarter}-TTM'
        
        data['date_label'] = data.apply(format_date_label, axis=1)
        
        fig = go.Figure()
        
        # 添加目标公司的线
        fig.add_trace(go.Scatter(
            x=data['date_label'].tolist(),
            y=data[col1].tolist(),
            mode='lines+markers',
            name=name1,
            line=dict(color='#C41E3A', width=3),
            marker=dict(size=6),
            hovertemplate=f'{name1}: ' + '%{y:.2f}' + unit
        ))
        
        # 添加全A股中位数的线
        fig.add_trace(go.Scatter(
            x=data['date_label'].tolist(),
            y=data[col2].tolist(),
            mode='lines+markers',
            name=name2,
            line=dict(color='#F5A623', width=3, dash='dash'),
            marker=dict(size=6),
            hovertemplate=f'{name2}: ' + '%{y:.2f}' + unit
        ))
        
        fig.update_layout(
            hovermode='x unified',
            plot_bgcolor='white',
            paper_bgcolor='white',
            height=400,
            font=dict(family="Microsoft YaHei, SimHei, Arial", size=12),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.25,
                xanchor="center",
                x=0.5,
                font=dict(size=11)
            ),
            margin=dict(l=60, r=60, t=40, b=80),
            showlegend=True,
            dragmode='zoom',  # 启用拖拽缩放
            modebar=dict(
                orientation='v',
                bgcolor='rgba(255,255,255,0.7)',
                activecolor='#C41E3A'
            )
        )
        
        fig.update_xaxes(
            title_text="",
            showgrid=False,
            showline=True,
            linewidth=1,
            linecolor='#E0E0E0',
            tickangle=0,
            tickfont=dict(size=10),
            type='category'
        )
        
        fig.update_yaxes(
            title_text=f'{indicator_name} ({unit})',
            showgrid=True,
            gridwidth=1,
            gridcolor='#F0F0F0',
            showline=True,
            linewidth=1,
            linecolor='#E0E0E0',
            tickfont=dict(size=10),
            fixedrange=False  # 允许用户调整Y轴范围
        )
        
        return fig.to_html(full_html=False, include_plotlyjs=False)
    
    def _create_percentile_chart(
        self,
        comparison_df: pd.DataFrame,
        indicator_name: str,
        column_name: str
    ) -> str:
        """
        创建分位数历史走势图
        """
        comp_data = comparison_df.copy()
        comp_data['report_date'] = pd.to_datetime(comp_data['report_date'])
        
        # 格式化为"YYYY年"，对于非12月31日的数据添加季度TTM标记
        def format_date_label(row):
            year = row['report_date'].year
            month = row['report_date'].month
            if month == 12 and row['report_date'].day == 31:
                return f'{year}年'
            else:
                # 根据月份确定季度
                quarter = (month - 1) // 3 + 1
                return f'{year}Q{quarter}-TTM'
        
        comp_data['date_label'] = comp_data.apply(format_date_label, axis=1)
        comp_data['percentile'] = comp_data['percentile'] * 100
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=comp_data['date_label'].tolist(),
            y=comp_data['percentile'].tolist(),
            mode='lines+markers',
            name='市场分位数',
            line=dict(color='#2196F3', width=2),
            marker=dict(size=8),
            fill='tozeroy',
            fillcolor='rgba(33, 150, 243, 0.1)'
        ))
        
        # 添加参考线
        fig.add_hline(y=50, line_dash="dash", line_color="gray", 
                     annotation_text="中位数(50%)", annotation_position="right")
        fig.add_hline(y=75, line_dash="dot", line_color="lightgray",
                     annotation_text="75%分位", annotation_position="right")
        fig.add_hline(y=25, line_dash="dot", line_color="lightgray",
                     annotation_text="25%分位", annotation_position="right")
        
        fig.update_layout(
            title=f'{indicator_name} - 在全A股中的分位数走势',
            xaxis_title='报告日期',
            yaxis_title='分位数 (%)',
            hovermode='x unified',
            template='plotly_white',
            height=400,
            font=dict(family="Microsoft YaHei, SimHei, Arial", size=12),
            yaxis=dict(range=[0, 100], fixedrange=False),  # 允许用户调整Y轴范围
            xaxis=dict(type='category', tickangle=0),
            dragmode='zoom',  # 启用拖拽缩放
            modebar=dict(
                orientation='v',
                bgcolor='rgba(255,255,255,0.7)',
                activecolor='#C41E3A'
            )
        )
        
        return fig.to_html(full_html=False, include_plotlyjs=False, div_id=f'percentile_{column_name}')
    
    def _create_distribution_chart(
        self,
        distribution: Dict,
        company_value: Optional[float],
        indicator_name: str,
        unit: str
    ) -> str:
        """
        创建市场分布直方图
        """
        hist = distribution['histogram']
        bin_edges = distribution['bin_edges']
        stats = distribution['statistics']
        
        # 计算bin中心点
        bin_centers = [(bin_edges[i] + bin_edges[i+1]) / 2 for i in range(len(bin_edges)-1)]
        
        # 转换百分比
        if unit == '%':
            bin_centers = [x * 100 for x in bin_centers]
            if company_value is not None:
                company_value = company_value
            for key in ['mean', 'median', 'min', 'max', 'q25', 'q75']:
                stats[key] = stats[key] * 100
        
        fig = go.Figure()
        
        # 直方图
        fig.add_trace(go.Bar(
            x=bin_centers,
            y=hist,
            name='公司数量分布',
            marker_color='rgba(33, 150, 243, 0.6)'
        ))
        
        # 添加公司位置标记
        if company_value is not None:
            fig.add_vline(
                x=company_value,
                line_dash="dash",
                line_color="red",
                line_width=3,
                annotation_text="目标公司",
                annotation_position="top"
            )
        
        fig.update_layout(
            title=f'{indicator_name} - 全A股当前分布（共{stats["count"]}家公司）',
            xaxis_title=f'{indicator_name} ({unit})',
            yaxis_title='公司数量',
            template='plotly_white',
            height=400,
            font=dict(family="Microsoft YaHei, SimHei, Arial", size=12),
            showlegend=False,
            dragmode='zoom',  # 启用拖拽缩放
            modebar=dict(
                orientation='v',
                bgcolor='rgba(255,255,255,0.7)',
                activecolor='#C41E3A'
            )
        )
        
        # 允许用户调整Y轴范围
        fig.update_yaxes(fixedrange=False)
        
        return fig.to_html(full_html=False, include_plotlyjs=False, div_id=f'dist_{indicator_name}')
    
    def export_to_excel(
        self,
        analysis_result: Dict,
        output_filename: Optional[str] = None
    ) -> str:
        """
        导出分析结果到Excel
        
        Args:
            analysis_result: 分析结果
            output_filename: 输出文件名
            
        Returns:
            生成的Excel文件路径
        """
        stock_code = analysis_result['stock_code']
        
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"{stock_code}_分析数据_{timestamp}.xlsx"
        
        output_path = os.path.join(self.output_dir, output_filename)
        
        # 创建Excel写入器
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # 写入基本信息
            info_df = pd.DataFrame([analysis_result['company_info']])
            info_df.to_excel(writer, sheet_name='基本信息', index=False)
            
            # 写入指标数据（显示所有季度数据）
            indicators = analysis_result['indicators'].copy()
            indicators['report_date'] = pd.to_datetime(indicators['report_date'])
            
            # 转换百分比列
            percentage_columns = ['gross_margin', 'working_capital_ratio', 'operating_cashflow_ratio']
            for col in percentage_columns:
                if col in indicators.columns:
                    indicators[col] = indicators[col] * 100
            
            # 重命名列
            column_names = {
                'report_date': '报告日期',
                'ar_turnover': '应收账款周转率(次)',
                'gross_margin': '毛利率(%)',
                'lt_asset_turnover': '长期资产周转率(次)',
                'working_capital_ratio': '营运净资本比率(%)',
                'operating_cashflow_ratio': '经营现金流比率(%)'
            }
            indicators = indicators.rename(columns=column_names)
            
            indicators.to_excel(writer, sheet_name='财务指标', index=False)
            
            # 写入市场对比数据
            market_comparison = analysis_result.get('market_comparison', {})
            for indicator_col, comparison_df in market_comparison.items():
                if isinstance(comparison_df, pd.DataFrame) and len(comparison_df) > 0:
                    comp_df = comparison_df.copy()
                    # 转换百分比
                    if indicator_col in ['gross_margin', 'working_capital_ratio', 'operating_cashflow_ratio']:
                        for col in ['company_value', 'market_median']:
                            if col in comp_df.columns:
                                comp_df[col] = comp_df[col] * 100
                    if 'percentile' in comp_df.columns:
                        comp_df['percentile'] = comp_df['percentile'] * 100
                    
                    # 重命名列
                    comp_df = comp_df.rename(columns={
                        'report_date': '报告日期',
                        'company_value': '公司值',
                        'market_median': '市场中位数',
                        'percentile': '分位数(%)'
                    })
                    
                    sheet_name = f'市场对比_{indicator_col[:10]}'
                    comp_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        return output_path
