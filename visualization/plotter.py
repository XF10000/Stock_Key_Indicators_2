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
        
        # HTML头部
        html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{company_name} 财务指标分析报告</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
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
                'title': '指标2：再投资质量'
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
                'title': '指标3：产业链地位'
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
                'title': '指标4：真实盈利能力'
            }
        ]
        
        for config in standard_indicators:
            html += f'<h2 style="color: #C41E3A; border-bottom: 2px solid #C41E3A; padding-bottom: 10px;">{config["title"]} - {config["name"]}</h2>'
            html += f'<p style="color: #666; margin-bottom: 20px;">{config["description"]}</p>'
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
        </div>
    </div>
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
        ar_data = ar_data[ar_data['ar_turnover'].notna() & ar_data['gross_margin'].notna()]
        
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
                chart3 = self._create_percentile_chart(
                    ar_comp_annual, '应收账款周转率对数', 'ar_turnover'
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
        valid_data = valid_data[valid_data[column_name].notna()]
        
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
                
                # 合并公司数据和市场中位数数据
                merged_data = valid_data.merge(median_data, on='report_date', how='outer')
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
                chart3 = self._create_percentile_chart(
                    percentile_data, indicator_name, column_name
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
        
        # 格式化为"YYYY年"，对于非12月31日的数据添加TTM标记
        def format_date_label(row):
            year = row['report_date'].year
            if row['report_date'].month == 12 and row['report_date'].day == 31:
                return f'{year}年'
            else:
                return f'{year}年TTM'
        
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
            margin=dict(l=60, r=60, t=40, b=80)
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
            tickfont=dict(color='#C41E3A', size=10)
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
            tickfont=dict(color='#F5A623', size=10)
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
        
        # 格式化为"YYYY年"，对于非12月31日的数据添加TTM标记
        def format_date_label(row):
            year = row['report_date'].year
            if row['report_date'].month == 12 and row['report_date'].day == 31:
                return f'{year}年'
            else:
                return f'{year}年TTM'
        
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
            showlegend=False
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
        
        # 格式化为"YYYY年"，对于非12月31日的数据添加TTM标记
        def format_date_label(row):
            year = row['report_date'].year
            if row['report_date'].month == 12 and row['report_date'].day == 31:
                return f'{year}年'
            else:
                return f'{year}年TTM'
        
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
            showlegend=True
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
        
        # 格式化为"YYYY年"，对于非12月31日的数据添加TTM标记
        def format_date_label(row):
            year = row['report_date'].year
            if row['report_date'].month == 12 and row['report_date'].day == 31:
                return f'{year}年'
            else:
                return f'{year}年TTM'
        
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
            yaxis=dict(range=[0, 100]),
            xaxis=dict(type='category', tickangle=0)
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
            showlegend=False
        )
        
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
