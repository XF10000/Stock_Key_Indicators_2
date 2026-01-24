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
        indicators = analysis_result['indicators']
        
        # HTML头部
        html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{stock_code} 财务指标分析报告</title>
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
        <h1>{stock_code} 财务指标分析报告</h1>
        
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
        
        # 添加各指标图表
        indicator_configs = [
            {
                'name': '应收账款周转率',
                'column': 'ar_turnover',
                'unit': '次',
                'description': '衡量公司应收账款管理效率和回款能力'
            },
            {
                'name': '毛利率',
                'column': 'gross_margin',
                'unit': '%',
                'description': '衡量公司产品或服务的盈利能力'
            },
            {
                'name': '长期资产周转率',
                'column': 'lt_asset_turnover',
                'unit': '次',
                'description': '衡量公司长期资产的使用效率和再投资质量'
            },
            {
                'name': '营运净资本比率',
                'column': 'working_capital_ratio',
                'unit': '%',
                'description': '衡量公司在产业链中的地位和议价能力'
            },
            {
                'name': '经营现金流比率',
                'column': 'operating_cashflow_ratio',
                'unit': '%',
                'description': '衡量公司真实的盈利能力和现金创造能力'
            }
        ]
        
        for config in indicator_configs:
            chart_html = self._create_indicator_chart(
                indicators,
                config['name'],
                config['column'],
                config['unit'],
                config['description']
            )
            html += chart_html
        
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
    
    def _create_indicator_chart(
        self,
        indicators: pd.DataFrame,
        indicator_name: str,
        column_name: str,
        unit: str,
        description: str
    ) -> str:
        """
        创建单个指标的图表HTML
        
        Args:
            indicators: 指标数据
            indicator_name: 指标名称
            column_name: 列名
            unit: 单位
            description: 描述
            
        Returns:
            图表HTML字符串
        """
        # 过滤有效数据
        valid_data = indicators[indicators[column_name].notna()].copy()
        
        if len(valid_data) == 0:
            return f"""
        <h2>{indicator_name}</h2>
        <p style="color: #999; font-style: italic;">暂无数据</p>
"""
        
        # 转换百分比单位
        if unit == '%':
            valid_data[column_name] = valid_data[column_name] * 100
        
        # 创建图表
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=valid_data['report_date'],
            y=valid_data[column_name],
            mode='lines+markers',
            name=indicator_name,
            line=dict(color='#4CAF50', width=2),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            title=f'{indicator_name}历史走势',
            xaxis_title='报告日期',
            yaxis_title=f'{indicator_name} ({unit})',
            hovermode='x unified',
            template='plotly_white',
            height=400,
            font=dict(family="Microsoft YaHei, SimHei, Arial", size=12)
        )
        
        # 转换为HTML
        chart_div = fig.to_html(
            full_html=False,
            include_plotlyjs=False,
            div_id=f'chart_{column_name}'
        )
        
        html = f"""
        <h2>{indicator_name}</h2>
        <p style="color: #666; margin-bottom: 20px;">{description}</p>
        <div class="chart-container">
            {chart_div}
        </div>
"""
        
        return html
    
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
            
            # 写入指标数据
            indicators = analysis_result['indicators'].copy()
            
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
        
        return output_path
