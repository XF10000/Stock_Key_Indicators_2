"""
AkShare API 客户端模块

负责从 akshare 获取股票财务数据
"""
import time
import pandas as pd
import akshare as ak
from typing import Optional, List
from datetime import datetime


class AkshareClient:
    """AkShare API 客户端"""
    
    def __init__(
        self,
        request_delay: float = 0.5,
        retry_times: int = 3,
        retry_delay: float = 5.0,
        timeout: float = 30.0
    ):
        """
        初始化 AkShare 客户端
        
        Args:
            request_delay: API 请求间隔（秒）
            retry_times: 失败重试次数
            retry_delay: 重试间隔（秒）
            timeout: API 请求超时时间（秒）
        """
        self.request_delay = request_delay
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.timeout = timeout
    
    def get_stock_list(self) -> pd.DataFrame:
        """
        获取全部 A 股股票列表
        
        Returns:
            包含股票代码和名称的 DataFrame
        """
        for attempt in range(self.retry_times):
            try:
                # 获取沪深京 A 股列表
                df = ak.stock_zh_a_spot_em()
                
                if df is not None and not df.empty:
                    # 提取股票代码和名称
                    # akshare 返回的列名可能是：代码、名称
                    result = pd.DataFrame({
                        'stock_code': df['代码'].values,
                        'stock_name': df['名称'].values
                    })
                    
                    # 添加市场标识
                    result['market_code'] = result['stock_code'].apply(self._add_market_prefix)
                    
                    return result
                
            except Exception as e:
                if attempt < self.retry_times - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise Exception(f"获取股票列表失败: {e}")
        
        raise Exception("获取股票列表失败")
    
    def _add_market_prefix(self, code: str) -> str:
        """
        为股票代码添加市场前缀
        
        Args:
            code: 股票代码
            
        Returns:
            带市场前缀的代码（如 SH600519）
        """
        if code.startswith('6'):
            return f'SH{code}'
        elif code.startswith('0') or code.startswith('3'):
            return f'SZ{code}'
        elif code.startswith('4') or code.startswith('8'):
            return f'BJ{code}'
        else:
            return f'SH{code}'  # 默认上海
    
    def get_balance_sheet(self, stock_code: str) -> Optional[pd.DataFrame]:
        """
        获取资产负债表
        
        Args:
            stock_code: 带市场标识的股票代码（如 SH600519）
            
        Returns:
            资产负债表 DataFrame，失败返回 None
        """
        return self._fetch_financial_data(
            ak.stock_balance_sheet_by_report_em,
            stock_code,
            "资产负债表"
        )
    
    def get_income_statement(self, stock_code: str) -> Optional[pd.DataFrame]:
        """
        获取利润表
        
        Args:
            stock_code: 带市场标识的股票代码（如 SH600519）
            
        Returns:
            利润表 DataFrame，失败返回 None
        """
        return self._fetch_financial_data(
            ak.stock_profit_sheet_by_report_em,
            stock_code,
            "利润表"
        )
    
    def get_cash_flow_statement(self, stock_code: str) -> Optional[pd.DataFrame]:
        """
        获取现金流量表
        
        Args:
            stock_code: 带市场标识的股票代码（如 SH600519）
            
        Returns:
            现金流量表 DataFrame，失败返回 None
        """
        return self._fetch_financial_data(
            ak.stock_cash_flow_sheet_by_report_em,
            stock_code,
            "现金流量表"
        )
    
    def _fetch_financial_data(
        self,
        fetch_func,
        stock_code: str,
        data_type: str
    ) -> Optional[pd.DataFrame]:
        """
        通用的财务数据获取方法
        
        Args:
            fetch_func: akshare 获取函数
            stock_code: 股票代码
            data_type: 数据类型（用于日志）
            
        Returns:
            DataFrame 或 None
        """
        for attempt in range(self.retry_times):
            try:
                # 请求延迟
                time.sleep(self.request_delay)
                
                df = fetch_func(symbol=stock_code)
                
                if df is not None and not df.empty:
                    return df
                else:
                    return None
                    
            except Exception as e:
                if attempt < self.retry_times - 1:
                    time.sleep(self.retry_delay)
                else:
                    # 最后一次尝试失败，返回 None
                    return None
        
        return None
    
    def get_all_financial_data(self, stock_code: str) -> dict:
        """
        获取指定股票的所有财务数据
        
        Args:
            stock_code: 带市场标识的股票代码
            
        Returns:
            包含三张报表的字典
        """
        return {
            'balance_sheet': self.get_balance_sheet(stock_code),
            'income_statement': self.get_income_statement(stock_code),
            'cash_flow_statement': self.get_cash_flow_statement(stock_code)
        }
