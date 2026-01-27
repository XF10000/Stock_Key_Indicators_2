"""
AkShare API 客户端模块

负责从 akshare 获取股票财务数据
"""
import time
import pandas as pd
import akshare as ak
from typing import Optional
from pathlib import Path
import json
from datetime import datetime, timedelta
import signal


class AkshareClient:
    """AkShare API 客户端"""
    
    def __init__(
        self,
        request_delay: float = 0.5,
        retry_times: int = 3,
        retry_delay: float = 5.0,
        timeout: float = 30.0,
        cache_dir: str = "cache",
        cache_days: int = 7
    ):
        """
        初始化 AkShare 客户端
        
        Args:
            request_delay: API 请求间隔（秒）
            retry_times: 失败重试次数
            retry_delay: 重试间隔（秒）
            timeout: API 请求超时时间（秒）
            cache_dir: 缓存目录
            cache_days: 缓存有效期（天）
        """
        self.request_delay = request_delay
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.cache_dir = Path(cache_dir)
        self.cache_days = cache_days
        
        # 创建缓存目录
        self.cache_dir.mkdir(exist_ok=True)
        self.stock_list_cache_file = self.cache_dir / "stock_list_cache.json"
    
    def get_stock_list(self, use_cache: bool = True) -> pd.DataFrame:
        """
        获取全部 A 股股票列表（支持本地缓存）
        
        Args:
            use_cache: 是否使用缓存，默认True
        
        Returns:
            包含股票代码和名称的 DataFrame
        """
        # 1. 检查缓存是否存在且有效
        if use_cache and self._is_cache_valid():
            try:
                cached_data = self._load_cache()
                if cached_data is not None:
                    print(f"✓ 使用本地缓存的股票列表（{len(cached_data)} 只股票）")
                    return cached_data
            except Exception as e:
                print(f"⚠ 加载缓存失败: {e}，将从API获取")
        
        # 2. 从API获取
        print("正在从API获取股票列表...")
        for attempt in range(self.retry_times):
            try:
                # 使用 stock_info_a_code_name 接口（更稳定）
                df = ak.stock_info_a_code_name()
                
                if df is not None and not df.empty:
                    # 提取股票代码和名称
                    # 返回的列名是：code、name
                    result = pd.DataFrame({
                        'stock_code': df['code'].values,
                        'stock_name': df['name'].values
                    })
                    
                    # 添加市场标识
                    result['market_code'] = result['stock_code'].apply(self._add_market_prefix)
                    
                    # 3. 保存到缓存
                    if use_cache:
                        self._save_cache(result)
                        print(f"✓ 已保存股票列表到缓存（{len(result)} 只股票）")
                    
                    return result
                
            except Exception as e:
                if attempt < self.retry_times - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise Exception(f"获取股票列表失败: {e}")
        
        raise Exception("获取股票列表失败")
    
    def _is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        if not self.stock_list_cache_file.exists():
            return False
        
        try:
            with open(self.stock_list_cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache_time = datetime.fromisoformat(cache_data['timestamp'])
            age = datetime.now() - cache_time
            
            return age.days < self.cache_days
        except Exception:
            return False
    
    def _load_cache(self) -> Optional[pd.DataFrame]:
        """从缓存加载股票列表"""
        try:
            with open(self.stock_list_cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            df = pd.DataFrame(cache_data['data'])
            return df
        except Exception as e:
            raise Exception(f"加载缓存失败: {e}")
    
    def _save_cache(self, df: pd.DataFrame):
        """保存股票列表到缓存"""
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'count': len(df),
                'data': df.to_dict('records')
            }
            
            with open(self.stock_list_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠ 保存缓存失败: {e}")
    
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
    ) -> pd.DataFrame:
        """
        通用的财务数据获取方法，带重试机制
        
        超时控制由外层的ThreadPoolExecutor的future.result(timeout=310)处理
        不在这里嵌套使用ThreadPoolExecutor，避免线程池死锁
        
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
                
                # 直接调用akshare API，超时由外层控制
                df = fetch_func(symbol=stock_code)
                
                if df is not None and not df.empty:
                    return df
                else:
                    return None
                    
            except Exception as e:
                # 任何异常都重试
                if attempt < self.retry_times - 1:
                    time.sleep(self.retry_delay)
                else:
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
