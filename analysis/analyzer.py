"""
市场分析模块

实现市场中位数计算、历史分位数计算和缓存逻辑
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from datetime import datetime, date

from data_provider.repository import Repository


class MarketAnalyzer:
    """市场分析器"""
    
    def __init__(self, repository: Repository, cache_version: str):
        """
        初始化市场分析器
        
        Args:
            repository: 数据库仓储
            cache_version: 缓存版本号
        """
        self.repository = repository
        self.cache_version = cache_version
        self._median_cache = {}  # 内存缓存
    
    def calculate_market_median(
        self,
        indicator_name: str,
        report_date: date,
        indicator_values: List[float]
    ) -> Optional[float]:
        """
        计算市场中位数
        
        Args:
            indicator_name: 指标名称
            report_date: 报告日期
            indicator_values: 全市场该指标的值列表
            
        Returns:
            中位数，计算失败返回None
        """
        # 检查内存缓存
        cache_key = f"{indicator_name}_{report_date}_{self.cache_version}"
        if cache_key in self._median_cache:
            return self._median_cache[cache_key]
        
        # 检查数据库缓存
        cached_value = self._get_cached_median(indicator_name, report_date)
        if cached_value is not None:
            self._median_cache[cache_key] = cached_value
            return cached_value
        
        # 计算中位数
        try:
            # 过滤None和无效值
            valid_values = [v for v in indicator_values if v is not None and not np.isnan(v)]
            
            if len(valid_values) == 0:
                return None
            
            median = float(np.median(valid_values))
            
            # 保存到缓存
            self._save_median_to_cache(indicator_name, report_date, median)
            self._median_cache[cache_key] = median
            
            return median
        except Exception:
            return None
    
    def calculate_percentile(
        self,
        value: float,
        indicator_values: List[float]
    ) -> Optional[float]:
        """
        计算分位数（目标公司在全市场中的排名）
        
        Args:
            value: 目标公司的指标值
            indicator_values: 全市场该指标的值列表
            
        Returns:
            分位数（0-1之间，如0.75表示75%分位），计算失败返回None
        """
        try:
            # 过滤None和无效值
            valid_values = [v for v in indicator_values if v is not None and not np.isnan(v)]
            
            if len(valid_values) == 0:
                return None
            
            # 计算分位数
            percentile = np.sum(np.array(valid_values) <= value) / len(valid_values)
            
            return float(percentile)
        except Exception:
            return None
    
    def calculate_distribution(
        self,
        indicator_values: List[float],
        bins: int = 20
    ) -> Dict[str, any]:
        """
        计算指标的分布情况
        
        Args:
            indicator_values: 全市场该指标的值列表
            bins: 分箱数量
            
        Returns:
            分布信息字典，包含：
            - histogram: 直方图数据
            - bin_edges: 分箱边界
            - statistics: 统计信息（均值、中位数、标准差等）
        """
        try:
            # 过滤None和无效值
            valid_values = [v for v in indicator_values if v is not None and not np.isnan(v)]
            
            if len(valid_values) == 0:
                return None
            
            values_array = np.array(valid_values)
            
            # 计算直方图
            hist, bin_edges = np.histogram(values_array, bins=bins)
            
            # 计算统计信息
            statistics = {
                'mean': float(np.mean(values_array)),
                'median': float(np.median(values_array)),
                'std': float(np.std(values_array)),
                'min': float(np.min(values_array)),
                'max': float(np.max(values_array)),
                'q25': float(np.percentile(values_array, 25)),
                'q75': float(np.percentile(values_array, 75)),
                'count': len(valid_values)
            }
            
            return {
                'histogram': hist.tolist(),
                'bin_edges': bin_edges.tolist(),
                'statistics': statistics
            }
        except Exception:
            return None
    
    def _get_cached_median(
        self,
        indicator_name: str,
        report_date: date
    ) -> Optional[float]:
        """
        从数据库获取缓存的中位数
        
        Args:
            indicator_name: 指标名称
            report_date: 报告日期
            
        Returns:
            缓存的中位数，不存在返回None
        """
        session = self.repository.get_session()
        try:
            from models import IndicatorMedian
            
            result = session.query(IndicatorMedian).filter_by(
                indicator_name=indicator_name,
                report_date=report_date,
                cache_version=self.cache_version
            ).first()
            
            if result:
                return result.median_value
            return None
        finally:
            session.close()
    
    def _save_median_to_cache(
        self,
        indicator_name: str,
        report_date: date,
        median_value: float
    ) -> None:
        """
        保存中位数到数据库缓存
        
        Args:
            indicator_name: 指标名称
            report_date: 报告日期
            median_value: 中位数值
        """
        self.repository.save_indicator_median(
            indicator_name=indicator_name,
            report_date=report_date,
            median_value=median_value,
            cache_version=self.cache_version
        )
    
    def clear_cache(self) -> None:
        """清空缓存"""
        # 清空内存缓存
        self._median_cache.clear()
        
        # 清空数据库缓存
        self.repository.clear_cache(self.cache_version)
    
    def get_market_data_for_indicator(
        self,
        indicator_name: str,
        report_date: date,
        stock_codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取全市场某个指标的数据
        
        Args:
            indicator_name: 指标名称
            report_date: 报告日期
            stock_codes: 股票代码列表（None表示全市场）
            
        Returns:
            包含股票代码和指标值的DataFrame
        """
        # TODO: 实现从数据库获取市场数据的逻辑
        # 这个方法将在后续实现，用于从数据库获取全市场数据
        pass
    
    def analyze_company_vs_market(
        self,
        stock_code: str,
        indicator_name: str,
        company_values: Dict[date, float],
        market_data: Dict[date, List[float]]
    ) -> pd.DataFrame:
        """
        分析目标公司与市场的对比
        
        Args:
            stock_code: 股票代码
            indicator_name: 指标名称
            company_values: 公司各期指标值 {报告日期: 指标值}
            market_data: 市场各期数据 {报告日期: [全市场指标值列表]}
            
        Returns:
            包含公司值、市场中位数、分位数的DataFrame
        """
        results = []
        
        for report_date, company_value in company_values.items():
            if report_date not in market_data:
                continue
            
            market_values = market_data[report_date]
            
            # 计算市场中位数
            median = self.calculate_market_median(
                indicator_name,
                report_date,
                market_values
            )
            
            # 计算分位数
            percentile = self.calculate_percentile(
                company_value,
                market_values
            )
            
            results.append({
                'report_date': report_date,
                'stock_code': stock_code,
                'company_value': company_value,
                'market_median': median,
                'percentile': percentile
            })
        
        return pd.DataFrame(results)
