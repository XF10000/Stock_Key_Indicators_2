"""
数据库仓储模块

负责与数据库的交互，包括数据的存储和读取
"""
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Dict, Any, Optional
from datetime import datetime, date

from models import Base, BalanceSheet, IncomeStatement, CashFlowStatement, IndicatorMedian


class Repository:
    """数据库仓储类"""
    
    def __init__(self, database_url: str):
        """
        初始化仓储
        
        Args:
            database_url: 数据库连接 URL
        """
        # 增加连接池大小，避免高并发时连接池耗尽
        # pool_size: 连接池大小（默认5）
        # max_overflow: 最大溢出连接数（默认10）
        # pool_recycle: 连接回收时间，避免长时间连接失效（秒）
        # pool_pre_ping: 使用连接前先ping，确保连接有效
        self.engine = create_engine(
            database_url, 
            echo=False,
            pool_size=20,           # 增加到20
            max_overflow=30,        # 最大溢出30
            pool_recycle=3600,      # 1小时回收
            pool_pre_ping=True      # 使用前ping
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def get_session(self) -> Session:
        """获取数据库会话"""
        return self.Session()
    
    def save_balance_sheets(
        self,
        df: pd.DataFrame,
        column_mapping: Dict[str, str]
    ) -> tuple[int, int]:
        """
        保存资产负债表数据
        
        Args:
            df: 原始 DataFrame
            column_mapping: 列名映射字典
            
        Returns:
            (新增记录数, 跳过记录数)
        """
        return self._save_financial_data(df, BalanceSheet, column_mapping)
    
    def save_income_statements(
        self,
        df: pd.DataFrame,
        column_mapping: Dict[str, str]
    ) -> tuple[int, int]:
        """
        保存利润表数据
        
        Args:
            df: 原始 DataFrame
            column_mapping: 列名映射字典
            
        Returns:
            (新增记录数, 跳过记录数)
        """
        return self._save_financial_data(df, IncomeStatement, column_mapping)
    
    def save_cash_flow_statements(
        self,
        df: pd.DataFrame,
        column_mapping: Dict[str, str]
    ) -> tuple[int, int]:
        """
        保存现金流量表数据
        
        Args:
            df: 原始 DataFrame
            column_mapping: 列名映射字典
            
        Returns:
            (新增记录数, 跳过记录数)
        """
        return self._save_financial_data(df, CashFlowStatement, column_mapping)
    
    def _save_financial_data(
        self,
        df: pd.DataFrame,
        model_class,
        column_mapping: Dict[str, str]
    ) -> tuple[int, int]:
        """
        通用的财务数据保存方法
        
        Args:
            df: 原始 DataFrame
            model_class: SQLAlchemy 模型类
            column_mapping: 列名映射字典
            
        Returns:
            (新增记录数, 跳过记录数)
        """
        if df is None or df.empty:
            return 0, 0
        
        session = self.get_session()
        added_count = 0
        skipped_count = 0
        
        try:
            # 重命名列
            df_renamed = df.rename(columns=column_mapping)
            
            # 只保留模型中存在的列
            valid_cols = [col for col in df_renamed.columns if hasattr(model_class, col)]
            df_to_save = df_renamed[valid_cols]
            
            for _, row in df_to_save.iterrows():
                data_dict = self._prepare_data_dict(row, valid_cols)
                
                # 检查记录是否已存在
                existing = session.query(model_class).filter_by(
                    stock_code=data_dict.get('stock_code'),
                    report_date=data_dict.get('report_date')
                ).first()
                
                if existing:
                    skipped_count += 1
                else:
                    try:
                        record = model_class(**data_dict)
                        session.add(record)
                        added_count += 1
                    except Exception as e:
                        # 单条记录失败不影响其他记录
                        continue
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
        
        return added_count, skipped_count
    
    def _prepare_data_dict(self, row: pd.Series, valid_cols: List[str]) -> Dict[str, Any]:
        """
        准备数据字典，处理类型转换
        
        Args:
            row: DataFrame 的一行
            valid_cols: 有效的列名列表
            
        Returns:
            处理后的数据字典
        """
        data_dict = {}
        
        for col in valid_cols:
            value = row[col]
            
            # 处理 Series 对象
            if isinstance(value, pd.Series):
                value = value.iloc[0] if len(value) > 0 else None
            
            # 处理 NaN
            try:
                if pd.isna(value):
                    data_dict[col] = None
                else:
                    # 特殊处理日期字段
                    if col in ['report_date', 'announcement_date']:
                        data_dict[col] = self._convert_to_date(value)
                    else:
                        data_dict[col] = value
            except (ValueError, TypeError):
                data_dict[col] = value
        
        return data_dict
    
    def _convert_to_date(self, value: Any) -> Optional[date]:
        """
        转换为日期对象
        
        Args:
            value: 日期值（字符串或 datetime）
            
        Returns:
            date 对象或 None
        """
        if value is None:
            return None
        
        if isinstance(value, date):
            return value
        
        if isinstance(value, datetime):
            return value.date()
        
        if isinstance(value, str):
            try:
                return pd.to_datetime(value).date()
            except:
                return None
        
        return None
    
    def get_processed_stocks(self) -> set:
        """
        获取已处理的股票代码集合
        
        Returns:
            已处理的股票代码集合
        """
        session = self.get_session()
        try:
            # 从资产负债表中获取已有的股票代码
            result = session.query(BalanceSheet.stock_code).distinct().all()
            return {row[0] for row in result}
        finally:
            session.close()
    
    def save_indicator_median(
        self,
        indicator_name: str,
        report_date: date,
        median_value: float,
        cache_version: str
    ) -> None:
        """
        保存指标中位数
        
        Args:
            indicator_name: 指标名称
            report_date: 报告日期
            median_value: 中位数值
            cache_version: 缓存版本号
        """
        session = self.get_session()
        try:
            # 检查是否已存在
            existing = session.query(IndicatorMedian).filter_by(
                indicator_name=indicator_name,
                report_date=report_date,
                cache_version=cache_version
            ).first()
            
            if not existing:
                record = IndicatorMedian(
                    indicator_name=indicator_name,
                    report_date=report_date,
                    median_value=median_value,
                    cache_version=cache_version
                )
                session.add(record)
                session.commit()
        finally:
            session.close()
    
    def clear_cache(self, cache_version: str) -> None:
        """
        清空指定版本的缓存
        
        Args:
            cache_version: 缓存版本号
        """
        session = self.get_session()
        try:
            session.query(IndicatorMedian).filter_by(
                cache_version=cache_version
            ).delete()
            session.commit()
        finally:
            session.close()
