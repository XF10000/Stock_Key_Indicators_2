"""
配置加载模块

负责加载和管理项目配置文件（config.yaml 和 column_mapping.yaml）
"""
import yaml
from pathlib import Path
from typing import Dict, Any


class ConfigLoader:
    """配置加载器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化配置加载器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._column_mapping: Dict[str, Any] = {}
        self._load_config()
        self._load_column_mapping()
    
    def _load_config(self) -> None:
        """加载主配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self._config = yaml.safe_load(f)
    
    def _load_column_mapping(self) -> None:
        """加载列名映射配置"""
        mapping_path = Path(self._config.get('column_mapping', {}).get('path', 'config/column_mapping.yaml'))
        
        if not mapping_path.exists():
            raise FileNotFoundError(f"列名映射文件不存在: {mapping_path}")
        
        with open(mapping_path, 'r', encoding='utf-8') as f:
            self._column_mapping = yaml.safe_load(f)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置项
        
        Args:
            key: 配置键，支持点号分隔的嵌套键（如 'database.path'）
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def get_column_mapping(self, report_type: str) -> Dict[str, str]:
        """
        获取指定报表类型的列名映射
        
        Args:
            report_type: 报表类型（'balance_sheet', 'income_statement', 'cash_flow_statement'）
            
        Returns:
            列名映射字典
        """
        mapping = self._column_mapping.get(report_type, {})
        common_mapping = self._column_mapping.get('common', {})
        
        # 合并通用映射和特定映射
        return {**mapping, **common_mapping}
    
    @property
    def database_path(self) -> str:
        """获取数据库路径"""
        return self.get('database.path', 'database.sqlite')
    
    @property
    def api_request_delay(self) -> float:
        """获取API请求延迟（秒）"""
        return float(self.get('api.request_delay', 0.5))
    
    @property
    def api_retry_times(self) -> int:
        """获取API重试次数"""
        return int(self.get('api.retry_times', 3))
    
    @property
    def api_retry_delay(self) -> float:
        """获取API重试延迟（秒）"""
        return float(self.get('api.retry_delay', 5))
    
    @property
    def batch_size(self) -> int:
        """获取批量写入大小"""
        return int(self.get('data_updater.batch_size', 100))
    
    @property
    def chunk_size(self) -> int:
        """获取分块处理大小"""
        return int(self.get('performance.chunk_size', 500))
