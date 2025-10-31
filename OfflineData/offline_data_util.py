"""
离线数据更新通用工具类
提供数据存储、管理和配置读取的基础功能
"""

import os
import sys
import sqlite3
import pickle
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Common.CEnum import DATA_FIELD, KL_TYPE, AUTYPE
from Common.CTime import CTime
from Common.func_util import str2float
from KLine.KLine_Unit import CKLine_Unit


# Monkey patch CTime.to_str to support 'fmt' argument
if not hasattr(CTime, '_to_str_original'):
    CTime._to_str_original = CTime.to_str
    def to_str_patched(self, fmt=None):
        if fmt:
            # Assuming CTime has year, month, day attributes.
            # The call in OfflineDataAPI implies it's a date format.
            # Let's construct a datetime object and format it.
            hour =getattr(self, 'hour', 0)
            minute = getattr(self, 'minute', 0)
            dt = datetime(self.year, self.month, self.day, hour, minute)
            return dt.strftime(fmt)
        return self._to_str_original()
    CTime.to_str = to_str_patched


class OfflineDataUtil:
    """离线数据管理通用工具类"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化离线数据工具类
        
        Args:
            config_path: 配置文件路径，默认为项目根目录下的config.yaml
        """
        self.config_path = config_path or self._get_default_config_path()
        self.config = self._load_config()
        self.logger = self._setup_logger()

    def _get_default_config_path(self) -> str:
        """获取默认配置文件路径"""
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(project_root, "Config", "config.yaml")
    
    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            配置字典
        """
        try:
            import yaml
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f)
            else:
                print(f"配置文件不存在: {self.config_path}，使用默认配置")
                return self._get_default_config()
        except ImportError:
            print("PyYAML未安装，使用默认配置")
            return self._get_default_config()
        except Exception as e:
            print(f"加载配置文件失败: {e}，使用默认配置")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return {
            'offline_data': {
                'path': os.path.join(project_root, 'data', 'offline'),
                'log_path': os.path.join(project_root, 'logs'),
                'pickle_path': os.path.join(project_root, 'data', 'pickle'),
            },
            'DB': {
                'TYPE': 'sqlite',
                'SQLITE_PATH': os.path.join(project_root, 'data', 'chan.db'),
                'TABLE': 'kline_data'
            }
        }
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('OfflineDataUtil')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # 创建日志目录
            log_path = self.get_log_path()
            os.makedirs(log_path, exist_ok=True)
            
            # 文件处理器
            log_file = os.path.join(log_path, f"offline_data_{datetime.now().strftime('%Y%m%d')}.log")
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            
            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # 格式化器
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    def get_offline_data_path(self) -> str:
        """获取离线数据存储路径"""
        path = self.config.get('offline_data', {}).get('path', './data/offline')
        os.makedirs(path, exist_ok=True)
        return path
    
    def get_log_path(self) -> str:
        """获取日志存储路径"""
        path = self.config.get('offline_data', {}).get('log_path', './logs')
        os.makedirs(path, exist_ok=True)
        return path
    
    def get_pickle_data_path(self) -> str:
        """获取pickle数据存储路径"""
        path = self.config.get('offline_data', {}).get('pickle_path', './data/pickle')
        os.makedirs(path, exist_ok=True)
        return path
    
    def get_data_file_path(self, stock_code: str, k_type: str, file_format: str = 'csv') -> str:
        """
        获取数据文件路径
        
        Args:
            stock_code: 股票代码
            k_type: K线类型
            file_format: 文件格式，csv或pkl
            
        Returns:
            数据文件路径
        """
        offline_path = self.get_offline_data_path()
        filename = f"{stock_code}_{k_type}.{file_format}"
        return os.path.join(offline_path, filename)
    
    def get_db_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        return self.config.get('DB', {})
    
    def create_data_file_path(self, code: str, k_type: KL_TYPE, autype: AUTYPE, data_format: str = 'csv') -> str:
        """
        创建数据文件路径
        
        Args:
            code: 股票代码
            k_type: K线类型
            autype: 复权类型
            data_format: 数据格式 (csv, pickle, sqlite)
            
        Returns:
            文件路径
        """
        offline_path = self.get_offline_data_path()
        
        # 根据K线类型和复权类型创建子目录
        k_type_name = k_type.name.lower().replace('k_', '')
        autype_name = autype.name.lower()
        _dir = os.path.join(offline_path, autype_name, k_type_name)
        os.makedirs(_dir, exist_ok=True)
        
        filename = f"{code}.{data_format}"
        
        return os.path.join(_dir, filename)
    
    def save_kline_data_csv(self, code: str, k_type: KL_TYPE, autype: AUTYPE, kline_data: List[CKLine_Unit]) -> str:
        """
        保存K线数据为CSV格式
        
        Args:
            code: 股票代码
            k_type: K线类型
            autype: 复权类型
            kline_data: K线数据列表
            
        Returns:
            保存的文件路径
        """
        file_path = self.create_data_file_path(code, k_type, autype, 'csv')
        
        with open(file_path, 'w', encoding='utf-8') as f:
            # 写入表头
            headers = ['time', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'turnrate']
            f.write(','.join(headers) + '\n')
            
            # 写入数据
            for kl_unit in kline_data:
                # 从trade_info中获取成交量等信息
                volume = kl_unit.trade_info.metric.get(DATA_FIELD.FIELD_VOLUME, 0)
                turnover = kl_unit.trade_info.metric.get(DATA_FIELD.FIELD_TURNOVER, 0)
                turnrate = kl_unit.trade_info.metric.get(DATA_FIELD.FIELD_TURNRATE, 0)
                
                row_data = [
                    kl_unit.time.to_str(),
                    str(kl_unit.open),
                    str(kl_unit.high),
                    str(kl_unit.low),
                    str(kl_unit.close),
                    str(volume or 0),
                    str(turnover or 0),
                    str(turnrate or 0)
                ]
                f.write(','.join(row_data) + '\n')
        
        self.logger.info(f"保存CSV数据: {file_path}, 共{len(kline_data)}条记录")
        return file_path
    
    def load_kline_data_csv(self, code: str, k_type: KL_TYPE, autype: AUTYPE) -> List[CKLine_Unit]:
        """
        从CSV文件加载K线数据
        
        Args:
            code: 股票代码
            k_type: K线类型
            autype: 复权类型
            
        Returns:
            K线数据列表
        """
        file_path = self.create_data_file_path(code, k_type, autype, 'csv')
        
        if not os.path.exists(file_path):
            self.logger.warning(f"CSV文件不存在: {file_path}")
            return []
        
        kline_data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            # 跳过表头
            for line in lines[1:]:
                if line.strip():
                    parts = line.strip().split(',')
                    if len(parts) >= 8:
                        try:
                            # 解析时间
                            time_str = parts[0]
                            if len(time_str) == 10:  # YYYY-MM-DD or YYYY/MM/DD
                                time_str = time_str.replace('/', '-')  # 规范化为 YYYY-MM-DD
                                year, month, day = map(int, time_str.split('-'))
                                time_obj = CTime(year, month,day, 0, 0)
                            else:  # 其他格式
                                time_obj = CTime.from_str(time_str)
                            
                            # 创建数据字典
                            item_dict = {
                                DATA_FIELD.FIELD_TIME: time_obj,
                                DATA_FIELD.FIELD_OPEN: str2float(parts[1]),
                                DATA_FIELD.FIELD_HIGH: str2float(parts[2]),
                                DATA_FIELD.FIELD_LOW: str2float(parts[3]),
                                DATA_FIELD.FIELD_CLOSE: str2float(parts[4]),
                                DATA_FIELD.FIELD_VOLUME: str2float(parts[5]),
                                DATA_FIELD.FIELD_TURNOVER: str2float(parts[6]),
                                DATA_FIELD.FIELD_TURNRATE: str2float(parts[7])
                            }
                            
                            kline_data.append(CKLine_Unit(item_dict))
                        except Exception as e:
                            self.logger.warning(f"解析K线数据失败: {line.strip()}, 错误: {e}")
        
        self.logger.info(f"加载CSV数据: {file_path}, 共{len(kline_data)}条记录")
        return kline_data
    
    def save_kline_datapickle(self, code: str, k_type: KL_TYPE, autype: AUTYPE, kline_data: List[CKLine_Unit]) -> str:
        """
        保存K线数据为Pickle格式
        
        Args:
            code: 股票代码
            k_type: K线类型
            autype: 复权类型
            kline_data: K线数据列表
            
        Returns:
            保存的文件路径
        """
        file_path = self.create_data_file_path(code, k_type, autype, 'pickle')
        
        with open(file_path, 'wb') as f:
            pickle.dump(kline_data, f)
        
        self.logger.info(f"保存Pickle数据: {file_path}, 共{len(kline_data)}条记录")
        return file_path
    
    def load_kline_data_pickle(self, code: str, k_type: KL_TYPE, autype: AUTYPE) -> List[CKLine_Unit]:
        """
        从Pickle文件加载K线数据
        
        Args:
            code: 股票代码
            k_type: K线类型
            autype: 复权类型
            
        Returns:
            K线数据列表
        """
        file_path = self.create_data_file_path(code, k_type, autype, 'pickle')
        
        if not os.path.exists(file_path):
            self.logger.warning(f"Pickle文件不存在: {file_path}")
            return []
        
        try:
            with open(file_path, 'rb') as f:
                kline_data = pickle.load(f)
            
            self.logger.info(f"加载Pickle数据: {file_path}, 共{len(kline_data)}条记录")
            return kline_data
        except Exception as e:
            self.logger.error(f"加载Pickle数据失败: {file_path}, 错误: {e}")
            return []
    
    def get_latest_data_time(self, code: str, k_type: KL_TYPE, autype: AUTYPE, data_format: str = 'csv') -> Optional[CTime]:
        """
        获取最新数据的时间
        
        Args:
            code: 股票代码
            k_type: K线类型
            autype: 复权类型
            data_format: 数据格式
            
        Returns:
            最新数据时间，如果没有数据返回None
        """
        if data_format == 'csv':
            kline_data = self.load_kline_data_csv(code, k_type, autype)
        elif data_format == 'pickle':
            kline_data = self.load_kline_data_pickle(code, k_type, autype)
        else:
            self.logger.warning(f"不支持的数据格式: {data_format}")
            return None
        
        if kline_data:
            return kline_data[-1].time
        return None
    
    def append_kline_data(self,code: str, k_type: KL_TYPE, autype: AUTYPE, new_data: List[CKLine_Unit], 
                         data_format: str = 'csv') -> str:
        """
        追加K线数据
        
        Args:
            code: 股票代码
            k_type: K线类型
            autype: 复权类型
            new_data: 新的K线数据
            data_format: 数据格式
            
        Returns:
            保存的文件路径
        """
        if data_format == 'csv':
            existing_data = self.load_kline_data_csv(code, k_type, autype)
        elif data_format == 'pickle':
            existing_data = self.load_kline_data_pickle(code, k_type, autype)
        else:
            self.logger.warning(f"不支持的数据格式: {data_format}")
            return ""
        
        # 合并数据，去重
        all_data = existing_data + new_data
        
        unique_data = {}
        # 按时间排序并
        for kl_unit in all_data:
            time_key = kl_unit.time.to_str()
            unique_data[time_key] = kl_unit
        
        sorted_data = sorted(unique_data.values(), key=lambda x: x.time.to_str())
        
        # 保存数据
        if data_format == 'csv':
            return self.save_kline_data_csv(code, k_type, autype, sorted_data)
        elif data_format == 'pickle':
            return self.save_kline_data_pickle(code, k_type, autype, sorted_data)
        return ""
    
    def get_stock_list(self) -> List[str]:
        """
        获取已下载数据的股票列表
        
        Returns:
            股票代码列表
        """
        offline_path = self.get_offline_data_path()
        stock_codes = set()
        
        for root, dirs, files in os.walk(offline_path):
            for file in files:
                if file.endswith('.csv') or file.endswith('.pkl'):
                    #从文件名提取股票代码
                    code = file.split('.')[0]
                    stock_codes.add(code)
        
        return sorted(list(stock_codes))
    
    def clean_old_logs(self, days: int = 30):
        """
        清理旧日志文件
        
        Args:
            days: 保留天数
        """
        log_path = self.get_log_path()
        cutoff_date = datetime.now() - timedelta(days=days)
        
        for file in os.listdir(log_path):
            if file.endswith('.log'):
                file_path = os.path.join(log_path, file)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                if file_time < cutoff_date:
                    os.remove(file_path)
                    self.logger.info(f"删除旧日志文件: {file}")
    
    def get_downloaded_stocks(self, autype: AUTYPE, stock_type: str = 'stock') -> List[str]:
        """
        获取指定复权类型和品种的已下载代码列表

        Args:
            autype: 复权类型
            stock_type: 'stock' 或 'reits'

        Returns:
            代码列表
        """
        offline_path = self.get_offline_data_path()
        autype_name = autype.name.lower()
        target_path = os.path.join(offline_path, autype_name)
        
        codes = set()
        if not os.path.exists(target_path):
            return[]

        for k_type_dir in os.listdir(target_path):
            full_k_type_path= os.path.join(target_path, k_type_dir)
            if os.path.isdir(full_k_type_path):
                for file in os.listdir(full_k_type_path):
                    if file.endswith('.csv'):
                        code = os.path.splitext(file)[0]
                        # 股票代码通常包含交易所前缀，如 "sz.000001" 或 "sh.600000"
                        is_stock = '.' in code
                        # 国债代码通常为 6 位纯数字且以 "01" 开头，例如 019547
                        is_bond = code.isdigit() and len(code) == 6 and code.startswith('01')
                        # REITs（粗略分类）：6 位纯数字但不以 "01" 开头，避免与国债混淆
                        is_reit = code.isdigit() and len(code) == 6 and not code.startswith('01')

                        if stock_type == 'reits' and is_reit:
                            codes.add(code)
                        elif stock_type == 'stock' and is_stock:
                            codes.add(code)
                        elif stock_type == 'bond' and is_bond:
                            codes.add(code)
        
        return sorted(list(codes))
    
    def get_data_statistics(self) -> Dict[str, Any]:
        """
        获取数据统计信息
        
        Returns:
            数据统计字典
        """
        offline_path = self.get_offline_data_path()
        stats = {
            'total_files': 0,
            'total_size_mb': 0,
            'stock_count': 0,
            'k_types': set(),
            'last_update': None
        }
        
        stock_codes = set()
        latest_time = 0
        
        for root, dirs, files in os.walk(offline_path):
            # 从目录路径中提取k线和复权类型
            relative_path = os.path.relpath(root, offline_path)
            path_parts = relative_path.split(os.sep)
            if len(path_parts) == 2:
                k_type, autype = path_parts
                stats['k_types'].add(k_type)

            for file in files:
                if file.endswith('.csv') or file.endswith('.pkl'):
                    file_path = os.path.join(root, file)
                    stats['total_files'] += 1
                    stats['total_size_mb'] += os.path.getsize(file_path) / (1024 * 1024)
                    
                    # 提取股票代码
                    code = file.split('.')[0]
                    stock_codes.add(code)
                    
                    #更新最新修改时间
                    file_time = os.path.getmtime(file_path)
                    if file_time > latest_time:
                        latest_time = file_time
        
        stats['stock_count'] = len(stock_codes)
        stats['k_types'] = list(stats['k_types'])
        stats['total_size_mb'] = round(stats['total_size_mb'], 2)
        
        if latest_time > 0:
            stats['last_update'] = datetime.fromtimestamp(latest_time).strftime('%Y-%m-%d %H:%M:%S')
        
        return stats
