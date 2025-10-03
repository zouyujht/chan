import os
import sys
from typing import Iterable

# 将项目根目录添加到系统路径，以便可以从兄弟目录（如OfflineData）导入模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Common.CEnum import KL_TYPE, AUTYPE
from KLine.KLine_Unit import CKLine_Unit
from OfflineData.offline_data_util import OfflineDataUtil
from .CommonStockAPI import CCommonStockApi


class CStockFileReader(CCommonStockApi):
    """
    离线数据API实现。
    从由OfflineData模块管理的本地文件中读取K线数据。
    这个类名是为了匹配README.md中 "custom:OfflineDataAPI:CStockFileReader" 的示例。
    """
    def __init__(self, code, k_type=KL_TYPE.K_DAY, begin_date=None, end_date=None, autype=AUTYPE.QFQ):
        """
        初始化离线数据读取器。

        Args:
            code (str): 股票代码。
            k_type (KL_TYPE): K线类型。
            begin_date (str, optional): 开始日期，格式 'YYYY-MM-DD'。默认为 None。
            end_date (str, optional): 结束日期，格式 'YYYY-MM-DD'。默认为 None。
            autype (AUTYPE, optional): 复权类型。默认为 AUTYPE.QFQ。
        """
        super().__init__(code, k_type, begin_date, end_date, autype)
        self.util = OfflineDataUtil()

    def get_kl_data(self) -> Iterable[CKLine_Unit]:
        """
        一个生成器，用于从本地存储中yield K线数据。

        它会优先尝试从速度更快的Pickle格式加载，如果失败则回退到CSV格式。
        并根据提供的日期范围过滤数据。

        Yields:
            CKLine_Unit: 代表单根K线数据的对象。
        """
        # 优先使用速度更快的Pickle格式，如果不存在则回退到CSV
        kline_data = self.util.load_kline_data_pickle(self.code, self.k_type)
        if not kline_data:
            kline_data = self.util.load_kline_data_csv(self.code, self.k_type)

        for kline in kline_data:
            # 确保时间可以与begin_date/end_date字符串进行比较
            klu_time_str = kline.time.to_str(fmt="%Y-%m-%d")
            if self.begin_date and klu_time_str < self.begin_date:
                continue
            if self.end_date and klu_time_str > self.end_date:
                continue
            yield kline

    def SetBasciInfo(self):
        """
        设置股票基本信息。
        对于离线数据，此信息不与K线一同存储，因此这里作为占位符。
        """
        self.name = self.code
        self.is_stock = True  # 默认假设是股票

    @classmethod
    def do_init(cls):
        """
        类级别的初始化。访问本地文件不需要执行任何操作。
        """
        pass

    @classmethod
    def do_close(cls):
        """
        类级别的清理。访问本地文件不需要执行任何操作。
        """
        pass
