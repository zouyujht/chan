# OfflineData模块
# 用于离线数据的下载、更新和管理

__version__ = "1.0.0"
__author__ = "chan.py"

from .offline_data_util import OfflineDataUtil
from .bao_download import BaoStockDownloader
from .bao_update import BaoStockUpdater
from .reits_download import AkshareReitsDownloader
from .reits_update import AkshareReitsUpdater

__all__ = [
    'OfflineDataUtil',
    'BaoStockDownloader', 
    'BaoStockUpdater',
    'AkshareReitsDownloader',
    'AkshareReitsUpdater'
]
