"""
Akshare REITS全量数据下载器
支持下载公募REITS的历史K线数据
"""

import os
import sys
import time
import argparse
from datetime import datetime
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor,as_completed

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd

from Common.CEnum import AUTYPE, DATA_FIELD, KL_TYPE
from Common.CTime import CTime
from Common.func_util import str2float
from KLine.KLine_Unit import CKLine_Unit
from OfflineData.offline_data_util import OfflineDataUtil


class AkshareReitsDownloader:
    """Akshare REITS数据下载器"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化下载器

        Args:
            config_path: 配置文件路径
        """
        self.util = OfflineDataUtil(config_path)
        self.logger = self.util.logger

        # 默认下载参数
        self.default_start_date = "2020-01-01"  # REITS市场较新
        self.default_end_date = datetime.now().strftime("%Y-%m-%d")
        self.default_k_types = [KL_TYPE.K_DAY]
        self.default_autype = AUTYPE.NONE # akshare reits_hist_em 不支持复权

        # 下载统计
        self.download_stats = {
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'total_records': 0,
            'failed_stocks': []
        }

    def get_all_reits_codes(self) -> List[str]:
        """
        获取所有公募REITS代码

        Returns:
            REITS代码列表
        """
        try:
            reits_info_df = self.get_reits_info()
            if '代码' in reits_info_df.columns:
                codes = reits_info_df['代码'].tolist()
                self.logger.info(f"获取到{len(codes)}只公募REITS")
                return codes
            else:
                self.logger.error("从akshare获取REITS列表失败，返回的DataFrame中没有'代码'列")
                return []
        except Exception as e:
            self.logger.error(f"通过akshare获取REITS列表异常: {e}")
            return []

    def get_reits_info(self, max_retries: int = 3, retry_delay: int = 5) -> pd.DataFrame:
        """
        获取REITS信息
       """
        for attempt in range(max_retries):
            try:
                reits_info_df = ak.reits_realtime_em()
                return reits_info_df
            except Exception as e:
                self.logger.warning(f"通过akshare获取REITS列表第 {attempt+ 1} 次尝试失败: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"{retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"通过akshare获取REITS列表失败，已达最大重试次数")
                    return pd.DataFrame()
        return pd.DataFrame()

    def _normalize_code(self, code: str) -> str:
        """
        规范化代码，移除可能的前缀
        akshare的reits接口似乎不需要sh/sz前缀
        """
        return code.replace("sh.", "").replace("sz.", "")

    def download_single_reit(self, code: str, k_type: KL_TYPE,
                             start_date: str, end_date: str,
                             autype: AUTYPE = AUTYPE.QFQ,
                             force_update: bool = False,
                             max_retries: int = 3,
                             retry_delay: int = 5) -> bool:
        """
        下载单只REIT的K线数据
        """
        # akshare reits_hist_em 接口仅支持日线、不复权数据
        if k_type != KL_TYPE.K_DAY:
            self.logger.warning(f"Akshare REITS接口当前仅支持日线数据下载，跳过 {k_type.name}。")
            return True

        if autype != AUTYPE.NONE:
            self.logger.warning(f"Akshare REITS接口仅支持不复权数据，您的 --autype={autype.name} 参数将被忽略，数据将作为不复权（NONE）保存。")
        
        storage_autype = AUTYPE.NONE

        if not force_update:
            file_path = self.util.create_data_file_path(code, k_type, storage_autype, 'csv')
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                self.logger.info(f"REIT {code} {k_type.name} {storage_autype.name} 数据已存在，跳过下载")
                self.download_stats['skipped_count'] += 1
                return True

        for attempt in range(max_retries):
            try:
                data_df = ak.reits_hist_em(symbol=code)

                if data_df.empty:
                    self.logger.warning(f"REIT {code} {k_type.name} 没有获取到数据")
                    return True

                data_df['日期'] = pd.to_datetime(data_df['日期']).dt.strftime('%Y-%m-%d')
                data_df = data_df[(data_df['日期'] >= start_date) & (data_df['日期'] <= end_date)]

                if data_df.empty:
                    self.logger.warning(f"REIT {code} {k_type.name} 在指定日期范围 {start_date} - {end_date} 内没有数据")
                    return True

                kline_data = []
                for _, row in data_df.iterrows():
                    dt = datetime.strptime(row['日期'], '%Y-%m-%d')
                    item_dict = {
                        DATA_FIELD.FIELD_TIME: CTime(dt.year, dt.month, dt.day, 0, 0),
                        DATA_FIELD.FIELD_OPEN: row['今开'],
                        DATA_FIELD.FIELD_HIGH: row['最高'],
                        DATA_FIELD.FIELD_LOW: row['最低'],
                        DATA_FIELD.FIELD_CLOSE: row['最新价'],
                        DATA_FIELD.FIELD_VOLUME: row['成交量'],
                        DATA_FIELD.FIELD_TURNOVER: row['成交额'],
                        DATA_FIELD.FIELD_TURNRATE: row.get('换手', 0.0)
                    }
                    kline_data.append(CKLine_Unit(item_dict))

                self.util.save_kline_data_csv(code, k_type, storage_autype, kline_data)

                self.download_stats['success_count'] += 1
                self.download_stats['total_records'] += len(kline_data)

                self.logger.info(f"成功下载 {code} {k_type.name} ({storage_autype.name}) 数据，共{len(kline_data)}条记录")
                return True

            except Exception as e:
                self.logger.warning(f"下载REIT {code} {k_type.name} 第 {attempt + 1} 次尝试失败: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"{retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"下载REIT {code} {k_type.name} 数据失败，已达最大重试次数")
                    self.download_stats['failed_count'] += 1
                    self.download_stats['failed_stocks'].append(f"{code}_{k_type.name}")
                    return False
        return False

    def download_reits_list(self, reits_codes: List[str],
                           k_types: Optional[List[KL_TYPE]] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           autype: AUTYPE = AUTYPE.QFQ,
                           force_update: bool = False,
                           max_workers: int = 1,
                           delay_seconds: float = 0.5) -> Dict[str, Any]:
        """
        批量下载REITS数据
        """
        if not reits_codes:
            self.logger.warning("REITS代码列表为空")
            return self.download_stats

        normalized_codes = [self._normalize_code(code) for code in reits_codes]

        k_types = k_types or self.default_k_types
        start_date = start_date or self.default_start_date
        end_date = end_date or self.default_end_date

        self.logger.info(f"开始批量下载，REITS数量: {len(normalized_codes)}, K线类型: {[kt.name for kt in k_types]}")
        self.logger.info(f"时间范围: {start_date} 到{end_date}")

        self.download_stats = {
            'success_count': 0,'failed_count': 0, 'skipped_count': 0,
            'total_records': 0, 'failed_stocks': []
        }

        start_time = time.time()

        tasks = [(code, k_type, start_date, end_date, autype, force_update)
                for code in normalized_codes for k_type in k_types]

        self.logger.info(f"总共{len(tasks)}个下载任务")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(self.download_single_reit, *task): task for task in tasks}

            completed = 0
            for future in as_completed(future_to_task):
                completed += 1
                if completed % 10 == 0:
                    progress = (completed / len(tasks)) * 100
                    self.logger.info(f"下载进度: {completed}/{len(tasks)} ({progress:.1f}%)")
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

        end_time = time.time()
        duration = end_time - start_time

        self.logger.info("=" * 50)
        self.logger.info("下载完成统计:")
        self.logger.info(f"总耗时: {duration:.2f}秒")
        self.logger.info(f"成功: {self.download_stats['success_count']}")
        self.logger.info(f"失败: {self.download_stats['failed_count']}")
        self.logger.info(f"跳过: {self.download_stats['skipped_count']}")
        self.logger.info(f"总记录数: {self.download_stats['total_records']}")
        if self.download_stats['failed_stocks']:
            self.logger.warning(f"失败的REITS: {self.download_stats['failed_stocks'][:10]}...")

        return self.download_stats

    def download_all_reits(self, **kwargs) -> Dict[str, Any]:
        """
        下载所有公募REITS数据
        """
        reits_codes = self.get_all_reits_codes()
        if not reits_codes:
            self.logger.error("未获取到REITS代码")
            return self.download_stats
        return self.download_reits_list(reits_codes, **kwargs)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Akshare REITS全量数据下载器')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--k-types', type=str, default='day', help='K线类型，逗号分隔 (当前仅支持day)')
    parser.add_argument('--autype', type=str, default='none', help='复权类型 (当前仅支持none)')
    parser.add_argument('--codes', type=str, help='指定REIT代码，逗号分隔')
    parser.add_argument('--force-update', action='store_true', help='强制更新已存在的数据')
    parser.add_argument('--max-workers', type=int, default=1, help='最大并发数')
    parser.add_argument('--delay', type=float, default=0.5, help='请求间隔秒数')

    args = parser.parse_args()

    k_type_map = {'day': KL_TYPE.K_DAY}
    k_types = [k_type_map[kt.strip()] for kt in args.k_types.split(',') if kt.strip() in k_type_map]
    if not k_types:
        k_types = [KL_TYPE.K_DAY]

    autype_map = {'none': AUTYPE.NONE}
    autype = autype_map.get(args.autype.lower(), AUTYPE.NONE)

    downloader = AkshareReitsDownloader(args.config)
    
    common_args = {
        'k_types': k_types,
        'start_date': args.start_date or downloader.default_start_date,
        'end_date': args.end_date or downloader.default_end_date,
        'autype': autype,
        'force_update': args.force_update,
        'max_workers': args.max_workers,
        'delay_seconds': args.delay
    }

    try:
        if args.codes:
            reits_codes = [code.strip() for code in args.codes.split(',')]
            stats = downloader.download_reits_list(reits_codes=reits_codes, **common_args)
        else:
            stats = downloader.download_all_reits(**common_args)

        print("\n下载完成!")
        print(f"成功: {stats['success_count']}")
        print(f"失败: {stats['failed_count']}")
        print(f"跳过: {stats['skipped_count']}")
        print(f"总记录数: {stats['total_records']}")

    except KeyboardInterrupt:
        print("\n用户中断下载")
    except Exception as e:
        print(f"下载过程中发生错误: {e}")


if __name__ == "__main__":
    main()
