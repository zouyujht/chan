"""
Akshare 债券全量数据下载器（集成离线数据管理）
支持下载沪深债券（日线）历史数据，转换为 CKLine_Unit 并通过 OfflineDataUtil 统一存储到 data/offline/none/day
参考 OfflineData/reits_download.py 的结构实现
"""

import os
import sys
import time
import argparse
from datetime import datetime
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd

from Common.CEnum import AUTYPE, DATA_FIELD, KL_TYPE
from Common.CTime import CTime
from KLine.KLine_Unit import CKLine_Unit
from OfflineData.offline_data_util import OfflineDataUtil


def normalize_symbol_candidates(code: str) -> List[str]:
    """将代码标准化为可能的 symbol 列表：已带前缀则直接返回，否则按 ['sh', 'sz'] 尝试"""
    _c = code.strip().lower()
    if _c.startswith(('sh', 'sz')):
        return [_c]
    return [f"sh{_c}", f"sz{_c}"]


def fetch_bond_df(code: str) -> pd.DataFrame:
    """尝试不同前缀，返回首个非空的 DataFrame；若失败返回空"""
    for sym in normalize_symbol_candidates(code):
        try:
            df = ak.bond_zh_hs_daily(symbol=sym)
            if df is not None and not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()


class AkshareBondDownloader:
    """Akshare 债券数据下载器（全量）"""

    def __init__(self, config_path: Optional[str] = None):
        self.util = OfflineDataUtil(config_path)
        self.logger = self.util.logger

        self.default_start_date = "2000-01-01"
        self.default_end_date = datetime.now().strftime("%Y-%m-%d")
        self.default_k_types = [KL_TYPE.K_DAY]
        self.default_autype = AUTYPE.NONE  # 债券仅不复权

        self.download_stats = {
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'total_records': 0,
            'failed_codes': []
        }

    def download_single_bond(self, code: str, k_type: KL_TYPE,
                             start_date: str, end_date: str,
                             autype: AUTYPE = AUTYPE.NONE,
                             force_update: bool = False,
                             max_retries: int = 3,
                             retry_delay: int = 5) -> bool:
        """下载单只债券的日线数据并保存到离线目录（CKLine_Unit）"""
        if k_type != KL_TYPE.K_DAY:
            self.logger.warning(f"债券接口当前仅支持日线数据下载，跳过 {k_type.name}。")
            return True

        if autype != AUTYPE.NONE:
            self.logger.warning(f"债券接口仅支持不复权数据，您的 --autype={autype.name} 参数将被忽略，数据将作为 NONE 保存。")
        storage_autype = AUTYPE.NONE

        # 若不强制更新，且文件已存在且非空，则跳过
        if not force_update:
            file_path = self.util.create_data_file_path(code, k_type, storage_autype, 'csv')
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                self.logger.info(f"债券 {code} {k_type.name} {storage_autype.name} 数据已存在，跳过下载")
                self.download_stats['skipped_count'] += 1
                return True

        # 重试机制
        for attempt in range(max_retries):
            try:
                df = fetch_bond_df(code)
                if df.empty:
                    self.logger.warning(f"债券 {code} {k_type.name} 未获取到数据")
                    return True

                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
                if df.empty:
                    self.logger.warning(f"债券 {code} {k_type.name} 在指定范围 {start_date} - {end_date} 内没有数据")
                    return True

                kline_data: List[CKLine_Unit] = []
                for _, row in df.iterrows():
                    dt = datetime.strptime(row['date'], '%Y-%m-%d')
                    item_dict = {
                        DATA_FIELD.FIELD_TIME: CTime(dt.year, dt.month, dt.day, 0, 0),
                        DATA_FIELD.FIELD_OPEN: float(row['open']) if pd.notna(row['open']) else 0.0,
                        DATA_FIELD.FIELD_HIGH: float(row['high']) if pd.notna(row['high']) else 0.0,
                        DATA_FIELD.FIELD_LOW: float(row['low']) if pd.notna(row['low']) else 0.0,
                        DATA_FIELD.FIELD_CLOSE: float(row['close']) if pd.notna(row['close']) else 0.0,
                        DATA_FIELD.FIELD_VOLUME: float(row['volume']) if pd.notna(row['volume']) else 0.0,
                        DATA_FIELD.FIELD_TURNOVER: 0.0,
                        DATA_FIELD.FIELD_TURNRATE: 0.0
                    }
                    kline_data.append(CKLine_Unit(item_dict))

                self.util.save_kline_data_csv(code, k_type, storage_autype, kline_data)

                self.download_stats['success_count'] += 1
                self.download_stats['total_records'] += len(kline_data)
                self.logger.info(f"成功下载 {code} {k_type.name} ({storage_autype.name}) 数据，共 {len(kline_data)} 条记录")
                return True

            except Exception as e:
                self.logger.warning(f"下载债券 {code} {k_type.name} 第 {attempt + 1} 次失败: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"{retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"下载债券 {code} {k_type.name} 数据失败，已达最大重试次数")
                    self.download_stats['failed_count'] += 1
                    self.download_stats['failed_codes'].append(code)
                    return False
        return False

    def download_bonds_list(self, bond_codes: List[str],
                            k_types: Optional[List[KL_TYPE]] = None,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None,
                            autype: AUTYPE = AUTYPE.NONE,
                            force_update: bool = False,
                            max_workers: int = 1,
                            delay_seconds: float = 0.5) -> Dict[str, Any]:
        if not bond_codes:
            self.logger.warning("债券代码列表为空")
            return self.download_stats

        k_types = k_types or self.default_k_types
        start_date = start_date or self.default_start_date
        end_date = end_date or self.default_end_date

        self.logger.info(f"开始批量下载，债券数量: {len(bond_codes)}, K线类型: {[kt.name for kt in k_types]}")
        self.logger.info(f"时间范围: {start_date} 到 {end_date}")

        self.download_stats = {
            'success_count': 0, 'failed_count': 0, 'skipped_count': 0,
            'total_records': 0, 'failed_codes': []
        }

        start_time = time.time()
        tasks = [(code, k_type, start_date, end_date, autype, force_update)
                 for code in bond_codes for k_type in k_types]
        self.logger.info(f"总共 {len(tasks)} 个下载任务")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(self.download_single_bond, *task): task for task in tasks}
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
        if self.download_stats['failed_codes']:
            self.logger.warning(f"失败的债券: {self.download_stats['failed_codes'][:10]}...")

        return self.download_stats


def main():
    parser = argparse.ArgumentParser(description='Akshare 债券全量数据下载器（离线集成）')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--k-types', type=str, default='day', help='K线类型（当前仅支持 day）')
    parser.add_argument('--autype', type=str, default='none', help='复权类型（当前仅支持 none）')
    parser.add_argument('--codes', type=str, help='指定债券代码，逗号分隔；可为纯数字或带前缀')
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

    downloader = AkshareBondDownloader(args.config)

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
            cleaned_codes = args.codes.replace('"', '').replace("'", "")
            bond_codes = [code.strip() for code in cleaned_codes.split(',') if code.strip()]
            stats = downloader.download_bonds_list(bond_codes=bond_codes, **common_args)
        else:
            print("请通过 --codes 指定债券代码（逗号分隔）。目前未提供自动全量债券列表获取功能。")
            return

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