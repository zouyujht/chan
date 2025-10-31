"""
Akshare 债券增量数据更新器
支持增量更新沪深债券（日线）历史数据，并按项目数据结构转换为 CKLine_Unit 统一存储
参考 OfflineData/reits_update.py 的结构实现
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
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
    """
    将代码标准化为可能的 symbol 列表
    - 若已带前缀（sh/sz），直接返回该前缀
    - 否则依次尝试 ['sh', 'sz']
    """
    _c = code.strip().lower()
    if _c.startswith(('sh', 'sz')):
        return [_c]
    return [f"sh{_c}", f"sz{_c}"]


class AkshareBondUpdater:
    """Akshare 债券增量数据更新器"""

    def __init__(self, config_path: Optional[str] = None):
        self.util = OfflineDataUtil(config_path)
        self.logger = self.util.logger

        # 默认仅支持：日线 + 不复权
        self.default_k_types = [KL_TYPE.K_DAY]
        self.default_autype = AUTYPE.NONE
        self.update_days = 90  # 若历史为空，默认更新最近 90 天

        self.update_stats = {
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'updated_count': 0,
            'new_records': 0,
            'failed_codes': []
        }

    def get_update_date_range(self, code: str, k_type: KL_TYPE, autype: AUTYPE) -> Tuple[Optional[str], str]:
        latest_time = self.util.get_latest_data_time(code, k_type, autype)
        end_date = datetime.now().strftime("%Y-%m-%d")

        if latest_time:
            latest_datetime = datetime(latest_time.year, latest_time.month, latest_time.day)
            start_date = (latest_datetime + timedelta(days=1)).strftime("%Y-%m-%d")
            if start_date > end_date:
                return None, end_date
            return start_date, end_date
        else:
            start_date = (datetime.now() - timedelta(days=self.update_days)).strftime("%Y-%m-%d")
            return start_date, end_date

    def _fetch_df_by_candidates(self, code: str) -> Tuple[Optional[str], pd.DataFrame]:
        """尝试不同交易所前缀，返回第一个成功的 (symbol, df)"""
        for sym in normalize_symbol_candidates(code):
            try:
                df = ak.bond_zh_hs_daily(symbol=sym)
                if df is not None and not df.empty:
                    return sym, df
            except Exception as e:
                self.logger.warning(f"尝试 {sym} 失败: {e}")
        return None, pd.DataFrame()

    def update_single_bond(self, code: str, k_type: KL_TYPE, autype: AUTYPE = AUTYPE.NONE,
                           force_full_update: bool = False) -> bool:
        """更新单只债券的 K 线数据（仅支持日线+不复权）"""
        try:
            if k_type != KL_TYPE.K_DAY:
                self.logger.warning(f"债券接口当前仅支持日线数据更新，跳过 {k_type.name}。")
                self.update_stats['skipped_count'] += 1
                return True

            if autype != AUTYPE.NONE:
                self.logger.warning(f"债券接口仅支持不复权数据，您的 --autype={autype.name} 参数将被忽略，作为 NONE 处理。")

            storage_autype = AUTYPE.NONE

            if force_full_update:
                self.util.delete_kline_data(code, k_type, storage_autype)
                start_date = "2000-01-01"  # 债券历史可较久远，给出较早起始
            else:
                start_date, _ = self.get_update_date_range(code, k_type, storage_autype)
                if start_date is None:
                    self.logger.info(f"债券 {code} {k_type.name} {storage_autype.name} 数据已是最新，跳过更新")
                    self.update_stats['skipped_count'] += 1
                    return True

            symbol, data_df = self._fetch_df_by_candidates(code)
            if symbol is None or data_df.empty:
                self.logger.warning(f"未能获取到债券数据: {code}")
                self.update_stats['failed_count'] += 1
                self.update_stats['failed_codes'].append(code)
                return False

            # 标准化日期，并截取增量范围
            data_df['date'] = pd.to_datetime(data_df['date']).dt.strftime('%Y-%m-%d')
            new_df = data_df[data_df['date'] >= start_date]
            if new_df.empty:
                self.logger.info(f"债券 {code} 在增量范围内没有新数据")
                self.update_stats['skipped_count'] += 1
                return True

            # 转换为 CKLine_Unit 列表
            new_data: List[CKLine_Unit] = []
            for _, row in new_df.iterrows():
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
                new_data.append(CKLine_Unit(item_dict))

            if force_full_update:
                self.util.save_kline_data_csv(code, k_type, storage_autype, new_data)
                log_msg = f"全量更新 {code} {k_type.name} ({storage_autype.name})，共 {len(new_data)} 条记录"
            else:
                self.util.append_kline_data(code, k_type, storage_autype, new_data)
                log_msg = f"增量更新 {code} {k_type.name} ({storage_autype.name})，新增 {len(new_data)} 条记录"

            self.update_stats['updated_count'] += 1
            self.update_stats['new_records'] += len(new_data)
            self.update_stats['success_count'] += 1
            self.logger.info(log_msg)
            return True

        except Exception as e:
            self.logger.error(f"更新债券 {code} {k_type.name} 数据失败: {e}")
            self.update_stats['failed_count'] += 1
            self.update_stats['failed_codes'].append(code)
            return False

    def update_bonds_list(self, bond_codes: List[str], **kwargs) -> Dict[str, Any]:
        """批量更新债券数据"""
        if not bond_codes:
            self.logger.warning("债券代码列表为空")
            return self.update_stats

        self.logger.info(f"开始批量更新，债券数量: {len(bond_codes)}")
        self.update_stats = {k: 0 if isinstance(v, int) else [] for k, v in self.update_stats.items()}
        start_time = time.time()

        tasks = [(code, k_type, kwargs.get('autype', self.default_autype), kwargs.get('force_full_update', False))
                 for code in bond_codes for k_type in kwargs.get('k_types', self.default_k_types)]

        with ThreadPoolExecutor(max_workers=kwargs.get('max_workers', 1)) as executor:
            future_to_task = {executor.submit(self.update_single_bond, *task): task for task in tasks}
            for i, future in enumerate(as_completed(future_to_task)):
                if (i + 1) % 10 == 0:
                    self.logger.info(f"更新进度: {i+1}/{len(tasks)}")
                time.sleep(kwargs.get('delay_seconds', 0.5))

        duration = time.time() - start_time
        self.logger.info("=" * 50)
        self.logger.info(f"更新完成统计 (耗时: {duration:.2f}秒):")
        for key, value in self.update_stats.items():
            self.logger.info(f"{key}: {value if isinstance(value, int) else len(value)}")
        return self.update_stats

    def update_all_downloaded_bonds(self, **kwargs) -> Dict[str, Any]:
        """
        更新所有已下载（数值型 6 位代码）的债券/数值型代码。
        注意：util.get_downloaded_stocks(stock_type='reits') 会返回所有 6 位数字代码，
        其中可能包含 REITS 与债券；本方法会对这些代码逐一尝试债券接口。
        """
        storage_autype = AUTYPE.NONE
        all_codes = self.util.get_downloaded_stocks(autype=storage_autype, stock_type='reits')
        self.logger.info(f"检测到 {len(all_codes)} 个已下载的数值型代码 ({storage_autype.name})")
        kwargs['autype'] = storage_autype
        return self.update_bonds_list(all_codes, **kwargs)


def main():
    parser = argparse.ArgumentParser(description='Akshare 债券增量数据更新器')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--autype', type=str, default='none', help='复权类型 (固定为none)')
    parser.add_argument('--k-types', type=str, default='day', help='K线类型 (固定为day)')
    parser.add_argument('--codes', type=str, help='指定债券代码，逗号分隔；可为纯数字或带前缀')
    parser.add_argument('--all', action='store_true', help='更新所有已下载的数值型代码')
    parser.add_argument('--force-full', action='store_true', help='强制全量更新')
    parser.add_argument('--max-workers', type=int, default=1, help='最大并发数')
    parser.add_argument('--delay', type=float, default=0.5, help='请求间隔秒数')
    args = parser.parse_args()

    k_type_map = {'day': KL_TYPE.K_DAY}
    k_types = [k_type_map[kt.strip()] for kt in args.k_types.split(',') if kt.strip() in k_type_map]
    if not k_types:
        k_types = [KL_TYPE.K_DAY]

    autype_map = {'none': AUTYPE.NONE}
    autype = autype_map.get(args.autype.lower(), AUTYPE.NONE)

    updater = AkshareBondUpdater(args.config)

    update_args = {
        'autype': autype,
        'k_types': k_types,
        'force_full_update': args.force_full,
        'max_workers': args.max_workers,
        'delay_seconds': args.delay
    }

    try:
        if args.codes:
            cleaned_codes = args.codes.replace('"', '').replace("'", "")
            codes = [code.strip() for code in cleaned_codes.split(',') if code.strip()]
            stats = updater.update_bonds_list(bond_codes=codes, **update_args)
        elif args.all:
            stats = updater.update_all_downloaded_bonds(**update_args)
        else:
            print("请指定更新模式: --codes 或 --all")
            return

        print("\n更新完成!")
        for key, value in stats.items():
            print(f"{key.replace('_', ' ').title()}: {value if isinstance(value, int) else len(value)}")

    except KeyboardInterrupt:
        print("\n用户中断更新")
    except Exception as e:
        print(f"更新过程中发生错误: {e}")


if __name__ == "__main__":
    main()