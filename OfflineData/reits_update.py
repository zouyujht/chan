"""
Akshare REITS增量数据更新器
支持增量更新公募REITS的K线数据
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
from Common.func_util import str2float
from KLine.KLine_Unit import CKLine_Unit
from OfflineData.offline_data_util import OfflineDataUtil


class AkshareReitsUpdater:
    """Akshare REITS数据更新器"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化更新器

        Args:
            config_path: 配置文件路径
        """
        self.util = OfflineDataUtil(config_path)
        self.logger = self.util.logger

        # 默认更新参数
        self.default_k_types = [KL_TYPE.K_DAY] # REITs 只有日线
        self.default_autype = AUTYPE.NONE # REITs 只有不复权
        self.update_days = 60  # 默认更新最近60天的数据

        # 更新统计
        self.update_stats = {
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'updated_count': 0,
            'new_records': 0,
            'failed_stocks': []
        }

    def get_update_date_range(self, code: str, k_type: KL_TYPE, autype: AUTYPE) -> Tuple[Optional[str], str]:
        """
        获取需要更新的日期范围

        Args:
            code: REIT代码
            k_type: K线类型
            autype: 复权类型

        Returns:
            (开始日期, 结束日期) 元组
        """
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

    def update_single_reit(self, code: str, k_type: KL_TYPE,                       autype: AUTYPE = AUTYPE.QFQ,
                           force_full_update: bool = False) -> bool:
        """
        更新单只REIT的K线数据
        """
        try:
            # akshare reits_hist_em 接口仅支持日线、不复权数据
            if k_type != KL_TYPE.K_DAY:
                self.logger.warning(f"Akshare REITS接口当前仅支持日线数据更新，跳过 {k_type.name}。")
                self.update_stats['skipped_count'] += 1
                return True

            if autype != AUTYPE.NONE:
                self.logger.warning(f"Akshare REITS接口仅支持不复权数据，您的 --autype={autype.name} 参数将被忽略，数据将作为不复权（NONE）处理。")
            
            # 强制使用不复权类型进行存储和日期范围检查
            storage_autype = AUTYPE.NONE

            if force_full_update:
                self.util.delete_kline_data(code, k_type, storage_autype) 
                start_date = "2020-01-01"
            else:
                start_date, _ = self.get_update_date_range(code, k_type, storage_autype)
                if start_date is None:
                    self.logger.info(f"REIT {code} {k_type.name} {storage_autype.name} 数据已是最新，跳过更新")
                    self.update_stats['skipped_count'] += 1
                    return True

            # 获取新数据 (无复权和周期参数)
            data_df =ak.reits_hist_em(symbol=code)
            if data_df.empty:
                self.logger.info(f"REIT {code} {k_type.name} 没有获取到新数据")
                self.update_stats['skipped_count'] += 1
                return True

            data_df['日期'] = pd.to_datetime(data_df['日期']).dt.strftime('%Y-%m-%d')
            new_data_df = data_df[data_df['日期'] >= start_date]

            if new_data_df.empty:
                self.logger.info(f"REIT {code} {k_type.name} 没有新数据")
                self.update_stats['skipped_count'] += 1
                return True

            new_data = []
            for _, row in new_data_df.iterrows():
                item_dict = {
                    DATA_FIELD.FIELD_TIME: CTime.from_str(row['日期']),
                    DATA_FIELD.FIELD_OPEN: row['开盘'],
                    DATA_FIELD.FIELD_HIGH: row['最高'],
                    DATA_FIELD.FIELD_LOW: row['最低'],
                    DATA_FIELD.FIELD_CLOSE: row['收盘'],
                    DATA_FIELD.FIELD_VOLUME: row['成交量'],
                    DATA_FIELD.FIELD_TURNOVER: row['成交额'],
                    DATA_FIELD.FIELD_TURNRATE: row.get('换手率', 0.0)
                }
                new_data.append(CKLine_Unit(item_dict))

            if force_full_update:
                self.util.save_kline_data_csv(code, k_type, storage_autype, new_data)
                log_msg = f"全量更新 {code} {k_type.name} ({storage_autype.name})，共{len(new_data)}条记录"
            else:
                self.util.append_kline_data(code, k_type, storage_autype, new_data)
                log_msg = f"增量更新 {code} {ktype.name} ({storage_autype.name})，新增{len(new_data)}条记录"

            self.update_stats['updated_count'] += 1
            self.update_stats['new_records'] += len(new_data)
            self.update_stats['success_count'] += 1
            self.logger.info(log_msg)
            return True

        except Exception as e:
            self.logger.error(f"更新REIT {code} {k_type.name} 数据失败: {e}")
            self.update_stats['failed_count'] += 1
            self.update_stats['failed_stocks'].append(f"{code}_{k_type.name}")
            return False

    def update_reits_list(self, reits_codes: List[str], **kwargs) -> Dict[str, Any]:
        """
        批量更新REITS数据
        """
        if not reits_codes:
            self.logger.warning("REITS代码列表为空")
            return self.update_stats

        self.logger.info(f"开始批量更新，REITS数量: {len(reits_codes)}")
        self.update_stats = {k: 0 if isinstance(v, int) else [] for k, v in self.update_stats.items()}
        start_time = time.time()

        tasks = [(code, k_type, kwargs.get('autype', self.default_autype), kwargs.get('force_full_update', False))
                 for code in reits_codes for k_type in kwargs.get('k_types', self.default_k_types)]

        with ThreadPoolExecutor(max_workers=kwargs.get('max_workers', 1)) as executor:
            future_to_task = {executor.submit(self.update_single_reit, *task):task for task in tasks}
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

    def update_all_downloaded_reits(self, **kwargs) -> Dict[str, Any]:
        """
       更新所有已下载的REITS数据
        """
        # REITs 只有不复权数据, 忽略传入的autype
        storage_autype= AUTYPE.NONE
        all_codes = self.util.get_downloaded_stocks(autype=storage_autype, stock_type='reits')
        self.logger.info(f"检测到 {len(all_codes)} 个已下载的REITS ({storage_autype.name})")
        kwargs['autype'] = storage_autype # 确保后续流程使用正确的autype
        return self.update_reits_list(all_codes, **kwargs)


def main():
    parser = argparse.ArgumentParser(description='Akshare REITS增量数据更新器')
    parser.add_argument('--config', type=str, help='配置文件路径')
    # REITs只有不复权(none)和日线(day)数据，因此移除相关参数，或设为固定值
    parser.add_argument('--autype', type=str, default='none', help='复权类型 (固定为none)')
    parser.add_argument('--k-types', type=str, default='day', help='K线类型 (固定为day)')
    parser.add_argument('--codes', type=str, help='指定REIT代码，逗号分隔')
    parser.add_argument('--all', action='store_true', help='更新所有已下载的REITS')
    parser.add_argument('--force-full', action='store_true', help='强制全量更新')
    parser.add_argument('--max-workers', type=int, default=1, help='最大并发数')
    parser.add_argument('--delay', type=float, default=0.5, help='请求间隔秒数')
    args = parser.parse_args()

    k_type_map = {'day': KL_TYPE.K_DAY, 'week': KL_TYPE.K_WEEK, 'mon': KL_TYPE.K_MON}
    k_types = [k_type_map[kt.strip()] for kt in args.k_types.split(',') if kt.strip() in k_type_map]

    autype_map = {'qfq': AUTYPE.QFQ, 'hfq': AUTYPE.HFQ, 'none': AUTYPE.NONE}
    autype = autype_map.get(args.autype.lower(), AUTYPE.QFQ)

    updater = AkshareReitsUpdater(args.config)
    
    update_args = {
        'autype': autype,
        'k_types': k_types,
        'force_full_update': args.force_full,
        'max_workers': args.max_workers,
        'delay_seconds': args.delay
    }

    try:
        if args.codes:
            codes = [code.strip() for code in args.codes.split(',')]
            stats = updater.update_reits_list(reits_codes=codes, **update_args)
        elif args.all:
            stats = updater.update_all_downloaded_reits(**update_args)
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
