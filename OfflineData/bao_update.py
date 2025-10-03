"""
BaoStock增量数据更新器
支持增量更新A股、指数的K线数据
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

import baostock as bs
from Common.CEnum import AUTYPE, DATA_FIELD, KL_TYPE
from Common.CTime import CTime
from Common.func_util import str2float
from KLine.KLine_Unit import CKLine_Unit
from DataAPI.BaoStockAPI import CBaoStock, create_item_dict, GetColumnNameFromFieldList
from OfflineData.offline_data_util import OfflineDataUtil


class BaoStockUpdater:
    """BaoStock数据更新器"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化更新器
        
        Args:
            config_path: 配置文件路径
        """
        self.util = OfflineDataUtil(config_path)
        self.logger = self.util.logger
        self.is_connected = False
        
        # 默认更新参数
        self.default_k_types = [KL_TYPE.K_DAY, KL_TYPE.K_WEEK, KL_TYPE.K_MON]
        self.default_autype = AUTYPE.QFQ
        self.update_days = 30  # 默认更新最近30天的数据
        
        # 更新统计
        self.update_stats = {
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'updated_count': 0,
            'new_records': 0,
            'failed_stocks': []
        }
    
    def connect(self) -> bool:
        """
        连接BaoStock
        
        Returns:
            连接是否成功
        """
        try:
            if not self.is_connected:
                result = bs.login()
                if result.error_code == '0':
                    self.is_connected = True
                    self.logger.info("BaoStock连接成功")
                    return True
                else:
                    self.logger.error(f"BaoStock连接失败: {result.error_msg}")
                    return False
            return True
        except Exception as e:
            self.logger.error(f"BaoStock连接异常: {e}")
            return False
    
    def disconnect(self):
        """断开BaoStock连接"""
        try:
            if self.is_connected:
                bs.logout()
                self.is_connected = False
                self.logger.info("BaoStock连接已断开")
        except Exception as e:
            self.logger.error(f"断开BaoStock连接异常: {e}")

    def _normalize_stock_code(self, code: str)-> str:
        """
        自动为股票代码添加 'sh.' 或 'sz.' 前缀
        """
        code = code.strip()
        if code.startswith('sh.') or code.startswith('sz.'):
            return code

        if not self.connect():
            self.logger.warning(f"无法连接BaoStock来规范化代码 {code}，将按原样返回。")
            return code

        # 尝试查询上海交易所
        rs_sh = bs.query_stock_basic(code=f"sh.{code}")
        if rs_sh.error_code == '0' and rs_sh.next():
            self.logger.info(f"代码 {code} 在上海交易所找到，自动添加 'sh.' 前缀。")
            return f"sh.{code}"

        # 尝试查询深圳交易所
        rs_sz = bs.query_stock_basic(code=f"sz.{code}")
        if rs_sz.error_code == '0' and rs_sz.next():
            self.logger.info(f"代码 {code} 在深圳交易所找到，自动添加 'sz.' 前缀。")
            return f"sz.{code}"

        self.logger.warning(f"无法为代码 {code} 确定交易所前缀，可能是一个无效的代码。")
        return code
    
    def get_update_date_range(self, code: str, k_type: KL_TYPE) -> Tuple[Optional[str], str]:
        """
        获取需要更新的日期范围
        
        Args:
            code: 股票代码
            k_type: K线类型
            
        Returns:
            (开始日期, 结束日期) 元组
        """
        try:
            # 获取现有数据的最新时间
            latest_time = self.util.get_latest_data_time(code, k_type)
            end_date = datetime.now().strftime("%Y-%m-%d")
            
            if latest_time:
# 将 CTime 转换为 datetime
                latest_datetime = datetime(latest_time.year, latest_time.month, latest_time.day)
                # 从最新数据时间的下一天开始更新
                start_date = (latest_datetime + timedelta(days=1)).strftime("%Y-%m-%d")
                
                # 如果开始日期已经超过结束日期，说明数据已是最新
                if start_date > end_date:
                    return None, end_date
                    
                return start_date, end_date
            else:
                # 如果没有现有数据，从指定天数前开始
                start_date = (datetime.now() - timedelta(days=self.update_days)).strftime("%Y-%m-%d")
                return start_date, end_date
                
        except Exception as e:
            self.logger.error(f"获取更新日期范围失败 {code} {k_type.name}: {e}")
            # 默认更新最近30天
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=self.update_days)).strftime("%Y-%m-%d")
            return start_date, end_date
    
    def update_single_stock(self, code: str, k_type: KL_TYPE, 
                          autype: AUTYPE = AUTYPE.QFQ,
                          force_full_update: bool = False) -> bool:
        """
        更新单只股票的K线数据
        
        Args:
            code: 股票代码
            k_type: K线类型
            autype: 复权类型
            force_full_update: 是否强制全量更新
            
        Returns:
            更新是否成功
        """
        try:
            if force_full_update:
                # 强制全量更新，删除现有数据
                self.util.delete_kline_data(code, k_type)
                start_date = (datetime.now() - timedelta(days=365*5)).strftime("%Y-%m-%d")  # 5年数据
                end_date = datetime.now().strftime("%Y-%m-%d")
            else:
                # 增量更新
                start_date, end_date = self.get_update_date_range(code, k_type)
                if start_date is None:
                    self.logger.info(f"股票 {code} {k_type.name} 数据已是最新，跳过更新")
                    self.update_stats['skipped_count'] += 1
                    return True
            
            if not self.connect():
                return False
            
            # 使用现有的BaoStock API获取数据
            bao_api = CBaoStock(code, k_type, start_date, end_date, autype)
            bao_api.SetBasciInfo()
            
            # 获取新数据
            new_data = []
            for kl_unit in bao_api.get_kl_data():
                new_data.append(kl_unit)
            
            if not new_data:
                self.logger.info(f"股票 {code} {k_type.name} 没有新数据")
                self.update_stats['skipped_count'] += 1
                return True
            
            if force_full_update:
                # 全量更新，直接保存
                self.util.save_kline_data_csv(code, k_type, new_data)
                self.update_stats['updated_count'] += 1
                self.update_stats['new_records'] += len(new_data)
                self.logger.info(f"全量更新 {code} {k_type.name}，共{len(new_data)}条记录")
            else:
                # 增量更新，追加数据
                self.util.append_kline_data(code, k_type, new_data)
                self.update_stats['updated_count'] += 1
                self.update_stats['new_records'] += len(new_data)
                self.logger.info(f"增量更新 {code} {k_type.name}，新增{len(new_data)}条记录")
            
            self.update_stats['success_count'] += 1
            return True
            
        except Exception as e:
            self.logger.error(f"更新股票 {code} {k_type.name} 数据失败: {e}")
            self.update_stats['failed_count'] += 1
            self.update_stats['failed_stocks'].append(f"{code}_{k_type.name}")
            return False
    
    def update_stock_list(self, stock_codes: List[str], 
                        k_types: Optional[List[KL_TYPE]] = None,
                        autype: AUTYPE = AUTYPE.QFQ,
                        force_full_update: bool = False,
                        max_workers: int = 1,
                        delay_seconds: float = 0.2) -> Dict[str, Any]:
        """
        批量更新股票数据
        
        Args:
            stock_codes: 股票代码列表
            k_types: K线类型列表
            autype: 复权类型
            force_full_update: 是否强制全量更新
            max_workers: 最大并发数
            delay_seconds: 请求间隔
            
        Returns:
            更新统计信息
        """
        if not stock_codes:
            self.logger.warning("股票代码列表为空")
            return self.update_stats
        
        # 自动补全股票代码前缀
        normalized_codes = [self._normalize_stock_code(code) for code in stock_codes]

        k_types = k_types or self.default_k_types
        
        self.logger.info(f"开始批量更新，股票数量: {len(normalized_codes)}, K线类型: {[kt.name for kt in k_types]}")
        
        # 重置统计信息
        self.update_stats = {
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'updated_count': 0,
            'new_records': 0,
            'failed_stocks': []
        }
        
        start_time = time.time()
        
        # 创建更新任务
        tasks = []
        for code in normalized_codes:
            for k_type in k_types:
                tasks.append((code, k_type, autype, force_full_update))
        
        self.logger.info(f"总共{len(tasks)}个更新任务")
        
        # 使用线程池更新
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(self.update_single_stock, *task): task 
                for task in tasks
            }
            
            completed = 0
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                code, k_type = task[0], task[1]
                
                try:
                    success = future.result()
                    completed += 1
                    
                    if completed % 10 == 0:
                        progress = (completed / len(tasks)) * 100
                        self.logger.info(f"更新进度: {completed}/{len(tasks)} ({progress:.1f}%)")
                    
                    # 添加延迟避免请求过快
                    if delay_seconds > 0:
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    self.logger.error(f"更新任务异常 {code} {k_type.name}: {e}")
                    self.update_stats['failed_count'] += 1
                    self.update_stats['failed_stocks'].append(f"{code}_{k_type.name}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 输出统计信息
        self.logger.info("=" * 50)
        self.logger.info("更新完成统计:")
        self.logger.info(f"总耗时: {duration:.2f}秒")
        self.logger.info(f"成功: {self.update_stats['success_count']}")
        self.logger.info(f"失败: {self.update_stats['failed_count']}")
        self.logger.info(f"跳过: {self.update_stats['skipped_count']}")
        self.logger.info(f"更新股票数: {self.update_stats['updated_count']}")
        self.logger.info(f"新增记录数: {self.update_stats['new_records']}")
        
        if self.update_stats['failed_stocks']:
            self.logger.warning(f"失败的股票: {self.update_stats['failed_stocks'][:10]}...")
        
        return self.update_stats
    
    def update_all_downloaded_stocks(self, k_types: Optional[List[KL_TYPE]] = None,
                                   force_full_update: bool = False,
                                   max_workers: int = 3) -> Dict[str, Any]:
        """
        更新所有已下载的股票数据
        
        Args:
            k_types: K线类型列表
            force_full_update: 是否强制全量更新
            max_workers: 最大并发数
            
        Returns:
            更新统计信息
        """
        # 获取已下载的股票列表
        downloaded_stocks = self.util.get_downloaded_stocks()
        
        if not downloaded_stocks:
            self.logger.warning("没有找到已下载的股票数据")
            return self.update_stats
        
        self.logger.info(f"找到{len(downloaded_stocks)}只已下载的股票")
        
        return self.update_stock_list(
            stock_codes=downloaded_stocks,
            k_types=k_types,
            force_full_update=force_full_update,
            max_workers=max_workers
        )
    
    def update_by_file(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """
        从文件读取股票代码列表并更新
        
        Args:
            file_path: 股票代码文件路径，每行一个代码
            **kwargs: 其他更新参数
            
        Returns:
            更新统计信息
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                stock_codes = [line.strip() for line in f if line.strip()]
            
            self.logger.info(f"从文件 {file_path} 读取到 {len(stock_codes)} 个股票代码")
            return self.update_stock_list(stock_codes, **kwargs)
            
        except Exception as e:
            self.logger.error(f"从文件读取股票代码失败: {e}")
            return self.update_stats
    
    def auto_update_schedule(self, k_types: Optional[List[KL_TYPE]] = None,
                           update_time: str = "15:30",
                           max_workers: int = 3) -> None:
        """
        定时自动更新
        
        Args:
            k_types: K线类型列表
            update_time: 更新时间 (HH:MM)
            max_workers: 最大并发数
        """
        import schedule
        
        def update_job():
            self.logger.info("开始定时更新任务")
            stats = self.update_all_downloaded_stocks(k_types, max_workers=max_workers)
            self.logger.info(f"定时更新完成: 成功{stats['success_count']}, 失败{stats['failed_count']}")
        
        # 设置定时任务
        schedule.every().day.at(update_time).do(update_job)
        
        self.logger.info(f"已设置定时更新任务，每天{update_time}执行")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            self.logger.info("定时更新任务已停止")
    
    def repair_data(self, code: str, k_type: KL_TYPE, 
                   check_days: int = 30) -> bool:
        """
        修复数据缺失
        
        Args:
            code: 股票代码
            k_type: K线类型
            check_days: 检查最近多少天的数据
            
        Returns:
            修复是否成功
        """
        try:
            # 检查数据完整性
            existing_data = self.util.load_kline_data_csv(code, k_type)
            if not existing_data:
                self.logger.warning(f"股票 {code} {k_type.name} 没有现有数据")
                return False
            
            # 检查最近数据是否有缺失
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=check_days)).strftime("%Y-%m-%d")
            
            # 获取应有的交易日期
            if not self.connect():
                return False
            
            bao_api = CBaoStock(code, k_type, start_date, end_date, AUTYPE.QFQ)
            bao_api.SetBasciInfo()
            
            expected_data = []
            for kl_unit in bao_api.get_kl_data():
                expected_data.append(kl_unit)
            
            if len(expected_data) > len(existing_data):
                self.logger.info(f"检测到数据缺失，开始修复 {code} {k_type.name}")
                # 重新下载最近的数据
                return self.update_single_stock(code, k_type, force_full_update=True)
            
            return True
            
        except Exception as e:
            self.logger.error(f"修复数据失败 {code} {k_type.name}: {e}")
            return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='BaoStock增量数据更新器')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--k-types', type=str, default='day,week,mon', 
                       help='K线类型，逗号分隔 (day,week,mon,5m,15m,30m,60m)')
    parser.add_argument('--codes', type=str, help='指定股票代码，逗号分隔')
    parser.add_argument('--codes-file', type=str, help='股票代码文件路径')
    parser.add_argument('--all', action='store_true', help='更新所有已下载的股票')
    parser.add_argument('--force-full', action='store_true', help='强制全量更新')
    parser.add_argument('--max-workers', type=int, default=1, help='最大并发数 (默认为1, 即单线程顺序更新)')
    parser.add_argument('--delay', type=float, default=0.2, help='请求间隔秒数')
    parser.add_argument('--schedule', type=str, help='定时更新时间 (HH:MM)')
    parser.add_argument('--repair', action='store_true', help='修复数据模式')
    
    args = parser.parse_args()
    
    # 解析K线类型
    k_type_map = {
        'day': KL_TYPE.K_DAY,
        'week': KL_TYPE.K_WEEK,
        'mon': KL_TYPE.K_MON,
        '5m': KL_TYPE.K_5M,
        '15m': KL_TYPE.K_15M,
        '30m': KL_TYPE.K_30M,
        '60m': KL_TYPE.K_60M,
    }
    
    k_types = []
    for k_type_str in args.k_types.split(','):
        k_type_str = k_type_str.strip()
        if k_type_str in k_type_map:
            k_types.append(k_type_map[k_type_str])
        else:
            print(f"警告: 未知的K线类型 {k_type_str}")
    
    if not k_types:
        k_types = [KL_TYPE.K_DAY]
    
    # 创建更新器
    updater = BaoStockUpdater(args.config)
    
    try:
        if args.schedule:
            # 定时更新模式
            updater.auto_update_schedule(k_types, args.schedule, args.max_workers)
        elif args.codes:
            # 更新指定股票
            stock_codes = [code.strip() for code in args.codes.split(',')]
            stats = updater.update_stock_list(
                stock_codes=stock_codes,
                k_types=k_types,
                force_full_update=args.force_full,
                max_workers=args.max_workers,
                delay_seconds=args.delay
            )
        elif args.codes_file:
            # 从文件更新
            stats = updater.update_by_file(
                file_path=args.codes_file,
                k_types=k_types,
                force_full_update=args.force_full,
                max_workers=args.max_workers,
                delay_seconds=args.delay
            )
        elif args.all:
            # 更新所有已下载的股票
            stats = updater.update_all_downloaded_stocks(
                k_types=k_types,
                force_full_update=args.force_full,
                max_workers=args.max_workers
            )
        else:
            print("请指定更新模式: --codes, --codes-file, --all 或 --schedule")
            return
        
        if not args.schedule:
            print("\n更新完成!")
            print(f"成功: {stats['success_count']}")
            print(f"失败: {stats['failed_count']}")
            print(f"跳过: {stats['skipped_count']}")
            print(f"更新股票数: {stats['updated_count']}")
            print(f"新增记录数: {stats['new_records']}")
        
    except KeyboardInterrupt:
        print("\n用户中断更新")
    except Exception as e:
        print(f"更新过程中发生错误: {e}")
    finally:
        updater.disconnect()


if __name__ == "__main__":
    main()
