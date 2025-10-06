"""
BaoStock全量A股数据下载器
支持下载A股、指数的历史K线数据
"""

import os
import sys
import time
import socket
import argparse
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
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


class BaoStockDownloader:
    """BaoStock数据下载器"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化下载器
        
        Args:
            config_path: 配置文件路径
        """
        self.util = OfflineDataUtil(config_path)
        self.logger = self.util.logger
        self.is_connected = False
        
        # 默认下载参数
        self.default_start_date = self.get_earliest_start_date()
        self.default_end_date = datetime.now().strftime("%Y-%m-%d")
        self.default_k_types = [KL_TYPE.K_DAY, KL_TYPE.K_WEEK, KL_TYPE.K_MON]
        self.default_autype = AUTYPE.QFQ
        
        # 下载统计
        self.download_stats = {
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'total_records': 0,
            'failed_stocks': []
        }
    
    def connect(self) ->bool:
        """
        连接BaoStock
        
        Returns:
            连接是否成功
        """
        try:
            socket.setdefaulttimeout(60)  # 设置全局超时
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
    
    def get_earliest_start_date(self) -> str:
        """
        获取所有A股最早的上市日期
        
        Returns:
            最早的上市日期 (YYYY-MM-DD)
        """
        if not self.connect():
            self.logger.warning("无法连接BaoStock，返回默认最早日期 1990-12-19")
            return "1990-12-19"

        earliest_date = "9999-12-31"
        try:
            # 查询当天所有股票信息
            rs = bs.query_all_stock(day=datetime.now().strftime("%Y-%m-%d"))
            if rs.error_code != '0':
                self.logger.error(f"获取股票列表失败: {rs.error_msg}，返回默认最早日期 1990-12-19")
                return "1990-12-19"

            while rs.error_code == '0' and rs.next():
                row = rs.get_row_data()
                ipo_date = row[2]  # 上市日期
                if ipo_date and ipo_date < earliest_date:
                    earliest_date = ipo_date
            
            if earliest_date == "9999-12-31":
                self.logger.warning("未能查询到最早上市日期，返回默认值 1990-12-19")
                return "1990-12-19"
            
            self.logger.info(f"查询到最早的股票上市日期: {earliest_date}")
            return earliest_date

        except Exception as e:
            self.logger.error(f"查询最早上市日期异常: {e}，返回默认最早日期 1990-12-19")
            return "1990-12-19"
    
    def get_all_stock_codes(self) -> List[str]:
        """
        获取所有A股股票代码
        
        Returns:
            股票代码列表
        """
        if not self.connect():
            return []
        
        stock_codes = []
        
        try:
            # 获取沪深A股列表
            for market in ['sh', 'sz']:
                rs = bs.query_all_stock(day=self.default_end_date)
                if rs.error_code != '0':
                    self.logger.error(f"获取{market}股票列表失败: {rs.error_msg}")
                    continue
                
                while rs.error_code == '0' and rs.next():
                    row = rs.get_row_data()
                    code = row[0]  # 股票代码
                    stock_type = row[4]  # 股票类型
                    status = row[5]  # 上市状态
                    
                    # 只获取正常交易的股票
                    if stock_type == '1' and status == '1':
                        stock_codes.append(code)
            
            self.logger.info(f"获取到{len(stock_codes)}只A股股票")
            return stock_codes
            
        except Exception as e:
            self.logger.error(f"获取股票列表异常: {e}")
            return []
    
    def get_index_codes(self) -> List[str]:
        """
        获取主要指数代码
        
        Returns:
            指数代码列表
        """
        # 主要指数代码
        index_codes = [
            'sh.000001',  # 上证指数
            'sz.399001',  # 深证成指
            'sz.399006',  # 创业板指
            'sh.000016',  # 上证50
            'sh.000300',  # 沪深300
            'sh.000905',  # 中证500
            'sz.399905',  # 中证500
            'sh.000852',  # 中证1000
        ]
        
        self.logger.info(f"获取到{len(index_codes)}个主要指数")
        return index_codes
    
    def _normalize_stock_code(self, code: str) -> str:
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

    def download_single_stock(self, code: str, k_type: KL_TYPE,
                                start_date: str, end_date: str,
                                autype: AUTYPE = AUTYPE.QFQ,
                                force_update: bool = False,
                                max_retries: int = 3,
                                retry_delay: int =5) -> bool:
        """
        下载单只股票的K线数据

        Args:
           code: 股票代码
            k_type: K线类型
            start_date: 开始日期
end_date: 结束日期
            autype: 复权类型
            force_update: 是否强制更新
            max_retries: 最大重试次数
            retry_delay: 重试间隔秒数

        Returns:
            下载是否成功
        """
        # 检查是否已存在数据
        if not force_update:
            file_path = self.util.create_data_file_path(code, k_type, autype, 'csv')
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                self.logger.info(f"股票 {code} {k_type.name} {autype.name} 数据已存在，跳过下载")
                self.download_stats['skipped_count'] += 1
                return True

        for attempt in range(max_retries):
            try:
                # 确保BaoStock已连接
                if not self.connect():
                    return False

                # 使用现有的BaoStock API下载数据
                bao_api = CBaoStock(code, k_type, start_date, end_date, autype)

                # 设置基本信息
                bao_api.SetBasciInfo()

                # 获取K线数据
                kline_data = list(bao_api.get_kl_data())

                if not kline_data:
                    self.logger.warning(f"股票 {code} {k_type.name} 没有获取到数据")
                    return False  # 如果没有数据，则不重试

                # 保存数据
                self.util.save_kline_data_csv(code, k_type, autype, kline_data)

                self.download_stats['success_count'] += 1
                self.download_stats['total_records'] += len(kline_data)

                self.logger.info(f"成功下载 {code} {k_type.name} 数据，共{len(kline_data)}条记录")
                return True

            except Exception as e:
                self.logger.warning(f"下载股票 {code} {k_type.name} 第 {attempt + 1} 次尝试失败 (可能为BaoStock接口不稳定或编码问题): {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"{retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"下载股票 {code} {k_type.name} 数据失败，已达最大重试次数")
                    self.download_stats['failed_count'] += 1
                    self.download_stats['failed_stocks'].append(f"{code}_{k_type.name}")
                    return False
        return False
    
    def download_stock_list(self, stock_codes: List[str], 
                          k_types: Optional[List[KL_TYPE]] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None,
                          autype: AUTYPE = AUTYPE.QFQ,
                          force_update: bool = False,
                          max_workers: int = 1,
                          delay_seconds: float = 0.1) -> Dict[str, Any]:
        """
        批量下载股票数据
        
        Args:
            stock_codes: 股票代码列表
            k_types: K线类型列表
            start_date: 开始日期
            end_date: 结束日期
            autype: 复权类型
            force_update: 是否强制更新
            max_workers: 最大并发数
            delay_seconds: 请求间隔
            
        Returns:
            下载统计信息
        """
        if not stock_codes:
            self.logger.warning("股票代码列表为空")
            return self.download_stats
        
        # 自动补全股票代码前缀
        normalized_codes = [self._normalize_stock_code(code) for code in stock_codes]
        
        k_types = k_types or self.default_k_types
        start_date = start_date or self.default_start_date
        end_date = end_date or self.default_end_date
        
        self.logger.info(f"开始批量下载，股票数量: {len(normalized_codes)}, K线类型: {[kt.name for kt in k_types]}")
        self.logger.info(f"时间范围: {start_date} 到 {end_date}")
        
        # 重置统计信息
        self.download_stats = {
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'total_records': 0,
            'failed_stocks': []
        }
        
        start_time = time.time()
        
        # 创建下载任务
        tasks = []
        for code in normalized_codes:
            for k_type in k_types:
                tasks.append((code, k_type, start_date, end_date, autype, force_update))
        
        self.logger.info(f"总共{len(tasks)}个下载任务")
        
        # 使用线程池下载
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(self.download_single_stock, *task): task 
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
                        self.logger.info(f"下载进度: {completed}/{len(tasks)} ({progress:.1f}%)")
                    
                    # 添加延迟避免请求过快
                    if delay_seconds > 0:
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    self.logger.error(f"下载任务异常 {code} {k_type.name}: {e}")
                    self.download_stats['failed_count'] += 1
                    self.download_stats['failed_stocks'].append(f"{code}_{k_type.name}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 输出统计信息
        self.logger.info("=" * 50)
        self.logger.info("下载完成统计:")
        self.logger.info(f"总耗时: {duration:.2f}秒")
        self.logger.info(f"成功: {self.download_stats['success_count']}")
        self.logger.info(f"失败: {self.download_stats['failed_count']}")
        self.logger.info(f"跳过: {self.download_stats['skipped_count']}")
        self.logger.info(f"总记录数: {self.download_stats['total_records']}")
        
        if self.download_stats['failed_stocks']:
            self.logger.warning(f"失败的股票: {self.download_stats['failed_stocks'][:10]}...")
        
        return self.download_stats
    
    def download_all_stocks(self, k_types: Optional[List[KL_TYPE]] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None,
                          autype: AUTYPE = AUTYPE.QFQ,
                          include_index: bool = True,
                          force_update: bool = False,
                          max_workers: int = 5) -> Dict[str, Any]:
        """
        下载所有A股数据
        
        Args:
            k_types: K线类型列表
            start_date: 开始日期
            end_date: 结束日期
            include_index: 是否包含指数
            force_update: 是否强制更新
            max_workers: 最大并发数
            
        Returns:
            下载统计信息
        """
        # 获取股票列表
        stock_codes = self.get_all_stock_codes()
        
        if include_index:
            index_codes = self.get_index_codes()
            stock_codes.extend(index_codes)
        
        if not stock_codes:
            self.logger.error("未获取到股票代码")
            return self.download_stats
        
        return self.download_stock_list(
            stock_codes=stock_codes,
            k_types=k_types,
            start_date=start_date,
            end_date=end_date,
            autype=autype,
            force_update=force_update,
            max_workers=max_workers
        )
    
    def download_by_file(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """
        从文件读取股票代码列表并下载
        
        Args:
            file_path: 股票代码文件路径，每行一个代码
            **kwargs: 其他下载参数
            
        Returns:
            下载统计信息
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                stock_codes = [line.strip() for line in f if line.strip()]
            
            self.logger.info(f"从文件 {file_path} 读取到 {len(stock_codes)} 个股票代码")
            return self.download_stock_list(stock_codes, **kwargs)
            
        except Exception as e:
            self.logger.error(f"从文件读取股票代码失败: {e}")
            return self.download_stats


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='BaoStock全量A股数据下载器')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--start-date', type=str, default='2010-01-01', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--k-types', type=str, default='day,week,mon', 
                       help='K线类型，逗号分隔 (day,week,mon,5m,15m,30m,60m)')
    parser.add_argument('--autype', type=str, default='qfq', help='复权类型 (qfq, hfq, none)')
    parser.add_argument('--codes', type=str, help='指定股票代码，逗号分隔')
    parser.add_argument('--codes-file', type=str, help='股票代码文件路径')
    parser.add_argument('--include-index', action='store_true', help='包含指数数据')
    parser.add_argument('--force-update', action='store_true', help='强制更新已存在的数据')
    parser.add_argument('--max-workers', type=int, default=1, help='最大并发数 (默认为1, 即单线程顺序下载)')
    parser.add_argument('--delay', type=float, default=0.1, help='请求间隔秒数')
    
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

    # 解析复权类型
    autype_map = {
        'qfq': AUTYPE.QFQ,
        'hfq': AUTYPE.HFQ,
        'none': AUTYPE.NONE,
    }
    autype = autype_map.get(args.autype.lower(), AUTYPE.QFQ)
    
    # 创建下载器
    downloader = BaoStockDownloader(args.config)
    
    try:
        end_date = args.end_date or datetime.now().strftime("%Y-%m-%d")
        
        if args.codes:
            # 下载指定股票
            stock_codes = [code.strip() for code in args.codes.split(',')]
            stats = downloader.download_stock_list(
                stock_codes=stock_codes,
                k_types=k_types,
                start_date=args.start_date,
                end_date=end_date,
                autype=autype,
                force_update=args.force_update,
                max_workers=args.max_workers,
                delay_seconds=args.delay
            )
        elif args.codes_file:
            # 从文件下载
            stats = downloader.download_by_file(
                file_path=args.codes_file,
                k_types=k_types,
                start_date=args.start_date,
                end_date=end_date,
                autype=autype,
                force_update=args.force_update,
                max_workers=args.max_workers, delay_seconds=args.delay
            )
        else:
            # 下载所有股票
            stats = downloader.download_all_stocks(
                k_types=k_types,
                start_date=args.start_date,
                end_date=end_date,
                autype=autype,
                include_index=args.include_index,
                force_update=args.force_update,
                max_workers=args.max_workers
            )
        
        print("\n下载完成!")
        print(f"成功: {stats['success_count']}")
        print(f"失败: {stats['failed_count']}")
        print(f"跳过: {stats['skipped_count']}")
        print(f"总记录数: {stats['total_records']}")
        
    except KeyboardInterrupt:
        print("\n用户中断下载")
    except Exception as e:
        print(f"下载过程中发生错误: {e}")
    finally:
        downloader.disconnect()


if __name__ == "__main__":
    main()
