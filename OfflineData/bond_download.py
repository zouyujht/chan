"""
Akshare 债券历史数据下载器
支持下载沪深交易所债券（含国债等）日线历史行情数据
参考 OfflineData/reits_download.py 的结构实现
"""

import os
import sys
import time
import argparse
from typing import List, Optional, Dict, Any

# 将项目根目录加入路径，便于在本项目中调用
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd


def normalize_symbol(code: str, exchange: Optional[str] = None) -> List[str]:
    """
    将纯债券代码（如 019547）规范为带前缀的交易所代码。
    若传入 exchange，则只返回该前缀；否则返回按优先顺序的候选列表。
    目前示例与 akshare 文档中使用的是上海前缀 "sh"，如 "sh019315"。

    Args:
        code: 债券代码，如 "019547" 或已经带前缀的 "sh019547"/"sz019547"
        exchange: 可选，指定交易所前缀（"sh" 或 "sz"）

    Returns:
        候选的完整 symbol 列表（至少包含一个）
    """
    code = code.strip()
    if code.lower().startswith(("sh", "sz")):
        # 已经带前缀，直接返回
        return [code.lower()]

    if exchange is not None:
        ex = exchange.lower()
        if ex not in {"sh", "sz"}:
            raise ValueError(f"exchange 参数非法: {exchange}，可选值: 'sh' 或 'sz'")
        return [f"{ex}{code}"]

    # 未指定交易所，先尝试上海，再尝试深圳
    return [f"sh{code}", f"sz{code}"]


def fetch_bond_daily(symbol: str, max_retries: int = 3, retry_delay: float = 3.0) -> pd.DataFrame:
    """
    通过 akshare 接口 bond_zh_hs_daily 获取单个债券的全部历史日线数据。

    Args:
        symbol: 带交易所前缀的代码，如 "sh019547"
        max_retries: 最大重试次数
        retry_delay: 重试间隔秒数

    Returns:
        pandas.DataFrame，列为 [date, open, high, low, close, volume]
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            df = ak.bond_zh_hs_daily(symbol=symbol)
            if df is None:
                df = pd.DataFrame()
            return df
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise e
    # 理论上不会到这里
    if last_exc:
        raise last_exc
    return pd.DataFrame()


def download_single_bond(code: str,
                         exchange: Optional[str] = None,
                         save_dir: Optional[str] = None,
                         force_overwrite: bool = False,
                         verbose: bool = True) -> Dict[str, Any]:
    """
    下载单只债券的日线历史数据，并可选保存为 CSV。

    Args:
        code: 债券代码（如 "019547" 或 "sh019547"）
        exchange: 指定交易所前缀（"sh"/"sz"），不指定则尝试两者
        save_dir: 保存目录；若为 None 则不保存，仅返回数据
        force_overwrite: 若目标 CSV 已存在，是否覆盖
        verbose: 是否打印日志

    Returns:
        dict: {
            'symbol': 使用的完整代码,
            'exchange_tried': 尝试的前缀列表,
            'success': 是否成功获取数据,
            'rows': 行数,
            'csv_path': 保存路径（如保存）,
            'data': DataFrame（如不太大可直接返回）
        }
    """
    tried = normalize_symbol(code, exchange)
    df = pd.DataFrame()
    used_symbol = None

    for sym in tried:
        if verbose:
            print(f"尝试获取债券数据: {sym} ...")
        try:
            df = fetch_bond_daily(sym)
        except Exception as e:
            if verbose:
                print(f"获取 {sym} 失败: {e}")
            continue
        # 接口返回空 DataFrame 时也可能表示该前缀不适用
        if df is not None and not df.empty:
            used_symbol = sym
            break
        else:
            if verbose:
                print(f"{sym} 返回空数据，尝试其它前缀...")

    result: Dict[str, Any] = {
        'symbol': used_symbol or (tried[0] if tried else code),
        'exchange_tried': tried,
        'success': used_symbol is not None and not df.empty,
        'rows': int(df.shape[0]) if df is not None else 0,
        'csv_path': None,
        'data': df
    }

    # 规范日期格式为 YYYY-MM-DD，确保一致性
    if result['success']:
        if 'date' in df.columns:
            try:
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            except Exception:
                # 如果无法解析日期，保留原始值
                pass

        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            csv_path = os.path.join(save_dir, f"{result['symbol']}.csv")
            if os.path.exists(csv_path) and not force_overwrite:
                if verbose:
                    print(f"CSV 已存在，跳过覆盖: {csv_path}")
                result['csv_path'] = csv_path
            else:
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                if verbose:
                    print(f"已保存 CSV: {csv_path} (行数: {result['rows']})")
                result['csv_path'] = csv_path
    else:
        if verbose:
            print("未能成功获取数据，请检查代码或交易所前缀是否正确。")

    return result


def main():
    parser = argparse.ArgumentParser(description='Akshare 债券历史数据下载器')
    parser.add_argument('--codes', type=str, help='债券代码，逗号分隔；可为纯数字(如019547)或带前缀(如sh019547)')
    parser.add_argument('--exchange', type=str, choices=['sh', 'sz'], help='指定交易所前缀（默认自动尝试）')
    parser.add_argument('--save-dir', type=str, default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Data', 'Bond'),
                        help='CSV 保存目录；默认保存到项目 Data/Bond 目录')
    parser.add_argument('--force', action='store_true', help='若 CSV 已存在则覆盖')
    parser.add_argument('--no-save', action='store_true', help='不保存 CSV，仅打印信息')
    args = parser.parse_args()

    if not args.codes:
        # 若未指定代码，默认用 019547 进行示例下载（用户要求测试用）
        codes = ['019547']
        print("未指定 --codes，默认使用 019547 进行测试（16国债19）")
    else:
        cleaned = args.codes.replace('"', '').replace("'", '')
        codes = [c.strip() for c in cleaned.split(',') if c.strip()]

    save_dir = None if args.no_save else args.save_dir

    total = 0
    success = 0
    for code in codes:
        total += 1
        res = download_single_bond(code, exchange=args.exchange, save_dir=save_dir, force_overwrite=args.force, verbose=True)
        if res['success']:
            success += 1
            print(f"成功: {code} -> {res['symbol']} 行数: {res['rows']} CSV: {res['csv_path']}")
            # 打印前 10 行供快速检查
            print(res['data'].head(10))
        else:
            print(f"失败: {code}，尝试过: {res['exchange_tried']}")

    print("=" * 60)
    print(f"任务完成，总数: {total}，成功: {success}，失败: {total - success}")


if __name__ == '__main__':
    main()