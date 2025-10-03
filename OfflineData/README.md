# OfflineData 离线数据模块

本模块提供了完整的离线数据管理功能，支持从BaoStock下载和更新A股、指数的K线数据，并与缠论框架无缝集成。

## 功能特性

- **全量数据下载**: 支持下载所有A股和主要指数的历史K线数据。
- **增量数据更新**: 智能检测并更新最新的数据，避免重复下载。
- **多种K线类型**: 支持日线、周线、月线、分钟线等多种K线类型。
- **健壮的错误处理**: 内置重试机制，可自动处理因网络波动或接口不稳导致的临时性错误。
- **顺序下载**: 默认采用单线程顺序下载，以规避`BaoStock`接口限制，确保下载过程的稳定性。
- **数据完整性**: 提供数据修复和完整性检查功能。
- **定时更新**: 支持定时自动更新数据。
- **统计监控**: 提供详细的下载和更新统计信息。

## 模块结构

```
OfflineData/
├── __init__.py              # 模块初始化文件
├── offline_data_util.py     # 离线数据通用工具类
├── bao_download.py          # BaoStock全量数据下载器
├── bao_update.py            # BaoStock增量数据更新器
├── test_integration.py      # 集成测试脚本
└── README.md                # 本文档
```

## 快速开始

### 1. 环境准备

确保已安装必要的依赖：

```bash
pip install baostock>=0.8.8
pip install schedule  # 如需定时更新功能
```

### 2. 基本使用

#### 下载全量数据

```python
from OfflineData import BaoStockDownloader
from Common.CEnum import KL_TYPE

# 创建下载器
downloader = BaoStockDownloader()

# 下载所有A股的日线数据（最近1年）
stats = downloader.download_all_stocks(
    k_types=[KL_TYPE.K_DAY],
    start_date="2023-01-01",
    include_index=True
)

print(f"下载完成: 成功{stats['success_count']}, 失败{stats['failed_count']}")
```

#### 下载指定股票

```python
# 下载指定股票的多种K线数据
test_codes = ["sh.000001", "sz.000001", "sz.399001"]
stats = downloader.download_stock_list(
    stock_codes=test_codes,
    k_types=[KL_TYPE.K_DAY, KL_TYPE.K_WEEK],
    start_date="2020-01-01"
)
```

#### 增量更新数据

```python
from OfflineData import BaoStockUpdater

# 创建更新器
updater = BaoStockUpdater()

# 更新所有已下载的股票数据
stats = updater.update_all_downloaded_stocks()

print(f"更新完成: 新增{stats['new_records']}条记录")
```

## 命令行使用

### 数据下载

```bash
# 下载所有A股日线、周线、月线数据（默认单线程顺序执行）
python OfflineData/bao_download.py --all --include-index

# 下载指定股票的多种K线数据
python OfflineData/bao_download.py --codes "000001,601998" --k-types "day,week,mon"

# 从文件读取股票列表下载
python OfflineData/bao_download.py --codes-file stocks.txt --start-date 2020-01-01

# 强制更新已存在的数据
python OfflineData/bao_download.py --codes "000001" --force-update
```

### 数据更新

```bash
# 更新所有已下载的股票
python OfflineData/bao_update.py --all

# 更新指定股票
python OfflineData/bao_update.py --codes "000001,601998"

# 强制全量更新
python OfflineData/bao_update.py --all --force-full

# 定时更新（每天15:30执行）
python OfflineData/bao_update.py --all --schedule "15:30"
```

## 错误处理与稳定性

脚本内置了强大的错误处理机制，以应对`BaoStock`接口可能出现的各种问题：

1.  **接口不稳定**: `BaoStock`服务偶尔会出现网络错误、超时或返回损坏/编码错误的数据。
2.  **自动重试**: 脚本在捕获到这些错误时，会自动进行最多3次尝试。
3.  **顺序下载**: 默认采用单线程顺序下载，避免了因并发请求同一股票而导致的接口拒绝服务问题。

因此，当您在日志中看到重试信息时，请不必担心，这表明脚本正在正常处理外部接口的临时故障。

## 数据存储结构

```
data/
└── offline/                 # 离线数据根目录
    ├── day/                 # 日线数据
    │   ├── sh.000001_day.csv
    │   └── sz.000001_day.csv
    ├── week/                # 周线数据
    └── mon/                 # 月线数据
```

CSV文件包含以下列： `time`, `open`, `high`, `low`, `close`, `volume`, `turnover`, `turnrate`。
