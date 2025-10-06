# OfflineData 离线数据模块

本模块提供了一套完整的离线数据管理工具，支持从不同数据源下载和更新金融市场数据，并与缠论分析框架无缝集成。

## 核心功能

- **多市场支持**:
  - **A股与指数**: 通过 `BaoStock` 接口下载和更新。
  - **公募REITs**: 通过 `Akshare` 接口下载和更新。
- **复权类型支持**: 所有数据均按复权类型 (`qfq` - 前复权, `hfq` - 后复权, `none` - 不复权) 进行独立的下载、存储和管理，确保数据源的纯净与准确。
- **全量与增量**:
  - **全量下载**: 一次性获取指定时间范围内的全部历史数据。
  - **增量更新**: 智能检测本地数据的最新时间点，仅下载此后的新数据，高效快捷。
- **多种K线周期**: 支持日线(`day`)、周线(`week`)、月线(`mon`)等多种K线周期。
- **命令行驱动**: 提供简单易用的命令行接口，方便自动化执行下载和更新任务。
- **健壮性设计**: 内置网络重试机制，有效应对临时性的接口不稳定问题。

## 数据存储结构

数据被严格按照 **复权类型 -> K线周期** 的目录结构进行组织，确保不同类型的数据互相隔离。

```
data/
└── offline/                 # 离线数据根目录
    ├── qfq/                 # 前复权数据
    │   ├── day/             # 日线
    │   │   ├── sh.000001.csv
    │   │   └── 508001.csv   # REITs代码
    │   └── week/            # 周线
    │       └── sh.000001.csv
    ├── hfq/                 # 后复权数据
    │   └── day/
    └── none/                # 不复权数据
        └── day/
```

## 命令行使用指南

所有脚本都支持通过命令行参数进行灵活配置。

### A股 & 指数 (BaoStock)

#### 数据下载 (`bao_download.py`)

```bash
# 下载所有A股和指数的日线、周线、月线数据（前复权）
# 这是最常用的初始数据准备命令
python OfflineData/bao_download.py --include-index --autype qfq

# 下载指定股票的日线数据（后复权）
python OfflineData/bao_download.py --codes "sh.600519,sz.000001" --k-types day --autype hfq

# 从文件读取股票列表进行下载（不复权）
pythonOfflineData/bao_download.py --codes-file my_stocks.txt --autype none

# 强制重新下载已存在的数据
python OfflineData/bao_download.py --codes "sh.600519" --force-update --autype qfq
```

#### 数据更新 (`bao_update.py`)

```bash
# 增量更新所有已下载的前复权数据
# 这是最常用的日常更新命令
python OfflineData/bao_update.py --all --autype qfq

# 更新指定股票的后复权数据
python OfflineData/bao_update.py --codes "sh.600519,sz.000001" --autype hfq

# 强制对所有已下载的前复权数据进行全量更新（会删除旧数据重新下载）
python OfflineData/bao_update.py --all --force-full --autype qfq
```

### 公募 REITs (Akshare)

#### 数据下载 (`reits_download.py`)

```bash
# 下载所有公募REITs的日线、周线、月线数据（前复权）
python OfflineData/reits_download.py --autype qfq

# 下载指定REITs的日线数据（后复权）
python OfflineData/reits_download.py --codes "508097,180101" --k-types day week mon --autype none
```

#### 数据更新 (`reits_update.py`)

```bash
# 增量更新所有已下载的前复权REITs数据
python OfflineData/reits_update.py --all --autype qfq

# 强制对指定的REITs进行全量更新
python OfflineData/reits_update.py --codes "508001" --force-full --autype qfq
