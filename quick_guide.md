# Chan.py 快速入门指南

本指南旨在帮助您快速开始使用 `chan.py` 项目，对 A 股和 REITs 进行缠论分析。

## 1. 环境准备

在开始之前，请确保您已安装 Python 环境。然后，通过以下命令安装项目所需的依赖库：

```bash
pip install -r Script/requirements.txt
```

## 2. 数据准备

本项目的分析功能依赖于本地存储的金融数据。您需要先下载并定期更新这些数据。

### 2.1 下载初始数据

- **下载股票数据**:
  使用 `OfflineData/bao_download.py` 脚本下载指定股票代码的历史数据。`--autype` 参数指定复权类型，推荐使用 `qfq` (前复权)。

  ```bash
  # 示例：下载某只股票的前复权日线数据
  python OfflineData/bao_download.py --codes 002572 --autype qfq
  ```

- **下载 REITs 数据**:
  使用 `OfflineData/reits_download.py` 脚本下载指定 REITs 的历史数据。REITs 数据无需复权。

  ```bash
  # 示例：下载某只REIT的数据
  python OfflineData/reits_download.py --codes 180301
  ```

### 2.2 更新本地数据

为了获取最新的行情，您需要定期运行更新脚本。

- **更新全部本地股票数据**:

  ```bash
  python OfflineData/bao_update.py --all --autype qfq
  ```

- **更新全部本地 REITs 数据**:

  ```bash
  python OfflineData/reits_update.py --all
  ```

## 3. 运行分析模板

项目提供了两个预设的分析模板，分别用于股票和 REITs 的可视化分析。

### 3.1 分析股票

`template_stock.py` 脚本会扫描本地已下载的股票数据，并提供一个交互式菜单供您选择。

- **运行命令**:
  ```bash
  python template_stock.py
  ```
- **功能**:
  1.  扫描 `data/offline/qfq/day` 目录下的所有股票数据。
  2.  分页显示股票列表，等待用户选择。
  3.  对选中的股票，依次展示月、周、日、60分钟、30分钟等多个等级别的缠论分析图表。

### 3.2 分析 REITs

`template_reits.py` 脚本专门用于分析 REITs。

- **运行命令**:
  ```bash
  python template_reits.py
  ```
- **功能**:
  1.  扫描 `data/offline/none/day` 目录下的所有 REITs 数据。
  2.  分页显示 REITs 列表，等待用户选择。
  3.  对选中的 REIT，展示近一年的日线级别缠论分析图表。

## 4. 自定义配置

您可以修改 `Config/` 目录下的 `.yaml` 文件来自定义分析和绘图参数。

- `config.yaml`: 全局配置文件，例如数据存储路径。
- `template_stock.yaml`: 股票分析模板的专属配置。
- `template_reits.yaml`: REITs 分析模板的专属配置。

通过调整这些配置，您可以深入定制分析逻辑和图表展示效果。
