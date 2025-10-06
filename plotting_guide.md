# chan.py 绘图功能说明文档

本文档详细说明 `chan.py` 项目中的绘图功能配置，主要围绕 `plot_config` 和 `plot_para` 两个核心配置字典展开。

## 快速开始

在 `main.py` 或您自己的脚本中，通过 `CPlotDriver` 类来调用绘图功能。您需要准备好 `CChan` 对象、`plot_config` 和 `plot_para`。

```python
from Chan import CChan
from ChanConfig import CChanConfig
from Plot.PlotDriver import CPlotDriver

# ... (省略 CChan 对象的初始化过程) ...

# 绘图元素开关
plot_config = {
    "plot_kline": True,
    "plot_bi": True,
    "plot_seg": True,
    "plot_zs":True,
    "plot_bsp": True,
}

# 绘图元素详细配置
plot_para= {
    "figure": {
      "width": 24,
    },
    "bi": {
       "show_num": True,
    },
}

# 绘制静态图
plot_driver = CPlotDriver(
    chan, # 您的 CChan 对象
    plot_config=plot_config,
    plot_para=plot_para,
)
plot_driver.figure.show()
```

---

## `config`：绘制元素开关

`plot_config` 是一个字典，用于控制在图上绘制哪些缠论元素。键是元素的名称，值为布尔类型（`True` 表示绘制，`False` 表示不绘制）。

**支持的键值：**

- `plot_kline`: 是否绘制K线。
- `plot_kline_combine`: 是否绘制合并后的K线。
- `plot_bi`: 是否绘制笔。
- `plot_seg`: 是否绘制线段。
- `plot_zs`: 是否绘制中枢。
- `plot_bsp`: 是否绘制形态学买卖点（理论买卖点）。
- `plot_cbsp`: 是否绘制自定义策略的买卖点。
- `plot_macd`: 是否在下方绘制 MACD 副图。
- `plot_demark`: 是否绘制 Demark 指标。
- `plot_rsi`: 是否绘制 RSI 指标。
- `plot_kdj`: 是否绘制 KDJ 指标。
- `plot_mean`: 是否绘制均线。
- `plot_boll`: 是否绘制布林线。
-channel`: 是否绘制上下轨道线。
- `plot_eigen`: 是否绘制特征序列（通常用于调试线段算法）。
- `plot_segseg`: 是否绘制线段的线段（父级别线段）。
- `plot_segzs`: 是否绘制线段中枢。
- `plot_segbsp`: 是否绘制线段的理论买卖点。
- `plot_segeigen`: 是否绘制线段分段的特征序列（通常用于调试）。
- `plot_tradeinfo`: 是否在另一Y轴上绘制额外信息（如成交量）。
- `plot_marker`: 是否添加自定义文本标记。

> **注意**: `plot_` 前缀可以省略，例如 `"kline": True` 也是有效的。

---

## `plot_para`：绘图细节参数

`plot_para` 是一个二级字典，用于精细控制每个绘图元素的显示样式。

### `figure`: 图形全局设置
- `w`, `h`: 图像的宽度和高度，默认为 `20`, `10`。
- `macd_h`: MACD 副图高度相对于主图的比例，默认为 `0.3`。
- `only_top_lv`: 是否只绘制最高级别的图像，默认为 `False`。
- `x_range`: 仅绘制最后几根K线，`0` 表示绘制全部。
- `x_bi_cnt`: 仅绘制最后几笔，`0` 表示绘制全部。
- `x_seg_cnt`: 仅绘制最后几根线段，`0` 表示绘制全部。
- `x_begin_date`, `x_end_date`: 指定绘制的开始和结束日期（格式 `YYYY/MM/DD`）。
- `x_tick_num`: X轴日期刻度的数量，默认为 `10`。
- `grid`: 绘制网格，可选 `'x'`, `'y'`, `'xy'`, `None`。

### `kl`: K线
- `width`: K线宽度，默认为 `0.4`。
- `rugd`: 是否红涨绿跌，默认为 `True`。
- `plot_mode`: 绘制模式，`'kl'` 为标准K线，也可设为 `'close'`, `'open'`, `'high'`, `'low'` 绘制对应的价格连线。

### `klc`: 合并K线
- `width`: 边框宽度，默认为 `0.4`。
- `plot_single_kl`: 当合并K线只包含一根K线时，是否也绘制边框，默认为 `True`。

### `bi`: 笔
- `color`: 颜色，默认为 `'black'`。
- `show_num`: 是否在笔中间显示序号，默认为 `False`。
- `num_color`, `num_fontsize`: 序号的颜色和字体大小。
- `disp_end`: 是否显示笔端点的价格，默认为 `False`。
- `end_color`, `end_fontsize`: 端点价格的颜色和字体大小。

### `seg`: 线段
- `width`: 线段宽度，默认为 `5`。
- `color`: 颜色，默认为 `'g'`。
- `show_num`: 是否在线段中间显示序号，默认为 `False`。
- `num_color`, `num_fontsize`: 序号的颜色和字体大小。
- `disp_end`: 是否显示线段端点的价格，默认为 `False`。
- `end_color`, `end_fontsize`: 端点价格的颜色和字体大小。
- `plot_trendline`: 是否绘制趋势线，默认为 `False`。
- `trendline_color`, `trendline_width`: 趋势线的颜色和宽度。

### `zs`: 中枢
- `color`: 颜色，默认为 `'orange'`。
- `linewidth`: 线宽，默认为 `2`。
- `sub_linewidth`: 子中枢的线宽，默认为 `0.5`。
- `show_text`: 是否显示中枢的高低点价格，默认为 `False`。
- `fontsize`, `text_color`: 价格文字的字体大小和颜色。
- `draw_one_bi_zs`: 是否绘制只有一笔的中枢，默认为 `False`。

### `bsp`: 形态学买卖点
- `buy_color`, `sell_color`: 买点和卖点的颜色。
- `fontsize`: 类别文字的字体大小。
- `arrow_l`, `arrow_h`, `arrow_w`: 箭头的长度、头部高度比例、宽度。

### `cbsp`: 自定义策略买卖点
- `buy_color`, `sell_color`: 买点和卖点的颜色。
- `fontsize`: 类别文字的字体大小。
- `arrow_l`, `arrow_h`, `arrow_w`: 箭头的长度、头部高度比例、宽度。
- `plot_cover`: 是否绘制策略的平仓点，默认为 `True`。
- `show_profit`: 是否显示收益率，默认为 `True`。

### `macd`: MACD 指标
- `width`: 红绿柱的宽度，默认为 `0.4`。

### `boll`: 布林线
- `mid_color`, `up_color`, `down_color`: 中轨、上轨、下轨的颜色。

### `channel`: 上下轨道
- `T`: 周期，必须在 `CChanConfig.trend_metrics` 中配置。
- `top_color`, `bottom_color`: 上下轨道的颜色。
- `linewidth`, `linestyle`: 轨道的线宽和线型。

### `tradeinfo`: K线指标 (如成交量)
- `info`: 要绘制的指标，可选 `'volume'`, `'turnover'`, `'turnover_rate'`。
- `color`: 指标颜色。
- `plot_outliner`: 是否绘制离群点，默认为 `True`。
- `outline_color`: 离群点颜色。

### `demark`: Demark 指标
- `setup_color`, `countdown_color`: `setup` 和 `countdown` 序号的颜色。
- `fontsize`: 序号字体大小。
- `max_countdown_background`: 达到最大 `countdown` 时的背景高亮颜色。

### `rsi`: RSI 指标
- `color`: 颜色，默认为 `'b'`。

### `kdj`: KDJ 指标
- `k_color`, `d_color`, `j_color`: K, D, J 线的颜色。
