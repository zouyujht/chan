import os
import yaml
import copy
from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, KL_TYPE
from Plot.PlotDriver import CPlotDriver
from datetime import datetime, timedelta
from DataAPI.BaoStockAPI import CBaoStock

def get_stock_list(path):
    stock_list = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".csv") and (file.startswith("sh.") or file.startswith("sz.")):
                stock_list.append(file.split('.csv')[0])
    return stock_list

def get_stock_name(code):
    try:
        stock_api = CBaoStock(code=code)
        stock_api.SetBasciInfo()
        return stock_api.name
    except Exception:
        return "N/A"

def display_menu(stock_dict, page_size=10):
    stock_items = list(stock_dict.items())
    page = 0
    while True:
        start_index = page * page_size
        end_index = start_index + page_size
        
        print("\n请选择要分析的股票:")
        for i, (code, name) in enumerate(stock_items[start_index:end_index], start=start_index):
            print(f"{i + 1}: {code} {name}")

        print("\n'n' for next page, 'p' for previous page, 'q' to quit.")
        choice = input(f"请输入选项 (1-{len(stock_items)}) 或命令: ")

        if choice.lower() == 'n':
            if end_index < len(stock_items):
                page += 1
            else:
                print("已经是最后一页。")
        elif choice.lower() == 'p':
            if page > 0:
                page -= 1
            else:
                print("已经是第一页。")
        elif choice.lower() == 'q':
            return None
        elif choice.isdigit() and 1 <= int(choice) <= len(stock_items):
            return stock_items[int(choice) - 1][0]
        else:
            print("无效输入，请重试。")


if __name__ == "__main__":
    # Load config from YAML file
    with open('Config/config.yaml', 'r', encoding='utf-8') as f:
        app_config = yaml.safe_load(f)
   
    with open('Config/template_stock.yaml', 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)

    offline_path = os.path.join(app_config.get('offline_data', {}).get('path', 'data/offline'), 'qfq', 'day')
    
    print("正在扫描本地股票数据...")
    stock_codes = get_stock_list(offline_path)
    
    if not stock_codes:
        print(f"在 {offline_path} 未找到股票数据。")
        exit()

    print("正在获取股票名称...")
    CBaoStock.do_init()
    stock_details = {code: get_stock_name(code) for code in stock_codes}
    CBaoStock.do_close()

    while True:
        selected_code = display_menu(stock_details)

        if not selected_code:
            print("未选择股票，程序退出。")
            break

        # Use selected code and load other configs
        code = selected_code
        end_time = config_data['end_time']
        data_src = config_data['data_src']
        chanconfig = CChanConfig(config_data['chan_config'])
        plot_config = config_data['plot_config']
        plot_para = config_data['plot_para']
        levels_to_process = config_data['levels']

        # Define levels and their corresponding start times
        now = datetime.now()
        level_start_times = {
            'K_MON': None,  # All data
            'K_WEEK': (now - timedelta(days=3 * 365)).strftime('%Y-%m-%d'),  # Last 3 years
            'K_DAY': (now - timedelta(days=365)).strftime('%Y-%m-%d'),  # Last 1 year
            'K_60M': (now - timedelta(days=60)).strftime('%Y-%m-%d'),   # Last 60 days
            'K_30M': (now - timedelta(days=30)).strftime('%Y-%m-%d'),   # Last 30 days
            'K_15M': (now - timedelta(days=15)).strftime('%Y-%m-%d'),   # Last 15 days
            'K_5M': (now - timedelta(days=5)).strftime('%Y-%m-%d'),    # Last 5 days
            'K_1M': (now - timedelta(days=1)).strftime('%Y-%m-%d'),     # Last 1 day
        }

        # 数据获取时间范围（所有级别都读取全部数据进行计算）
        data_begin_time = None  # None表示读取全部可用数据

        for lv_str in levels_to_process:
            lv = KL_TYPE[lv_str]
            plot_begin_time = level_start_times.get(lv_str)  # 每个级别使用不同的绘图时间范围
            
            print(f"\n正在处理 {code} 的 {lv.name} 数据...")

            chan = CChan(
                code=code,
                begin_time=data_begin_time,  # None表示读取全部可用数据
                end_time=end_time,
                data_src=data_src,
                lv_list=[lv],
                config=chanconfig,
                autype=AUTYPE.QFQ,
            )

            # 每次绘图前深拷贝 plot_para，避免跨股票的配置污染
            plot_para_local = copy.deepcopy(plot_para)

            # 设置绘图的时间范围（只影响显示，不影响计算）
            if plot_begin_time is not None:
                # 将plot_begin_time转换为PlotDriver支持的格式 "YYYY/MM/DD"
                plot_begin_date = plot_begin_time.replace('-', '/')
                
                # 确保plot_para中有figure配置
                if "figure" not in plot_para_local:
                    plot_para_local["figure"] = {}
                
                # 更新plot_para中的figure配置，设置绘图开始日期
                plot_para_local["figure"]["x_begin_date"] = plot_begin_date
            else:
                # 如果plot_begin_time为None，则显示所有数据，清除x_begin_date设置
                if "figure" in plot_para_local and "x_begin_date" in plot_para_local["figure"]:
                    del plot_para_local["figure"]["x_begin_date"]

            plot_driver= CPlotDriver(
                chan,
                plot_config=plot_config,
                plot_para=plot_para_local,
            )
            # Maximize the plot window
            mng = plot_driver.figure.canvas.manager
            mng.window.state('zoomed')
            plot_driver.figure.show()

            # 打印统计信息对比
            from Common.CTime import CTime
            if plot_begin_time is not None:
                year, month, day = map(int, plot_begin_time.split('-'))
                plot_begin_time_obj = CTime(year, month, day, 0, 0)
                plot_start_idx = 0
                for i, klc in enumerate(chan[0].lst):
                    if klc.time_begin >= plot_begin_time_obj:
                        plot_start_idx = i
                        break
                
                print(f"\n数据获取时间范围: 全部可用数据")
                print(f"绘图显示时间范围: {plot_begin_time} 到最新")
                print(f"总K线数量: {len(chan[0].lst)}")
                print(f"显示K线数量: {len(chan[0].lst) - plot_start_idx}")
            else:
                print(f"\n数据获取时间范围: 全部可用数据")
                print(f"绘图显示时间范围: 全部数据")
                print(f"总K线数量: {len(chan[0].lst)}")
                print(f"显示K线数量: {len(chan[0].lst)}")
            
            print(f"中枢数量: {len(chan[0].zs_list)}")
            print(f"买卖点数量: {len(chan.get_bsp())}")

            # 仅在日线/周线/月线级别输出最近一个中枢的顶和底价格
            if lv in [KL_TYPE.K_DAY, KL_TYPE.K_WEEK, KL_TYPE.K_MON]:
                if len(chan[0].zs_list) > 0:
                    last_zs = chan[0].zs_list[-1]
                    try:
                        print(f"最近一个中枢（{lv.name.replace('K_', '')}）顶: {last_zs.high:.2f} 底: {last_zs.low:.2f}")
                    except Exception:
                        print(f"最近一个中枢（{lv.name.replace('K_', '')}）顶: {last_zs.high} 底: {last_zs.low}")
                else:
                    # 没有该级别的中枢则不输出
                    pass

        # 查看完一个股票后提示继续选择或退出
        choice = input("输入 'q' 退出，或按回车继续选择其他股票: ")
        if choice.lower() == 'q':
            print("程序退出。")
            break
