import os
import yaml
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

    selected_code = display_menu(stock_details)

    if not selected_code:
        print("未选择股票，程序退出。")
        exit()

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

    for lv_str in levels_to_process:
        lv = KL_TYPE[lv_str]
        begin_time = level_start_times.get(lv_str)
        
        print(f"\n正在处理 {code} 的 {lv.name} 数据...")
        chan = CChan(
            code=code,
            begin_time=begin_time,
            end_time=end_time,
            data_src=data_src,
            lv_list=[lv],
            config=chanconfig,
            autype=AUTYPE.QFQ,
        )

        plot_driver= CPlotDriver(
            chan,
            plot_config=plot_config,
            plot_para=plot_para,
        )
        # Maximize the plot window
        mng = plot_driver.figure.canvas.manager
        mng.window.state('zoomed')
        plot_driver.figure.show()

    input("按回车键退出...")
