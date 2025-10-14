import os
import yaml
import akshare as ak
from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, KL_TYPE
from Plot.PlotDriver import CPlotDriver
from datetime import datetime, timedelta

def get_reits_list(path):
    """
    Scans the directory for REITs data files.
    REITs codes are pure numbers.
    """
    reits_list = []
    for root, dirs, files in os.walk(path):
        for file in files:
            # Check if the filename (without extension) is a pure number
            if file.endswith(".csv") and file.split('.csv')[0].isdigit():
                reits_list.append(file.split('.csv')[0])
    return reits_list

def get_reits_name_map():
    """
    Fetches REITs information from akshare and returnsa code-to-name mapping.
    """
    try:
        reits_info_df = ak.reits_realtime_em()
        # Create a dictionary mapping '代码' to '名称'
        return dict(zip(reits_info_df['代码'], reits_info_df['名称']))
    except Exception as e:
        print(f"Error fetching REITs info from akshare: {e}")
        return {}

def display_menu(reits_dict, page_size=10):
    """
    Displays a paginated menu of REITs for user selection.
    """
    reits_items = list(reits_dict.items())
    if not reits_items:
        print("No local REITs data found to display.")
        return None    
    page = 0
    while True:
        start_index = page * page_size
        end_index = start_index + page_size
        
        print("\nPlease select the REIT to analyze:")
        for i, (code, name) in enumerate(reits_items[start_index:end_index], start=start_index):
            print(f"{i + 1}: {code} {name}")

        print("\n'n' for next page, 'p' for previous page, 'q' to quit.")
        choice = input(f"Enter option (1-{len(reits_items)}) or command: ")

        if choice.lower() == 'n':
            if end_index < len(reits_items):
                page += 1
            else:
                print("Already on the last page.")
        elif choice.lower() == 'p':
            if page > 0:
                page -= 1
            else:
                print("Already on the first page.")
        elif choice.lower() == 'q':
            return None
        elif choice.isdigit() and 1 <= int(choice) <= len(reits_items):
            return reits_items[int(choice) - 1][0]
        else:
            print("Invalid input, please try again.")


if __name__ == "__main__":
    # Load config from YAML files
    with open('Config/config.yaml', 'r', encoding='utf-8') as f:
        app_config =yaml.safe_load(f)
   
    with open('Config/template_reits.yaml', 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)

    # REITs data is daily and non-adjusted, stored in the 'none' folder
    offline_path = os.path.join(app_config.get('offline_data', {}).get('path', 'data/offline'), 'none', 'day')
    
    print("Scanning for local REITs data...")
    local_reits_codes = get_reits_list(offline_path)
    if not local_reits_codes:
        print(f"No REITs data found in {offline_path}.")
        exit()

    print("Fetching REITs names from akshare...")
    name_map = get_reits_name_map()
    
    # Filter the name map to only include REITs we have locally
    local_reits_details = {code: name_map.get(code, "N/A") for code in local_reits_codes}

    selected_code = display_menu(local_reits_details)

    if not selected_code:
        print("No REIT selected. Exiting.")
        exit()

    # Load configurations for the selected REIT
    data_src = config_data['data_src']
    chanconfig = CChanConfig(config_data['chan_config'])
    plot_config = config_data['plot_config']
    plot_para = config_data['plot_para']

    print(f"\nProcessing daily data for REIT: {selected_code}...")
    # 数据获取和绘图范围分离
    now = datetime.now()
    # 数据获取：使用更早的开始时间确保完整的缠论计算（3年数据）
    data_begin_time = (now - timedelta(days=3*365)).strftime('%Y-%m-%d')
    # 绘图显示：使用一年前的时间（用户关心的时间段）
    plot_begin_time = (now - timedelta(days=365)).strftime('%Y-%m-%d')

    chan = CChan(
        code=selected_code,
        begin_time=data_begin_time,  # 数据获取用更早的时间
        end_time=None,
        data_src=data_src,
        lv_list=[KL_TYPE.K_DAY],  # Only process daily data for REITs
        config=chanconfig,
        autype=AUTYPE.NONE, # Use non-adjusted data
    )

    # 设置绘图的时间范围（只影响显示，不影响计算）
    # 将plot_begin_time转换为PlotDriver支持的格式 "YYYY/MM/DD"
    plot_begin_date = plot_begin_time.replace('-', '/')
    
    # 确保plot_para中有figure配置
    if "figure" not in plot_para:
        plot_para["figure"] = {}
    
    # 更新plot_para中的figure配置，设置绘图开始日期
    plot_para["figure"]["x_begin_date"] = plot_begin_date

    plot_driver = CPlotDriver(
        chan,
        plot_config=plot_config,
        plot_para=plot_para,
    )
    # Maximize the plot window
    mng = plot_driver.figure.canvas.manager
    mng.window.state('zoomed')
    plot_driver.figure.show()

    # 打印统计信息对比
    from Common.CTime import CTime
    year, month, day = map(int, plot_begin_time.split('-'))
    plot_begin_time_obj = CTime(year, month, day, 0, 0)
    plot_start_idx = 0
    for i, klc in enumerate(chan[0].lst):
        if klc.time_begin >= plot_begin_time_obj:
            plot_start_idx = i
            break
    
    print(f"\n数据获取时间范围: {data_begin_time} 到最新")
    print(f"绘图显示时间范围: {plot_begin_time} 到最新")
    print(f"总K线数量: {len(chan[0].lst)}")
    print(f"显示K线数量: {len(chan[0].lst) - plot_start_idx}")
    print(f"中枢数量: {len(chan[0].zs_list)}")
    print(f"买卖点数量: {len(chan.get_bsp())}")

    input("Press Enter to exit...")
