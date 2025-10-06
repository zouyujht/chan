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
    # Set begin_time to one year ago for daily data
    now = datetime.now()
    begin_time = (now - timedelta(days=365)).strftime('%Y-%m-%d')

    chan = CChan(
        code=selected_code,
        begin_time=begin_time,
        end_time=None,
        data_src=data_src,
        lv_list=[KL_TYPE.K_DAY],  # Only process daily data for REITs
        config=chanconfig,
        autype=AUTYPE.NONE, # Use non-adjusted data
    )

    plot_driver = CPlotDriver(
        chan,
        plot_config=plot_config,
        plot_para=plot_para,
    )
    # Maximize the plot window
    mng = plot_driver.figure.canvas.manager
    mng.window.state('zoomed')
    plot_driver.figure.show()

    input("Press Enter to exit...")
