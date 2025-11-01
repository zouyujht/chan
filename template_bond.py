import os
import yaml
import copy
import akshare as ak
from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, KL_TYPE
from Plot.PlotDriver import CPlotDriver
from datetime import datetime, timedelta


def get_bond_list(path):
    """
    扫描目录，获取本地已下载的国债代码列表（按文件名判断）。
    仅识别 6 位纯数字且以 '01' 开头的代码（如 019547）。
    """
    bond_list = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.csv'):
                code = file.split('.csv')[0]
                if code.isdigit() and len(code) == 6 and code.startswith('01'):
                    bond_list.append(code)
    return bond_list


def get_bond_name_map():
    """
    通过 Akshare 获取沪深债券现货列表，生成 代码→名称 的映射。
    若接口失败，返回空字典。
    """
    try:
        # Akshare 债券现货接口（含名称和代码），不同版本列名可能略有不同
        df = ak.bond_zh_hs_spot()
        code_col = None
        name_col = None
        # 兼容常见列名
        for col in df.columns:
            if str(col).strip() in ['代码', 'bond_code', 'code']:
                code_col = col
            if str(col).strip() in ['名称', 'bond_name', 'name']:
                name_col = col
        if code_col is None or name_col is None:
            return {}
        return dict(zip(df[code_col].astype(str), df[name_col].astype(str)))
    except Exception as e:
        print(f"从 Akshare 获取债券名称失败: {e}")
        return {}


def display_menu(items_dict, page_size=10):
    """
    分页显示代码与名称并让用户选择。
    返回用户选择的代码，或 None 表示退出。
    """
    items = list(items_dict.items())
    if not items:
        print("未找到可用的本地债券数据。")
        return None
    page = 0
    while True:
        start_index = page * page_size
        end_index = start_index + page_size

        print("\n请选择要分析的债券（国债）：")
        for i, (code, name) in enumerate(items[start_index:end_index], start=start_index):
            print(f"{i + 1}: {code} {name}")

        print("\n'n' 下一页, 'p' 上一页, 'q' 退出。")
        choice = input(f"请输入序号 (1-{len(items)}) 或命令: ")

        if choice.lower() == 'n':
            if end_index < len(items):
                page += 1
            else:
                print("已是最后一页。")
        elif choice.lower() == 'p':
            if page > 0:
                page -= 1
            else:
                print("已是第一页。")
        elif choice.lower() == 'q':
            return None
        elif choice.isdigit() and 1 <= int(choice) <= len(items):
            return items[int(choice) - 1][0]
        else:
            print("输入无效，请重试。")


if __name__ == "__main__":
    # 读取全局与模板配置
    with open('Config/config.yaml', 'r', encoding='utf-8') as f:
        app_config = yaml.safe_load(f)
    with open('Config/template_bond.yaml', 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)

    # 债券数据按日线、无复权存储在 none/day 目录
    offline_path = os.path.join(app_config.get('offline_data', {}).get('path', 'data/offline'), 'none', 'day')

    print("正在扫描本地债券（国债）数据...")
    local_bond_codes = get_bond_list(offline_path)
    if not local_bond_codes:
        print(f"在 {offline_path} 下未找到债券数据。")
        exit()

    print("正在获取债券名称映射...")
    name_map = get_bond_name_map()
    local_bond_details = {code: name_map.get(code, 'N/A') for code in local_bond_codes}

    while True:
        selected_code = display_menu(local_bond_details)
        if not selected_code:
            print("未选择债券，程序退出。")
            break

        # 加载模板配置
        data_src = config_data['data_src']
        chanconfig = CChanConfig(config_data['chan_config'])
        plot_config = config_data['plot_config']
        plot_para = config_data['plot_para']

        print(f"\n处理债券（日线）: {selected_code} ...")
        now = datetime.now()
        # 数据获取范围尽量更长，保证缠论结构完整（3 年）
        data_begin_time = (now - timedelta(days=3 * 365)).strftime('%Y-%m-%d')
        # 绘图显示范围默认近一年
        plot_begin_time = (now - timedelta(days=365)).strftime('%Y-%m-%d')

        chan = CChan(
            code=selected_code,
            begin_time=data_begin_time,
            end_time=None,
            data_src=data_src,
            lv_list=[KL_TYPE.K_DAY],
            config=chanconfig,
            autype=AUTYPE.NONE,
        )

        # 设置绘图起始日期（只影响显示，不影响计算）
        plot_begin_date = plot_begin_time.replace('-', '/')
        plot_para_local = copy.deepcopy(plot_para)
        if "figure" not in plot_para_local:
            plot_para_local["figure"] = {}
        plot_para_local["figure"]["x_begin_date"] = plot_begin_date

        plot_driver = CPlotDriver(
            chan,
            plot_config=plot_config,
            plot_para=plot_para_local,
        )
        # 最大化绘图窗口
        mng = plot_driver.figure.canvas.manager
        mng.window.state('zoomed')
        plot_driver.figure.show()

        # 额外绘制：全历史日线缠论分析（不绘制K线与Demark，其余保持一致）
        plot_para_full = copy.deepcopy(plot_para)
        # 移除绘图开始日期，显示全部数据
        if "figure" in plot_para_full and "x_begin_date" in plot_para_full["figure"]:
            del plot_para_full["figure"]["x_begin_date"]

        # 在原绘图配置基础上禁用K线与Demark
        plot_config_full = copy.deepcopy(plot_config)
        # 兼容未添加前缀的情况
        for key in ("plot_kline", "kline"):
            plot_config_full[key] = False
        for key in ("plot_demark", "demark"):
            plot_config_full[key] = False

        plot_driver_full = CPlotDriver(
            chan,
            plot_config=plot_config_full,
            plot_para=plot_para_full,
        )
        mng_full = plot_driver_full.figure.canvas.manager
        mng_full.window.state('zoomed')
        plot_driver_full.figure.show()

        # 打印基本统计信息
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

        # 输出最近一个中枢的顶和底价格（仅日线）
        if len(chan[0].zs_list) > 0:
            last_zs = chan[0].zs_list[-1]
            try:
                print(f"最近一个中枢（日线）顶: {last_zs.high:.2f} 底: {last_zs.low:.2f}")
            except Exception:
                # 兜底处理，确保即使价格不是数值类型也能打印
                print(f"最近一个中枢（日线）顶: {last_zs.high} 底: {last_zs.low}")
        else:
            print("暂无日线中枢数据，未输出顶/底价格。")

        choice = input("输入 'q' 退出，或按回车继续选择其他债券: ")
        if choice.lower() == 'q':
            print("程序退出。")
            break