from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Plot.PlotDriver import CPlotDriver
from Common.CTime import CTime

if __name__ == "__main__":
    code = "sz.000001"
    
    # 数据获取：使用更早的开始时间确保完整的缠论计算
    data_begin_time = "2015-01-01"  # 用于数据获取的开始时间（更早）
    plot_begin_time = "2024-06-01"  # 用于绘图显示的开始时间
    end_time = None
    data_src = DATA_SRC.BAO_STOCK
    lv_list = [KL_TYPE.K_DAY]

    config = CChanConfig({
        "bi_strict": True,
        "trigger_step": False,
        "skip_step": 0,
        "divergence_rate": float("inf"),
        "bsp2_follow_1": False,
        "bsp3_follow_1": False,
        "min_zs_cnt": 1,  # 你的原始设置
        "bs1_peak": False,
        "macd_algo": "peak",
        "bs_type": '1,2,3a,1p,2s,3b',
        "print_warning": True,
        "zs_algo": "normal",
    })

    plot_config = {
        "plot_kline": True,
        "plot_kline_combine": True,
        "plot_bi": True,
        "plot_seg": True,
        "plot_eigen": False,
        "plot_zs": True,
        "plot_macd": False,
        "plot_mean": False,
        "plot_channel": False,
        "plot_bsp": True,
        "plot_extrainfo": False,
        "plot_demark": False,
        "plot_marker": False,
        "plot_rsi": False,
        "plot_kdj": False,
    }

    plot_para = {
        "seg": {
            # "plot_trendline": True,
        },
        "bi": {
            # "plot_trendline": True,
        },
        "figure": {
            # 移除x_range设置，改用x_begin_date
            # "x_range": 200,
        },
        "marker": {
            # "marker_type": "circle",
            # "marker_size": 10,
        }
    }

    # 使用更早的开始时间获取数据，确保缠论计算的完整性
    chan = CChan(
        code=code,
        begin_time=data_begin_time,  # 数据获取用更早的时间
        end_time=end_time,
        data_src=data_src,
        lv_list=lv_list,
        config=config,
        autype=AUTYPE.QFQ,
    )

    if not config.trigger_step:
        plot_driver = CPlotDriver(
            chan,
            plot_config=plot_config,
            plot_para=plot_para,
        )
        
        # 设置绘图的时间范围（只影响显示，不影响计算）
        # 将plot_begin_time转换为PlotDriver支持的格式 "YYYY/MM/DD"
        plot_begin_date = plot_begin_time.replace('-', '/')
        
        # 更新plot_para中的figure配置，设置绘图开始日期
        plot_para["figure"]["x_begin_date"] = plot_begin_date
        
        # 重新创建PlotDriver以应用新的配置
        plot_driver = CPlotDriver(
            chan,
            plot_config=plot_config,
            plot_para=plot_para,
        )
        
        plot_driver.figure.show()
        plot_driver.save2img("./test_with_plot_range.png")
        
        # 计算显示的K线数量用于统计
        year, month, day = map(int, plot_begin_time.split('-'))
        plot_begin_time_obj = CTime(year, month, day, 0, 0)
        plot_start_idx = 0
        for i, klc in enumerate(chan[0].lst):
            if klc.time_begin >= plot_begin_time_obj:
                plot_start_idx = i
                break
        
        # 打印统计信息对比
        print(f"数据获取时间范围: {data_begin_time} 到 {end_time or '最新'}")
        print(f"绘图显示时间范围: {plot_begin_time} 到 {end_time or '最新'}")
        print(f"总K线数量: {len(chan[0].lst)}")
        print(f"显示K线数量: {len(chan[0].lst) - plot_start_idx}")
        print(f"中枢数量: {len(chan[0].zs_list)}")
        print(f"买卖点数量: {len(chan.get_bsp())}")
    else:
        from Plot.AnimatePlotDriver import CAnimateDriver
        CAnimateDriver(
            chan,
            plot_config=plot_config,
            plot_para=plot_para,
        )