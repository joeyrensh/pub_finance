#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from datetime import datetime, timedelta
import re
from base64 import b64encode
from io import BytesIO
from matplotlib_inline.backend_inline import set_matplotlib_formats
import matplotlib.pyplot as plt
import sys
import numpy as np
from matplotlib.patches import Wedge, Circle, Rectangle
from matplotlib.collections import PolyCollection
from matplotlib import colors as mcolors


class ToolKit:
    def __init__(self, val):
        self.val = val
        print("\n" + val + "...")

    """ 执行进度显示 """

    def progress_bar(self, var, i) -> None:
        # 清屏
        sys.stdout.write("\033[2J")
        sys.stdout.flush()

        # 移动光标到顶部
        sys.stdout.write("\033[H")
        sys.stdout.flush()
        print(
            "\r{}: {}%: ".format(self.val, int(i * 100 / var)),
            "▋" * (i * 100 // var),
            end="",
            flush=True,
        )

    """ 判断今日是否美股交易日 """

    @staticmethod
    def is_us_trade_date(trade_date) -> bool:
        """
        当前北京时间 UTC+8
        utc_loc = datetime.now()
        当前美国时间 UTC-4
        """
        # utc_us = datetime.now() - timedelta(hours=12)
        utc_us = datetime.strptime(trade_date, "%Y%m%d")
        """ 
        utc_us = datetime.fromisoformat('2021-01-18 01:00:00')
        美股休市日，https://www.nyse.com/markets/hours-calendars
        marketclosed.config 是2021和2022两年的美股法定休市配置文件 
        """
        f = open("./usstockinfo/marketclosed.config").readlines()
        x = []
        for i in f:
            x.append(i.split(",")[0].strip())
        """ 周末正常休市 """
        if utc_us.isoweekday() in [1, 2, 3, 4, 5]:
            if str(utc_us)[0:10] in x:
                # print("今日非美国交易日")
                return False
            else:
                return True
        else:
            # print("今日非美国交易日")
            return False

    """ 获取美股最新交易日期 """

    @staticmethod
    def get_us_latest_trade_date(offset) -> str | None:
        """
        utc_us = datetime.fromisoformat('2021-01-18 01:00:00')
        美股休市日，https://www.nyse.com/markets/hours-calendars
        marketclosed.config 是2021和2022两年的美股法定休市配置文件
        """
        if offset == 0:
            return str(datetime.now() - timedelta(hours=12))[0:10].replace("-", "")
        f = open("./usstockinfo/marketclosed.config").readlines()
        x = []
        for i in f:
            x.append(i.split(",")[0].strip())
        """ 循环遍历最近一个交易日期 """
        counter = 0
        for h in range(1, 365):
            """当前美国时间 UTC-4"""
            # utc_us = datetime.now() - timedelta(hours=12) - timedelta(days=h)
            utc_us = datetime.now() - timedelta(days=h)
            """ 周末正常休市 """
            if utc_us.isoweekday() in [1, 2, 3, 4, 5]:
                if str(utc_us)[0:10] in x:
                    continue
                else:
                    """返回日期字符串格式20200101"""
                    counter += 1
                    if counter == offset:  # 找到第 offset 个交易日
                        print("trade date: ", str(utc_us)[0:10].replace("-", ""))
                        return str(utc_us)[0:10].replace("-", "")
            else:
                continue

    """ 获取美股两个日期间的交易天数 """

    @staticmethod
    def get_us_trade_off_days(cur, before) -> int | None:
        """
        取两个交易日起之间的交易天数，用来过滤塞选滞胀的股票
        cur, before都需是datetime类型
        """
        f = open("./usstockinfo/marketclosed.config").readlines()
        x = []
        for i in f:
            x.append(i.split(",")[0].strip())
        """ 循环遍历最近一个交易日期 """
        timer = 0
        for h in range(0, 365):
            """周末正常休市"""
            utc_us = cur - timedelta(days=h)
            if utc_us.isoweekday() in [1, 2, 3, 4, 5] and str(utc_us)[0:10] not in x:
                timer = timer + 1
            else:
                continue
            """ 当前美国时间 UTC-4 """
            if utc_us == before:
                return timer

    """ 判断今日是否A股交易日 """

    @staticmethod
    def is_cn_trade_date(trade_date) -> bool:
        # utc_cn = datetime.now()
        utc_cn = datetime.strptime(trade_date, "%Y%m%d")
        """
        utc_cn = datetime.fromisoformat('2021-01-18 01:00:00')
        A股休市日
        marketclosed.config 是2021和2022两年的美股法定休市配置文件 
        """
        f = open("./cnstockinfo/marketclosed.config").readlines()
        x = []
        for i in f:
            x.append(i.split(",")[0].strip())
        """ 周末正常休市 """
        if utc_cn.isoweekday() in [1, 2, 3, 4, 5]:
            if str(utc_cn)[0:10] in x:
                # print("今日非A股交易日")
                return False
            else:
                return True
        else:
            # print("今日非A股交易日")
            return False

    """ 获取A股最新交易日期 """

    @staticmethod
    def get_cn_latest_trade_date(offset) -> str | None:
        if offset == 0:
            return str(datetime.now())[0:10].replace("-", "")
        f = open("./cnstockinfo/marketclosed.config").readlines()
        x = []
        for i in f:
            x.append(i.split(",")[0].strip())
        """ 循环遍历最近一个交易日期 """
        counter = 0
        for h in range(1, 365):
            """当前北京时间 UTC+8"""
            utc_cn = datetime.now() - timedelta(days=h)
            """ 周末正常休市 """
            if utc_cn.isoweekday() in [1, 2, 3, 4, 5]:
                if str(utc_cn)[0:10] in x:
                    continue
                else:
                    """返回日期字符串格式20200101"""
                    counter += 1
                    if counter == offset:  # 找到第 offset 个交易日
                        print("trade date: ", str(utc_cn)[0:10].replace("-", ""))
                        return str(utc_cn)[0:10].replace("-", "")
            else:
                continue

    """ 获取A股两个日期间的交易天数 """

    @staticmethod
    def get_cn_trade_off_days(cur, before) -> int | None:
        """
        取两个交易日起之间的交易天数，用来过滤塞选滞胀的股票
        cur, before都需是datetime类型
        """
        f = open("./cnstockinfo/marketclosed.config").readlines()
        x = []
        for i in f:
            x.append(i.split(",")[0].strip())
        """ 循环遍历最近一个交易日期 """
        timer = 0
        for h in range(0, 365):
            """周末正常休市"""
            utc_cn = cur - timedelta(days=h)
            if utc_cn.isoweekday() in [1, 2, 3, 4, 5] and str(utc_cn)[0:10] not in x:
                timer = timer + 1
            else:
                continue
            """ 当前美国时间 UTC-4 """
            if utc_cn == before:
                return timer

    # area chart按照时间渐变透明度填充
    # @staticmethod
    # def create_line(*args, second_plot_type="area"):
    #     """
    #     增强版绘图函数，支持单列与双列输入。
    #     双轴时左右 Y 轴独立缩放（不再强制统一刻度），但仍保证主轴线不会被右轴覆盖。
    #     对常数序列或极小幅度序列做最小跨度扩展以保证可见性。

    #     Args:
    #         *args: 数据序列，1个参数时绘制单线，2个参数时双轴
    #         second_plot_type: 第二组数据的绘制类型，"area"=面积图，"line"=折线图
    #     """
    #     set_matplotlib_formats("svg")

    #     # 配置参数
    #     config = {
    #         "up_color": "#ff4444",
    #         "down_color": "#00a859",
    #         "line_width": 2,
    #         "secondary_line_width": (
    #             0.5 if second_plot_type == "area" else 1.5
    #         ),  # 根据类型调整线宽
    #         "secondary_line_alpha": (
    #             0.8 if second_plot_type == "area" else 1.0
    #         ),  # 根据类型调整透明度
    #         "gradient_steps": 100,
    #         "fill_alpha_range": (0.06, 0.25),
    #         # alpha_power >1 强调最近端（非线性映射）， =1 为线性
    #         "alpha_power": 1.5,
    #         # "fill_alpha_range": (0.06, 0.25),
    #         "marker_size": 3.5,
    #         "zero_line_alpha": 0,
    #     }

    #     # 解析输入
    #     if len(args) == 1:
    #         # 单列模式
    #         if isinstance(args[0], (list, tuple, np.ndarray)):
    #             data = list(args[0])
    #         else:
    #             data = [args[0]] if args[0] is not None else []
    #         datasets = [data]
    #         is_single_mode = True
    #         if len(data) >= 2:
    #             is_uptrend = data[-1] >= data[-2]
    #         else:
    #             is_uptrend = True
    #         colors = [config["up_color"] if is_uptrend else config["down_color"]]
    #         fill_colors = [colors[0]]
    #     elif len(args) >= 2:
    #         # 双列模式，只取前两列
    #         datasets = []
    #         for arg in args[:2]:
    #             if isinstance(arg, (list, tuple, np.ndarray)):
    #                 datasets.append(list(arg))
    #             else:
    #                 datasets.append([arg])
    #         is_single_mode = False
    #         if len(datasets[0]) >= 2:
    #             is_uptrend = datasets[0][-1] >= datasets[0][-2]
    #         else:
    #             is_uptrend = True
    #         first_color = config["up_color"] if is_uptrend else config["down_color"]
    #         if is_uptrend:
    #             secondary_line_color = "#ff6666"
    #             secondary_fill_color = "#ff4444"
    #         else:
    #             secondary_line_color = "#66b266"
    #             secondary_fill_color = "#00a859"
    #         colors = [first_color, secondary_line_color]
    #         fill_colors = [first_color, secondary_fill_color]
    #     else:
    #         datasets = [[]]
    #         is_single_mode = True
    #         colors = [config["up_color"]]
    #         fill_colors = [config["up_color"]]

    #     # 初始化画布
    #     fig, ax1 = plt.subplots(1, 1, figsize=(2.5, 1), facecolor="none")
    #     fig.patch.set_alpha(0.0)

    #     if not is_single_mode and len(datasets) >= 2:
    #         ax2 = ax1.twinx()

    #         # 主轴置顶并隐藏背景，防止右轴遮挡左轴线
    #         ax1.set_zorder(ax2.get_zorder() + 1)
    #         ax1.patch.set_visible(False)
    #         try:
    #             ax2.patch.set_alpha(0.0)
    #         except Exception:
    #             ax2.patch.set_visible(False)
    #         # 让右轴绘图位于更低层
    #         ax2.set_zorder(ax1.get_zorder() - 1)
    #         axes = [ax1, ax2]
    #     else:
    #         axes = [ax1]

    #     # 截断到相等长度（双列）
    #     if not is_single_mode and len(datasets) >= 2:
    #         min_length = min(len(datasets[0]), len(datasets[1]))
    #         datasets[0] = datasets[0][:min_length]
    #         datasets[1] = datasets[1][:min_length]

    #     # 清理并计算范围
    #     def clean_and_convert(arr):
    #         clean = []
    #         for x in arr:
    #             if isinstance(x, (int, float, np.number)):
    #                 clean.append(float(x))
    #             else:
    #                 try:
    #                     clean.append(float(x))
    #                 except (ValueError, TypeError):
    #                     clean.append(0.0)
    #         return clean

    #     clean_datasets = [clean_and_convert(ds) for ds in datasets]

    #     # 计算每组数据的 dmin/dmax，并为常数或极小跨度序列扩展最小跨度
    #     data_ranges = []
    #     for cd in clean_datasets:
    #         if cd:
    #             dmin, dmax = min(cd), max(cd)
    #             span = dmax - dmin
    #             # 最小视觉跨度：相对值(2%)或绝对最小值 1e-3
    #             rel_min_span = max((abs(dmax) + abs(dmin)) * 0.02, 1e-3)
    #             if span < rel_min_span:
    #                 mid = (dmax + dmin) / 2.0
    #                 dmin = mid - rel_min_span / 2.0
    #                 dmax = mid + rel_min_span / 2.0
    #             data_ranges.append((dmin, dmax))
    #         else:
    #             data_ranges.append((-1, 1))

    #     # 为左右轴分别设置独立 ylim（不再尝试强制对齐）
    #     if not is_single_mode and len(clean_datasets) >= 2:
    #         # 左轴（主轴）范围
    #         min1, max1 = data_ranges[0]
    #         span1 = max1 - min1 if (max1 - min1) > 0 else 1.0
    #         margin1 = max(span1 * 0.15, (abs(max1) + abs(min1)) * 0.02, 1e-3)
    #         ax1.set_ylim(min1 - margin1, max1 + margin1)

    #         # 右轴（第二组数据）范围
    #         min2, max2 = data_ranges[1]
    #         span2 = max2 - min2 if (max2 - min2) > 0 else 1.0
    #         margin2 = max(span2 * 0.12, (abs(max2) + abs(min2)) * 0.02, 1e-3)
    #         ax2.set_ylim(min2 - margin2, max2 + margin2)
    #     elif is_single_mode and clean_datasets[0]:
    #         min_val, max_val = data_ranges[0]
    #         range_val = max_val - min_val if (max_val - min_val) > 0 else 1.0
    #         margin = max(range_val * 0.15, (abs(max_val) + abs(min_val)) * 0.02, 1e-3)
    #         ax1.set_ylim(min_val - margin, max_val + margin)

    #     # 设置边距
    #     for ax in axes:
    #         ax.margins(x=0.02, y=0.0)

    #     # 绘制数据
    #     for i, (dataset, color) in enumerate(zip(clean_datasets, colors)):
    #         if not dataset:
    #             continue

    #         # 选择轴与样式
    #         if is_single_mode or i == 0:
    #             ax = ax1
    #             line_width = config["line_width"]
    #             line_alpha = 1.0
    #             fill_color = color
    #         else:
    #             ax = ax2
    #             line_width = config["secondary_line_width"]
    #             line_alpha = config["secondary_line_alpha"]
    #             fill_color = fill_colors[i] if i < len(fill_colors) else fill_colors[-1]

    #         x = np.arange(len(dataset))
    #         y = np.array(dataset)

    #         # 绘制折线（主轴 zorder 已确保在上层）
    #         ax.plot(x, y, color=color, lw=line_width, alpha=line_alpha, zorder=3)

    #         # 第二组数据的特殊处理
    #         if not is_single_mode and i == 1 and len(dataset) > 0:
    #             if second_plot_type == "area":
    #                 # 面积图模式：填充到0
    #                 alphas = np.linspace(
    #                     config["fill_alpha_range"][0],
    #                     config["fill_alpha_range"][1],
    #                     len(dataset),
    #                 )
    #                 for j in range(len(dataset) - 1):
    #                     x_segment = x[j : j + 2]
    #                     y_segment = y[j : j + 2]
    #                     local_alpha = float(np.mean(alphas[j : j + 2]))
    #                     ax.fill_between(
    #                         x_segment,
    #                         y_segment,
    #                         0,
    #                         color=fill_color,
    #                         alpha=local_alpha,
    #                         edgecolor="none",
    #                         zorder=2,
    #                     )
    #                 # 非线性 alpha 映射：对时间序列末端（最近）给予更高透明度（更显眼）
    #                 a_min, a_max = config.get("fill_alpha_range", (0.06, 0.25))
    #                 power = float(config.get("alpha_power", 1.0))
    #                 steps = len(dataset)
    #                 # 生成 0..1 基准并做幂映射（power>1 强调末端）
    #                 base = np.linspace(0.0, 1.0, steps)
    #                 mapped = np.power(base, power)
    #                 alphas = a_min + (a_max - a_min) * mapped
    #                 # 填充分段渲染
    #                 for j in range(len(dataset) - 1):
    #                     x_segment = x[j : j + 2]
    #                     y_segment = y[j : j + 2]
    #                     local_alpha = float(np.mean(alphas[j : j + 2]))
    #                     # cap alpha 防止过于不透明
    #                     local_alpha = min(max(local_alpha, 0.0), 0.6)
    #                     ax.fill_between(
    #                         x_segment,
    #                         y_segment,
    #                         0,
    #                         color=fill_color,
    #                         alpha=local_alpha,
    #                         edgecolor="none",
    #                         zorder=2,
    #                     )

    #             elif second_plot_type == "line":
    #                 # 折线图模式：不填充，但可以添加端点标记
    #                 ax.scatter(
    #                     len(dataset) - 1,
    #                     dataset[-1],
    #                     color=color,
    #                     s=config["marker_size"] ** 2,
    #                     edgecolor="white",
    #                     linewidth=0.4,
    #                     zorder=4,
    #                 )

    #         # 端点强化（第一组或单列显示，如果是第二组且为line模式已在上面处理）
    #         if (i == 0 or is_single_mode) and not (
    #             i == 1 and second_plot_type == "line"
    #         ):
    #             ax.scatter(
    #                 len(dataset) - 1,
    #                 dataset[-1],
    #                 color=color,
    #                 s=config["marker_size"] ** 2,
    #                 edgecolor="white" if (is_single_mode or i == 0) else "#2a2e39",
    #                 linewidth=0.4,
    #                 zorder=4,
    #             )

    #     # 左轴零线（仅装饰，左右轴独立刻度）
    #     zero_color = colors[0] if colors else config["up_color"]
    #     ax1.axhline(
    #         0, color=zero_color, alpha=config["zero_line_alpha"], lw=0.8, zorder=1
    #     )

    #     # 隐藏坐标轴（仅保留图形）
    #     ax1.axis("off")
    #     if not is_single_mode and len(datasets) >= 2:
    #         ax2.axis("off")

    #     # 统一X轴范围（x 轴共享）
    #     if not is_single_mode and len(clean_datasets) >= 2:
    #         xlen = max(1, len(clean_datasets[0]) - 1)
    #         ax1.set_xlim(0, xlen)
    #         ax2.set_xlim(0, xlen)
    #     else:
    #         xlen = max(1, len(clean_datasets[0]) - 1)
    #         ax1.set_xlim(0, xlen)

    #     # 布局与导出
    #     plt.tight_layout(pad=0)
    #     plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    #     img = BytesIO()
    #     fig.savefig(
    #         img,
    #         format="png",
    #         dpi=150,
    #         transparent=True,
    #         bbox_inches="tight",
    #         pad_inches=0,
    #         facecolor="none",
    #     )
    #     plt.close(fig)

    #     return (
    #         f'<img src="data:image/png;base64,{b64encode(img.getvalue()).decode()}" />'
    #     )

    # area chart按照y轴渐变透明度填充
    @staticmethod
    def create_line(*args, second_plot_type="area"):
        """
        高效绘图函数，支持单列和双列数据。
        双轴独立缩放，area chart 渐变沿 Y 轴透明度变化。
        """
        # 配置
        config = {
            "up_color": "#ff4444",
            "down_color": "#00a859",
            "line_width": 2,
            "secondary_line_width": 0.5 if second_plot_type == "area" else 1.5,
            "secondary_line_alpha": 0.8 if second_plot_type == "area" else 1.0,
            "fill_alpha_range": (0.07, 0.34),
            "alpha_power": 1.2,
            "marker_size": 3.5,
            "zero_line_alpha": 0,
        }

        # 数据解析
        if len(args) == 1:
            datasets = [
                (
                    list(args[0])
                    if isinstance(args[0], (list, tuple, np.ndarray))
                    else [args[0]]
                )
            ]
            is_single_mode = True
            is_uptrend = (
                datasets[0][-1] >= datasets[0][-2] if len(datasets[0]) >= 2 else True
            )
            colors = [config["up_color"] if is_uptrend else config["down_color"]]
            fill_colors = [colors[0]]
        elif len(args) >= 2:
            datasets = []
            for arg in args[:2]:
                datasets.append(
                    list(arg) if isinstance(arg, (list, tuple, np.ndarray)) else [arg]
                )
            is_single_mode = False
            is_uptrend = (
                datasets[0][-1] >= datasets[0][-2] if len(datasets[0]) >= 2 else True
            )
            colors = [config["up_color"] if is_uptrend else config["down_color"]]
            fill_colors = [
                "#ff4444" if is_uptrend else "#00a859",
                "#ff6666" if is_uptrend else "#66b266",
            ]
        else:
            datasets = [[]]
            is_single_mode = True
            colors = [config["up_color"]]
            fill_colors = [config["up_color"]]

        # 初始化画布
        fig, ax1 = plt.subplots(1, 1, figsize=(2.5, 1), facecolor="none")
        fig.patch.set_alpha(0.0)

        axes = [ax1]
        if not is_single_mode and len(datasets) >= 2:
            ax2 = ax1.twinx()
            ax1.set_zorder(ax2.get_zorder() + 1)
            ax1.patch.set_visible(False)
            try:
                ax2.patch.set_alpha(0.0)
            except:
                ax2.patch.set_visible(False)
            ax2.set_zorder(ax1.get_zorder() - 1)
            axes.append(ax2)

        # 截断到相同长度
        if not is_single_mode and len(datasets) >= 2:
            min_len = min(len(datasets[0]), len(datasets[1]))
            datasets[0] = datasets[0][:min_len]
            datasets[1] = datasets[1][:min_len]

        # 清理数据
        def clean_and_convert(arr):
            clean = []
            for x in arr:
                try:
                    clean.append(float(x))
                except:
                    clean.append(0.0)
            return clean

        datasets = [clean_and_convert(ds) for ds in datasets]

        # 计算范围并最小跨度扩展
        data_ranges = []
        for ds in datasets:
            if ds:
                dmin, dmax = min(ds), max(ds)
                span = dmax - dmin
                rel_min_span = max((abs(dmax) + abs(dmin)) * 0.02, 1e-3)
                if span < rel_min_span:
                    mid = (dmax + dmin) / 2.0
                    dmin = mid - rel_min_span / 2
                    dmax = mid + rel_min_span / 2
                data_ranges.append((dmin, dmax))
            else:
                data_ranges.append((-1, 1))

        # 设置独立Y轴
        if not is_single_mode and len(datasets) >= 2:
            for ax, (dmin, dmax), i in zip([ax1, ax2], data_ranges, [0, 1]):
                span = max(dmax - dmin, 1.0)
                margin = max(span * 0.12, (abs(dmin) + abs(dmax)) * 0.02, 1e-3)
                ax.set_ylim(dmin - margin, dmax + margin)
        else:
            dmin, dmax = data_ranges[0]
            span = max(dmax - dmin, 1.0)
            margin = max(span * 0.15, (abs(dmin) + abs(dmax)) * 0.02, 1e-3)
            ax1.set_ylim(dmin - margin, dmax + margin)

        # 绘制数据
        for i, ds in enumerate(datasets):
            if not ds:
                continue
            ax = ax1 if i == 0 or is_single_mode else ax2
            color = colors[i] if i < len(colors) else colors[0]
            fill_color = fill_colors[i] if i < len(fill_colors) else fill_colors[-1]
            x = np.arange(len(ds))
            y = np.array(ds)

            # 绘制折线
            lw = (
                config["line_width"]
                if (i == 0 or is_single_mode)
                else config["secondary_line_width"]
            )
            alpha = (
                1.0 if (i == 0 or is_single_mode) else config["secondary_line_alpha"]
            )
            ax.plot(x, y, color=color, lw=lw, alpha=alpha, zorder=3)

            # 第二组 area chart（高效渐变沿 Y 轴）
            if i == 1 and not is_single_mode and second_plot_type == "area":
                n_steps = 28  # 分段数，性能和视觉平衡
                y_base = 0
                a_min, a_max = config["fill_alpha_range"]
                power = config["alpha_power"]

                verts = []
                colors_poly = []
                for j in range(len(y) - 1):
                    x_seg = x[j : j + 2]
                    y_seg = y[j : j + 2]
                    for k in range(1, n_steps + 1):
                        y_bottom = y_seg * (k - 1) / n_steps
                        y_top = y_seg * k / n_steps
                        verts.append(
                            [
                                (x_seg[0], y_bottom[0]),
                                (x_seg[1], y_bottom[1]),
                                (x_seg[1], y_top[1]),
                                (x_seg[0], y_top[0]),
                            ]
                        )
                        alpha_poly = a_min + (a_max - a_min) * ((k / n_steps) ** power)
                        alpha_poly = np.clip(alpha_poly, 0.0, 0.6)
                        colors_poly.append((*mcolors.to_rgb(fill_color), alpha_poly))

                from matplotlib.collections import PolyCollection

                poly = PolyCollection(
                    verts, facecolors=colors_poly, edgecolors="none", zorder=2
                )
                ax.add_collection(poly)

            # 端点强化
            ax.scatter(
                len(y) - 1,
                y[-1],
                color=color,
                s=config["marker_size"] ** 2,
                edgecolor="white" if (i == 0 or is_single_mode) else "#2a2e39",
                linewidth=0.4,
                zorder=4,
            )

        # 零线
        ax1.axhline(
            0,
            color=colors[0] if colors else config["up_color"],
            alpha=config["zero_line_alpha"],
            lw=0.8,
            zorder=1,
        )
        for ax in axes:
            ax.axis("off")
        plt.tight_layout(pad=0)
        plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

        # 导出为 base64 图片
        img = BytesIO()
        fig.savefig(
            img,
            format="png",
            dpi=150,
            transparent=True,
            bbox_inches="tight",
            pad_inches=0,
        )
        plt.close(fig)
        return (
            f'<img src="data:image/png;base64,{b64encode(img.getvalue()).decode()}" />'
        )
