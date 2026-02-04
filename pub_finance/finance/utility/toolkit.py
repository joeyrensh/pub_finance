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
import pandas as pd
import pathlib
import hashlib
import os


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
        # ===============================
        # 配置
        # ===============================
        config = {
            "up_color": "#ff4444",
            "down_color": "#00a859",
            "line_width": 2,
            "secondary_line_width": 0.6,
            "secondary_line_alpha": 0.9,
            # "fill_alpha_range": (0.12, 0.55),
            # "alpha_power": 2,  # 控制顶部更深
            "fill_alpha_range": (0.02, 0.65),
            "alpha_power": 2.0,
            "marker_size": 3.5,
            "zero_line_alpha": 0,
        }

        # ===============================
        # 数据解析
        # ===============================
        if len(args) == 1:
            datasets = [list(args[0])]
            is_single = True
        else:
            datasets = [list(args[0]), list(args[1])]
            is_single = False

        def clean(arr):
            out = []
            for v in arr:
                try:
                    out.append(float(v))
                except Exception:
                    out.append(0.0)
            return out

        datasets = [clean(d) for d in datasets]
        if not is_single:
            min_len = min(len(datasets[0]), len(datasets[1]))
            datasets = [d[:min_len] for d in datasets]

        is_up = datasets[0][-1] >= datasets[0][-2] if len(datasets[0]) > 1 else True
        main_color = config["up_color"] if is_up else config["down_color"]
        sec_color = "#ff4444" if is_up else "#00a859"

        # ===============================
        # 画布
        # ===============================
        fig, ax1 = plt.subplots(figsize=(2.5, 1), facecolor="none")
        fig.patch.set_alpha(0)

        if not is_single:
            ax2 = ax1.twinx()
            ax1.set_zorder(ax2.get_zorder() + 1)
            ax1.patch.set_visible(False)
            ax2.patch.set_alpha(0)
            ax2.set_zorder(ax1.get_zorder() - 1)

        # ===============================
        # Y 轴范围
        # ===============================
        def set_ylim(ax, y):
            y = np.asarray(y)
            ymin, ymax = np.min(y), np.max(y)
            span = max(ymax - ymin, 1e-3)
            pad = span * 0.15
            ax.set_ylim(ymin - pad, ymax + pad)

        set_ylim(ax1, datasets[0])
        if not is_single:
            set_ylim(ax2, datasets[1])

        # ===============================
        # 主线
        # ===============================
        x = np.arange(len(datasets[0]))
        ax1.plot(x, datasets[0], color=main_color, lw=config["line_width"], zorder=4)
        ax1.scatter(
            x[-1],
            datasets[0][-1],
            s=config["marker_size"] ** 2,
            color=main_color,
            edgecolor=main_color,
            linewidth=0.4,
            zorder=5,
        )

        # ===============================
        # 第二组：area（纵向渐变）
        # ===============================
        if not is_single and second_plot_type == "area":
            ax = ax2
            y = np.asarray(datasets[1])
            ymin, ymax = ax.get_ylim()

            # ---- RGBA 渐变纵向 ----
            grad_steps = 256
            a_min, a_max = config["fill_alpha_range"]
            power = config["alpha_power"]

            t = np.linspace(0, 1, grad_steps)  # 0=底部, 1=顶部
            alpha = a_min + (a_max - a_min) * (t**power)

            r, g, b = mcolors.to_rgb(sec_color)
            grad = np.zeros((grad_steps, 1, 4))
            grad[..., 0] = r
            grad[..., 1] = g
            grad[..., 2] = b
            grad[..., 3] = alpha[:, None]

            img = ax.imshow(
                grad,
                # extent=[x[0], x[-1], 0, ymax],
                extent=[x[0], x[-1], 0, np.max(y)],
                origin="lower",
                aspect="auto",
                zorder=2,
            )

            # ---- 用 area 路径裁剪 ----
            area = ax.fill_between(
                x, y, 0, facecolor="none", edgecolor="none", zorder=2
            )
            img.set_clip_path(area.get_paths()[0], transform=ax.transData)

            # 轮廓线
            ax.plot(
                x,
                y,
                color=sec_color,
                lw=config["secondary_line_width"],
                alpha=config["secondary_line_alpha"],
                zorder=3,
            )

        # ===============================
        # 修饰
        # ===============================
        ax1.axis("off")
        if not is_single:
            ax2.axis("off")

        ax1.set_xlim(x[0], x[-1])
        if not is_single:
            ax2.set_xlim(x[0], x[-1])

        plt.tight_layout(pad=0)
        plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

        # ===============================
        # 导出
        # ===============================
        buf = BytesIO()
        fig.savefig(
            buf,
            format="png",
            dpi=120,
            transparent=True,
            bbox_inches="tight",
            pad_inches=0,
        )
        plt.close(fig)

        return (
            f'<img src="data:image/png;base64,{b64encode(buf.getvalue()).decode()}" />'
        )

    @staticmethod
    def score_and_select_symbols(
        df: pd.DataFrame,
        column_map: dict,
        market: str,
        trade_date: str,
        *,
        quantile: float = 0.9,
    ) -> set[str]:
        """
        执行打分逻辑 + 筛选 symbol
        返回：被选中的 symbol 集合
        """

        def extract_arrow_num(s):
            # 去除 HTML 标签
            clean_text = re.sub(r"<[^<]+?>", "", s)

            arrow_num = 0  # 默认：没有箭头 → 0

            # 向上箭头 ↑
            up_match = re.search(r"↑(\d+)", clean_text)
            if up_match:
                arrow_num = int(up_match.group(1))
            else:
                # 向下箭头 ↓
                down_match = re.search(r"↓(\d+)", clean_text)
                if down_match:
                    arrow_num = -int(down_match.group(1))

            # 斜杠后的数字（如 ↑7/4）
            bracket_match = re.search(r"/(\d+)", clean_text)
            bracket_num = int(bracket_match.group(1)) if bracket_match else None

            return arrow_num, bracket_num

        # 分位函数
        def rank_score(
            s: pd.Series,
            *,
            higher_is_better: bool = True,
            mid: float | None = None,
        ) -> pd.Series:
            """
            统一评分函数（最终版）
            - mid=None → 单边归一化 [0,1]，最大值=1，最小值=0
            - mid=float → 双边归一 [-1,1]，value==mid → score=0
            - higher_is_better=True → 值越大越好
            - NaN → 返回0
            """
            s_num = pd.to_numeric(s, errors="coerce")
            score = pd.Series(0.0, index=s.index)
            valid = s_num.dropna()
            if valid.empty:
                return score

            # ---------- 单边归一化 ----------
            if mid is None:
                vals = valid.copy()
                if not higher_is_better:
                    vals = -vals  # 负指标统一方向
                vmin, vmax = vals.min(), vals.max()
                if vmin == vmax:
                    score.loc[valid.index] = 1.0
                else:
                    score.loc[valid.index] = (vals - vmin) / (vmax - vmin)
                return score

            # ---------- 双边归一化 ----------
            aligned = valid - mid
            pos = aligned[aligned > 0]
            neg = aligned[aligned < 0]

            if not pos.empty:
                max_pos = pos.max()
                if max_pos != 0:
                    score.loc[pos.index] = pos / max_pos
            if not neg.empty:
                min_neg = neg.min()
                if min_neg != 0:
                    score.loc[neg.index] = neg / abs(min_neg)
            # aligned==0 → score保持0
            return score

        df = df.copy()

        # ===== 列映射 =====
        c = column_map
        sym_col = c["symbol"]

        # ===== 行业解析 =====
        df[["IND_ARROW_NUM", "IND_BRACKET_NUM"]] = df[c["industry"]].apply(
            lambda x: pd.Series(extract_arrow_num(x))
        )

        df["industry_arrow_score"] = rank_score(
            df["IND_ARROW_NUM"], higher_is_better=True, mid=0
        )

        df["industry_bracket_score"] = rank_score(
            df["IND_BRACKET_NUM"], higher_is_better=False
        )

        industry_score = (
            0.5 * df["industry_arrow_score"] + 0.5 * df["industry_bracket_score"]
        )

        # ===== ERP =====
        df[c["erp"]] = pd.to_numeric(df[c["erp"]], errors="coerce")

        ind_cnt = df.groupby(c["industry"])[sym_col].transform("count")
        invalid_ind = ind_cnt < 3

        df["erp_score"] = np.where(
            invalid_ind,
            rank_score(df[c["erp"]], higher_is_better=True, mid=0),
            df.groupby(c["industry"])[c["erp"]].transform(
                lambda x: rank_score(x, higher_is_better=True, mid=0)
            ),
        )

        # ===== PNL =====
        trade_dt = pd.to_datetime(trade_date)
        open_dt = pd.to_datetime(df[c["open_date"]], errors="coerce")

        days = (trade_dt - open_dt).dt.days.clip(lower=1)
        df["pnl_score"] = rank_score(
            df[c["pnl_ratio"]] / days, higher_is_better=True, mid=0
        )

        # ===== 稳定性 =====
        df["win_rate_score"] = rank_score(df[c["win_rate"]], mid=0.5)
        df["avg_trans_score"] = rank_score(df[c["avg_trans"]], higher_is_better=False)
        df["sortino_score"] = rank_score(df[c["sortino"]], mid=1)
        df["maxdd_score"] = rank_score(df[c["max_dd"]])

        stability_score = (
            0.3 * df["win_rate_score"]
            + 0.2 * df["avg_trans_score"]
            + 0.2 * df["sortino_score"]
            + 0.3 * df["maxdd_score"]
        )

        # ===== 总分 =====
        df["total_score"] = (
            0.25 * industry_score
            + 0.25 * df["pnl_score"]
            + 0.40 * stability_score
            + 0.10 * df["erp_score"]
        )

        # ===== 筛选 =====
        threshold = df["total_score"].quantile(quantile)
        selected = df.loc[df["total_score"] > threshold, sym_col]

        selected_symbols = set(selected.astype(str).str.strip().unique())

        return selected_symbols

    @staticmethod
    def export_if_changed(selected_symbols, market):
        """
        当命中的 symbol 列表发生变化时才写出 CSV
        CSV 格式：
            symbol
            AAPL
            MSFT
            ...
        """
        out_file = (
            pathlib.Path(__file__).resolve().parent.parent
            / f"{market}stockinfo"
            / "dynamic_list.csv"
        )
        # 将 selected_symbols 转为已排序的字符串列表，保证顺序确定且可用于 pandas
        symbols_list = sorted([str(s).strip() for s in selected_symbols])
        # 当前内容 hash（使用排序后的列表以确保可复现的哈希）
        content = "\n".join(symbols_list).encode("utf-8")
        new_md5 = hashlib.md5(content).hexdigest()

        # 2️⃣ 如果文件已存在，计算旧文件的 hash
        if os.path.exists(out_file):
            old_df = pd.read_csv(out_file)

            if "symbol" in old_df.columns:
                old_symbols = sorted(
                    old_df["symbol"].astype(str).str.strip().unique().tolist()
                )

                old_content = "\n".join(old_symbols).encode("utf-8")
                old_md5 = hashlib.md5(old_content).hexdigest()

                if old_md5 == new_md5:
                    return  # 内容一致，不写文件

        # 3️⃣ 内容不同，写文件
        pd.DataFrame({"symbol": symbols_list}).to_csv(out_file, index=False)
