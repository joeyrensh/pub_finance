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

    # @staticmethod
    # def create_line(data):
    #     data = list(data)
    #     set_matplotlib_formats("svg")

    #     # 配置参数 (Yahoo Finance 视觉规范)
    #     config = {
    #         "up_color": "#ff4444",  # Yahoo 上涨绿色
    #         "down_color": "#00a859",  # Yahoo 下跌红色
    #         "line_width": 2,
    #         "marker_size": 3.5,
    #         "gradient_steps": 100,  # 渐变精度
    #         "zero_line_alpha": 0.2,
    #         "fill_alpha_range": (0.08, 0.3),  # 透明度动态范围
    #     }

    #     # 计算全局特征
    #     # 涨跌判断逻辑
    #     if len(data) >= 2:
    #         is_uptrend = data[-1] >= data[-2]  # 用最后两个点判断趋势
    #     else:
    #         is_uptrend = True  # 默认值
    #     max_val, min_val = max(data), min(data)
    #     range_val = max_val - min_val if (max_val != min_val) else 1

    #     # 初始化画布
    #     fig, ax = plt.subplots(1, 1, figsize=(2.5, 1), facecolor="none")  # 调整长宽比
    #     fig.patch.set_alpha(0.0)
    #     ax.margins(x=0.02, y=0.15)  # 边距优化

    #     # 主线条绘制
    #     line_color = config["up_color"] if is_uptrend else config["down_color"]
    #     ax.plot(data, color=line_color, lw=config["line_width"], zorder=3)

    #     # 渐变填充算法
    #     if data:
    #         x = np.arange(len(data))
    #         y = np.array(data)

    #         # 生成渐变蒙版
    #         y_norm = (y - min_val) / range_val  # 归一化到 [0,1]
    #         alphas = np.linspace(
    #             config["fill_alpha_range"][0], config["fill_alpha_range"][1], len(data)
    #         )

    #         # 分块渲染优化性能
    #         for i in range(len(data) - 1):
    #             x_segment = x[i : i + 2]
    #             y_segment = y[i : i + 2]

    #             # 计算局部透明度
    #             local_alpha = np.mean(alphas[i : i + 2])

    #             # 填充到零轴
    #             ax.fill_between(
    #                 x_segment,
    #                 y_segment,
    #                 0,
    #                 color=line_color,
    #                 alpha=local_alpha,
    #                 edgecolor="none",
    #                 zorder=2,
    #             )

    #         # 端点强化
    #         ax.scatter(
    #             len(data) - 1,
    #             data[-1],
    #             color=line_color,
    #             s=config["marker_size"] ** 2,
    #             edgecolor="white" if is_uptrend else "#2a2e39",
    #             linewidth=0.4,
    #             zorder=4,
    #         )

    #     # 零轴样式
    #     ax.axhline(
    #         0, color=line_color, alpha=config["zero_line_alpha"], lw=0.8, zorder=1
    #     )

    #     # 视觉微调
    #     ax.axis("off")
    #     # plt.tight_layout(pad=0)
    #     plt.subplots_adjust(left=0, right=1, top=1, bottom=0)  # 新增，去除边距

    #     # 输出优化
    #     img = BytesIO()
    #     fig.savefig(
    #         img,
    #         format="png",
    #         dpi=150,  # 提升DPI
    #         transparent=True,
    #         # bbox_inches="tight",
    #         bbox_inches=None,
    #         facecolor="none",
    #     )
    #     plt.close(fig)

    #     return (
    #         f'<img src="data:image/png;base64,{b64encode(img.getvalue()).decode()}" />'
    #     )

    @staticmethod
    def create_line(*args):
        """
        增强版绘图函数，支持单列与双列输入。
        双轴时左右 Y 轴独立缩放（不再强制统一刻度），但仍保证主轴线不会被右轴覆盖。
        对常数序列或极小幅度序列做最小跨度扩展以保证可见性。
        """
        set_matplotlib_formats("svg")

        # 配置参数
        config = {
            "up_color": "#ff4444",
            "down_color": "#00a859",
            "line_width": 2,
            "secondary_line_width": 0.5,
            "secondary_line_alpha": 0.8,
            "gradient_steps": 100,
            "fill_alpha_range": (0.08, 0.3),
            "marker_size": 3.5,
            "zero_line_alpha": 0,
        }

        # 解析输入
        if len(args) == 1:
            # 单列模式
            if isinstance(args[0], (list, tuple, np.ndarray)):
                data = list(args[0])
            else:
                data = [args[0]] if args[0] is not None else []
            datasets = [data]
            is_single_mode = True
            if len(data) >= 2:
                is_uptrend = data[-1] >= data[-2]
            else:
                is_uptrend = True
            colors = [config["up_color"] if is_uptrend else config["down_color"]]
            fill_colors = [colors[0]]
        elif len(args) >= 2:
            # 双列模式，只取前两列
            datasets = []
            for arg in args[:2]:
                if isinstance(arg, (list, tuple, np.ndarray)):
                    datasets.append(list(arg))
                else:
                    datasets.append([arg])
            is_single_mode = False
            if len(datasets[0]) >= 2:
                is_uptrend = datasets[0][-1] >= datasets[0][-2]
            else:
                is_uptrend = True
            first_color = config["up_color"] if is_uptrend else config["down_color"]
            if is_uptrend:
                secondary_line_color = "#ff6666"
                secondary_fill_color = "#ff4444"
            else:
                secondary_line_color = "#66b266"
                secondary_fill_color = "#00a859"
            colors = [first_color, secondary_line_color]
            fill_colors = [first_color, secondary_fill_color]
        else:
            datasets = [[]]
            is_single_mode = True
            colors = [config["up_color"]]
            fill_colors = [config["up_color"]]

        # 初始化画布
        fig, ax1 = plt.subplots(1, 1, figsize=(2.5, 1), facecolor="none")
        fig.patch.set_alpha(0.0)

        if not is_single_mode and len(datasets) >= 2:
            ax2 = ax1.twinx()

            # 主轴置顶并隐藏背景，防止右轴遮挡左轴线
            ax1.set_zorder(ax2.get_zorder() + 1)
            ax1.patch.set_visible(False)
            try:
                ax2.patch.set_alpha(0.0)
            except Exception:
                ax2.patch.set_visible(False)
            # 让右轴绘图位于更低层
            ax2.set_zorder(ax1.get_zorder() - 1)
            axes = [ax1, ax2]
        else:
            axes = [ax1]

        # 截断到相等长度（双列）
        if not is_single_mode and len(datasets) >= 2:
            min_length = min(len(datasets[0]), len(datasets[1]))
            datasets[0] = datasets[0][:min_length]
            datasets[1] = datasets[1][:min_length]

        # 清理并计算范围
        def clean_and_convert(arr):
            clean = []
            for x in arr:
                if isinstance(x, (int, float, np.number)):
                    clean.append(float(x))
                else:
                    try:
                        clean.append(float(x))
                    except (ValueError, TypeError):
                        clean.append(0.0)
            return clean

        clean_datasets = [clean_and_convert(ds) for ds in datasets]

        # 计算每组数据的 dmin/dmax，并为常数或极小跨度序列扩展最小跨度
        data_ranges = []
        for cd in clean_datasets:
            if cd:
                dmin, dmax = min(cd), max(cd)
                span = dmax - dmin
                # 最小视觉跨度：相对值(2%)或绝对最小值 1e-3
                rel_min_span = max((abs(dmax) + abs(dmin)) * 0.02, 1e-3)
                if span < rel_min_span:
                    mid = (dmax + dmin) / 2.0
                    dmin = mid - rel_min_span / 2.0
                    dmax = mid + rel_min_span / 2.0
                data_ranges.append((dmin, dmax))
            else:
                data_ranges.append((-1, 1))

        # 为左右轴分别设置独立 ylim（不再尝试强制对齐）
        if not is_single_mode and len(clean_datasets) >= 2:
            # 左轴（主轴）范围
            min1, max1 = data_ranges[0]
            span1 = max1 - min1 if (max1 - min1) > 0 else 1.0
            margin1 = max(span1 * 0.15, (abs(max1) + abs(min1)) * 0.02, 1e-3)
            ax1.set_ylim(min1 - margin1, max1 + margin1)

            # 右轴（成交量）范围
            min2, max2 = data_ranges[1]
            span2 = max2 - min2 if (max2 - min2) > 0 else 1.0
            margin2 = max(span2 * 0.12, (abs(max2) + abs(min2)) * 0.02, 1e-3)
            ax2.set_ylim(min2 - margin2, max2 + margin2)
        elif is_single_mode and clean_datasets[0]:
            min_val, max_val = data_ranges[0]
            range_val = max_val - min_val if (max_val - min_val) > 0 else 1.0
            margin = max(range_val * 0.15, (abs(max_val) + abs(min_val)) * 0.02, 1e-3)
            ax1.set_ylim(min_val - margin, max_val + margin)

        # 设置边距
        for ax in axes:
            ax.margins(x=0.02, y=0.0)

        # 绘制数据
        for i, (dataset, color) in enumerate(zip(clean_datasets, colors)):
            if not dataset:
                continue

            # 选择轴与样式
            if is_single_mode or i == 0:
                ax = ax1
                line_width = config["line_width"]
                line_alpha = 1.0
                fill_color = color
            else:
                ax = ax2
                line_width = config["secondary_line_width"]
                line_alpha = config["secondary_line_alpha"]
                fill_color = fill_colors[i] if i < len(fill_colors) else fill_colors[-1]

            x = np.arange(len(dataset))
            y = np.array(dataset)

            # 绘制折线（主轴 zorder 已确保在上层）
            ax.plot(x, y, color=color, lw=line_width, alpha=line_alpha, zorder=3)

            # 第二组使用填充到0（成交量填充），基于右轴的 0
            if not is_single_mode and i == 1 and len(dataset) > 0:
                alphas = np.linspace(
                    config["fill_alpha_range"][0],
                    config["fill_alpha_range"][1],
                    len(dataset),
                )
                for j in range(len(dataset) - 1):
                    x_segment = x[j : j + 2]
                    y_segment = y[j : j + 2]
                    local_alpha = float(np.mean(alphas[j : j + 2]))
                    ax.fill_between(
                        x_segment,
                        y_segment,
                        0,
                        color=fill_color,
                        alpha=local_alpha,
                        edgecolor="none",
                        zorder=2,
                    )

            # 端点强化（仅第一组或单列显示）
            if i == 0 or is_single_mode:
                ax.scatter(
                    len(dataset) - 1,
                    dataset[-1],
                    color=color,
                    s=config["marker_size"] ** 2,
                    edgecolor="white" if (is_single_mode or i == 0) else "#2a2e39",
                    linewidth=0.4,
                    zorder=4,
                )

        # 左轴零线（仅装饰，左右轴独立刻度）
        zero_color = colors[0] if colors else config["up_color"]
        ax1.axhline(
            0, color=zero_color, alpha=config["zero_line_alpha"], lw=0.8, zorder=1
        )

        # 隐藏坐标轴（仅保留图形）
        ax1.axis("off")
        if not is_single_mode and len(datasets) >= 2:
            ax2.axis("off")

        # 统一X轴范围（x 轴共享）
        if not is_single_mode and len(clean_datasets) >= 2:
            xlen = max(1, len(clean_datasets[0]) - 1)
            ax1.set_xlim(0, xlen)
            ax2.set_xlim(0, xlen)
        else:
            xlen = max(1, len(clean_datasets[0]) - 1)
            ax1.set_xlim(0, xlen)

        # 布局与导出
        plt.tight_layout(pad=0)
        plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

        img = BytesIO()
        fig.savefig(
            img,
            format="png",
            dpi=150,
            transparent=True,
            bbox_inches="tight",
            pad_inches=0,
            facecolor="none",
        )
        plt.close(fig)

        return (
            f'<img src="data:image/png;base64,{b64encode(img.getvalue()).decode()}" />'
        )
