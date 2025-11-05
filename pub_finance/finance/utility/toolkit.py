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
        增强版绘图函数，支持两种输入方式：
        1. 单列数据: 保持原有功能和视觉效果
        2. 双列数据: 使用双Y轴，第一组在左侧，第二组在右侧，确保0点对齐
        第二组填充色模仿原始单图版本的渐变填充
        """
        set_matplotlib_formats("svg")

        # 配置参数
        config = {
            # 第一组数据配置
            "up_color": "#ff4444",  # 第一组上涨颜色
            "down_color": "#00a859",  # 第一组下跌颜色
            "line_width": 2,  # 第一组线条宽度
            # 第二组数据配置
            "secondary_line_width": 0.5,  # 第二组线条宽度缩小为0.5
            "secondary_line_alpha": 0.8,  # 第二组线条透明度
            # 渐变填充配置 (与原始单图版本相同)
            "gradient_steps": 100,  # 渐变精度
            "fill_alpha_range": (0.08, 0.3),  # 透明度动态范围
            # 通用配置
            "marker_size": 3.5,
            "zero_line_alpha": 0.2,
        }

        # 解析输入参数
        if len(args) == 1:
            # 单列输入模式 - 保持完全兼容
            if isinstance(args[0], (list, tuple, np.ndarray)):
                data = list(args[0])
            else:
                data = [args[0]] if args[0] is not None else []

            datasets = [data]
            is_single_mode = True

            # 计算单列的颜色
            if len(data) >= 2:
                is_uptrend = data[-1] >= data[-2]
            else:
                is_uptrend = True
            colors = [config["up_color"] if is_uptrend else config["down_color"]]

        elif len(args) >= 2:
            # 多列输入模式 - 使用双Y轴
            datasets = []
            for arg in args[:2]:  # 只取前两列
                if isinstance(arg, (list, tuple, np.ndarray)):
                    datasets.append(list(arg))
                else:
                    datasets.append([arg])
            is_single_mode = False

            # 第一组数据颜色根据趋势决定
            if len(datasets[0]) >= 2:
                is_uptrend = datasets[0][-1] >= datasets[0][-2]
            else:
                is_uptrend = True

            # 第一组数据使用红/绿色，第二组数据使用与第一组相同但更浅的颜色
            first_color = config["up_color"] if is_uptrend else config["down_color"]

            # 根据第一组颜色生成第二组颜色（更浅的版本）
            if is_uptrend:
                # 红色系 - 使用更浅的红色
                secondary_line_color = "#ff6666"  # 中等红色
                secondary_fill_color = "#ff4444"  # 与第一组相同的红色，但通过透明度控制
            else:
                # 绿色系 - 使用更浅的绿色
                secondary_line_color = "#66b266"  # 中等绿色
                secondary_fill_color = "#00a859"  # 与第一组相同的绿色，但通过透明度控制

            colors = [first_color, secondary_line_color]
            fill_colors = [first_color, secondary_fill_color]
        else:
            # 无数据情况
            datasets = [[]]
            is_single_mode = True
            colors = [config["up_color"]]
            fill_colors = [config["up_color"]]

        # 初始化画布
        fig, ax1 = plt.subplots(1, 1, figsize=(2.5, 1), facecolor="none")
        fig.patch.set_alpha(0.0)

        # 如果是双列模式，创建第二个Y轴
        if not is_single_mode and len(datasets) >= 2:
            ax2 = ax1.twinx()
            axes = [ax1, ax2]
        else:
            axes = [ax1]

        # 确保两组数据长度一致，如果不同则截断到最小长度
        if not is_single_mode and len(datasets) >= 2:
            min_length = min(len(datasets[0]), len(datasets[1]))
            datasets[0] = datasets[0][:min_length]
            datasets[1] = datasets[1][:min_length]

        # 计算各组数据的范围 - 简化版本
        data_ranges = []
        for dataset in datasets:
            if dataset:
                # 清理数据，确保都是数值
                clean_data = []
                for x in dataset:
                    if isinstance(x, (int, float, np.number)):
                        clean_data.append(float(x))
                    else:
                        try:
                            clean_data.append(float(x))
                        except (ValueError, TypeError):
                            clean_data.append(0.0)

                if clean_data:
                    data_min, data_max = min(clean_data), max(clean_data)
                    # 确保数据范围不为零
                    if data_min == data_max:
                        data_min -= 0.5
                        data_max += 0.5
                    data_ranges.append((data_min, data_max))
                else:
                    data_ranges.append((-1, 1))
            else:
                data_ranges.append((-1, 1))

        # 简化的Y轴范围设置 - 不再尝试复杂的0点对齐
        if not is_single_mode and len(datasets) >= 2:
            # 分别设置两个Y轴的范围
            min1, max1 = data_ranges[0]
            min2, max2 = data_ranges[1]

            # 为第一组数据添加更多边距
            range1 = max1 - min1
            margin1 = range1 * 0.15  # 增加边距到15%
            ax1.set_ylim(min1 - margin1, max1 + margin1)

            # 为第二组数据添加更多边距
            range2 = max2 - min2
            margin2 = range2 * 0.15  # 增加边距到15%
            ax2.set_ylim(min2 - margin2, max2 + margin2)
        elif is_single_mode and datasets[0]:
            # 单列模式，使用计算出的范围
            min_val, max_val = data_ranges[0]
            range_val = max_val - min_val
            margin = range_val * 0.15  # 增加边距到15%
            ax1.set_ylim(min_val - margin, max_val + margin)

        # 设置边距
        for ax in axes:
            ax.margins(x=0.02, y=0.15)

        # 绘制每组数据
        for i, (dataset, color) in enumerate(zip(datasets, colors)):
            if not dataset:
                continue

            # 清理数据，确保都是数值
            clean_data = []
            for x in dataset:
                if isinstance(x, (int, float, np.number)):
                    clean_data.append(float(x))
                else:
                    try:
                        clean_data.append(float(x))
                    except (ValueError, TypeError):
                        clean_data.append(0.0)

            if not clean_data:
                continue

            # 选择对应的Y轴和配置
            if is_single_mode or i == 0:
                ax = ax1
                line_width = config["line_width"]
                line_alpha = 1.0  # 第一组线条完全不透明
                fill_color = color  # 第一组不使用填充，但保留变量
            else:
                ax = ax2
                line_width = config["secondary_line_width"]  # 第二组线条宽度为0.5
                line_alpha = config[
                    "secondary_line_alpha"
                ]  # 第二组线条使用配置的透明度
                fill_color = fill_colors[i]  # 第二组使用根据第一组颜色决定的填充色

            # 主线条绘制
            ax.plot(clean_data, color=color, lw=line_width, alpha=line_alpha, zorder=3)

            # 渐变填充算法 - 只在第二组数据上使用填充（模仿原始单图版本）
            if not is_single_mode and i == 1:  # 只有第二组数据使用填充
                if clean_data:
                    x = np.arange(len(clean_data))
                    y = np.array(clean_data)

                    # 计算全局特征（使用第二组数据自己的范围）
                    max_val, min_val = max(clean_data), min(clean_data)
                    range_val = max_val - min_val if (max_val != min_val) else 1

                    # 生成渐变蒙版（与原始单图版本完全相同）
                    y_norm = (y - min_val) / range_val  # 归一化到 [0,1]
                    alphas = np.linspace(
                        config["fill_alpha_range"][0],
                        config["fill_alpha_range"][1],
                        len(clean_data),
                    )

                    # 分块渲染优化性能（与原始单图版本完全相同）
                    for j in range(len(clean_data) - 1):
                        x_segment = x[j : j + 2]
                        y_segment = y[j : j + 2]

                        # 计算局部透明度
                        local_alpha = np.mean(alphas[j : j + 2])

                        # 填充到零轴（与原始单图版本完全相同）
                        ax.fill_between(
                            x_segment,
                            y_segment,
                            0,
                            color=fill_color,
                            alpha=local_alpha,
                            edgecolor="none",
                            zorder=2,
                        )

            # 端点强化 - 只在第一组数据上显示端点
            if i == 0 or is_single_mode:
                ax.scatter(
                    len(clean_data) - 1,
                    clean_data[-1],
                    color=color,
                    s=config["marker_size"] ** 2,
                    edgecolor="white" if (is_single_mode or i == 0) else "#2a2e39",
                    linewidth=0.4,
                    zorder=4,
                )

        # 零轴样式 - 只在第一组数据的Y轴上显示
        if is_single_mode:
            zero_color = colors[0]
        else:
            zero_color = colors[0]  # 使用第一组数据的颜色

        ax1.axhline(
            0, color=zero_color, alpha=config["zero_line_alpha"], lw=0.8, zorder=1
        )

        # 隐藏所有坐标轴
        ax1.axis("off")
        if not is_single_mode and len(datasets) >= 2:
            ax2.axis("off")

        # 确保两个Y轴共享相同的X轴范围
        if not is_single_mode and len(datasets) >= 2:
            ax1.set_xlim(0, len(datasets[0]) - 1)
            ax2.set_xlim(0, len(datasets[1]) - 1)

        # 强制重新计算布局
        plt.tight_layout(pad=0)
        plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

        # 输出优化
        img = BytesIO()
        fig.savefig(
            img,
            format="png",
            dpi=150,
            transparent=True,
            bbox_inches="tight",  # 使用tight而不是None
            pad_inches=0,  # 确保没有内边距
            facecolor="none",
        )
        plt.close(fig)

        return (
            f'<img src="data:image/png;base64,{b64encode(img.getvalue()).decode()}" />'
        )
