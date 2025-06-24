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
            x.append(re.sub(",.*\n", "", i))
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
            x.append(re.sub(",.*\n", "", i))
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
            x.append(re.sub(",.*\n", "", i))
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
            x.append(re.sub(",.*\n", "", i))
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
            x.append(re.sub(",.*\n", "", i))
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
            x.append(re.sub(",.*\n", "", i))
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

    @staticmethod
    def create_line(data):
        data = list(data)
        set_matplotlib_formats("svg")

        # 配置参数 (Yahoo Finance 视觉规范)
        config = {
            "up_color": "#ff4444",  # Yahoo 上涨绿色
            "down_color": "#00a859",  # Yahoo 下跌红色
            "line_width": 1.2,
            "marker_size": 3.5,
            "gradient_steps": 100,  # 渐变精度
            "zero_line_alpha": 0.2,
            "fill_alpha_range": (0.08, 0.3),  # 透明度动态范围
        }

        # 计算全局特征
        # 涨跌判断逻辑
        if len(data) >= 2:
            is_uptrend = data[-1] >= data[-2]  # 用最后两个点判断趋势
        else:
            is_uptrend = True  # 默认值
        max_val, min_val = max(data), min(data)
        range_val = max_val - min_val if (max_val != min_val) else 1

        # 初始化画布
        fig, ax = plt.subplots(1, 1, figsize=(3, 1), facecolor="none")  # 调整长宽比
        fig.patch.set_alpha(0.0)
        ax.margins(x=0.02, y=0.15)  # 边距优化

        # 主线条绘制
        line_color = config["up_color"] if is_uptrend else config["down_color"]
        ax.plot(data, color=line_color, lw=config["line_width"], zorder=3)

        # 渐变填充算法
        if data:
            x = np.arange(len(data))
            y = np.array(data)

            # 生成渐变蒙版
            y_norm = (y - min_val) / range_val  # 归一化到 [0,1]
            alphas = np.linspace(
                config["fill_alpha_range"][0], config["fill_alpha_range"][1], len(data)
            )

            # 分块渲染优化性能
            for i in range(len(data) - 1):
                x_segment = x[i : i + 2]
                y_segment = y[i : i + 2]

                # 计算局部透明度
                local_alpha = np.mean(alphas[i : i + 2])

                # 填充到零轴
                ax.fill_between(
                    x_segment,
                    y_segment,
                    0,
                    color=line_color,
                    alpha=local_alpha,
                    edgecolor="none",
                    zorder=2,
                )

            # 端点强化
            ax.scatter(
                len(data) - 1,
                data[-1],
                color=line_color,
                s=config["marker_size"] ** 2,
                edgecolor="white" if is_uptrend else "#2a2e39",
                linewidth=0.4,
                zorder=4,
            )

        # 零轴样式
        ax.axhline(
            0, color=line_color, alpha=config["zero_line_alpha"], lw=0.8, zorder=1
        )

        # 视觉微调
        ax.axis("off")
        plt.tight_layout(pad=0)

        # 输出优化
        img = BytesIO()
        fig.savefig(
            img,
            format="png",
            dpi=150,  # 提升DPI
            transparent=True,
            bbox_inches="tight",
            facecolor="none",
        )
        plt.close(fig)

        return (
            f'<img src="data:image/png;base64,{b64encode(img.getvalue()).decode()}" />'
        )
