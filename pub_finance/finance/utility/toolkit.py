#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from datetime import datetime, timedelta
import re
from base64 import b64encode
from io import BytesIO
from matplotlib_inline.backend_inline import set_matplotlib_formats
import matplotlib.pyplot as plt
import sys


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
        for h in range(offset, 365):
            """当前美国时间 UTC-4"""
            utc_us = datetime.now() - timedelta(hours=12) - timedelta(days=h)
            """ 周末正常休市 """
            if utc_us.isoweekday() in [1, 2, 3, 4, 5]:
                if str(utc_us)[0:10] in x:
                    continue
                else:
                    """返回日期字符串格式20200101"""
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
        for h in range(offset, 365):
            """当前北京时间 UTC+8"""
            utc_cn = datetime.now() - timedelta(days=h)
            """ 周末正常休市 """
            if utc_cn.isoweekday() in [1, 2, 3, 4, 5]:
                if str(utc_cn)[0:10] in x:
                    continue
                else:
                    """返回日期字符串格式20200101"""
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
        colors = ["#FF0000", "#009900", "#FFCCCC", "#CCFFCC"]
        line_width = 1.5

        # initialise the plot as you usually would
        fig, ax = plt.subplots(1, 1, figsize=(3, 1), facecolor="none")

        # Iterate over each data point
        for i in range(len(data) - 1):
            if data[i] >= 0 and data[i + 1] >= 0:
                line_color = colors[0]
                fill_color = colors[2]
                ax.plot(
                    [i, i + 1],
                    [data[i], data[i + 1]],
                    color=line_color,
                    linewidth=line_width,
                )
                ax.fill_between([i, i + 1], [data[i], data[i + 1]], color=fill_color)
            elif data[i] < 0 and data[i + 1] < 0:
                line_color = colors[1]
                fill_color = colors[3]
                ax.plot(
                    [i, i + 1],
                    [data[i], data[i + 1]],
                    color=line_color,
                    linewidth=line_width,
                )
                ax.fill_between([i, i + 1], [data[i], data[i + 1]], color=fill_color)
            elif data[i] >= 0 and data[i + 1] < 0:
                ax.plot([i, i], [data[i], 0], color="#FF0000", linewidth=line_width)
                ax.fill_between([i, i], [data[i], 0], color="#FFCCCC")
                ax.plot(
                    [i, i + 1], [0, data[i + 1]], color="#009900", linewidth=line_width
                )
                ax.fill_between([i, i + 1], [0, data[i + 1]], color="#CCFFCC")
            elif data[i] < 0 and data[i + 1] >= 0:
                ax.plot([i, i], [data[i], 0], color="#009900", linewidth=line_width)
                ax.fill_between([i, i], [data[i], 0], color="#CCFFCC")
                ax.plot(
                    [i, i + 1], [0, data[i + 1]], color="#FF0000", linewidth=line_width
                )
                ax.fill_between([i, i + 1], [0, data[i + 1]], color="#00CC00")
            else:
                line_color = "#808080"
                ax.plot(
                    [i, i + 1],
                    [data[i], data[i + 1]],
                    color=line_color,
                    linewidth=line_width,
                )

        # turn on zero axis
        ax.axhline(0, color="black", linestyle="--", linewidth=0.5, dashes=(5, 5))
        # turn off axis
        ax.axis("off")

        # add a marker at the last data point
        # plt.plot(len(data) - 1, data[len(data) - 1], "b.")
        if data[len(data) - 1] >= 0:
            marker_color = colors[0]
        else:
            marker_color = colors[1]

        plt.plot(
            len(data) - 1,
            data[len(data) - 1],
            marker="o",
            markersize=4,
            color=marker_color,
        )

        # close the figure
        plt.close(fig)

        # create a Bytes object
        img = BytesIO()

        # store the above plot to this Bytes object
        fig.savefig(img, format="png", dpi=120)

        # Encode object as base64 byte string
        encoded = b64encode(img.getvalue())

        # The above cannote be printed directly. We need to convert it to utf-8 format
        decoded = encoded.decode("utf-8")

        # Return the corresponding HTML tag
        return '<img src="data:image/png;base64,{}"/>'.format(decoded)
