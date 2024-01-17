#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from datetime import datetime, timedelta
import re


class ToolKit():
    def __init__(self, val):
        self.val = val
        print("\n" + val + "...")

    """ 执行进度显示 """

    def progress_bar(self, var, i) -> None:
        print(
            "\r{}: {}%: ".format(self.val, int(i * 100 / var)),
            "▋" * (i * 100 // var),
            end="",
            flush=True,
        )

    """ 判断今日是否美股交易日 """

    @staticmethod
    def is_us_trade_date() -> bool:
        """
        当前北京时间 UTC+8
        utc_loc = datetime.now()
        当前美国时间 UTC-4
        """
        utc_us = datetime.now() - timedelta(hours=12)
        """ 
        utc_us = datetime.fromisoformat('2021-01-18 01:00:00')
        美股休市日，https://www.nyse.com/markets/hours-calendars
        USStockMarketClosed.config 是2021和2022两年的美股法定休市配置文件 
        """
        f = open("./usstockinfo/USStockMarketClosed.config").readlines()
        x = []
        for i in f:
            x.append(re.sub(",.*\n", "", i))
        """ 周末正常休市 """
        if utc_us.isoweekday() in [1, 2, 3, 4, 5]:
            if str(utc_us)[0:10] in x:
                print("今日非美国交易日")
                return False
            else:
                return True
        else:
            print("今日非美国交易日")
            return False

    """ 获取美股最新交易日期 """

    @staticmethod
    def get_us_latest_trade_date(offset) -> (str | None):
        """
        utc_us = datetime.fromisoformat('2021-01-18 01:00:00')
        美股休市日，https://www.nyse.com/markets/hours-calendars
        USStockMarketClosed.config 是2021和2022两年的美股法定休市配置文件
        """
        f = open("./usstockinfo/USStockMarketClosed.config").readlines()
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
                    # print("当前获取的美股日期是：", str(utc_us)[0:10].replace("-", ""))
                    return str(utc_us)[0:10].replace("-", "")
            else:
                continue

    """ 获取美股两个日期间的交易天数 """

    @staticmethod
    def get_us_trade_off_days(cur, before) -> (int | None):
        """
        取两个交易日起之间的交易天数，用来过滤塞选滞胀的股票
        cur, before都需是datetime类型
        """
        f = open("./usstockinfo/USStockMarketClosed.config").readlines()
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
    def is_cn_trade_date() -> bool:
        utc_cn = datetime.now()
        """
        utc_cn = datetime.fromisoformat('2021-01-18 01:00:00')
        A股休市日
        CNStockMarketClosed.config 是2021和2022两年的美股法定休市配置文件 
        """
        f = open("./cnstockinfo/CNStockMarketClosed.config").readlines()
        x = []
        for i in f:
            x.append(re.sub(",.*\n", "", i))
        """ 周末正常休市 """
        if utc_cn.isoweekday() in [1, 2, 3, 4, 5]:
            if str(utc_cn)[0:10] in x:
                print("今日非A股交易日")
                return False
            else:
                return True
        else:
            print("今日非A股交易日")
            return False

    """ 获取A股最新交易日期 """

    @staticmethod
    def get_cn_latest_trade_date(offset) -> (str | None):
        f = open("./cnstockinfo/CNStockMarketClosed.config").readlines()
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
                    # print("当前获取的A股日期是：", str(utc_cn)[0:10].replace("-", ""))
                    return str(utc_cn)[0:10].replace("-", "")
            else:
                continue

    """ 获取A股两个日期间的交易天数 """

    @staticmethod
    def get_cn_trade_off_days(cur, before) -> (int | None):
        """
        取两个交易日起之间的交易天数，用来过滤塞选滞胀的股票
        cur, before都需是datetime类型
        """
        f = open("./cnstockinfo/CNStockMarketClosed.config").readlines()
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
