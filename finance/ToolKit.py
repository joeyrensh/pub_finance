#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from datetime import datetime, timedelta
import re


class ToolKit(object):

    def __init__(self, val):
        self.val = val
        print('\n' + val + '开始处理...')

    # 执行进度显示
    def progress_bar(self, var, i):
        print("\r{}处理进度: {}%: ".format(self.val, int(i * 100 / var)),
              "▋" * (i * 100 // var), end="", flush=True)

    # check今日是否交易日
    @staticmethod
    def is_us_trade_date():
        # 当前北京时间 UTC+8
        # utc_loc = datetime.now()
        # 当前美国时间 UTC-4
        utc_us = datetime.now() - timedelta(hours=12)
        # utc_us = datetime.fromisoformat('2021-01-18 01:00:00')
        # 美股休市日，https://www.nyse.com/markets/hours-calendars
        # USStockMarketClosed.config 是2021和2022两年的美股法定休市配置文件
        f = open('./USStockMarketClosed.config').readlines()
        x = []
        for i in f:
            x.append(re.sub(',.*\n', '', i))
        # 周末正常休市
        if utc_us.isoweekday() in [1, 2, 3, 4, 5]:
            if str(utc_us)[0: 10] in x:
                print('今日非美国交易日')
                return False
            else:
                return True
        else:
            print('今日非美国交易日')
            return False
