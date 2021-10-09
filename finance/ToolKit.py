#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import sys
import datetime
import time
import requests
import re


class ToolKit(object):

    _url = 'https://hq.sinajs.cn/?rn=unix_time&list=gb_ixic'

    def __init__(self, val):
        self.val = val
        print('\n' + val + '开始处理...')

    # 执行进度显示
    def progress_bar(self, var, i):
        print("\r{}处理进度: {}%: ".format(self.val, int(i * 100 / var)), "▋" * (i * 100 // var), end="", flush=True)

    # check今日是否交易日
    def is_us_trade_date(self):
        current_timestamp = int(time.mktime(datetime.datetime.now().timetuple()))
        url = self._url.replace('unix_time', str(current_timestamp))
        res = requests.get(url).text
        res_p = re.sub('"', '', re.sub('="', ',', re.sub('.*gb_', '', res))).split(',')
        if str(res_p[4])[0: 10] == str(datetime.datetime.now())[0: 10]:
            return True


