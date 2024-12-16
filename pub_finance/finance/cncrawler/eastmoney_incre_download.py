#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import time
import requests
import re
import json
import pandas as pd
from datetime import datetime
import os
from utility.fileinfo import FileInfo


class EMCNWebCrawler:
    def __init__(self):
        """A股日数据url, 东方财经
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH
        """
        self.__url = (
            "http://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery&pn=i&pz=20000"
            "&po=1&np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:market"
            "&fields=f2,f3,f4,f5,f6,f7,f9,f12,f14,f15,f16,f17,f18,f20,f21&_=unix_time"
        )

    """ 获取数据列表 """

    def get_cn_daily_stock_info(self, trade_date):
        """url里需要传递unixtime当前时间戳"""
        current_timestamp = int(time.mktime(datetime.now().timetuple()))
        """
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH
        """
        list = []
        dict = {}
        for market in ["0", "1"]:
            for i in range(1, 100):
                url = (
                    self.__url.replace("unix_time", str(current_timestamp))
                    .replace("market", market)
                    .replace("pn=i", "pn=" + str(i))
                )
                res = requests.get(url).text
                """ 替换成valid json格式 """
                res_p = re.sub("\\].*", "]", re.sub(".*:\\[", "[", res, 1), 1)
                try:
                    json_object = json.loads(res_p)
                except ValueError:
                    break
                """
                f12: 股票代码, f14: 公司名称, f2: 最新报价, f3: 涨跌幅, f4: 涨跌额, f5: 成交量, f6: 成交额
                f7: 振幅, f9: 市盈率, f15: 最高, f16: 最低, f17: 今开, f18: 昨收, f20:总市值 f21:流通值
                f12: symbol, f14: name, f2: close, f3: chg, f4: change, f5: volume, f6: turnover
                f7: amplitude, f9: pe, f15: high, f16: low, f17: open, f18: preclose, f20: total_value, f21: circulation_value
                """

                def market_code(x):
                    return "SZ" if x == "0" else "SH"

                m = market_code(market)
                """ 重构数据字典 """
                for i in json_object:
                    if (
                        i["f12"] == "-"
                        or i["f17"] == "-"
                        or i["f2"] == "-"
                        or i["f15"] == "-"
                        or i["f16"] == "-"
                        or i["f5"] == "-"
                        or i["f20"] == "-"
                        or i["f21"] == "-"
                    ):
                        continue
                    dict = {
                        "symbol": m + i["f12"],
                        "name": i["f14"],
                        "open": i["f17"],
                        "close": i["f2"],
                        "high": i["f15"],
                        "low": i["f16"],
                        "volume": i["f5"],
                        "turnover": i["f6"],
                        "chg": i["f3"],
                        "change": i["f4"],
                        "amplitude": i["f7"],
                        "preclose": i["f18"],
                        "total_value": i["f20"],
                        "circulation_value": i["f21"],
                        "pe": i["f9"],
                    }
                    list.append(dict)
        """ 获取A股数据文件地址 """
        file_name_d = FileInfo(trade_date, "cn").get_file_path_latest
        """ 每日一个文件，根据交易日期创建 """
        if os.path.exists(file_name_d):
            os.remove(file_name_d)
        """ 将list转化为dataframe，并且存储为csv文件，带index和header """
        df = pd.DataFrame(list)
        date = datetime.strptime(trade_date, "%Y%m%d")
        df["date"] = date
        df.to_csv(file_name_d, mode="w", index=True, header=True)
