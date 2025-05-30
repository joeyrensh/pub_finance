#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import time
import random
import requests
import re
import json
import pandas as pd
from datetime import datetime
import os
from utility.fileinfo import FileInfo
import csv


class EMWebCrawler:
    def __init__(self):
        """美股日数据url, 东方财经
        市场代码：
        105:纳斯达克 NASDAQ
        106:纽交所 NYSE
        107:美国交易所 AMEX
        """
        self.__url = (
            "https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery"
            "&pn=i&pz=200&po=1&np=1&ut=&fltt=2&invt=2&fid=f12&fs=m:mkt_code&fields=f2,f3,f4,f5,f6,f7,f9,f12,f14,f15,f16,f17,f18,f20,f21&_=unix_time"
        )

    """ 获取数据列表 """

    def get_us_daily_stock_info(self, trade_date):
        """url里需要传递unixtime当前时间戳"""
        current_timestamp = int(time.mktime(datetime.now().timetuple()))
        """ 请求url，获取数据response """
        list = []
        dic_a = {}
        for mkt_code in ["105", "106", "107"]:
            for i in range(1, 500):
                url = (
                    self.__url.replace("unix_time", str(current_timestamp))
                    .replace("mkt_code", mkt_code)
                    .replace("pn=i", "pn=" + str(i))
                )
                time.sleep(random.uniform(0.5, 1))
                res = requests.get(url).text
                """ 替换成valid json格式 """
                res_p = re.sub("\\].*", "]", re.sub(".*:\\[", "[", res, 1), 1)
                """         
                f12: 股票代码, f14: 公司名称, f2: 最新报价, f3: 涨跌幅, f4: 涨跌额, f5: 成交量, f6: 成交额
                f7: 振幅,f9: 市盈率, f15: 最高, f16: 最低, f17: 今开, f18: 昨收, f20:总市值 f21:流通值
                f12: symbol, f14: name, f2: close, f3: chg, f4: change, f5: volume, f6: turnover
                f7: amplitude,f9: pe, f15: high, f16: low, f17: open, f18: preclose, f20: total_value, f21: circulation_value
                """
                try:
                    json_object = json.loads(res_p)
                except ValueError:
                    break
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

                    dic_a = {
                        "symbol": i["f12"],
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
                    list.append(dic_a)
        """ 获取美股数据文件地址 """
        file_name_d = FileInfo(trade_date, "us").get_file_path_latest
        """ 每日一个文件，根据交易日期创建 """
        if os.path.exists(file_name_d):
            os.remove(file_name_d)
        """ 将list转化为dataframe，并且存储为csv文件，带index和header """
        df = pd.DataFrame(list)
        date = datetime.strptime(trade_date, "%Y%m%d")
        df["date"] = date
        df.to_csv(file_name_d, mode="w", index=True, header=True)

        # 10年期国债收益率
        url = "https://quote.eastmoney.com/center/api/qqzq.js?"
        res = requests.get(url).text

        lines = res.strip().split("\n")
        data = []
        for line in lines:
            fields = line.split(",")
            data.append(fields)

        filtered_rows = []
        for row in data[2:]:
            if len(row) > 2:  # 确保parts列表至少有两个元素
                code = row[1]
                if code == "US10Y_B":
                    code = row[1]
                    name = row[2]
                    date = row[3]
                    new = row[5]

                    # 将这些字段添加到一个新的列表中，准备写入CSV文件
                    filtered_rows.append([code, name, date, new])

        output_filename = FileInfo(trade_date, "us").get_file_path_gz
        with open(output_filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # 可选：写入表头
            writer.writerow(["code", "name", "date", "new"])
            # 写入数据行
            writer.writerows(filtered_rows)
