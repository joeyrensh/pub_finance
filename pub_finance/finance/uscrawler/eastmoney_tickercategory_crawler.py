#!/usr/bin/env python3
# -*- coding: UTF-8 -*-


from utility.toolkit import ToolKit
import re
from datetime import datetime
import time
import random
import pandas as pd
import requests
import json
from utility.fileinfo import FileInfo
import os
import math
from utility.em_stock_uti import EMWebCrawlerUti

""" 东方财经美股股票对应行业板块数据获取接口 """


class EMUsTickerCategoryCrawler:
    def __init__(self):
        self.proxy = {
            # "http": "http://60.210.40.190:9091",
            # "https": "http://60.210.40.190:9091",
        }
        self.proxy = None
        self.headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "host": "push2.eastmoney.com",
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

    def get_us_ticker_category(self, trade_date):
        """市场代码：
        105:纳斯达克 NASDAQ O
        106:纽交所 NYSE N
        107:美国交易所 AMEX A

        https://emweb.eastmoney.com/pc_usf10/CompanyInfo/PageAjax?fullCode=WISH.O
        """

        list = []
        dict = {}
        tool = ToolKit("行业下载进度")
        """ 遍历股票列表获取对应行业板块信息 """
        em = EMWebCrawlerUti()
        tick_list = em.get_stock_list(
            "us", cache_path="../usstockinfo/us_stock_list_cache.csv"
        )
        for i in tick_list:
            url = "https://emweb.eastmoney.com/pc_usf10/CompanyInfo/PageAjax"
            if i["mkt_code"] == "105":
                mkt_code = "O"
            elif i["mkt_code"] == "106":
                mkt_code = "N"
            else:
                mkt_code = "A"
            params = {
                "fullCode": f"{i['symbol']}.{mkt_code}",
            }
            time.sleep(random.uniform(1, 2))
            res = requests.get(
                url, params=params, proxies=self.proxy, headers=self.headers
            ).text.lower()
            try:
                json_object = json.loads(res)
            except ValueError:
                continue
            if "data" in json_object and "gszl" in json_object["data"]:
                for h in json_object["data"]["gszl"]:
                    if h["industry"] == "--":
                        continue
                    dict = {"symbol": i["symbol"], "industry": h["industry"]}
                list.append(dict)
            tool.progress_bar(len(tick_list), tick_list.index(i))
        df = pd.DataFrame(list)
        df.drop_duplicates(subset=["symbol", "industry"], keep="last", inplace=True)
        """ 获取板块文件信息 """
        file = FileInfo(trade_date, "us")
        file_path_industry = file.get_file_path_industry
        df.to_csv(file_path_industry, mode="w", index=True, header=True)
