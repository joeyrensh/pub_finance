#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from utility.toolkit import ToolKit
import time
import random
import pandas as pd
import requests
import json
from utility.fileinfo import FileInfo
from utility.em_stock_uti import EMWebCrawlerUti

""" 东方财经A股股票对应行业板块数据获取接口 """


class EMCNTickerCategoryCrawler:
    def __init__(self):
        self.item = "http://60.210.40.190:9091"
        self.proxy = {
            "http": self.item,
            "https": self.item,
        }
        self.proxy = None
        self.headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "host": "push2.eastmoney.com",
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

    def get_cn_ticker_category(self, trade_date):
        """
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH

        https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=SH688798
        """
        list = []
        dict = {}
        """ 遍历股票列表获取对应行业板块信息 """
        em = EMWebCrawlerUti()
        tick_list = em.get_stock_list(
            "cn", cache_path="./cnstockinfo/cn_stock_list_cache.csv"
        )
        tool = ToolKit("行业下载进度")
        for i in tick_list:
            # ETF基金不需要获取行业板块信息
            if str(i["symbol"]).startswith("ETF"):
                continue
            url = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax"
            params = {
                "code": f"{i['symbol']}",
            }
            time.sleep(random.uniform(0.1, 0.5))
            res = requests.get(url, params=params, proxies=self.proxy).text.lower()
            try:
                json_object = json.loads(res)
            except ValueError:
                continue
            if "status" in json_object and json_object["status"] != 0:
                print(
                    f"Error fetching data for {i['symbol']}: {json_object['message']}"
                )

            if (
                json_object is not None
                and "jbzl" in json_object
                and json_object["jbzl"] is not None
                and "sshy" in json_object["jbzl"]
            ):
                if json_object["jbzl"]["sshy"] == "--":
                    continue
                dict = {"symbol": i["symbol"], "industry": json_object["jbzl"]["sshy"]}
                list.append(dict)
            tool.progress_bar(len(tick_list), tick_list.index(i))
        df = pd.DataFrame(list)
        df.drop_duplicates(subset=["symbol", "industry"], keep="last", inplace=True)
        """ 获取板块文件信息 """
        file = FileInfo(trade_date, "cn")
        file_path_industry = file.get_file_path_industry
        df.to_csv(file_path_industry, mode="w", index=True, header=True)
