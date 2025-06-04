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

    def get_us_stock_list(self):
        """url里需要传递unixtime当前时间戳"""
        current_timestamp = int(time.mktime(datetime.now().timetuple()))

        """     
        市场代码：
        105:纳斯达克 NASDAQ
        106:纽交所 NYSE
        107:美国交易所 AMEX    
        """
        dict = {}
        list = []
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery"
            "&pn=i&pz=200&po=1&np=1&ut=&fltt=2&invt=2&fid=f12&fs=m:mkt_code&fields=f2,f3,f4,f5,f6,f7,f12,f14,f15,f16,f17,f18,f20,f21&_=unix_time"
        )
        for mkt_code in ["105", "106", "107"]:
            for i in range(1, 500):
                """请求url，获取数据response"""
                url_re = (
                    url.replace("unix_time", str(current_timestamp))
                    .replace("mkt_code", mkt_code)
                    .replace("pn=i", "pn=" + str(i))
                )
                time.sleep(random.uniform(1, 2))
                res = requests.get(
                    url_re, proxies=self.proxy, headers=self.headers
                ).text
                """ 替换成valid json格式 """
                res_p = re.sub("\\].*", "]", re.sub(".*:\\[", "[", res, 1), 1)
                try:
                    json_object = json.loads(res_p)
                except ValueError:
                    break
                for i in json_object:
                    if (
                        i["f12"] == "-"
                        or i["f14"] == "-"
                        or i["f17"] == "-"
                        or i["f2"] == "-"
                        or i["f15"] == "-"
                        or i["f16"] == "-"
                        or i["f5"] == "-"
                        or i["f20"] == "-"
                    ):
                        continue
                    dict = {"symbol": i["f12"], "mkt_code": mkt_code}
                    list.append(dict)
        return list

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
        tick_list = self.get_us_stock_list()
        for i in tick_list:
            url = "https://emweb.eastmoney.com/pc_usf10/CompanyInfo/PageAjax?fullCode=symbol.mkt_code"
            if i["mkt_code"] == "105":
                mkt_code = "O"
            elif i["mkt_code"] == "106":
                mkt_code = "N"
            else:
                mkt_code = "A"
            url_re = url.replace("symbol", i["symbol"]).replace("mkt_code", mkt_code)
            time.sleep(random.uniform(1, 2))
            res = requests.get(
                url_re, proxies=self.proxy, headers=self.headers
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
