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

""" 东方财经A股股票对应行业板块数据获取接口 """


class EMCNTickerCategoryCrawler:
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

    def get_total_pages(self, url, market):
        current_timestamp = int(time.mktime(datetime.now().timetuple()))
        url_re = (
            url.replace("unix_time", str(current_timestamp))
            .replace("mkt_code", market)
            .replace("pn=i", "pn=1")
        )
        res = requests.get(
            url_re, proxies=self.proxy, headers=self.headers
        ).text.strip()
        if res.startswith("jQuery") and res.endswith(");"):
            res = res[res.find("(") + 1 : -2]
        total_page_no = math.ceil(json.loads(res)["data"]["total"] / 100)
        return total_page_no

    def get_cn_stock_list(self, cache_path="./cnstockinfo/cn_stock_list_cache.csv"):
        # 如果缓存文件存在，直接读取
        if os.path.exists(cache_path):
            print(f"读取A股列表缓存: {cache_path}")
            return pd.read_csv(cache_path).to_dict(orient="records")
        """url里需要传递unixtime当前时间戳"""
        current_timestamp = int(time.mktime(datetime.now().timetuple()))

        """ 
        A股日数据url, 东方财经
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH
        """
        dict = {}
        list = []
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery"
            "&pn=i&pz=200&po=1&np=1&ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&fid=f12&fs=m:mkt_code&fields=f2,f5,f9,f12,f14,f15,f16,f17,f20&_=unix_time"
        )
        for mkt_code in ["0", "1"]:
            max_page = self.get_total_pages(url, mkt_code)
            """请求url，获取数据response"""
            for i in range(1, max_page + 1):
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
                if mkt_code == "0":
                    market = "SZ"
                else:
                    market = "SH"
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
                    dict = {"symbol": market + i["f12"], "mkt_code": mkt_code}
                    list.append(dict)
        # 保存到本地缓存
        pd.DataFrame(list).to_csv(cache_path, index=False)
        return list

    def get_cn_ticker_category(self, trade_date):
        """
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH

        https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=SH688798
        """
        list = []
        dict = {}
        tool = ToolKit("行业下载进度")
        """ 遍历股票列表获取对应行业板块信息 """
        tick_list = self.get_cn_stock_list()
        # tick_list = [{"symbol":"600444", "mkt_code":"SH"}]
        for i in tick_list:
            # if i["symbol"] == '600444':
            #     print(i)
            url = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=mkt_codesymbol"
            url_re = url.replace("mkt_codesymbol", i["symbol"])
            time.sleep(random.uniform(1, 2))
            res = requests.get(
                url_re, proxies=self.proxy, headers=self.headers
            ).text.lower()
            # print(res)
            try:
                json_object = json.loads(res)
            except ValueError:
                continue
            if (
                json_object is not None
                and "jbzl" in json_object
                and json_object["jbzl"] is not None
                and "sshy" in json_object["jbzl"]
            ):
                if json_object["jbzl"]["sshy"] == "--":
                    continue
                # print(json_object["jbzl"]["sshy"])
                dict = {"symbol": i["symbol"], "industry": json_object["jbzl"]["sshy"]}
                list.append(dict)
                # print(dict)
            tool.progress_bar(len(tick_list), tick_list.index(i))
        df = pd.DataFrame(list)
        df.drop_duplicates(subset=["symbol", "industry"], keep="last", inplace=True)
        """ 获取板块文件信息 """
        file = FileInfo(trade_date, "cn")
        file_path_industry = file.get_file_path_industry
        df.to_csv(file_path_industry, mode="w", index=True, header=True)
