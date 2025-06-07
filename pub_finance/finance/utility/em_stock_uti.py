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
import math


class EMWebCrawlerUti:
    def __init__(self):
        """
        # 美股日数据url, 东方财经
        市场代码：
        105:纳斯达克 NASDAQ
        106:纽交所 NYSE
        107:美国交易所 AMEX
        # A股日数据url, 东方财经
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH

        """
        self.__url = (
            "https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery"
            "&pn=i&pz=100&po=1&np=1&ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&fid=f12&fs=m:mkt_code&fields=f2,f5,f9,f12,f14,f15,f16,f17,f20&_=unix_time"
        )
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

    def get_total_pages(self, market):
        current_timestamp = int(time.mktime(datetime.now().timetuple()))
        url = (
            self.__url.replace("unix_time", str(current_timestamp))
            .replace("mkt_code", market)
            .replace("pn=i", "pn=1")
        )
        res = requests.get(url, proxies=self.proxy, headers=self.headers).text.strip()
        if res.startswith("jQuery") and res.endswith(");"):
            res = res[res.find("(") + 1 : -2]
        total_page_no = math.ceil(json.loads(res)["data"]["total"] / 100)
        print(f"市场代码: {market}, 总页数: {total_page_no}")
        return total_page_no

    def get_us_stock_list(self, cache_path="../usstockinfo/us_stock_list_cache.csv"):
        # 如果缓存文件存在，直接读取
        if os.path.exists(cache_path):
            print(f"读取美股列表缓存: {cache_path}")
            return pd.read_csv(cache_path).to_dict(orient="records")
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
        for mkt_code in ["105", "106", "107"]:
            max_page = self.get_total_pages(mkt_code)
            for i in range(1, max_page + 1):
                """请求url，获取数据response"""
                url_re = (
                    self.__url.replace("unix_time", str(current_timestamp))
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
        # 保存到本地缓存
        pd.DataFrame(list).to_csv(cache_path, index=False)
        return list

    def get_cn_stock_list(self, cache_path="../cnstockinfo/cn_stock_list_cache.csv"):
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
        for mkt_code in ["0", "1"]:
            max_page = self.get_total_pages(mkt_code)
            """请求url，获取数据response"""
            for i in range(1, max_page + 1):
                url_re = (
                    self.__url.replace("unix_time", str(current_timestamp))
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
