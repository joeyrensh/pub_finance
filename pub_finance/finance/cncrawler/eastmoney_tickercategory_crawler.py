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

""" 东方财经A股股票对应行业板块数据获取接口 """


class EMCNTickerCategoryCrawler:
    def get_cn_stock_list(self):
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
            "&pn=i&pz=200&po=1&np=1&ut=&fltt=2&invt=2&fid=f12&fs=m:mkt_code&fields=f2,f3,f4,f5,f6,f7,f12,f14,f15,f16,f17,f18,f20,f21&_=unix_time"
        )
        for mkt_code in ["0", "1"]:
            """请求url，获取数据response"""
            for i in range(1, 500):
                url_re = (
                    url.replace("unix_time", str(current_timestamp))
                    .replace("mkt_code", mkt_code)
                    .replace("pn=i", "pn=" + str(i))
                )
                time.sleep(random.uniform(0.5, 1))
                res = requests.get(url_re).text
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
                        or i["f17"] == "-"
                        or i["f2"] == "-"
                        or i["f15"] == "-"
                        or i["f16"] == "-"
                        or i["f5"] == "-"
                        or i["f20"] == "-"
                        or i["f21"] == "-"
                    ):
                        continue
                    dict = {"symbol": market + i["f12"], "mkt_code": mkt_code}
                    list.append(dict)
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
            time.sleep(random.uniform(0.5, 1))
            res = requests.get(url_re).text.lower()
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
