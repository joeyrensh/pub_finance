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
import akshare as ak


class EMCNWebCrawler:
    def __init__(self):
        """A股日数据url, 东方财经
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH
        """
        self.__url = (
            "http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery&pn=i&pz=200"
            "&po=1&np=1&ut=&fltt=2&invt=2&fid=f12&fs=m:market"
            "&fields=f2,f5,f9,f12,f14,f15,f16,f17,f20&_=unix_time"
        )
        self.proxy = {
            # "http": "http://120.25.1.15:7890",
            # "https": "http://120.25.1.15:7890",
        }
        self.proxy = None
        self.headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "host": "push2.eastmoney.com",
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

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
            for i in range(1, 500):
                url = (
                    self.__url.replace("unix_time", str(current_timestamp))
                    .replace("market", market)
                    .replace("pn=i", "pn=" + str(i))
                )
                print("url: ", url)
                time.sleep(random.uniform(1, 2))
                res = requests.get(url, proxies=self.proxy, headers=self.headers).text
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
                        or i["f14"] == "-"
                        or i["f17"] == "-"
                        or i["f2"] == "-"
                        or i["f15"] == "-"
                        or i["f16"] == "-"
                        or i["f5"] == "-"
                        or i["f20"] == "-"
                    ):
                        continue
                    if "ETF" in i["f14"]:
                        symbol_val = "ETF" + i["f12"]
                    else:
                        symbol_val = m + i["f12"]
                    dict = {
                        "symbol": symbol_val,
                        "name": i["f14"],
                        "open": i["f17"],
                        "close": i["f2"],
                        "high": i["f15"],
                        "low": i["f16"],
                        "volume": i["f5"],
                        "total_value": i["f20"],
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

        # 10年期国债收益率
        url = "https://quote.eastmoney.com/center/api/qqzq.js?"
        res = requests.get(url, proxies=self.proxy, headers=self.headers).text

        lines = res.strip().split("\n")
        data = []
        for line in lines:
            fields = line.split(",")
            data.append(fields)

        filtered_rows = []
        for row in data[2:]:
            if len(row) > 2:  # 确保parts列表至少有两个元素
                code = row[1]
                if code == "CN10Y_B":
                    code = row[1]
                    name = row[2]
                    date = row[3]
                    new = row[5]

                    # 将这些字段添加到一个新的列表中，准备写入CSV文件
                    filtered_rows.append([code, name, date, new])

        output_filename = FileInfo(trade_date, "cn").get_file_path_gz
        with open(output_filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # 可选：写入表头
            writer.writerow(["code", "name", "date", "new"])
            # 写入数据行
            writer.writerows(filtered_rows)

    """ AKshare获取A股历史数据"""

    def get_cn_daily_stock_info_ak(self, trade_date):
        """
        获取A股数据，上海市场
        """
        stock_sh_a_spot_em = ak.stock_sh_a_spot_em()
        pd_stock_sh = stock_sh_a_spot_em[
            [
                "代码",
                "名称",
                "今开",
                "最新价",
                "最高",
                "最低",
                "成交量",
                "总市值",
                "市盈率-动态",
            ]
        ].copy()
        pd_stock_sh = pd_stock_sh[pd_stock_sh["最新价"] > 0]
        pd_stock_sh = pd_stock_sh.rename(
            columns={
                "代码": "symbol",
                "名称": "name",
                "今开": "open",
                "最新价": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "总市值": "total_value",
                "市盈率-动态": "pe",
            }
        )
        # 给 symbol 字段增加前缀（如 "SH"）
        pd_stock_sh["symbol"] = "SH" + pd_stock_sh["symbol"]

        """ 获取A股数据文件地址 """
        file_name_d = FileInfo(trade_date, "cn").get_file_path_latest
        """ 每日一个文件，根据交易日期创建 """
        if os.path.exists(file_name_d):
            os.remove(file_name_d)
        date = datetime.strptime(trade_date, "%Y%m%d")
        pd_stock_sh["date"] = date
        pd_stock_sh.to_csv(file_name_d, mode="w", index=True, header=True)

        """
        获取A股数据, 深圳市场
        """
        stock_sz_a_spot_em = ak.stock_sz_a_spot_em()
        pd_stock_sz = stock_sz_a_spot_em[
            [
                "代码",
                "名称",
                "今开",
                "最新价",
                "最高",
                "最低",
                "成交量",
                "总市值",
                "市盈率-动态",
            ]
        ].copy()
        pd_stock_sz = pd_stock_sz[pd_stock_sz["最新价"] > 0]
        pd_stock_sz = pd_stock_sz.rename(
            columns={
                "代码": "symbol",
                "名称": "name",
                "今开": "open",
                "最新价": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "总市值": "total_value",
                "市盈率-动态": "pe",
            }
        )
        # 给 symbol 字段增加前缀（如 "SZ"）
        pd_stock_sz["symbol"] = "SZ" + pd_stock_sz["symbol"]
        pd_stock_sz["date"] = date
        pd_stock_sz.to_csv(file_name_d, mode="a", index=True, header=False)

        """
        ETF数据
        """
        fund_etf_spot_em_df = ak.fund_etf_spot_em()
        pd_etf = fund_etf_spot_em_df[
            [
                "代码",
                "名称",
                "开盘价",
                "最新价",
                "最高价",
                "最低价",
                "成交量",
                "总市值",
            ]
        ].copy()
        pd_etf = pd_etf[pd_etf["最新价"] > 0]
        pd_etf = pd_etf.rename(
            columns={
                "代码": "symbol",
                "名称": "name",
                "开盘价": "open",
                "最新价": "close",
                "最高价": "high",
                "最低价": "low",
                "成交量": "volume",
                "总市值": "total_value",
            }
        )
        # 给 symbol 字段增加前缀（如 "ETF"）
        pd_etf["symbol"] = "ETF" + pd_etf["symbol"]
        pd_etf["pe"] = "-"
        pd_etf["date"] = date
        pd_etf.to_csv(file_name_d, mode="a", index=True, header=False)

        # 10年期国债收益率
        url = "https://quote.eastmoney.com/center/api/qqzq.js?"
        res = requests.get(url, proxies=self.proxy).text
        lines = res.strip().split("\n")
        data = []
        for line in lines:
            fields = line.split(",")
            data.append(fields)

        filtered_rows = []
        for row in data[2:]:
            if len(row) > 2:  # 确保parts列表至少有两个元素
                code = row[1]
                if code == "CN10Y_B":
                    code = row[1]
                    name = row[2]
                    date = row[3]
                    new = row[5]

                    # 将这些字段添加到一个新的列表中，准备写入CSV文件
                    filtered_rows.append([code, name, date, new])

        output_filename = FileInfo(trade_date, "cn").get_file_path_gz
        with open(output_filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # 可选：写入表头
            writer.writerow(["code", "name", "date", "new"])
            # 写入数据行
            writer.writerows(filtered_rows)
