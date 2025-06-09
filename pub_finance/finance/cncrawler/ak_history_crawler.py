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
import concurrent.futures
import akshare as ak
import os
import math
from utility.em_stock_uti import EMWebCrawlerUti

"""
东方财经的A股日K数据获取接口：

市场代码：
0: 深证/创业板/新三板/ SZ
1: 上证/科创板		SH

股票列表URL：
http://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery&pn=1&pz=20000&po=1&
np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:0&fields=f2,f3,f4,f5,f6,f7,f12,f15,f16,f17,f18&_=1636942751421

历史数据URL：
https://92.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery&
secid=1.600066&ut=&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101
&fqt=1&beg=20211101&end=20211115&smplmt=755&lmt=1000000&_=1636942751421

历史数据返回字段列表：
date,open,close,high,low,volume,turnover,amplitude,chg,change,换手率

"""


class AKCNHistoryDataCrawler:
    def __init__(self):
        self.proxy = {
            "http": "http://120.25.1.15:7890",
            "https": "http://120.25.1.15:7890",
        }
        # self.proxy = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Host": "push2.eastmoney.com",
            "Accept-Encoding": "gzip, deflate",
            "Accept": "*/*",
            "Accept-language": "zh-CN,zh;q=0.9",
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "Connection": "close",
        }

    """
    akshare获取A股历史数据
    """

    def get_cn_stock_history_ak(self, start_date, end_date, file_path):
        """
        获取A股历史数据，上海市场
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
        tool = ToolKit("历史数据下载")
        list = []
        for index, row in pd_stock_sh.iterrows():
            symbol = "sh" + row["symbol"]
            # 获取历史数据-东方财富
            # stock_zh_a_hist_df = ak.stock_zh_a_hist(
            #     symbol=symbol,
            #     period="daily",
            #     start_date=start_date,
            #     end_date=end_date,
            #     adjust="qfq",
            # )
            # 获取历史数据-新浪
            try:
                stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                )
                for i, r in stock_zh_a_daily_qfq_df.iterrows():
                    dict = {
                        "symbol": symbol.upper(),
                        "name": row["name"],
                        "open": r["open"],
                        "close": r["close"],
                        "high": r["high"],
                        "low": r["low"],
                        "volume": r["volume"],
                        "date": r["date"],
                    }
                    list.append(dict)
            except Exception as e:
                print("获取历史数据失败：", symbol, e)
                continue
            tool.progress_bar(len(pd_stock_sh), index)
        df = pd.DataFrame(list)
        df.to_csv(
            file_path,
            mode="a",
            index=True,
            header=True,
        )
        """
        获取A股历史数据, 深圳市场
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
        tool = ToolKit("历史数据下载")
        list = []
        for index, row in pd_stock_sz.iterrows():
            symbol = "sz" + row["symbol"]
            # 获取历史数据-东方财富
            # stock_zh_a_hist_df = ak.stock_zh_a_hist(
            #     symbol=symbol,
            #     period="daily",
            #     start_date=start_date,
            #     end_date=end_date,
            #     adjust="qfq",
            # )
            # 获取历史数据-新浪
            try:
                stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                )
                for i, r in stock_zh_a_daily_qfq_df.iterrows():
                    dict = {
                        "symbol": symbol.upper(),
                        "name": row["name"],
                        "open": r["open"],
                        "close": r["close"],
                        "high": r["high"],
                        "low": r["low"],
                        "volume": r["volume"],
                        "date": r["date"],
                    }
                    list.append(dict)
            except Exception as e:
                print("获取历史数据失败：", symbol, e)
                continue
            tool.progress_bar(len(pd_stock_sz), index)
        df = pd.DataFrame(list)
        df.to_csv(
            file_path,
            mode="a",
            index=True,
            header=False,
        )
        """
        ETF历史数据
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
        tool = ToolKit("历史数据下载")
        list = []
        for index, row in pd_etf.iterrows():
            symbol = row["symbol"]
            try:
                time.sleep(1 + random.uniform(1, 3))
                fund_etf_hist_em_df = ak.fund_etf_hist_em(
                    symbol=symbol,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                )
                for i, r in fund_etf_hist_em_df.iterrows():
                    dict = {
                        "symbol": "ETF" + symbol.upper(),
                        "name": row["name"],
                        "open": r["开盘"],
                        "close": r["收盘"],
                        "high": r["最高"],
                        "low": r["最低"],
                        "volume": r["成交量"],
                        "date": r["日期"],
                    }
                    list.append(dict)
            except Exception as e:
                print("获取ETF历史数据失败：", symbol, e)
                continue
            tool.progress_bar(len(pd_etf), index)
        df = pd.DataFrame(list)
        df.to_csv(
            file_path,
            mode="a",
            index=True,
            header=False,
        )
