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


class EMCNHistoryDataDownload:
    def __init__(self):
        self.proxy = {
            "http": "http://203.19.38.114:1080",
            "https": "http://203.19.38.114:1080",
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
        self.cookies = {
            "qgqp_b_id": "378b9e6080d1d273d0660a6cf2e3f3c4",
            "st_pvi": "19026945941901",
            "st_si": "33255977060012",
            "st_asi": "delete",
            "st_inirUrl": "https://quote.eastmoney.com",
        }

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
            "&pn=i&pz=200&po=1&np=1&ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&fid=f12&fs=m:mkt_code&fields=f2,f5,f9,f12,f14,f15,f16,f17,f20&_=unix_time"
        )
        for mkt_code in ["0", "1"]:
            """请求url，获取数据response"""
            for i in range(1, 500):
                url_re = (
                    url.replace("unix_time", str(current_timestamp))
                    .replace("mkt_code", mkt_code)
                    .replace("pn=i", "pn=" + str(i))
                )
                time.sleep(random.uniform(1, 2))
                print("url: ", url_re)
                """ 获取数据 """
                res = requests.get(
                    url_re,
                    proxies=self.proxy,
                    headers=self.headers,
                    cookies=self.cookies,
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

    def get_his_tick_info(self, mkt_code, symbol, start_date, end_date):
        """
        历史数据URL：
        https://92.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery&secid=0.300063&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20210101&end=20500101&smplmt=755&lmt=1000000&_=1636002112627

        历史数据返回字段列表：
        date,open,close,high,low,volume,turnover,amplitude,chg,change,换手率
        """
        """ url里需要传递unixtime当前时间戳 """
        current_timestamp = int(time.mktime(datetime.now().timetuple()))

        """     
        beg: 开始日期 例如 20200101
        end: 结束日期 例如 20200201
        klt: k线间距 默认为 101 即日k
                klt:1 1 分钟
                klt:5 5 分钟
                klt:101 日
                klt:102 周
            fqt: 复权方式
                不复权 : 0
                前复权 : 1
                后复权 : 2
        """

        url = (
            "https://push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery"
            "&secid=mkt_code.symbol&ut=fa5fd1943c7b386f172d6893dbfba10b"
            "&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56"
            "&klt=101&fqt=1&beg=start_date&end=end_date&smplmt=755&lmt=1000&_=unix_time"
        )

        url_re = (
            url.replace("mkt_code", mkt_code)
            .replace("symbol", symbol)
            .replace("start_date", start_date)
            .replace("end_date", end_date)
            .replace("unix_time", str(current_timestamp))
        )

        res = requests.get(
            url_re, proxies=self.proxy, headers=self.headers, cookies=self.cookies
        ).text
        """ 抽取公司名称 """
        name = re.search('\\"name\\":\\"(.*?)\\",', res).group(1)
        print("开始处理：", name)
        """ 替换成valid json格式 """
        res_p = re.sub("\\].*", "]", re.sub(".*:\\[", "[", res, 1), 1)
        json_object = json.loads(res_p)
        dict = {}
        list = []
        if mkt_code == "0":
            market = "SZ"
        else:
            market = "SH"
        for i in json_object:
            """
            历史数据返回字段列表：
            date,open,close,high,low,volume,turnover,amplitude,chg,change,换手率
            """
            if (
                i.split(",")[1] == "-"
                or i.split(",")[2] == "-"
                or i.split(",")[3] == "-"
                or i.split(",")[4] == "-"
                or i.split(",")[5] == "-"
            ):
                continue
            if "ETF" in i["f14"]:
                symbol_val = "ETF" + symbol
            else:
                symbol_val = market + symbol
            dict = {
                "symbol": symbol_val,
                "name": name,
                "open": i.split(",")[1],
                "close": i.split(",")[2],
                "high": i.split(",")[3],
                "low": i.split(",")[4],
                "volume": i.split(",")[5],
                "date": i.split(",")[0],
            }
            list.append(dict)
        return list

    def set_his_tick_info_to_csv(self, start_date, end_date, file_path):
        """获取股票列表"""
        tickinfo = self.get_cn_stock_list()
        """ 多线程获取，每次步长为3，为3线程 """
        batch_size = 10  # Number of tickinfo items to process in each batch
        batch_count = 0
        tool = ToolKit("历史数据下载")

        with open(file_path, "a") as csvfile:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                for h in range(0, len(tickinfo), batch_size):
                    """休眠, 避免IP Block"""
                    time.sleep(1 + random.uniform(1, 3))
                    batch_count += 1
                    batch_list = []
                    futures = []
                    for t in range(1, batch_size + 1):
                        index = h + t - 1
                        if index < len(tickinfo):
                            future = executor.submit(
                                self.get_his_tick_info,
                                tickinfo[index]["mkt_code"],
                                tickinfo[index]["symbol"],
                                start_date,
                                end_date,
                            )
                            futures.append(future)
                    for future in concurrent.futures.as_completed(futures):
                        list1 = future.result()
                        if list1 is not None:  # Only append if list1 is not empty
                            batch_list.extend(list1)

                    tool.progress_bar(len(tickinfo), h)
                    # Write batch data to CSV file
                    if len(batch_list) > 0:  # Check if batch_list is not empty
                        try:
                            df = pd.DataFrame(batch_list)
                            df.to_csv(
                                csvfile,
                                mode="a",
                                index=True,
                                header=(h == 0),
                            )
                        except IOError:
                            pass

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
        # 过滤“最新价”大于0的数据
        pd_stock_sh = pd_stock_sh[pd_stock_sh["最新价"] > 0]

        # 重命名列名
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
        # 过滤“最新价”大于0的数据
        pd_stock_sz = pd_stock_sz[pd_stock_sz["最新价"] > 0]

        # 重命名列名
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
        # 重命名列名
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


# 历史数据起始时间，结束时间
# 文件名称定义

emc = EMCNHistoryDataDownload()
start_date = "20240101"
end_date = "20250603"
file_path = "./cnstockinfo/stock_20250603.csv"
emc.set_his_tick_info_to_csv(start_date, end_date, file_path)
# emc.get_cn_stock_history_ak(start_date, end_date, file_path)
