#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import time
import random
import requests
import pandas as pd
from datetime import datetime
import os
from utility.fileinfo import FileInfo
import csv
import math
from utility.toolkit import ToolKit
import concurrent.futures


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

        # A股/美股最新股票数据获取
        https://92.push2.eastmoney.com/api/qt/clist/get?pn=i&pz=100&po=1&np=1&ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&fid=f12&fs=m:mkt_code&fields=f2,f5,f9,f12,f14,f15,f16,f17,f20
        # A股/美股历史数据获取
        https://92.push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.600066&ut=&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20211101&end=20211115&smplmt=755&lmt=1000000
        """
        self.__url_list = "http://92.push2.eastmoney.com/api/qt/clist/get"
        self.__url_history = "http://92.push2his.eastmoney.com/api/qt/stock/kline/get"
        # 不配置proxy，klines有时候返回为空，但response status是正常的
        # self.item = "http://183.6.44.203:1081"
        # self.item = "http://27.156.105.51:7788"
        # self.item = "http://101.66.195.41:8085"
        self.item = "http://39.101.132.59:8443"
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

    def get_total_pages(self, mkt_code):
        params = {
            "pn": "1",
            "pz": "100",
            "po": "1",
            "np": "1",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": f"m:{mkt_code}",
            "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
        }

        res = requests.get(
            self.__url_list, params=params, proxies=self.proxy, headers=self.headers
        ).json()

        total_page_no = math.ceil(res["data"]["total"] / 100)
        print(f"市场代码: {mkt_code}, 总页数: {total_page_no}")
        return total_page_no

    def get_stock_list(self, market, cache_path=None):
        # 如果缓存文件存在，直接读取
        # 美股："./usstockinfo/us_stock_list_cache.csv"
        # A股："./cnstockinfo/cn_stock_list_cache.csv"
        if os.path.exists(cache_path):
            print(f"读取股票列表缓存: {cache_path}")
            return pd.read_csv(cache_path).to_dict(orient="records")

        """
        市场代码：
        105:纳斯达克 NASDAQ
        106:纽交所 NYSE
        107:美国交易所 AMEX
        1: 上交所
        0: 深交所
        """
        dict = {}
        list = []
        if market == "us":
            mkt_code = ["105", "106", "107"]
        elif market == "cn":
            mkt_code = ["0", "1"]
        for m in mkt_code:
            max_page = self.get_total_pages(m)
            tool = ToolKit(f"市场代码{m}，下载中...")
            for i in range(1, max_page + 1):
                tool.progress_bar(max_page, i)
                params = {
                    "pn": f"{i}",
                    "pz": "100",
                    "po": "1",
                    "np": "1",
                    "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                    "fltt": "2",
                    "invt": "2",
                    "fid": "f12",
                    "fs": f"m:{m}",
                    "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
                }
                time.sleep(random.uniform(1, 3))
                for _ in range(2):  # 重试2次
                    try:
                        res = requests.get(
                            self.__url_list,
                            params=params,
                            proxies=self.proxy,
                            headers=self.headers,
                        ).json()
                        break  # 成功就跳出循环
                    except Exception:
                        print("请求失败，正在重试...", _)
                        continue  # 失败就继续循环
                else:
                    if (
                        res.get("rc") != 0
                        or not res.get("data")
                        or not res["data"].get("diff")
                    ):
                        print(f"返回值为：{res}")
                        raise [res]  # 返回错误信息
                for i in res["data"]["diff"]:
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
                    if m in ["105", "106", "107"]:
                        dict = {"symbol": i["f12"], "mkt_code": m}
                    elif m in {"0", "1"}:
                        prefix = (
                            "ETF" if "ETF" in i["f14"] else {"0": "SZ", "1": "SH"}[m]
                        )
                        dict = {"symbol": prefix + i["f12"], "mkt_code": m}
                    list.append(dict)
        # 保存到本地缓存
        pd.DataFrame(list).to_csv(cache_path, index=False)
        return list

    def get_daily_gz_info(self, market, trade_date):
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
                if (market == "us" and code == "US10Y_B") or (
                    market == "cn" and code == "CN10Y_B"
                ):
                    code = row[1]
                    name = row[2]
                    date = row[3]
                    new = row[5]

                    # 将这些字段添加到一个新的列表中，准备写入CSV文件
                    filtered_rows.append([code, name, date, new])

        output_filename = FileInfo(trade_date, market).get_file_path_gz
        with open(output_filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # 可选：写入表头
            writer.writerow(["code", "name", "date", "new"])
            # 写入数据行
            writer.writerows(filtered_rows)

    def get_daily_stock_info(self, market, trade_date):
        dict = {}
        list = []
        if market == "us":
            mkt_code = ["105", "106", "107"]
        elif market == "cn":
            mkt_code = ["0", "1"]
        for m in mkt_code:
            max_page = self.get_total_pages(m)
            tool = ToolKit(f"市场代码{m}，共{max_page}页")
            for i in range(1, max_page + 1):
                tool.progress_bar(max_page, i)
                params = {
                    "pn": f"{i}",
                    "pz": "100",
                    "po": "1",
                    "np": "1",
                    "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                    "fltt": "2",
                    "invt": "2",
                    "fid": "f12",
                    "fs": f"m:{m}",
                    "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
                }
                time.sleep(random.uniform(1, 3))
                res = requests.get(
                    self.__url_list,
                    params=params,
                    proxies=self.proxy,
                    headers=self.headers,
                ).json()
                if (
                    res.get("rc") != 0
                    or not res.get("data")
                    or not res["data"].get("diff")
                ):
                    return [res]  # 返回错误信息
                """         
                f12: 股票代码, f14: 公司名称, f2: 最新报价, f3: 涨跌幅, f4: 涨跌额, f5: 成交量, f6: 成交额
                f7: 振幅,f9: 市盈率, f15: 最高, f16: 最低, f17: 今开, f18: 昨收, f20:总市值 f21:流通值
                f12: symbol, f14: name, f2: close, f3: chg, f4: change, f5: volume, f6: turnover
                f7: amplitude,f9: pe, f15: high, f16: low, f17: open, f18: preclose, f20: total_value, f21: circulation_value
                """
                for i in res["data"]["diff"]:
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
                    if m in ["105", "106", "107"]:
                        symbol_val = i["f12"]
                    elif m in {"0", "1"}:
                        prefix = (
                            "ETF" if "ETF" in i["f14"] else {"0": "SZ", "1": "SH"}[m]
                        )
                        symbol_val = prefix + i["f12"]
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
        """ 获取美股数据文件地址 """
        file_name_d = FileInfo(trade_date, market).get_file_path_latest
        """ 每日一个文件，根据交易日期创建 """
        if os.path.exists(file_name_d):
            os.remove(file_name_d)
        """ 将list转化为dataframe，并且存储为csv文件，带index和header """
        df = pd.DataFrame(list)
        date = datetime.strptime(trade_date, "%Y%m%d")
        df["date"] = date
        df.to_csv(file_name_d, mode="w", index=True, header=True)

        self.get_daily_gz_info(market, trade_date)

    def get_his_stock_info(
        self, mkt_code, symbol, start_date, end_date, cache_path=None
    ):
        """
        历史数据URL：可以是http或者https
        https://92.push2his.eastmoney.com/api/qt/stock/kline/get?secid=106.BABA&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20210101&end=20500101&smplmt=755&lmt=1000000

        历史数据返回字段列表：
        date,open,close,high,low,volume,turnover,amplitude,chg,change,换手率

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

        params = {
            "secid": f"{mkt_code}.{symbol}",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56",
            "klt": "101",
            "fqt": "1",
            "beg": start_date,
            "end": end_date,
            "smplmt": "755",
            "lmt": "1000000",
        }

        """ 请求url, 获取数据response """
        try:
            res = requests.get(
                self.__url_history,
                params=params,
                proxies=self.proxy,
                headers=self.headers,
            ).json()
        except requests.RequestException as e:
            return [res]
        if res.get("rc") != 0 or not res.get("data"):
            return [res]  # 返回错误信息

        # 获取klines数据
        klines = res.get("data", {}).get("klines", [])
        if not klines:
            if cache_path is not None:
                # 记录 symbol 到空klines缓存文件
                with open(cache_path, "a") as f:
                    f.write(f"{symbol}\n")
                return []  # 返回空列表

        # 提取公司名称
        name = res.get("data", {}).get("name", "")
        print("开始处理：", name)

        # 处理 klines 数据
        list = []
        if str(mkt_code) in ["105", "106", "107"]:
            symbol_val = symbol
        elif str(mkt_code) in ["0", "1"]:
            prefix = "ETF" if "ETF" in name else {"0": "SZ", "1": "SH"}[mkt_code]
            symbol_val = prefix + symbol
        for i in klines:
            """
            历史数据返回字段列表：
            open,close,high,low,volume,date
            """
            if (
                i.split(",")[1] == "-"
                or i.split(",")[2] == "-"
                or i.split(",")[3] == "-"
                or i.split(",")[4] == "-"
                or i.split(",")[5] == "-"
            ):
                continue

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

    def get_his_stock_info_list(
        self,
        market,
        start_date,
        end_date,
        file_path,
        empty_klines_cache_path=None,
        stock_list_cache_path=None,
    ):
        paths = {
            "us": {
                "empty_klines": "./usstockinfo/us_empty_klines_cache.csv",
                "stock_list": "./usstockinfo/us_stock_list_cache.csv",
            },
            "cn": {
                "empty_klines": "./cnstockinfo/cn_empty_klines_cache.csv",
                "stock_list": "./cnstockinfo/cn_stock_list_cache.csv",
            },
        }

        empty_klines_cache_path = (
            empty_klines_cache_path or paths[market]["empty_klines"]
        )
        stock_list_cache_path = stock_list_cache_path or paths[market]["stock_list"]
        """获取股票列表"""
        tickinfo = self.get_stock_list(market, cache_path=stock_list_cache_path)
        # tickinfo = [{"symbol": "BABA", "mkt_code": "106"}]
        # 断点续爬：读取已存在的symbol
        done_symbols = set()
        if os.path.exists(file_path):
            df_exist = pd.read_csv(file_path)
            if "symbol" in df_exist.columns:
                done_symbols = set(df_exist["symbol"].astype(str).unique())

        # 读取 klines 为空的 symbol
        empty_klines_symbols = set()
        if os.path.exists(empty_klines_cache_path):
            with open(empty_klines_cache_path, "r") as f:
                empty_klines_symbols = set(line.strip() for line in f if line.strip())

        # 过滤已完成和klines为空的symbol
        tickinfo = [
            item
            for item in tickinfo
            if item["symbol"] not in done_symbols
            and item["symbol"] not in empty_klines_symbols
        ]
        print(
            f"总共{len(tickinfo)}只股票，已完成{len(done_symbols)}，klines为空{len(empty_klines_symbols)}，待处理{len(tickinfo)}"
        )
        batch0 = len(done_symbols)
        """ 多线程获取，多个worker * 一个batch为10"""
        batch_size = 10
        batch_count = 0
        tool = ToolKit("历史数据下载")

        with open(file_path, "a") as csvfile:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                for h in range(0, len(tickinfo), batch_size):
                    batch_count += 1
                    batch_list = []
                    futures = []
                    for t in range(1, batch_size + 1):
                        """休眠, 避免IP Block"""
                        time.sleep(random.uniform(1, 3))
                        index = h + t - 1
                        if index < len(tickinfo):
                            future = executor.submit(
                                self.get_his_stock_info,
                                tickinfo[index]["mkt_code"],
                                tickinfo[index]["symbol"],
                                start_date,
                                end_date,
                                empty_klines_cache_path,
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
                            columns = [
                                "symbol",
                                "name",
                                "open",
                                "close",
                                "high",
                                "low",
                                "volume",
                                "date",
                            ]
                            df = pd.DataFrame(batch_list)
                            df = df[columns]
                            df = df.dropna(how="all")  # 过滤全为空的行
                            df.to_csv(
                                csvfile,
                                mode="a",
                                index=True,
                                header=(h == 0 and batch0 == 0),
                            )
                        except IOError:
                            pass
