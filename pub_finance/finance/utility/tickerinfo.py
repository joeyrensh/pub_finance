#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
import numpy as np
from utility.fileinfo import FileInfo
from utility.toolkit import ToolKit
import multiprocessing
import gc


""" 组合每日股票数据为一个dataframe """


class TickerInfo:
    def __init__(self, trade_date, market) -> None:
        """获取市场代码"""
        self.market = market
        """ 获取交易日期 """
        self.trade_date = trade_date
        """ 获取文件列表 """
        file = FileInfo(trade_date, market)
        """ 获取交易日当天日数据文件 """
        self.file_day = file.get_file_path_latest
        """ 获取截止交易日当天历史日数据文件列表 """
        self.files = file.get_file_list
        """ 获取行业板块文件路径 """
        self.file_industry = file.get_file_path_industry

    """ 获取股票代码列表 """

    def get_stock_list(self):
        tickers = list()
        df = pd.read_csv(
            self.file_day,
            usecols=[
                "symbol",
                "name",
                "open",
                "close",
                "high",
                "low",
                "volume",
                "total_value",
                "pe",
                "date",
            ],
        )
        df.drop_duplicates(subset=["symbol", "date"], keep="first", inplace=True)
        df.sort_values(by=["symbol"], ascending=True, inplace=True)
        """ 匹配行业信息 """
        df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
        df_n = pd.merge(df, df_o, how="inner", on="symbol")
        if self.market == "us":
            for index, i in df_n.iterrows():
                """
                美股：10亿以上的公司
                A股：100亿以上的公司
                """
                if (
                    (
                        (
                            float(i["close"]) * float(i["volume"]) >= 100000000
                            and float(i["total_value"]) >= 1000000000
                            and float(i["total_value"]) < 10000000000
                        )
                        or float(i["total_value"]) >= 10000000000
                    )
                    and float(i["close"]) > 1
                    and float(i["close"]) < 10000
                    and float(i["open"]) > 0
                    and float(i["high"]) > 0
                    and float(i["low"]) > 0
                ):
                    tickers.append(i["symbol"])
        elif self.market == "cn":
            for index, i in df_n.iterrows():
                if (
                    (
                        (
                            float(i["close"]) * float(i["volume"]) * 100 >= 200000000
                            and float(i["total_value"]) >= 5000000000
                            and float(i["total_value"]) < 10000000000
                        )
                        or float(i["total_value"]) >= 10000000000
                    )
                    and float(i["close"]) > 1
                    and float(i["close"]) < 10000
                    and float(i["open"]) > 0
                    and float(i["high"]) > 0
                    and float(i["low"]) > 0
                ):
                    tickers.append(i["symbol"])
        return tickers

    """ 获取最新一天股票数据 """

    def get_stock_data_for_day(self):
        df = pd.read_csv(
            self.file_day,
            usecols=[
                "symbol",
                "name",
                "open",
                "close",
                "high",
                "low",
                "volume",
                "total_value",
                "pe",
                "date",
            ],
        )
        df.drop_duplicates(subset=["symbol", "date"], keep="first", inplace=True)
        """ 匹配行业信息 """
        df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
        df_n = pd.merge(df, df_o, how="inner", on="symbol")
        return df_n

    """ 获取历史数据 """

    def get_history_data(self):
        dic = {}
        for j in range(len(self.files)):
            df = pd.read_csv(
                self.files[j],
                usecols=[
                    "symbol",
                    "open",
                    "close",
                    "high",
                    "low",
                    "volume",
                    "date",
                ],
                dtype={
                    "symbol": str,
                    "open": np.float32,
                    "close": np.float32,
                    "high": np.float32,
                    "low": np.float32,
                    "volume": np.float64,
                    "date": str,
                },
            )
            df.drop_duplicates(subset=["symbol", "date"], keep="first", inplace=True)
            dic[j] = df
        df = pd.concat(list(dic.values()), ignore_index=True)
        df.sort_values(by=["symbol", "date"], ascending=[True, True], inplace=True)
        return df

    """ 
    获取backtrader所需的datafeed
    将历史数据按照backtrader datafeed格式重构
    """

    def get_backtrader_data_feed(self):
        tickers = self.get_stock_list()
        his_data = self.get_history_data().groupby(by="symbol")
        t = ToolKit("读取历史数据文件")
        list_results = []

        with multiprocessing.Pool(processes=4) as pool:
            results = [
                pool.apply_async(self.reconstruct_dataframe, (his_data.get_group(i), i))
                for i in tickers
            ]
            for idx, result in enumerate(results):
                df = result.get()
                if not df.empty:
                    list_results.append(df)
                t.progress_bar(len(results), idx)

        # 手动清理不再需要的对象
        his_data = None
        gc.collect()

        return list_results

    """ 重构dataframe封装 """

    def reconstruct_dataframe(self, group_obj, i):
        """
        过滤历史数据不完整的股票
        小于120天的股票暂时不进入回测列表
        """
        if len(group_obj) < 241:
            return pd.DataFrame()
        """ 适配BackTrader数据结构 """
        if self.market == "us":
            market = 1
        elif self.market == "cn":
            market = 2
        df_copy = pd.DataFrame(
            {
                "open": group_obj["open"]
                .fillna(0)
                .values.astype("float32")
                .round(decimals=2),
                "close": group_obj["close"]
                .fillna(0)
                .values.astype("float32")
                .round(decimals=2),
                "high": group_obj["high"]
                .fillna(0)
                .values.astype("float32")
                .round(decimals=2),
                "low": group_obj["low"]
                .fillna(0)
                .values.astype("float32")
                .round(decimals=2),
                "volume": group_obj["volume"].fillna(0).values.astype("int64"),
                "symbol": i,
                "market": market,
                "datetime": pd.to_datetime(group_obj["date"].values, format="%Y-%m-%d"),
            },
        ).sort_values(by=["datetime"])
        return df_copy

    def get_backtrader_data_feed_testonly(self, stocklist):
        tickers = stocklist
        his_data = self.get_history_data().groupby(by="symbol")
        t = ToolKit("加载历史数据")
        """ 存放策略结果 """
        list = []
        results = []
        """ 创建多进程 """
        pool = multiprocessing.Pool(processes=4)
        for i in tickers:
            """
            适配BackTrader数据结构
            每个股票数据为一组
            """
            group_obj = his_data.get_group(i)
            result = pool.apply_async(self.reconstruct_dataframe, (group_obj, i))
            results.append(result)
        """ 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用 """
        pool.close()
        """ 等待进程池中的所有进程执行完毕 """
        pool.join()

        """ 获取进程内数据 """
        for dic in results:
            if len(dic.get()) > 0:
                list.append(dic.get())
            t.progress_bar(len(results), results.index(dic))
        """ 垃圾回收 """
        his_data = None
        results = None
        gc.collect()
        return list

    def get_etf_list(self):
        tickers = list()
        df = pd.read_csv(
            self.file_day,
            usecols=[
                "symbol",
                "name",
                "open",
                "close",
                "high",
                "low",
                "volume",
                "total_value",
                "pe",
                "date",
            ],
        )
        df.drop_duplicates(subset=["symbol", "date"], keep="first", inplace=True)
        df.sort_values(by=["symbol"], ascending=True, inplace=True)
        if self.market == "cn":
            for index, i in df.iterrows():
                if (
                    "ETF" in str(i["name"]).upper()
                    # and float(i["close"]) * float(i["volume"]) * 100 >= 500000000
                    and float(i["total_value"]) >= 5000000000
                ):
                    tickers.append(i["symbol"])
        return tickers

    def get_etf_backtrader_data_feed(self):
        tickers = self.get_etf_list()
        his_data = self.get_history_data().groupby(by="symbol")
        t = ToolKit("读取历史数据文件")
        list_results = []

        with multiprocessing.Pool(processes=4) as pool:
            results = [
                pool.apply_async(self.reconstruct_dataframe, (his_data.get_group(i), i))
                for i in tickers
            ]
            for idx, result in enumerate(results):
                df = result.get()
                if not df.empty:
                    list_results.append(df)
                t.progress_bar(len(results), idx)

        # 手动清理不再需要的对象
        his_data = None
        gc.collect()

        return list_results
