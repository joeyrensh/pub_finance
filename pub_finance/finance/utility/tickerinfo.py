#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
import numpy as np
from utility.fileinfo import FileInfo
from utility.toolkit import ToolKit
import multiprocessing
import gc
import datetime


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
        if self.market in ("us", "cn"):
            # 预定义列的数据类型
            column_dtypes = {
                "symbol": str,
                "open": np.float32,
                "close": np.float32,
                "high": np.float32,
                "low": np.float32,
                "volume": np.float64,
                "total_value": np.float64,
                "date": str,
            }

            dfs = []
            for file in self.files:
                # 读取数据
                df = pd.read_csv(file)

                # 检查并添加 total_value 列（如果不存在）
                if "total_value" not in df.columns:
                    df["total_value"] = 0.0

                # 选择我们需要的列并转换数据类型
                df = df[list(column_dtypes.keys())].astype(column_dtypes)
                dfs.append(df)

            df_all = pd.concat(dfs, ignore_index=True)
            df_all.drop_duplicates(
                subset=["symbol", "date"], keep="first", inplace=True
            )

            # 获取近180天的日期
            trade_date_dt = datetime.datetime.strptime(str(self.trade_date), "%Y%m%d")
            date_threshold = trade_date_dt - datetime.timedelta(days=120)

            # 将 datetime 对象转换回字符串格式 (YYYYMMDD)
            date_threshold_str = date_threshold.strftime("%Y-%m-%d")

            # 使用相同格式的字符串进行筛选
            df_recent = df_all[df_all["date"] >= date_threshold_str]
            dfs, df_all, df = None, None, None
            gc.collect()

        tickers = []

        if self.market == "us":
            # 美股筛选条件
            # 条件1: 市值超过100亿的股票直接入选
            large_cap_cond = (
                (df_recent["total_value"] >= 10000000000)
                & (df_recent["close"] > 1)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
                & ((df_recent["close"] - df_recent["open"]) / df_recent["open"] < 2)
                & (
                    df_recent["close"] * df_recent["volume"]
                    >= 0.015 * df_recent["total_value"]
                )
            )
            large_cap_symbols = (
                df_recent.loc[large_cap_cond, "symbol"].unique().tolist()
            )
            tickers.extend(large_cap_symbols)

            # 条件2: 市值小于100亿但超过10个交易日成交额大于5%市值的股票
            small_cap_cond = (
                (df_recent["total_value"] < 10000000000)
                & (df_recent["total_value"] > 2000000000)
                & (df_recent["close"] > 1)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
                & ((df_recent["close"] - df_recent["open"]) / df_recent["open"] < 2)
                & (
                    df_recent["close"] * df_recent["volume"]
                    >= 0.05 * df_recent["total_value"]
                )
            )

            # 筛选出满足条件的行
            filtered_df = df_recent[small_cap_cond]

            # 计算每个股票满足条件的次数
            symbol_counts = filtered_df["symbol"].value_counts()

            # 只选择出现10次或以上的股票
            frequent_symbols = symbol_counts[symbol_counts >= 10].index.tolist()
            tickers.extend(frequent_symbols)

        elif self.market == "cn":
            # A股筛选条件
            # 条件1: 市值超过100亿的股票直接入选
            large_cap_cond = (
                (df_recent["total_value"] >= 20000000000)
                & (df_recent["close"] > 1)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
                & (df_recent["close"] * df_recent["volume"] * 100 >= 1000000000)
            )
            large_cap_symbols = (
                df_recent.loc[large_cap_cond, "symbol"].unique().tolist()
            )
            tickers.extend(large_cap_symbols)

            # 条件2: 市值小于100亿但超过10个交易日成交额大于5%市值的股票
            small_cap_cond = (
                (df_recent["total_value"] < 20000000000)
                & (df_recent["total_value"] > 5000000000)
                & (df_recent["close"] > 1)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
                & (df_recent["close"] * df_recent["volume"] * 100 >= 300000000)
            )

            # 筛选出满足条件的行
            filtered_df = df_recent[small_cap_cond]

            # 计算每个股票满足条件的次数
            symbol_counts = filtered_df["symbol"].value_counts()

            # 只选择出现10次或以上的股票
            frequent_symbols = symbol_counts[symbol_counts >= 10].index.tolist()
            tickers.extend(frequent_symbols)

        elif self.market == "us_special":
            # 特殊美股筛选条件
            tickers = self.get_special_us_stock_list_180d()

        # 去重并返回
        df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
        valid_symbols = df_o["symbol"].unique()
        stock_list_with_industry = [s for s in list(set(tickers)) if s in valid_symbols]
        print(f"满足条件的股票数量: {len(stock_list_with_industry)}")
        return stock_list_with_industry

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
        if self.market in ("us", "us_special"):
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

    def get_special_us_stock_list_180d(self):
        """
        获取近180天内，任意一天满足：
        1. total_value < 10亿
        2. close > 1元
        3. close * volume / total_value >= 0.05
        的股票代码列表
        """
        # 预定义列的数据类型
        column_dtypes = {
            "symbol": str,
            "open": np.float32,
            "close": np.float32,
            "high": np.float32,
            "low": np.float32,
            "volume": np.float64,
            "total_value": np.float64,
            "date": str,
        }

        dfs = []
        for file in self.files:
            # 读取数据
            df = pd.read_csv(file)

            # 检查并添加 total_value 列（如果不存在）
            if "total_value" not in df.columns:
                df["total_value"] = 0.0

            # 选择我们需要的列并转换数据类型
            df = df[list(column_dtypes.keys())].astype(column_dtypes)
            dfs.append(df)

        df_all = pd.concat(dfs, ignore_index=True)
        df_all.drop_duplicates(subset=["symbol", "date"], keep="first", inplace=True)
        # 2. 取近180天的日期
        trade_date_dt = datetime.datetime.strptime(str(self.trade_date), "%Y%m%d")
        date_threshold = trade_date_dt - datetime.timedelta(days=120)

        # 将 datetime 对象转换回字符串格式 (YYYYMMDD)
        date_threshold_str = date_threshold.strftime("%Y-%m-%d")

        # 使用相同格式的字符串进行筛选
        df_recent = df_all[df_all["date"] >= date_threshold_str]

        # 3. 条件筛选
        cond = (
            (df_recent["total_value"] < 2000000000)
            & (df_recent["total_value"] > 100000000)
            & (df_recent["close"] > 1)
            & (
                df_recent["close"] * df_recent["volume"]
                >= 0.05 * df_recent["total_value"]
            )
        )
        # 筛选出满足条件的行
        filtered_df = df_recent[cond]

        # 计算每个股票满足条件的次数
        symbol_counts = filtered_df["symbol"].value_counts()

        # 只选择出现10次或以上的股票
        frequent_symbols = symbol_counts[symbol_counts >= 10].index.tolist()

        # 更新 stock_list
        stock_list = frequent_symbols

        dfs, df_all, df_recent, df = None, None, None, None
        gc.collect()
        return stock_list

    def get_special_us_backtrader_data_feed(self):
        tickers = self.get_special_us_stock_list_180d()
        tickers_clean = [
            t for t in tickers if isinstance(t, str) and t != "nan" and t != ""
        ]

        his_data = self.get_history_data().groupby(by="symbol")
        t = ToolKit("读取历史数据文件")
        list_results = []

        with multiprocessing.Pool(processes=4) as pool:
            results = [
                pool.apply_async(self.reconstruct_dataframe, (his_data.get_group(i), i))
                for i in tickers_clean
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
