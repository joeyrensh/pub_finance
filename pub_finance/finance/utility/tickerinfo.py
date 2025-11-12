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
        """ 获取截止交易日当天历史国债数据文件列表 """
        self.files_gz = file.get_gz_file_list

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
            df_all["date"] = pd.to_datetime(
                df_all["date"], errors="coerce", format="%Y-%m-%d"
            )

            # 获取近60天的日期
            trade_date_dt = datetime.datetime.strptime(str(self.trade_date), "%Y%m%d")
            date_threshold = trade_date_dt - datetime.timedelta(days=30)

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
            # 基础条件（适用于所有股票）
            # 定义市值阈值（单位：人民币元，假设1美元≈7人民币）
            SMALL_CAP_THRESHOLD = 2000000000  # 20亿美元 ≈ 140亿人民币
            MID_CAP_THRESHOLD = 10000000000  # 100亿美元 ≈ 700亿人民币
            LARGE_CAP_THRESHOLD = 100000000000  # 1000亿美元 ≈ 7000亿人民币

            # 定义活跃度阈值（成交额占市值的百分比）
            MID_CAP_TURNOVER = 0.03  # 3%
            LARGE_CAP_TURNOVER = 0.005  # 0.5%
            MEGA_CAP_TURNOVER = 0.003  # 0.1%
            # 首先找出所有单日涨幅超过200%的股票，这些将被排除
            high_increase_symbols = (
                df_recent[
                    ((df_recent["close"] - df_recent["open"]) / df_recent["open"] >= 2)
                ]["symbol"]
                .unique()
                .tolist()
            )
            base_cond = (
                (df_recent["close"] > 1)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
            )

            mid_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= SMALL_CAP_THRESHOLD)
                & (df_recent["total_value"] < MID_CAP_THRESHOLD)
                & (
                    df_recent["close"] * df_recent["volume"]
                    >= MID_CAP_TURNOVER * df_recent["total_value"]
                )
            )

            large_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= MID_CAP_THRESHOLD)
                & (df_recent["total_value"] < LARGE_CAP_THRESHOLD)
                & (
                    df_recent["close"] * df_recent["volume"]
                    >= LARGE_CAP_TURNOVER * df_recent["total_value"]
                )
            )

            mega_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= LARGE_CAP_THRESHOLD)
                & (
                    df_recent["close"] * df_recent["volume"]
                    >= MEGA_CAP_TURNOVER * df_recent["total_value"]
                )
            )

            # 合并所有条件
            all_cond = mid_cap_cond | large_cap_cond | mega_cap_cond
            filtered_df = df_recent[all_cond]
            # 计算每个股票满足条件的次数
            symbol_counts = filtered_df["symbol"].value_counts()

            # 只选择出现1次或以上的股票
            avail_date_count = (
                df_recent[
                    df_recent["total_value"].notnull() & (df_recent["total_value"] > 0)
                ]["date"]
                .unique()
                .size
            )
            # 阈值：小于10取实际可用日期数，超过10取10，最少取1
            threshold = max(1, min(10, int(avail_date_count)))
            print(f"可用日期数: {avail_date_count}, 阈值: {threshold}")
            frequent_symbols = symbol_counts[symbol_counts >= threshold].index.tolist()

            # 排除单日涨幅超过200%的股票
            filtered_symbols = [
                symbol
                for symbol in frequent_symbols
                if symbol not in high_increase_symbols
            ]
            tickers.extend(filtered_symbols)

        elif self.market == "cn":
            # A股筛选条件
            # 条件1: 市值超过100亿的股票直接入选
            # 定义A股市值阈值（单位：人民币元）
            SMALL_CAP_THRESHOLD = 5000000000  # 50亿人民币
            MID_CAP_THRESHOLD = 20000000000  # 200亿人民币
            LARGE_CAP_THRESHOLD = 100000000000  # 1万亿人民币

            # 定义A股活跃度阈值（成交金额，单位：人民币元）
            SMALL_CAP_TURNOVER = 300000000  # 3亿人民币
            MID_CAP_TURNOVER = 500000000  # 5亿人民币
            LARGE_CAP_TURNOVER = 1000000000  # 10亿人民币

            # 基础条件（不包含涨幅限制）
            base_cond = (
                (df_recent["close"] > 1)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
            )

            # 小盘股条件（50-200亿市值）
            small_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= SMALL_CAP_THRESHOLD)  # 50亿人民币
                & (df_recent["total_value"] < MID_CAP_THRESHOLD)  # 200亿人民币
                & (df_recent["close"] * df_recent["volume"] * 100 >= SMALL_CAP_TURNOVER)
            )

            # 中盘股条件（200-1000亿市值）
            mid_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= MID_CAP_THRESHOLD)  # 200亿人民币
                & (df_recent["total_value"] < LARGE_CAP_TURNOVER)  # 1000亿人民币
                & (df_recent["close"] * df_recent["volume"] * 100 >= MID_CAP_TURNOVER)
            )

            # 大盘股条件（≥1000亿市值）
            large_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= LARGE_CAP_THRESHOLD)  # 1000亿人民币
                & (df_recent["close"] * df_recent["volume"] * 100 >= LARGE_CAP_TURNOVER)
            )

            # 合并所有条件
            all_cond = small_cap_cond | mid_cap_cond | large_cap_cond

            # 筛选出满足条件的行
            filtered_df = df_recent[all_cond]

            # 计算每个股票满足条件的次数
            symbol_counts = filtered_df["symbol"].value_counts()

            # 只选择出现1次或以上的股票
            avail_date_count = (
                df_recent[
                    df_recent["total_value"].notnull() & (df_recent["total_value"] > 0)
                ]["date"]
                .unique()
                .size
            )
            # 阈值：小于10取实际可用日期数，超过10取10，最少取1
            threshold = max(1, min(10, int(avail_date_count)))
            print(f"可用日期数: {avail_date_count}, 阈值: {threshold}")
            filtered_symbols = symbol_counts[symbol_counts >= threshold].index.tolist()

            # 合并所有符合条件的股票
            tickers.extend(filtered_symbols)

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
        list_results.sort(key=lambda x: x["datetime"].min())
        return list_results

    """ 重构dataframe封装 """

    def reconstruct_dataframe(self, group_obj, i):
        """
        过滤历史数据不完整的股票
        小于120天的股票暂时不进入回测列表
        同时检查股票数据中是否存在交易日期对应的数据
        """
        # 检查数据长度是否足够
        if len(group_obj) < 241:
            return pd.DataFrame()

        # 将self.trade_date从"20251016"转换为"2025-10-16"格式
        try:
            from datetime import datetime

            trade_date_dt = datetime.strptime(self.trade_date, "%Y%m%d")
            trade_date_formatted = trade_date_dt.strftime("%Y-%m-%d")
            print(f"处理交易日期: {self.trade_date} -> {trade_date_formatted}")
            print(
                f"股票代码: {i}, 数据日期范围: {group_obj['date'].min()} - {group_obj['date'].max()}"
            )
        except ValueError as e:
            print(f"交易日期格式错误: {self.trade_date}, 期望格式: YYYYMMDD, 错误: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"交易日期处理未知错误: {e}")
            return pd.DataFrame()

        # 检查交易日期是否存在于股票数据中
        if trade_date_formatted not in group_obj["date"].values:
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
        list.sort(key=lambda x: x["datetime"].min())
        return list

    def get_etf_list(self):
        # 预定义列的数据类型
        column_dtypes = {
            "symbol": str,
            "name": str,
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
        # 2. 取近60天的日期
        trade_date_dt = datetime.datetime.strptime(str(self.trade_date), "%Y%m%d")
        date_threshold = trade_date_dt - datetime.timedelta(days=30)

        # 将 datetime 对象转换回字符串格式 (YYYYMMDD)
        date_threshold_str = date_threshold.strftime("%Y-%m-%d")

        # 使用相同格式的字符串进行筛选
        df_recent = df_all[df_all["date"] >= date_threshold_str]

        # 3. 条件筛选
        cond = (df_recent["total_value"] > 5000000000) & (
            df_recent["name"].str.upper().str.contains("ETF")
        )
        # 筛选出满足条件的行
        filtered_df = df_recent[cond]

        # 计算每个股票满足条件的次数
        symbol_counts = filtered_df["symbol"].value_counts()

        # 只选择出现1次或以上的股票
        avail_date_count = (
            df_recent[
                df_recent["total_value"].notnull() & (df_recent["total_value"] > 0)
            ]["date"]
            .unique()
            .size
        )
        # 阈值：小于10取实际可用日期数，超过10取10，最少取1
        threshold = max(1, min(10, int(avail_date_count)))
        print(f"可用日期数: {avail_date_count}, 阈值: {threshold}")
        frequent_symbols = symbol_counts[symbol_counts >= threshold].index.tolist()

        # 更新 stock_list
        stock_list = frequent_symbols

        dfs, df_all, df_recent, df = None, None, None, None
        gc.collect()
        return stock_list

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
        list_results.sort(key=lambda x: x["datetime"].min())
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
        # 2. 取近60天的日期
        trade_date_dt = datetime.datetime.strptime(str(self.trade_date), "%Y%m%d")
        date_threshold = trade_date_dt - datetime.timedelta(days=30)

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

        # 只选择出现1次或以上的股票
        avail_date_count = (
            df_recent[
                df_recent["total_value"].notnull() & (df_recent["total_value"] > 0)
            ]["date"]
            .unique()
            .size
        )
        # 阈值：小于10取实际可用日期数，超过10取10，最少取1
        threshold = max(1, min(10, int(avail_date_count)))
        print(f"可用日期数: {avail_date_count}, 阈值: {threshold}")
        frequent_symbols = symbol_counts[symbol_counts >= threshold].index.tolist()

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
        list_results.sort(key=lambda x: x["datetime"].min())

        return list_results

    def get_recent_pe_data(self):
        """读取历史数据，过滤最近180天内存在pe和total_value的数据，并排除大于trade_date的数据"""

        # 计算日期范围
        trade_date = pd.to_datetime(self.trade_date, format="%Y%m%d")
        cutoff_date = trade_date - datetime.timedelta(days=180)

        # 读取并处理所有文件
        data = []
        for file_path in self.files:
            try:
                # 检查文件是否包含所需的列
                columns = pd.read_csv(file_path, nrows=0).columns.tolist()
                required_columns = ["symbol", "date", "pe", "total_value"]

                if not all(col in columns for col in required_columns):
                    continue

                # 读取文件
                df = pd.read_csv(
                    file_path,
                    usecols=required_columns,
                    dtype={
                        "symbol": str,
                        "date": str,
                        "pe": str,
                        "total_value": np.float64,
                    },
                )

                # 转换日期格式并过滤
                df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
                df = df[(df["date"] >= cutoff_date)]

                # 添加到数据列表
                data.append(df)

            except Exception as e:
                print(f"跳过文件 {file_path}: {e}")

        # 合并所有数据
        if data:
            result_df = pd.concat(data, ignore_index=True)

            # 去重和排序
            result_df.drop_duplicates(
                subset=["symbol", "date"], keep="first", inplace=True
            )
            result_df.sort_values(
                by=["symbol", "date"], ascending=[True, True], inplace=True
            )
            result_df.reset_index(drop=True, inplace=True)

            return result_df
        else:
            return pd.DataFrame(columns=["symbol", "date", "pe", "total_value"])

    def get_recent_gz_data(self):
        """读取gz文件，提取最近180天内的date和new字段"""

        # 计算日期范围
        trade_date = pd.to_datetime(self.trade_date, format="%Y%m%d")
        cutoff_date = trade_date - datetime.timedelta(days=180)

        # 读取并处理所有文件
        data = []
        for file_path in self.files_gz:
            try:
                # 检查文件是否包含所需的列
                columns = pd.read_csv(file_path, nrows=0).columns.tolist()
                required_columns = ["date", "new"]

                if not all(col in columns for col in required_columns):
                    continue

                # 读取文件
                df = pd.read_csv(
                    file_path,
                    usecols=required_columns,
                    dtype={
                        "date": str,
                        "new": np.float64,
                    },
                )

                if len(df) == 0:
                    continue

                # 获取第一行
                row = df.iloc[0]

                formatted_date = (
                    f"{row["date"][:4]}-{row["date"][4:6]}-{row["date"][6:8]}"
                )
                date_dt = pd.to_datetime(formatted_date)

                # 检查是否在180天内
                if date_dt >= cutoff_date:
                    data.append({"date": formatted_date, "new": row["new"]})

            except Exception as e:
                print(f"跳过文件 {file_path}: {e}")
                # 打印更详细的异常信息
                import traceback

                print(traceback.format_exc())

        return pd.DataFrame(data) if data else pd.DataFrame(columns=["date", "new"])
