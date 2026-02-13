#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
import numpy as np
from utility.fileinfo import FileInfo
from utility.toolkit import ToolKit
import multiprocessing
import gc
import datetime
import os

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
        """ 获取固定追踪股票列表文件路径 """
        self.file_fixed_list = file.get_file_path_fixed_list
        """ 获取动态追踪股票列表文件路径 """
        self.file_dynamic_list = file.get_file_path_dynamic_list

    def _top_by_activity(self, cond, df, percent=0.2, max_activity=0.15):
        df_g = df.loc[cond].copy()
        if df_g.empty:
            return []

        df_g["activity"] = np.where(
            df_g["total_value"] > 0,
            (
                df_g["close"] * df_g["volume"] * 100 / df_g["total_value"]
                if self.market.startswith("cn")
                else df_g["close"] * df_g["volume"] / df_g["total_value"]
            ),
            0.0,
        )

        # 找出有极值的 symbol
        symbols_with_extreme = df_g.loc[
            df_g["activity"] > max_activity, "symbol"
        ].unique()

        # 剔除这些 symbol 的所有行
        df_g = df_g[~df_g["symbol"].isin(symbols_with_extreme)]

        sym_act = df_g.groupby("symbol", sort=False)["activity"].mean()

        sym_act = sym_act[sym_act > 0]
        if sym_act.empty:
            return []

        top_n = max(1, int(len(sym_act) * percent))
        return sym_act.nlargest(top_n).index.tolist()

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
            SMALL_CAP_THRESHOLD = 2000000000  # 20亿美元 ≈ 140亿人民币
            LARGE_CAP_THRESHOLD = 10000000000  # 100亿美元 ≈ 700亿人民币
            MEGA_CAP_THRESHOLD = 100000000000  # 1000亿美元 ≈ 7000亿人民币

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

            small_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= SMALL_CAP_THRESHOLD)
                & (df_recent["total_value"] < LARGE_CAP_THRESHOLD)
            )

            large_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= LARGE_CAP_THRESHOLD)
                & (df_recent["total_value"] < MEGA_CAP_THRESHOLD)
            )

            mega_cap_cond = base_cond & (df_recent["total_value"] >= MEGA_CAP_THRESHOLD)

            # 合并所有条件
            small_top = self._top_by_activity(small_cap_cond, df_recent, 0.1)
            large_top = self._top_by_activity(large_cap_cond, df_recent, 0.3)
            mega_top = self._top_by_activity(mega_cap_cond, df_recent, 0.5)

            # 合并三组的 top20% symbol
            combined_symbols = list(set(small_top + large_top + mega_top))

            # 排除单日涨幅超过200%的股票
            filtered_symbols = [
                symbol
                for symbol in combined_symbols
                if symbol not in high_increase_symbols
            ]
            tickers.extend(filtered_symbols)

        elif self.market == "cn":
            # A股筛选条件
            SMALL_CAP_THRESHOLD = 5000000000  # 50亿人民币
            LARGE_CAP_THRESHOLD = 10000000000  # 200亿人民币
            MEGA_CAP_THRESHOLD = 100000000000  # 1千亿人民币

            # 基础条件（不包含涨幅限制）
            base_cond = (
                (df_recent["close"] > 1)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
            )

            small_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= SMALL_CAP_THRESHOLD)
                & (df_recent["total_value"] < LARGE_CAP_THRESHOLD)
            )

            large_cap_cond = (
                base_cond
                & (df_recent["total_value"] >= LARGE_CAP_THRESHOLD)
                & (df_recent["total_value"] < MEGA_CAP_THRESHOLD)
            )

            mega_cap_cond = base_cond & (df_recent["total_value"] >= MEGA_CAP_THRESHOLD)

            small_top = self._top_by_activity(small_cap_cond, df_recent, 0.3)
            large_top = self._top_by_activity(large_cap_cond, df_recent, 0.3)
            mega_top = self._top_by_activity(mega_cap_cond, df_recent, 0.2)

            combined_symbols = list(set(small_top + large_top + mega_top))

            # 合并所有符合条件的股票
            tickers.extend(combined_symbols)

        elif self.market == "us_special":
            # 特殊美股筛选条件
            tickers = self.get_special_us_stock_list_180d()
        elif self.market in ("us_dynamic", "cn_dynamic"):
            # 动态追踪股票列表
            tickers = self.get_dynamic_stock_list()

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
        print(
            f"第一组股票symbol为: {list_results[0]['symbol'].iloc[0]}, 数据起始日期: {list_results[0]['datetime'].min()}, 结束日期: {list_results[0]['datetime'].max()}"
        )
        print(
            f"最后一组股票symbol为: {list_results[-1]['symbol'].iloc[0]}, 数据起始日期: {list_results[-1]['datetime'].min()}, 结束日期: {list_results[-1]['datetime'].max()}"
        )
        return list_results

    """ 重构dataframe封装 """

    def reconstruct_dataframe(self, group_obj, i):
        """
        过滤历史数据不完整的股票
        小于120天的股票暂时不进入回测列表
        同时检查股票数据中是否存在交易日期对应的数据
        """
        # 检查数据长度是否足够
        if len(group_obj) < 61:
            return pd.DataFrame()

        # 将self.trade_date从"20251016"转换为"2025-10-16"格式
        try:
            trade_date_dt = datetime.datetime.strptime(self.trade_date, "%Y%m%d")
            trade_date_formatted = trade_date_dt.strftime("%Y-%m-%d")
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
        if self.market in ("us", "us_special", "us_dynamic"):
            market = 1
        elif self.market in ("cn", "cn_dynamic", "cnetf"):
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

        # ----------------------------
        # 4. 构造 mock bar（关键新增部分）
        # ----------------------------
        last_row = df_copy.iloc[-1].copy()

        # 日期 +1 天
        last_row["datetime"] = last_row["datetime"] + datetime.timedelta(days=1)

        # volume 可设为 0（推荐）
        # last_row["volume"] = 0

        # 拼接 mock bar
        df_copy = pd.concat(
            [df_copy, pd.DataFrame([last_row])],
            ignore_index=True,
        )

        return df_copy

    def get_backtrader_data_feed_testonly(self, stocklist):
        tickers = stocklist
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
        print(
            f"第一组股票symbol为: {list_results[0]['symbol'].iloc[0]}, 数据起始日期: {list_results[0]['datetime'].min()}, 结束日期: {list_results[0]['datetime'].max()}"
        )
        print(
            f"最后一组股票symbol为: {list_results[-1]['symbol'].iloc[0]}, 数据起始日期: {list_results[-1]['datetime'].min()}, 结束日期: {list_results[-1]['datetime'].max()}"
        )
        return list_results

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

        etf_top = self._top_by_activity(cond, df_recent, 0.3)

        combined_symbols = list(set(etf_top))

        dfs, df_all, df_recent, df = None, None, None, None
        gc.collect()
        return combined_symbols

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
        print(
            f"第一组股票symbol为: {list_results[0]['symbol'].iloc[0]}, 数据起始日期: {list_results[0]['datetime'].min()}, 结束日期: {list_results[0]['datetime'].max()}"
        )
        print(
            f"最后一组股票symbol为: {list_results[-1]['symbol'].iloc[0]}, 数据起始日期: {list_results[-1]['datetime'].min()}, 结束日期: {list_results[-1]['datetime'].max()}"
        )
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
        tiny_cond = (
            (df_recent["total_value"] < 2000000000)
            & (df_recent["total_value"] > 100000000)
            & (df_recent["close"] > 1)
        )

        tiny_top = self._top_by_activity(tiny_cond, df_recent, 0.05)

        combined_symbols = list(set(tiny_top))

        # 更新 stock_list
        stock_list = combined_symbols

        # ===== 新增：读取 fixed_list.csv 并合并 =====
        if os.path.exists(self.file_fixed_list):
            try:
                fixed_df = pd.read_csv(self.file_fixed_list, comment="#")
                if not fixed_df.empty:
                    if "symbol" in fixed_df.columns:
                        fixed_symbols = fixed_df["symbol"]
                        print(f"固定追踪列表: {len(fixed_symbols)}")
                    else:
                        # 没有 symbol 表头时，默认取第一列
                        fixed_symbols = fixed_df.iloc[:, 0]

                    fixed_symbols = fixed_symbols.astype(str).unique().tolist()
                    stock_list = list(set(stock_list) | set(fixed_symbols))

            except Exception as e:
                print(f"读取 fixed_list.csv 失败: {e}")

        dfs, df_all, df_recent, df = None, None, None, None
        gc.collect()
        return stock_list

    def get_special_us_backtrader_data_feed(self):
        tickers = self.get_stock_list()
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
        print(
            f"第一组股票symbol为: {list_results[0]['symbol'].iloc[0]}, 数据起始日期: {list_results[0]['datetime'].min()}, 结束日期: {list_results[0]['datetime'].max()}"
        )
        print(
            f"最后一组股票symbol为: {list_results[-1]['symbol'].iloc[0]}, 数据起始日期: {list_results[-1]['datetime'].min()}, 结束日期: {list_results[-1]['datetime'].max()}"
        )

        return list_results

    def get_dynamic_stock_list(self):
        # ===== 新增：读取 dynamic_list.csv 并合并 =====
        if os.path.exists(self.file_dynamic_list):
            try:
                dynamic_df = pd.read_csv(self.file_dynamic_list, comment="#")
                if not dynamic_df.empty:
                    if "symbol" in dynamic_df.columns:
                        dynamic_symbols = dynamic_df["symbol"]
                        print(f"动态追踪列表: {len(dynamic_symbols)}")
                    else:
                        # 没有 symbol 表头时，默认取第一列
                        dynamic_symbols = dynamic_df.iloc[:, 0]

                    dynamic_symbols = dynamic_symbols.astype(str).unique().tolist()
                    stock_list = list(set(dynamic_symbols))

            except Exception as e:
                print(f"读取 dynamic_list.csv 失败: {e}")

        dfs, df_all, df_recent, df = None, None, None, None
        gc.collect()
        return stock_list

    def get_dynamic_backtrader_data_feed(self):
        tickers = self.get_dynamic_stock_list()
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
        print(
            f"第一组股票symbol为: {list_results[0]['symbol'].iloc[0]}, 数据起始日期: {list_results[0]['datetime'].min()}, 结束日期: {list_results[0]['datetime'].max()}"
        )
        print(
            f"最后一组股票symbol为: {list_results[-1]['symbol'].iloc[0]}, 数据起始日期: {list_results[-1]['datetime'].min()}, 结束日期: {list_results[-1]['datetime'].max()}"
        )

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
