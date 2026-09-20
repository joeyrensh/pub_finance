#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import glob
import os
import random
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Union

import akshare as ak
import numpy as np
import pandas as pd
from finance import FINANCE_ROOT
from finance.utility.fileinfo import FileInfo
from finance.utility.toolkit import ToolKit


class AKCNHistoryDataCrawler:

    def __init__(self):
        self.proxy = None

    def _get_tickers_from_local_csv(self):
        """从 FINANCE_ROOT / cnstockinfo/ 目录下自动读取最新日期的 CSV 文件提取 ticker 列表，

        根据带有 SZ/SH/ETF 前缀的 symbol 进行分类归集。
        """
        dir_path = FINANCE_ROOT / "cnstockinfo"
        if not os.path.exists(dir_path):
            dir_path = "cnstockinfo"
            if not os.path.exists(dir_path):
                raise FileNotFoundError(f"目录不存在: {dir_path}")

        csv_files = glob.glob(os.path.join(str(dir_path), "stock_*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"在 {dir_path} 下未找到任何 stock_*.csv 文件")

        latest_file = sorted(csv_files)[-1]
        print(f"读取本地最新股票列表文件: {latest_file}")

        df_info = pd.read_csv(latest_file, dtype=str)

        symbol_col = next(
            (c for c in df_info.columns if c.lower() in ["symbol", "代码"]), None
        )
        name_col = next(
            (c for c in df_info.columns if c.lower() in ["name", "名称"]), None
        )

        if not symbol_col or not name_col:
            raise KeyError(f"文件 {latest_file} 中缺少 symbol 或 name 标识列")

        sh_list, sz_list, etf_list = [], [], []

        for _, row in df_info.iterrows():
            sym_raw = str(row[symbol_col]).strip().upper()
            name = str(row[name_col]).strip()

            if sym_raw.startswith("SH"):
                raw_num = sym_raw[2:]
                sh_list.append(
                    {
                        "symbol_ak": f"sh{raw_num}",
                        "symbol_out": f"SH{raw_num}",
                        "name": name,
                    }
                )
            elif sym_raw.startswith("SZ"):
                raw_num = sym_raw[2:]
                sz_list.append(
                    {
                        "symbol_ak": f"sz{raw_num}",
                        "symbol_out": f"SZ{raw_num}",
                        "name": name,
                    }
                )
            elif sym_raw.startswith("ETF"):
                raw_num = sym_raw[3:]
                # 新浪接口要求带 sh/sz 前缀，如 sh510300, sz159915
                prefix = "sh" if raw_num.startswith(("5", "6", "9")) else "sz"
                etf_list.append(
                    {
                        "symbol_ak": f"{prefix}{raw_num}",  # 新浪接口专用
                        "symbol_out": f"ETF{raw_num}",      # 落盘标示专用
                        "name": name,
                    }
                )

        return sh_list, sz_list, etf_list

    def _get_downloaded_symbols(self, file_path: str) -> Set[str]:
        """流式分块读取大文件 CSV，获取所有已落盘的唯一 symbol 集合，实现断点续传且不爆内存"""
        downloaded = set()
        if not os.path.exists(file_path):
            return downloaded

        try:
            print(f"正在扫描已落盘目标文件 [{file_path}] 以获取断点续传列表...")
            for chunk in pd.read_csv(
                file_path, usecols=["symbol"], chunksize=100000, dtype=str
            ):
                if "symbol" in chunk.columns:
                    downloaded.update(chunk["symbol"].dropna().unique())
            print(f"扫描完成！共检测到 {len(downloaded)} 个股票/ETF 已经成功下载，将自动跳过。")
        except Exception as e:
            print(f"读取已落盘文件出现警告（如果是空文件可忽略）: {e}")

        return downloaded

    def get_cn_stock_history_ak(
        self,
        start_date: str,
        end_date: str,
        file_path: str,
        source_type: str = "file",
        symbols: Optional[Union[str, List[str]]] = None,
    ):
        """获取A股及ETF历史数据（新浪ETF直连 + 高性能向量化 + 断点续传版）

        :param start_date: 开始日期 (YYYYMMDD 或 YYYY-MM-DD)
        :param end_date: 结束日期 (YYYYMMDD 或 YYYY-MM-DD)
        :param file_path: 输出文件路径
        :param source_type: "file" 从本地文件读取列表；"akshare" 从网络实时获取列表
        :param symbols: 可选参数。单个标的字符串如 "SH600000"，或标的列表如 ["SH600000", "ETF510300"]。未指定时获取全量。
        """
        batch_size = 50
        is_first_write = not os.path.exists(file_path)

        # 统一把传入的 symbols 转为标准大写的 set 集合
        target_symbols: Optional[Set[str]] = None
        if symbols is not None:
            if isinstance(symbols, str):
                target_symbols = {symbols.strip().upper()}
            elif isinstance(symbols, (list, tuple, set)):
                target_symbols = {str(s).strip().upper() for s in symbols}

        # 1. 动态扫描已落盘数据的 unique symbol 集合
        downloaded_symbols = self._get_downloaded_symbols(file_path)

        # 2. 准备基础列表中数据
        if source_type == "file":
            sh_tickers, sz_tickers, etf_tickers = (
                self._get_tickers_from_local_csv()
            )
            # 如果指定了 target_symbols，在本地列表的基础上进行过滤
            if target_symbols:
                sh_tickers = [x for x in sh_tickers if x["symbol_out"] in target_symbols]
                sz_tickers = [x for x in sz_tickers if x["symbol_out"] in target_symbols]
                etf_tickers = [x for x in etf_tickers if x["symbol_out"] in target_symbols]
        else:
            sh_tickers, sz_tickers, etf_tickers = [], [], []

        # 转换为标准的 YYYY-MM-DD 格式以匹配新浪财经的时间筛选条件
        s_date = pd.to_datetime(start_date).strftime("%Y-%m-%d")
        e_date = pd.to_datetime(end_date).strftime("%Y-%m-%d")

        # ==================== 1. 上证 A 股 ====================
        if source_type == "akshare":
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
            for _, row in pd_stock_sh.reset_index(drop=True).iterrows():
                raw_num = str(row["代码"]).strip()
                symbol_out = "SH" + raw_num
                if target_symbols is None or symbol_out in target_symbols:
                    sh_tickers.append(
                        {
                            "symbol_ak": "sh" + raw_num,
                            "symbol_out": symbol_out,
                            "name": row["名称"],
                        }
                    )

        tool = ToolKit("上证历史数据下载")
        list_dfs = []
        sh_total = len(sh_tickers)

        for index, item_info in enumerate(sh_tickers):
            symbol_ak = item_info["symbol_ak"]
            symbol_out = item_info["symbol_out"]
            name = item_info["name"]

            # 断点续传检查：已存在则跳过
            if symbol_out in downloaded_symbols:
                tool.progress_bar(sh_total, index)
                continue

            try:
                df_raw = ak.stock_zh_a_daily(
                    symbol=symbol_ak,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )

                if df_raw is not None and not df_raw.empty:
                    df_raw["symbol"] = symbol_out
                    df_raw["name"] = name
                    df_sub = df_raw[
                        [
                            "symbol",
                            "name",
                            "open",
                            "close",
                            "high",
                            "low",
                            "volume",
                            "date",
                        ]
                    ].reset_index(drop=True)
                    list_dfs.append(df_sub)
            except Exception as e:
                print(f"获取上证历史数据失败 [{symbol_ak}]: {e}")

            # 批量合并落盘
            if (index + 1) % batch_size == 0 or (index + 1) == sh_total:
                if list_dfs:
                    batch_df = pd.concat(list_dfs, ignore_index=True)
                    batch_df.to_csv(
                        file_path,
                        mode="a",
                        index=False,
                        header=is_first_write,
                    )
                    is_first_write = False
                    list_dfs.clear()

            tool.progress_bar(sh_total, index)

        # ==================== 2. 深证 A 股 ====================
        if source_type == "akshare":
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
            for _, row in pd_stock_sz.reset_index(drop=True).iterrows():
                raw_num = str(row["代码"]).strip()
                symbol_out = "SZ" + raw_num
                if target_symbols is None or symbol_out in target_symbols:
                    sz_tickers.append(
                        {
                            "symbol_ak": "sz" + raw_num,
                            "symbol_out": symbol_out,
                            "name": row["名称"],
                        }
                    )

        tool = ToolKit("深证历史数据下载")
        list_dfs = []
        sz_total = len(sz_tickers)

        for index, item_info in enumerate(sz_tickers):
            symbol_ak = item_info["symbol_ak"]
            symbol_out = item_info["symbol_out"]
            name = item_info["name"]

            # 断点续传检查：已存在则跳过
            if symbol_out in downloaded_symbols:
                tool.progress_bar(sz_total, index)
                continue

            try:
                df_raw = ak.stock_zh_a_daily(
                    symbol=symbol_ak,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )

                if df_raw is not None and not df_raw.empty:
                    df_raw["symbol"] = symbol_out
                    df_raw["name"] = name
                    df_sub = df_raw[
                        [
                            "symbol",
                            "name",
                            "open",
                            "close",
                            "high",
                            "low",
                            "volume",
                            "date",
                        ]
                    ].reset_index(drop=True)
                    list_dfs.append(df_sub)
            except Exception as e:
                print(f"获取深证历史数据失败 [{symbol_ak}]: {e}")

            # 批量合并落盘
            if (index + 1) % batch_size == 0 or (index + 1) == sz_total:
                if list_dfs:
                    batch_df = pd.concat(list_dfs, ignore_index=True)
                    batch_df.to_csv(
                        file_path,
                        mode="a",
                        index=False,
                        header=is_first_write,
                    )
                    is_first_write = False
                    list_dfs.clear()

            tool.progress_bar(sz_total, index)

        # ==================== 3. ETF 历史数据（全量使用新浪接口） ====================
        if source_type == "akshare":
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
            for _, row in pd_etf.reset_index(drop=True).iterrows():
                raw_num = str(row["代码"]).strip()
                prefix = "sh" if raw_num.startswith(("5", "6", "9")) else "sz"
                symbol_out = f"ETF{raw_num}"
                if target_symbols is None or symbol_out in target_symbols:
                    etf_tickers.append(
                        {
                            "symbol_ak": f"{prefix}{raw_num}",  # 新浪接口所需的 sh510300 格式
                            "symbol_out": symbol_out,           # 落盘标示 ETF510300 格式
                            "name": row["名称"],
                        }
                    )

        tool = ToolKit("ETF历史数据下载(新浪接口)")
        list_dfs = []
        etf_total = len(etf_tickers)

        # 1. 预先加载除权派息/拆股因子库
        file = FileInfo("20260101", "cn")
        actions_file_path = file.get_file_path_actions_history

        try:
            df_actions = pd.read_csv(actions_file_path)
            df_actions["date"] = df_actions["date"].astype(str)
            # 填充空值，默认无分红(0.0)，无拆股(1.0)
            df_actions["dividend"] = df_actions["dividend"].fillna(0.0)
            df_actions["split_ratio"] = df_actions["split_ratio"].replace(0.0, np.nan).fillna(1.0)
        except Exception as e:
            print(f"⚠️ 无法加载公司行为历史文件: {e}")
            df_actions = pd.DataFrame(columns=["symbol", "date", "dividend", "split_ratio"])

        for index, item_info in enumerate(etf_tickers):
            symbol_ak = item_info["symbol_ak"]  # 例如 "sh510300"
            symbol_out = item_info["symbol_out"]  # 例如 "ETF510300"
            name = item_info["name"]

            # 断点续传检查：已存在则跳过
            if symbol_out in downloaded_symbols:
                tool.progress_bar(etf_total, index)
                continue

            try:
                # 直接调用新浪接口获取 ETF 历史日线
                df_sina = ak.fund_etf_hist_sina(symbol=symbol_ak)

                if df_sina is not None and not df_sina.empty:
                    df_sina["date"] = df_sina["date"].astype(str)

                    # ==================== 对齐除权日真实价格 ====================
                    actions_sub = df_actions[df_actions["symbol"] == symbol_out][["date", "dividend", "split_ratio"]]
                    
                    if not actions_sub.empty:
                        df_sina = pd.merge(df_sina, actions_sub, on="date", how="left")
                        df_sina["dividend"] = df_sina["dividend"].fillna(0.0)
                        df_sina["split_ratio"] = df_sina["split_ratio"].fillna(1.0)

                        needs_fix = (df_sina["dividend"] > 0) | (df_sina["split_ratio"] != 1.0)
                        
                        if needs_fix.any():
                            price_cols = ["open", "high", "low", "close"]
                            for col in price_cols:
                                df_sina[col] = df_sina[col].astype(float)
                                df_sina[col] = (df_sina[col] - df_sina["dividend"]) / df_sina["split_ratio"]
                                df_sina[col] = df_sina[col].round(3)

                            df_sina["volume"] = (df_sina["volume"].astype(float) * df_sina["split_ratio"]).round(0)

                        df_sina.drop(columns=["dividend", "split_ratio"], inplace=True)
                    # ==========================================================

                    # 按照日期范围进行切片
                    df_filtered = df_sina[
                        (df_sina["date"] >= s_date) & (df_sina["date"] <= e_date)
                    ].copy()

                    if not df_filtered.empty:
                        df_filtered["symbol"] = symbol_out
                        df_filtered["name"] = name

                        # 向量化提取并格式化
                        df_sub = df_filtered[
                            [
                                "symbol",
                                "name",
                                "open",
                                "close",
                                "high",
                                "low",
                                "volume",
                                "date",
                            ]
                        ].reset_index(drop=True)
                        list_dfs.append(df_sub)
            except Exception as e:
                print(f"获取 ETF 历史数据失败 [{symbol_ak}]: {e}")

            # 批量合并落盘
            if (index + 1) % batch_size == 0 or (index + 1) == etf_total:
                if list_dfs:
                    batch_df = pd.concat(list_dfs, ignore_index=True)
                    
                    # 对齐股票格式：插入第一列自增 index
                    batch_df.insert(0, "index", range(len(batch_df)))

                    batch_df.to_csv(
                        file_path,
                        mode="a",
                        index=False,
                        header=is_first_write,
                    )
                    is_first_write = False
                    list_dfs.clear()

            tool.progress_bar(etf_total, index)