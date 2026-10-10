#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
import numpy as np
from finance.utility.fileinfo import FileInfo
from finance.utility.toolkit import ToolKit
import multiprocessing
import gc
import datetime
import os
import json
from finance import FINANCE_ROOT
from typing import List, Optional

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
        """ 获取复权因子文件路径 """
        self.file_actions_history = file.get_file_path_actions_history

        # 获取交易日 & 股票过滤条件权重
        weights_cfg = ToolKit.get_config()
        self.collection_days = weights_cfg["stock_filter"]["collection_days"]
        self.capital_flow_weights = weights_cfg["stock_filter"]["capital_flow"]
        self.up_days_weights = weights_cfg["stock_filter"]["up_days"]
        # 获取市值阈值配置
        self.group_cfg = weights_cfg.get(f"grouping_settings_{self.market}", {})
        self.mode = self.group_cfg.get("grouping_mode", "manual")
        self.top_n = self.group_cfg.get("top_n_per_group", 100)
        self.n_groups = self.group_cfg.get("n_groups", 5)
        # 统一解析 bins（无论模式，都存储并解析，作为备用与市值阈值来源）
        default_bins_str = (
            "5e9, 1e10, 5e10, 1e11, 2e11, inf"
            if self.market == "cn"
            else "2e9, 1e10, 5e10, 1e11, 2e11, inf"
        )
        bins_str = self.group_cfg.get("bins", default_bins_str)

        self.bins = [
            (
                np.inf
                if x.strip().lower() in ("inf", "+inf", "np.inf")
                else float(x.strip())
            )
            for x in str(bins_str).split(",")
            if x.strip()
        ]

        # 关键点：用 bins 的第一个值作为市值门槛 (SMALL_CAP_THRESHOLD)
        self.small_cap_threshold = self.bins[0] if self.bins else 2e9

        # 读取 ETF 专属分组配置 (仅 auto 模式)
        self.etf_cfg = weights_cfg.get("grouping_settings_etf", {})
        self.etf_n_groups = self.etf_cfg.get("n_groups", 1)
        self.etf_top_n = self.etf_cfg.get("top_n_per_group", 50)
        etf_min_threshold = self.etf_cfg.get("min_threshold", "5e9")
        self.etf_min_threshold = (
            float(etf_min_threshold)
            if str(etf_min_threshold).lower() != "inf"
            else np.inf
        )

        if market.startswith("us"):
            self.date_threshold = ToolKit(
                f"获取{self.collection_days}天前交易日"
            ).get_us_trade_date_by_delta(self.collection_days, trade_date)
        elif market.startswith("cn"):
            self.date_threshold = ToolKit(
                f"获取{self.collection_days}天前交易日"
            ).get_cn_trade_date_by_delta(self.collection_days, trade_date)

    def _top_by_activity(
        self,
        cond,
        df,
        n_groups=None,
        top_n_per_group=100,
        max_turnover=0.25,
        group_bins=None,
        target_symbols=None,
    ):
        # 初始化诊断记录
        diag = {}
        target_set = set(target_symbols) if target_symbols else set()
        for sym in target_set:
            diag[sym] = {"status": "alive", "reason": None, "group": None, "rank": None}

        # 辅助函数：标记股票被过滤
        def mark_filtered(reason):
            for sym in list(target_set):
                diag[sym] = {"status": "filtered", "reason": reason}
                target_set.remove(sym)

        # 1. 找出最新交易日满足 cond 的股票
        df_cond = df.loc[cond].copy()
        if df_cond.empty:
            mark_filtered("cond 无数据")
            return []

        latest_date = df_cond["date"].max()
        symbols_latest = df_cond[df_cond["date"] == latest_date]["symbol"].unique()
        not_in_latest = target_set - set(symbols_latest)
        for sym in list(not_in_latest):
            diag[sym] = {"status": "filtered", "reason": "最新交易日不满足cond"}
            target_set.remove(sym)

        if not symbols_latest.size:
            return []

        # 2. 从原始数据中提取这些股票的所有行
        df_g = df[df["symbol"].isin(symbols_latest)].copy()
        mcap_latest = df_g[df_g["date"] == latest_date].set_index("symbol")[
            "total_value"
        ]

        # 3. 剔除高换手股票
        df_g["turnover"] = np.where(
            df_g["total_value"] > 0,
            (
                df_g["close"] * df_g["volume"] * 100 / df_g["total_value"]
                if self.market.startswith("cn")
                else df_g["close"] * df_g["volume"] / df_g["total_value"]
            ),
            0.0,
        )
        # 检查目标股票是否换手率超标
        for sym in list(target_set):
            if sym in df_g["symbol"].values:
                turnover_vals = df_g[df_g["symbol"] == sym]["turnover"]
                if not turnover_vals.empty and turnover_vals.max() > max_turnover:
                    diag[sym] = {
                        "status": "filtered",
                        "reason": f"换手率 > {max_turnover}",
                    }
                    target_set.remove(sym)

        symbols_with_extreme = df_g.loc[
            df_g["turnover"] > max_turnover, "symbol"
        ].unique()
        df_g = df_g[~df_g["symbol"].isin(symbols_with_extreme)]
        if df_g.empty:
            mark_filtered("剔除高换手后无数据")
            return []

        # 4. 计算 activity
        # 方法A：简单计算日内资金流动
        factor = 100 if self.market.startswith("cn") else 1
        df_g["activity"] = (df_g["close"] - df_g["open"]) * df_g["volume"] * factor
        df_g["up"] = (df_g["close"] > df_g["open"]).astype(int)  # 上涨日标记

        # 分组聚合
        sym_act = df_g.groupby("symbol")["activity"].mean()
        sym_up = df_g.groupby("symbol")["up"].sum()  # 0~10

        # 归一化（Min-Max）
        act_norm = (sym_act - sym_act.min()) / (sym_act.max() - sym_act.min() + 1e-6)
        up_norm = sym_up / self.collection_days

        # 综合得分：资金流权重0.6，上涨天数权重0.4（可调）
        score = self.capital_flow_weights * act_norm + self.up_days_weights * up_norm
        sym_act = score.sort_values(ascending=False)

        # 检查 activity <= 0
        for sym in list(target_set):
            if sym in sym_act.index and sym_act[sym] <= 0:
                diag[sym] = {"status": "filtered", "reason": "平均 activity <= 0"}
                target_set.remove(sym)

        sym_act = sym_act[sym_act > 0]
        if sym_act.empty and target_set:
            mark_filtered("无 activity > 0 的股票")
            return []

        # 5. 使用最新日市值
        sym_mcap = mcap_latest[sym_act.index]

        # 6. 对齐索引
        common_syms = sym_act.index.intersection(sym_mcap.index)
        for sym in list(target_set):
            if sym not in common_syms:
                diag[sym] = {"status": "filtered", "reason": "市值与 activity 对齐失败"}
                target_set.remove(sym)

        if not common_syms.size:
            return []
        sym_act = sym_act[common_syms]
        sym_mcap = sym_mcap[common_syms]

        # 7. 分组
        if group_bins is not None:
            labels = pd.cut(sym_mcap, bins=group_bins, right=False)
            valid_mask = labels.notna()
            # 检查市值超边界
            for sym in list(target_set):
                if sym in sym_mcap.index and not valid_mask[sym]:
                    diag[sym] = {"status": "filtered", "reason": "市值超出自定义边界"}
                    target_set.remove(sym)
            if not valid_mask.all():
                print(f"警告: {valid_mask.sum()} 只股票市值超出自定义边界，将被忽略")
                sym_act = sym_act[valid_mask]
                sym_mcap = sym_mcap[valid_mask]
                labels = labels[valid_mask]
            groups = []
            categories = labels.cat.categories
            for cat in categories:
                group_syms = labels[labels == cat].index.tolist()
                groups.append(group_syms)
        else:
            if n_groups is None:
                raise ValueError("必须提供 n_groups 或 group_bins 参数")
            syms_sorted = sym_mcap.sort_values().index.tolist()
            n = len(syms_sorted)
            group_size = n // n_groups
            groups = []
            for i in range(n_groups):
                start = i * group_size
                end = (i + 1) * group_size if i < n_groups - 1 else n
                groups.append(syms_sorted[start:end])

        # 8. 每组取 activity 最高的 top_n_per_group 只
        top_per_group = []
        for i, group_syms in enumerate(groups):
            if not group_syms:
                continue
            act_series = sym_act[group_syms].sort_values(ascending=False)
            top_n = act_series.head(top_n_per_group).index.tolist()
            top_per_group.extend(top_n)
            # 计算阈值：入选股票的最低 activity（若组内股票不足 top_n_per_group，则取全部股票的最低值）
            threshold_activity = act_series.head(top_n_per_group).min()
            print(
                f"Group {i}: {len(top_n)} symbols taken (group size: {len(group_syms)}), "
                f"activity threshold: {threshold_activity:.2f}"
            )

            # 记录存活目标在本组的信息
            for sym in list(target_set):
                if sym in group_syms:
                    rank = act_series.index.get_loc(sym) + 1
                    selected = sym in top_n
                    # 获取该股票的 activity 值
                    activity_value = act_series[sym]

                    diag[sym] = {
                        "status": "alive" if selected else "filtered",
                        "reason": (
                            None
                            if selected
                            else f"Group {i}, 排名 {rank}, 超出前 {top_n_per_group}, activity: {activity_value:.2f}"
                        ),
                        "group": i,
                        "rank": rank,
                        "selected": selected,
                    }
                    target_set.remove(sym)  # 无论是否选中，都已处理，从待处理集合移除

        # 处理未被任何组覆盖的存活目标
        for sym in list(target_set):
            diag[sym] = {"status": "filtered", "reason": "未出现在任何分组中"}

        # 输出诊断结果
        if target_symbols:
            print("\n===== 诊断结果 =====")
            for sym in target_symbols:
                info = diag.get(sym, {})
                status = info.get("status", "unknown")
                reason = info.get("reason")
                if status == "alive":
                    print(
                        f"{sym}: 存活，Group {info['group']}，排名 {info['rank']}，选中 {info['selected']}"
                    )
                elif reason:
                    print(f"{sym}: 被过滤，原因: {reason}")
                else:
                    print(f"{sym}: 未找到诊断信息")

        return top_per_group

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

            # 将 datetime 对象转换回字符串格式 (YYYYMMDD)
            date_threshold_str = datetime.datetime.strptime(
                self.date_threshold, "%Y%m%d"
            ).strftime("%Y-%m-%d")

            # 使用相同格式的字符串进行筛选
            df_recent = df_all[df_all["date"] >= date_threshold_str]
            # 去重并返回
            df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
            valid_symbols = df_o["symbol"].unique()
            df_recent = df_recent[df_recent["symbol"].isin(valid_symbols)]

            dfs, df_all, df = None, None, None
            gc.collect()

        tickers = []

        if self.market == "us":
            # 美股筛选条件
            base_cond = (
                (df_recent["total_value"] > self.small_cap_threshold)
                & (df_recent["close"] > 3)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
            )
            if self.mode == "manual":
                filtered_top = self._top_by_activity(
                    base_cond,
                    df_recent,
                    group_bins=self.bins,
                    top_n_per_group=self.top_n,
                )
            else:
                filtered_top = self._top_by_activity(
                    base_cond,
                    df_recent,
                    n_groups=self.n_groups,
                    top_n_per_group=self.top_n,
                )

            # 合并三组的 top20% symbol
            combined_symbols = list(set(filtered_top))

            tickers.extend(combined_symbols)

        elif self.market == "cn":

            # 基础条件（不包含涨幅限制）
            base_cond = (
                (df_recent["total_value"] > self.small_cap_threshold)
                & (df_recent["close"] > 3)
                & (df_recent["close"] < 10000)
                & (df_recent["open"] > 0)
                & (df_recent["high"] > 0)
                & (df_recent["low"] > 0)
            )
            if self.mode == "manual":
                filtered_top = self._top_by_activity(
                    base_cond,
                    df_recent,
                    group_bins=self.bins,
                    top_n_per_group=self.top_n,
                )
            else:
                filtered_top = self._top_by_activity(
                    base_cond,
                    df_recent,
                    n_groups=self.n_groups,
                    top_n_per_group=self.top_n,
                )

            combined_symbols = list(set(filtered_top))

            # 合并所有符合条件的股票
            tickers.extend(combined_symbols)

        elif self.market == "us_special":
            # 特殊美股筛选条件
            tickers = self.get_special_us_stock_list_180d()
        elif self.market in ("us_dynamic", "cn_dynamic"):
            # 动态追踪股票列表
            tickers = self.get_dynamic_stock_list()

        print(f"满足条件的股票数量: {len(tickers)}")
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
        # """ 匹配行业信息 """
        # df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
        # df_n = pd.merge(df, df_o, how="inner", on="symbol")
        return df

    """ 获取历史数据 """

    def get_history_data(
        self, target_symbols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """读取历史 K 线数据

        :param target_symbols: 目标股票代码列表，用于源头过滤以提升性能
        """
        ticker_set = set(target_symbols) if target_symbols else None
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

            # 根据 target_symbols 过滤数据
            if ticker_set is not None:
                df["symbol"] = df["symbol"].str.strip()
                df = df[df["symbol"].isin(ticker_set)]

            if not df.empty:
                df.drop_duplicates(
                    subset=["symbol", "date"], keep="first", inplace=True
                )
                dic[j] = df

        if not dic:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "open",
                    "close",
                    "high",
                    "low",
                    "volume",
                    "date",
                ]
            )

        df = pd.concat(list(dic.values()), ignore_index=True)
        df.sort_values(
            by=["symbol", "date"], ascending=[True, True], inplace=True
        )
        return df

    """ 
    获取backtrader所需的datafeed
    将历史数据按照backtrader datafeed格式重构
    """

    def get_backtrader_data_feed(self):
        tickers = self.get_stock_list()
        # 切换不复权数据源
        his_data = self.get_history_data_fqt(target_symbols=tickers)
        return self.format_backtrader_feed(
            df_raw=his_data,
            target_tickers=tickers,
            trade_date=self.trade_date,
            market_str=self.market,  # 内部自动精确判定 1 或 2
            min_bars=61,
            add_mock_bar=True
        )

    """ 重构dataframe封装 """

    def format_backtrader_feed(
        self,
        df_raw: pd.DataFrame,
        target_tickers: list,
        trade_date: str,
        market_str: str,
        min_bars: int = 61,
        add_mock_bar: bool = True
    ) -> list[pd.DataFrame]:
        """
        通用 Backtrader 数据结构转换与格式化函数 (零 Loop 高性能向量化版)
        
        :param df_raw: 包含历史数据的 DataFrame (必须包含列: ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])
        :param target_tickers: 需要筛选的目标标的列表
        :param trade_date: 必须包含的交易日期 (格式: 'YYYYMMDD' 或 'YYYY-MM-DD')
        :param market_str: 对应 self.market 的字符串值
        :param min_bars: 过滤的最小 K 线数量阈值，默认 61
        :param add_mock_bar: 是否在末尾自动追加一天 Mock Bar
        :return: 适合 Backtrader 加载的 List[pd.DataFrame]
        """
        if df_raw.empty or not target_tickers:
            return []

        # 1. 规范化 trade_date 格式为 'YYYY-MM-DD'
        clean_trade_date = str(trade_date).replace("-", "")
        try:
            trade_date_dt = datetime.datetime.strptime(clean_trade_date, "%Y%m%d")
            trade_date_formatted = trade_date_dt.strftime("%Y-%m-%d")
        except Exception as e:
            print(f"❌ 交易日期解析失败: {trade_date}, 错误: {e}")
            return []

        # 2. 严格遵循原语义的市场类型判定
        if market_str in ("us", "us_special", "us_dynamic", "us_backtest"):
            market_val = 1
        elif market_str in ("cn", "cn_dynamic", "cnetf", "cn_backtest"):
            market_val = 2
        else:
            market_val = 0

        # 3. 筛选指定 Target Symbols 范围的数据
        tickers_set = set(target_tickers)
        df = df_raw[df_raw["symbol"].isin(tickers_set)].copy()
        if df.empty:
            return []

        # 4. 全表向量化过滤无效 Symbol (条数 < min_bars 或 不含特定交易日)
        counts = df["symbol"].value_counts()
        valid_len_symbols = set(counts[counts >= min_bars].index)
        valid_date_symbols = set(df[df["date"] == trade_date_formatted]["symbol"].unique())

        valid_symbols = valid_len_symbols.intersection(valid_date_symbols)
        if not valid_symbols:
            return []

        df = df[df["symbol"].isin(valid_symbols)].copy()

        # 5. 全表向量化转换类型与字段重命名/规范
        df["datetime"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df["market"] = market_val

        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].fillna(0.0).astype("float32").round(2)
        df["volume"] = df["volume"].fillna(0).astype("int64")

        # 保留标准列集合并组内升序
        target_cols = ["datetime", "open", "high", "low", "close", "volume", "symbol", "market"]
        df = df[target_cols].sort_values(by=["symbol", "datetime"]).reset_index(drop=True)

        # 6. 向量化构造 Mock Bar (假数据追加)
        if add_mock_bar:
            mock_bars = df.groupby("symbol", as_index=False).last()
            mock_bars["datetime"] = mock_bars["datetime"] + pd.Timedelta(days=1)
            # mock_bars["volume"] = 0  # 可选：重置假数据的成交量
            
            df = pd.concat([df, mock_bars], ignore_index=True)
            df.sort_values(by=["symbol", "datetime"], inplace=True)

        # 7. 切分为按 Symbol 独立且按起始日期排序的 List[DataFrame]
        list_results = [group.reset_index(drop=True) for _, group in df.groupby("symbol")]
        list_results.sort(key=lambda x: x["datetime"].min())

        # 强制清理局部临时数据源
        del df
        gc.collect()

        return list_results

    def get_backtrader_data_feed_testonly(self, stocklist):
        tickers = stocklist
        # 切换不复权数据源
        his_data = self.get_history_data_fqt(target_symbols=tickers)
        return self.format_backtrader_feed(
            df_raw=his_data,
            target_tickers=tickers,
            trade_date=self.trade_date,
            market_str=self.market,  # 内部自动精确判定 1 或 2
            min_bars=61,
            add_mock_bar=True
        )

    def get_etf_list(self):
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

        use_cols = list(column_dtypes.keys())

        dfs = []
        for file in self.files:
            # 1. 动态判断 total_value 是否存在
            sample_df = pd.read_csv(file, nrows=1)
            actual_use_cols = [c for c in use_cols if c in sample_df.columns]

            # 2. 分块读取，在内存载入的第一时间立刻用 startswith 过滤
            for chunk in pd.read_csv(file, usecols=actual_use_cols, chunksize=100000):
                # 极其高效的前缀过滤：只保留 symbol 以 'ETF' 开头的行
                chunk_etf = chunk[chunk["symbol"].astype(str).str.startswith("ETF")]

                if not chunk_etf.empty:
                    chunk_etf = chunk_etf.copy()
                    if "total_value" not in chunk_etf.columns:
                        chunk_etf["total_value"] = 0.0

                    chunk_etf = chunk_etf[use_cols].astype(column_dtypes)
                    dfs.append(chunk_etf)

        if not dfs:
            return []

        # 3. 合并全量 ETF 数据（此时内存占用极小）
        df_all = pd.concat(dfs, ignore_index=True)
        df_all.drop_duplicates(subset=["symbol", "date"], keep="first", inplace=True)

        # 4. 近 60 天筛选
        date_threshold_str = datetime.datetime.strptime(
            self.date_threshold, "%Y%m%d"
        ).strftime("%Y-%m-%d")

        df_recent = df_all[df_all["date"] >= date_threshold_str].copy()

        # 5. 条件筛选
        cond = df_recent["total_value"] > self.etf_min_threshold
        etf_top = self._top_by_activity(
            cond, df_recent, n_groups=self.etf_n_groups, top_n_per_group=self.etf_top_n
        )

        return list(set(etf_top))

    def get_etf_backtrader_data_feed(self):
        tickers = self.get_etf_list()
        # 切换不复权数据源
        his_data = self.get_history_data_fqt(target_symbols=tickers)
        return self.format_backtrader_feed(
            df_raw=his_data,
            target_tickers=tickers,
            trade_date=self.trade_date,
            market_str=self.market,  # 内部自动精确判定 1 或 2
            min_bars=61,
            add_mock_bar=True
        )

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
        date_threshold_str = datetime.datetime.strptime(
            self.date_threshold, "%Y%m%d"
        ).strftime("%Y-%m-%d")

        # 使用相同格式的字符串进行筛选
        df_recent = df_all[df_all["date"] >= date_threshold_str]
        df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
        valid_symbols = df_o["symbol"].unique()
        df_recent = df_recent[df_recent["symbol"].isin(valid_symbols)]

        # 3. 条件筛选
        tiny_cond = (
            (df_recent["total_value"] < 2e9)
            & (df_recent["total_value"] > 1e9)
            & (df_recent["close"] > 3)
        )

        tiny_top = self._top_by_activity(
            tiny_cond, df_recent, n_groups=1, top_n_per_group=50
        )

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
        # 切换不复权数据源
        his_data = self.get_history_data_fqt(target_symbols=tickers_clean)
        return self.format_backtrader_feed(
            df_raw=his_data,
            target_tickers=tickers_clean,
            trade_date=self.trade_date,
            market_str=self.market,  # 内部自动精确判定 1 或 2
            min_bars=61,
            add_mock_bar=True
        )

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
        # 切换不复权数据源
        his_data = self.get_history_data_fqt(target_symbols=tickers_clean)
        return self.format_backtrader_feed(
            df_raw=his_data,
            target_tickers=tickers_clean,
            trade_date=self.trade_date,
            market_str=self.market,  # 内部自动精确判定 1 或 2
            min_bars=61,
            add_mock_bar=True
        )

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
                    f"{row['date'][:4]}-{row['date'][4:6]}-{row['date'][6:8]}"
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

    def get_history_data_fqt(
        self, target_symbols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """计算前复权历史数据（支持股票与 ETF 动态自动识别计算）

        - ETF 标的：拆并股事件平移至 T+1（下一个交易日）生效；
        - 普通股票：除权拆股事件保留在 T 日生效。
        """
        # 1. 传递 target_symbols 给底层 get_history_data，源头过滤 K 线数据
        df = self.get_history_data(target_symbols=target_symbols)
        if df.empty:
            return df

        actions_file = self.file_actions_history
        if not os.path.exists(actions_file):
            return df

        # 基础类型清洗与去空
        df["symbol"] = df["symbol"].astype(str).str.strip()
        df["date"] = df["date"].astype(str).str.strip()

        df.sort_values(["symbol", "date"], ascending=[True, True], inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 获取实际加载成功的 symbol 集合
        active_target_symbols = set(df["symbol"].unique())

        # 2. 读取除权分红事件表
        df_actions = pd.read_csv(
            actions_file,
            usecols=["symbol", "date", "dividend", "split_ratio"],
            dtype={"symbol": str, "date": str},
        )
        df_actions["symbol"] = df_actions["symbol"].astype(str).str.strip()
        df_actions["date"] = df_actions["date"].astype(str).str.strip()

        # 核心优化：仅保留 active_target_symbols 对应的事件数据
        df_actions = df_actions[df_actions["symbol"].isin(active_target_symbols)]
        if df_actions.empty:
            return df

        # 预处理：合并同一标的同一天的多个事件
        df_actions = df_actions.groupby(["symbol", "date"], as_index=False).agg(
            {"dividend": "sum", "split_ratio": "prod"}
        )

        # ==================== 动态识别 ETF 并执行事件平移 ====================
        if not df_actions.empty:
            # 1. 检查 market 是否以 'cn' 开头（忽略大小写，例如 'cn', 'cn_a', 'cn_etf'）
            is_cn_market = str(getattr(self, "market", "")).lower().startswith("cn")

            # 如果不是中国市场，美股等其它市场无需处理 ETF 拆并股平移，直接保持原 df_actions
            if is_cn_market:
                # 建立映射：(symbol, current_date) -> next_trading_date
                dates_df = df[["symbol", "date"]].copy()
                dates_df["next_date"] = dates_df.groupby("symbol")["date"].shift(-1)

                # 关联 next_date 到事件表中
                act_m = pd.merge(
                    df_actions, dates_df, on=["symbol", "date"], how="left"
                )

                # 2. 检查 symbol 是否以 'ETF' 开头（忽略大小写）
                is_etf_symbol = act_m["symbol"].str.upper().str.startswith("ETF")

                # 判定基础条件
                has_div = act_m["dividend"] > 0
                has_split = (act_m["split_ratio"] != 1.0) & (act_m["split_ratio"] > 0)
                can_shift = act_m["next_date"].notna() & (act_m["next_date"] != "")  # 有效的 T+1 交易日

                # 切片处理逻辑：
                # 1) 普通股票 OR 边界最新一天交易日 -> 保留原样（T 日）
                df_no_shift = act_m[(~is_etf_symbol) | (~can_shift)][
                    ["symbol", "date", "dividend", "split_ratio"]
                ]

                # 2) ETF 的现金分红事件 -> 保留在 T 日，拆股比率置 1.0
                etf_div_mask = is_etf_symbol & can_shift & has_div
                df_etf_div = act_m[etf_div_mask][
                    ["symbol", "date", "dividend", "split_ratio"]
                ].copy()
                if not df_etf_div.empty:
                    df_etf_div["split_ratio"] = 1.0

                # 3) ETF 的拆并股事件 -> 向量化平移至 T+1 日（next_date），分红置 0.0
                etf_split_mask = is_etf_symbol & can_shift & has_split
                df_etf_split = act_m[etf_split_mask][
                    ["symbol", "next_date", "dividend", "split_ratio"]
                ].copy()
                if not df_etf_split.empty:
                    df_etf_split.rename(columns={"next_date": "date"}, inplace=True)
                    df_etf_split["dividend"] = 0.0

                # 重新拼接组装
                frames = [f for f in [df_no_shift, df_etf_div, df_etf_split] if not f.empty]
                if frames:
                    df_actions = pd.concat(frames, ignore_index=True)
                    df_actions = df_actions.groupby(
                        ["symbol", "date"], as_index=False
                    ).agg({"dividend": "sum", "split_ratio": "prod"})
                else:
                    df_actions = pd.DataFrame(
                        columns=["symbol", "date", "dividend", "split_ratio"]
                    )

        # ==================== 标准向量化复权计算引擎 ====================
        df = pd.merge(df, df_actions, on=["symbol", "date"], how="left")
        del df_actions

        df["dividend"] = df["dividend"].fillna(0.0)
        df["split_ratio"] = df["split_ratio"].fillna(1.0).replace(0.0, 1.0)

        # 全量数据无除权事件时直接返回
        if (df["dividend"] == 0).all() and (df["split_ratio"] == 1.0).all():
            df.drop(columns=["dividend", "split_ratio"], inplace=True)
            return df

        # 单步拆股与成交量缩放因子
        price_split_factor = 1.0 / df["split_ratio"]
        vol_split_factor = df["split_ratio"]

        rev_index = df.index[::-1]
        rev_symbol = df.loc[rev_index, "symbol"]

        # 倒序累乘计算拆股因子 (shift 1 确保因子仅作用于 T 日之前的历史数据)
        cum_split_price = (
            price_split_factor.reindex(rev_index)
            .groupby(rev_symbol)
            .shift(1, fill_value=1.0)
            .groupby(rev_symbol)
            .cumprod()
            .reindex(df.index)
        )

        cum_vol_factor = (
            vol_split_factor.reindex(rev_index)
            .groupby(rev_symbol)
            .shift(1, fill_value=1.0)
            .groupby(rev_symbol)
            .cumprod()
            .reindex(df.index)
        )

        # 计算分红因子
        prev_close = df.groupby("symbol")["close"].shift(1)
        prev_adjusted_close = prev_close * cum_split_price

        div = df["dividend"].values
        prev_p = prev_adjusted_close.values

        div_factor = np.where(
            (div > 0) & (prev_p > 0), (prev_p - div) / prev_p, 1.0
        )
        np.maximum(div_factor, 0.0001, out=div_factor)

        del prev_close, prev_adjusted_close, div, prev_p

        # 计算总价格累积复权因子
        step_price_factor = price_split_factor * div_factor
        cum_price_factor = (
            step_price_factor.reindex(rev_index)
            .groupby(rev_symbol)
            .shift(1, fill_value=1.0)
            .groupby(rev_symbol)
            .cumprod()
            .reindex(df.index)
        )

        del price_split_factor, div_factor, step_price_factor

        # 映射价格与成交量
        for col in ["open", "high", "low", "close"]:
            df[col] = (df[col] * cum_price_factor).round(4)

        df["volume"] = (df["volume"] * cum_vol_factor).round(0)

        df.drop(columns=["dividend", "split_ratio"], inplace=True)
        del cum_price_factor, cum_vol_factor, cum_split_price

        return df