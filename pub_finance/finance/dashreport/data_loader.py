from __future__ import annotations

import pathlib
import pickle
import pandas as pd
from typing import Dict, Iterable, Optional, Tuple
from functools import lru_cache
import copy
import os


class ReportDataLoader:
    """
    Dash 报告页面数据统一加载器（mtime-aware LRU）
    """

    BASE_PATH = pathlib.Path(__file__).resolve().parent.parent
    DATA_PATH = BASE_PATH / "data"
    CACHE_PATH = BASE_PATH / "cache"

    # =========================
    # Table schemas（唯一真源）
    # =========================
    CATEGORY_SCHEMA = {
        "columns": [
            "IDX",
            "IND",
            "OPEN",
            "LRATIO",
            "L5 OPEN",
            "L5 CLOSE",
            "ERP",
            "PROFIT",
            "PNL RATIO",
            "AVG TRANS",
            "AVG DAYS",
            "WIN RATE",
            "PROFIT TREND",
        ],
        "formats": {
            "OPEN": ("int",),
            "L5 OPEN": ("int",),
            "L5 CLOSE": ("int",),
            "LRATIO": ("ratio", "format"),
            "PROFIT": ("int", "format"),
            "PNL RATIO": ("ratio", "format"),
            "AVG TRANS": ("float",),
            "AVG DAYS": ("float",),
            "WIN RATE": ("ratio", "format"),
            "ERP": ("float",),
        },
    }

    DETAIL_SCHEMA = {
        "columns": [
            "IDX",
            "SYMBOL",
            "IND",
            "NAME",
            "TOTAL VALUE",
            "ERP",
            "SHARPE RATIO",
            "SORTINO RATIO",
            "MAX DD",
            "OPEN DATE",
            "BASE",
            "ADJBASE",
            "PNL",
            "PNL RATIO",
            "AVG TRANS",
            "AVG DAYS",
            "WIN RATE",
            "TOTAL PNL RATIO",
            "STRATEGY",
        ],
        "formats": {
            "BASE": ("float",),
            "ADJBASE": ("float",),
            "PNL": ("int", "format"),
            "AVG TRANS": ("int",),
            "AVG DAYS": ("float",),
            "PNL RATIO": ("ratio", "format"),
            "WIN RATE": ("ratio", "format"),
            "TOTAL PNL RATIO": ("ratio", "format"),
            "OPEN DATE": ("date", "format"),
            "TOTAL VALUE": ("float",),
            "ERP": ("float",),
            "SHARPE RATIO": ("float",),
            "SORTINO RATIO": ("float",),
            "MAX DD": ("ratio", "format"),
        },
    }

    DETAIL_SHORT_SCHEMA = {
        "columns": [
            "IDX",
            "SYMBOL",
            "IND",
            "NAME",
            "TOTAL VALUE",
            "ERP",
            "OPEN DATE",
            "CLOSE DATE",
            "BASE",
            "ADJBASE",
            "PNL",
            "PNL RATIO",
            "HIS DAYS",
            "STRATEGY",
        ],
        "formats": {
            "TOTAL VALUE": ("float",),
            "ERP": ("float",),
            "BASE": ("float",),
            "ADJBASE": ("float",),
            "PNL": ("int", "format"),
            "PNL RATIO": ("ratio", "format"),
            "HIS DAYS": ("int",),
        },
    }

    CNETF_SCHEMA = {
        "columns": [
            "IDX",
            "SYMBOL",
            "NAME",
            "TOTAL VALUE",
            "OPEN DATE",
            "BASE",
            "ADJBASE",
            "PNL",
            "PNL RATIO",
            "AVG TRANS",
            "AVG DAYS",
            "WIN RATE",
            "TOTAL PNL RATIO",
            "STRATEGY",
        ],
        "formats": {
            "BASE": ("float",),
            "ADJBASE": ("float",),
            "PNL": ("int", "format"),
            "AVG TRANS": ("int",),
            "AVG DAYS": ("float",),
            "PNL RATIO": ("ratio", "format"),
            "WIN RATE": ("ratio", "format"),
            "TOTAL PNL RATIO": ("ratio", "format"),
            "OPEN DATE": ("date", "format"),
            "TOTAL VALUE": ("float",),
        },
    }

    @staticmethod
    def _apply_schema(df: pd.DataFrame, schema: dict) -> Dict[str, object]:
        """
        统一处理：
        - IDX
        - 列裁剪 / 顺序
        - 返回 df + columns + formats
        """
        if df.empty:
            return {
                "df": df,
                "columns": schema["columns"],
                "formats": schema["formats"],
            }

        df["IDX"] = df.index

        df = df[schema["columns"]]

        return {
            "df": df,
            "columns": schema["columns"],
            "formats": schema["formats"],
        }

    # =========================================================
    # 对外入口（不缓存，返回 deepcopy）
    # =========================================================
    @staticmethod
    def load(
        prefix: str,
        datasets: Optional[Iterable[str]] = None,
    ) -> Dict[str, object]:

        datasets_key = ReportDataLoader._normalize_datasets(datasets)
        mtime_sig = ReportDataLoader._mtime_signature(prefix, datasets_key)

        data = ReportDataLoader._load_cached(
            prefix,
            datasets_key,
            mtime_sig,
        )

        # 防止外部修改污染缓存
        return copy.deepcopy(data)

    # =========================================================
    # mtime-aware LRU（真正缓存点）
    # =========================================================
    @staticmethod
    @lru_cache(maxsize=16)
    def _load_cached(
        prefix: str,
        datasets: Tuple[str, ...],
        mtime_sig: Tuple[Tuple[str, float], ...],
    ) -> Dict[str, object]:

        loaders = {
            "overall": ReportDataLoader._load_overall,
            "annual_return": ReportDataLoader._load_annual_return,
            "heatmap": ReportDataLoader._load_heatmap,
            "strategy": ReportDataLoader._load_strategy,
            "trade": ReportDataLoader._load_trade_info,
            "pnl_trend": ReportDataLoader._load_pnl_trend,
            "industry_position": ReportDataLoader._load_industry_position,
            "industry_profit": ReportDataLoader._load_industry_profit,
            "category": ReportDataLoader._load_category,
            "detail": ReportDataLoader._load_detail,
            "detail_short": ReportDataLoader._load_detail_short,
            "cn_etf": ReportDataLoader._load_cnetf,
        }
        print("PID:", os.getpid(), "loading CSV")

        result: Dict[str, object] = {}

        for name in datasets:
            loader = loaders.get(name)
            if loader:
                result[name] = loader(prefix)

        return result

    # =========================================================
    # datasets 规范化
    # =========================================================
    @staticmethod
    def _normalize_datasets(
        datasets: Optional[Iterable[str]],
    ) -> Tuple[str, ...]:

        if datasets is None:
            return tuple(
                sorted(
                    (
                        "overall",
                        "annual_return",
                        "heatmap",
                        "strategy",
                        "trade",
                        "pnl_trend",
                        "industry_position",
                        "industry_profit",
                        "category",
                        "detail",
                        "detail_short",
                        "cn_etf",
                    )
                )
            )
        return tuple(sorted(datasets))

    # =========================================================
    # mtime 签名（cache 失效关键）
    # =========================================================
    @staticmethod
    def _mtime_signature(
        prefix: str,
        datasets: Tuple[str, ...],
    ) -> Tuple[Tuple[str, float], ...]:

        files = []

        for ds in datasets:
            files.extend(ReportDataLoader._dataset_files(prefix, ds))

        sig = []
        for path in files:
            try:
                sig.append((path.name, path.stat().st_mtime))
            except FileNotFoundError:
                sig.append((path.name, -1))

        return tuple(sorted(sig))

    @staticmethod
    def _dataset_files(prefix: str, dataset: str):

        D = ReportDataLoader.DATA_PATH
        C = ReportDataLoader.CACHE_PATH

        mapping = {
            "overall": [D / f"{prefix}_df_result.csv"],
            "heatmap": [D / f"{prefix}_pd_calendar_heatmap.csv"],
            "strategy": [D / f"{prefix}_pd_strategy_tracking_lst180days.csv"],
            "trade": [D / f"{prefix}_pd_trade_info_lst180days.csv"],
            "pnl_trend": [D / f"{prefix}_pd_top5_industry_profit_trend.csv"],
            "industry_position": [D / f"{prefix}_pd_top20_industry.csv"],
            "industry_profit": [D / f"{prefix}_pd_top20_profit_industry.csv"],
            "category": [D / f"{prefix}_category.csv"],
            "detail": [D / f"{prefix}_stockdetail.csv"],
            "detail_short": [D / f"{prefix}_stockdetail_short.csv"],
            "cn_etf": [D / f"{prefix}_etf.csv"],
        }

        if dataset == "annual_return":
            overall = D / f"{prefix}_df_result.csv"
            if not overall.exists():
                return []
            try:
                df = pd.read_csv(overall, usecols=[4], nrows=1)
                trade_date = str(df.iloc[0, 0]).replace("-", "")
                return [
                    overall,
                    C / f"pnl_{prefix}_{trade_date}.pkl",
                ]
            except Exception:
                return [overall]

        return mapping.get(dataset, [])

    # =========================================================
    # 数据加载器（dtype 全部就地定义）
    # =========================================================
    @staticmethod
    def _load_overall(prefix: str) -> pd.DataFrame:
        path = ReportDataLoader.DATA_PATH / f"{prefix}_df_result.csv"

        if not path.exists():
            return pd.DataFrame(
                columns=["cash", "final_value", "stock_cnt", "end_date"]
            )

        df = pd.read_csv(
            path,
            usecols=[1, 2, 3, 4],
            dtype={
                "cash": "float64",
                "final_value": "float64",
                "stock_cnt": "Int64",
                "end_date": "string",
            },
        )

        if df.empty or pd.isna(df.at[0, "stock_cnt"]) or df.at[0, "stock_cnt"] <= 0:
            df.at[0, "stock_cnt"] = 1

        return df

    @staticmethod
    def _load_annual_return(prefix: str):
        overall = ReportDataLoader._load_overall(prefix)
        if overall.empty:
            return None

        trade_date = str(overall.at[0, "end_date"]).replace("-", "")
        pkl = ReportDataLoader.CACHE_PATH / f"pnl_{prefix}_{trade_date}.pkl"

        if not pkl.exists():
            return None

        with open(pkl, "rb") as f:
            return pickle.load(f)

    @staticmethod
    def _safe_csv(filename: str) -> pd.DataFrame:
        path = ReportDataLoader.DATA_PATH / filename
        return pd.read_csv(path) if path.exists() else pd.DataFrame()

    @staticmethod
    def _load_heatmap(prefix: str) -> pd.DataFrame:
        return ReportDataLoader._safe_csv(f"{prefix}_pd_calendar_heatmap.csv")

    @staticmethod
    def _load_strategy(prefix: str) -> pd.DataFrame:
        return ReportDataLoader._safe_csv(
            f"{prefix}_pd_strategy_tracking_lst180days.csv"
        )

    @staticmethod
    def _load_trade_info(prefix: str) -> pd.DataFrame:
        return ReportDataLoader._safe_csv(f"{prefix}_pd_trade_info_lst180days.csv")

    @staticmethod
    def _load_pnl_trend(prefix: str) -> pd.DataFrame:
        return ReportDataLoader._safe_csv(f"{prefix}_pd_top5_industry_profit_trend.csv")

    @staticmethod
    def _load_industry_position(prefix: str) -> pd.DataFrame:
        return ReportDataLoader._safe_csv(f"{prefix}_pd_top20_industry.csv")

    @staticmethod
    def _load_industry_profit(prefix: str) -> pd.DataFrame:
        return ReportDataLoader._safe_csv(f"{prefix}_pd_top20_profit_industry.csv")

    @staticmethod
    def _load_category(prefix: str) -> Dict[str, object]:
        path = ReportDataLoader.DATA_PATH / f"{prefix}_category.csv"
        if not path.exists():
            return ReportDataLoader._apply_schema(
                pd.DataFrame(),
                ReportDataLoader.CATEGORY_SCHEMA,
            )

        df = pd.read_csv(
            path,
            usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15],
            dtype={
                "IND": "string",
                "OPEN": "int64",
                "L5 OPEN": "int64",
                "L5 CLOSE": "int64",
                "ERP": "string",
                "PROFIT": "float64",
                "PNL RATIO": "float64",
                "LRATIO": "float64",
                "AVG TRANS": "float64",
                "AVG DAYS": "float64",
                "WIN RATE": "float64",
                "PROFIT TREND": "string",
            },
        )

        return ReportDataLoader._apply_schema(
            df,
            ReportDataLoader.CATEGORY_SCHEMA,
        )

    @staticmethod
    def _load_detail(prefix: str) -> Dict[str, object]:
        path = ReportDataLoader.DATA_PATH / f"{prefix}_stockdetail.csv"
        if not path.exists():
            return ReportDataLoader._apply_schema(
                pd.DataFrame(),
                ReportDataLoader.DETAIL_SCHEMA,
            )

        df = pd.read_csv(
            path,
            usecols=range(1, 19),
            dtype={
                "SYMBOL": "string",
                "IND": "string",
                "NAME": "string",
                "TOTAL VALUE": "float64",
                "ERP": "string",
                "SHARPE RATIO": "float64",
                "SORTINO RATIO": "float64",
                "MAX DD": "float64",
                "OPEN DATE": "string",
                "BASE": "float64",
                "ADJBASE": "float64",
                "PNL": "float64",
                "PNL RATIO": "float64",
                "AVG TRANS": "float64",
                "AVG DAYS": "float64",
                "WIN RATE": "float64",
                "TOTAL PNL RATIO": "float64",
                "STRATEGY": "string",
            },
        )

        return ReportDataLoader._apply_schema(
            df,
            ReportDataLoader.DETAIL_SCHEMA,
        )

    @staticmethod
    def _load_detail_short(prefix: str) -> Dict[str, object]:
        path = ReportDataLoader.DATA_PATH / f"{prefix}_stockdetail_short.csv"
        if not path.exists():
            return ReportDataLoader._apply_schema(
                pd.DataFrame(),
                ReportDataLoader.DETAIL_SHORT_SCHEMA,
            )

        df = pd.read_csv(
            path,
            usecols=range(1, 14),
            dtype={
                "SYMBOL": "string",
                "IND": "string",
                "NAME": "string",
                "TOTAL VALUE": "float64",
                "ERP": "string",
                "OPEN DATE": "string",
                "CLOSE DATE": "string",
                "BASE": "float64",
                "ADJBASE": "float64",
                "PNL": "float64",
                "PNL RATIO": "float64",
                "HIS DAYS": "float64",
                "STRATEGY": "string",
            },
        )

        return ReportDataLoader._apply_schema(
            df,
            ReportDataLoader.DETAIL_SHORT_SCHEMA,
        )

    @staticmethod
    def _load_cnetf(prefix: str) -> Dict[str, object]:
        path = ReportDataLoader.DATA_PATH / f"{prefix}_etf.csv"
        if not path.exists():
            return ReportDataLoader._apply_schema(
                pd.DataFrame(),
                ReportDataLoader.CNETF_SCHEMA,
            )

        df = pd.read_csv(
            path,
            usecols=range(1, 14),
            dtype={
                "SYMBOL": str,
                "NAME": str,
                "TOTAL VALUE": float,
                "OPEN DATE": str,
                "BASE": float,
                "ADJBASE": float,
                "PNL": float,
                "PNL RATIO": float,
                "AVG TRANS": float,
                "AVG DAYS": float,
                "WIN RATE": float,
                "TOTAL PNL RATIO": float,
                "STRATEGY": str,
            },
        )

        return ReportDataLoader._apply_schema(
            df,
            ReportDataLoader.CNETF_SCHEMA,
        )
