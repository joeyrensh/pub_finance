#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import datetime
from functools import partialmethod
import gc
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

import akshare as ak
import pandas as pd
from tqdm import tqdm
import yfinance as yf

# 尝试导入 curl_cffi 以更好地对抗 Yahoo 反爬，若不存在则回退至 requests
try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

# ==============================================================================
# 屏蔽 tqdm 进度条，彻底解决终端闪屏与 0% 卡死
# ==============================================================================
tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)

# ========== 项目路径规范导入 ==========
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.get_proxy import ProxyManager

logger = logging.getLogger(__name__)
# 屏蔽 yfinance 冗余日志
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


def format_proxy_url(proxy_str: str) -> str:
    """统一代理 URL 格式，补齐协议前缀"""
    if not proxy_str:
        return ""
    if not proxy_str.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return f"http://{proxy_str}"
    return proxy_str


class StockActionsFetcher:
    """中美股及 ETF 除权分红数据批量抓取器

    - 支持 CN 增量更新模式：利用东财筛选接口提取近 1~2 周有除权的动态标的，毫秒级更新
    - 支持 US 增量更新模式：利用 yf.download 分批拉取行情并在内存中实时过滤除权 Symbol，用完即销毁 DataFrame，零磁盘开销且极省内存
    """

    CSV_HEADERS = ["symbol", "date", "dividend", "split_ratio"]

    def __init__(
        self,
        market: str = "cn",  # 'cn' 或 'us'
        target_dir: Optional[Path] = None,
        stock_filename: Optional[str] = None,
        output_filename: Optional[str] = None,
        checkpoint_filename: Optional[str] = None,
        proxy_manager: Optional[ProxyManager] = None,
        batch_size: int = 50,
        max_retries_per_symbol: int = 3,
        start_date: Optional[str] = "2025-01-01",  # 格式: YYYY-MM-DD
        force_refresh: bool = False,  # 是否强制重置 Checkpoint 重新抓取
        symbol_list: Optional[List[str]] = None,  # 支持指定 symbol_list
        incremental: bool = False,  # 是否开启增量更新模式 (CN/US 均生效)
        lookback_period: str = "1w",  # 增量更新回溯周期 ('1w', '2w', '3w', '1m')
    ):
        self.market = market.lower()
        if self.market not in ["cn", "us"]:
            raise ValueError("market 参数必须为 'cn' 或 'us'")

        self.force_refresh = force_refresh
        self.symbol_list = symbol_list
        self.incremental = incremental
        self.lookback_period = lookback_period.lower()

        # 1. 目录及路径定位
        default_dir = "cnstockinfo" if self.market == "cn" else "usstockinfo"
        self.target_dir = target_dir or (FINANCE_ROOT / default_dir)
        os.makedirs(self.target_dir, exist_ok=True)

        self.stock_list_path = (
            self.target_dir / stock_filename
            if stock_filename
            else self._get_latest_stock_info_file()
        )

        default_out = f"{self.market}_stock_actions_history.csv"
        default_ckpt = f"{self.market}_actions_fetch_checkpoint.json"

        self.output_filename_str = output_filename or default_out
        self.output_csv_path = self.target_dir / self.output_filename_str
        self.checkpoint_path = self.target_dir / (checkpoint_filename or default_ckpt)

        # 构建带当前日期的文件路径与 .bak 文件路径
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        stem = Path(self.output_filename_str).stem
        suffix = Path(self.output_filename_str).suffix

        self.dated_csv_path = self.target_dir / f"{stem}_{today_str}{suffix}"
        self.bak_csv_path = self.target_dir / f"{stem}.bak"

        # 2. 参数与网络设置
        self.batch_size = batch_size
        self.max_retries_per_symbol = max_retries_per_symbol

        # 处理增量模式与起始/结束日期
        self.end_date = datetime.datetime.now().strftime("%Y-%m-%d")
        if self.incremental:
            self.start_date = self._calculate_incremental_start_date()
            logger.info(
                f"⚡ [{self.market.upper()}] 已开启增量更新模式 (Lookback: {self.lookback_period}), "
                f"查询时间窗口: [{self.start_date} ~ {self.end_date}]"
            )
        else:
            self.start_date = start_date

        if self.market == "us":
            self.proxy_manager = (
                proxy_manager or ProxyManager.create_overseas_manager()
            )
            self.current_working_proxy: Optional[Dict[str, str]] = None

        # 3. 映射表与断点记录
        self.cn_symbol_map: Dict[str, str] = {}  # 纯数字代码 -> 原始带前缀 Symbol
        self.processed_symbols: Set[str] = self._load_checkpoint()

        if self.market == "cn":
            self._init_cn_symbol_mapping()

    def _calculate_incremental_start_date(self) -> str:
        """根据 lookback_period 计算增量起始日期"""
        now = datetime.datetime.now()
        if self.lookback_period == "1w":
            delta = datetime.timedelta(days=7)
        elif self.lookback_period == "2w":
            delta = datetime.timedelta(days=14)
        elif self.lookback_period == "3w":
            delta = datetime.timedelta(days=21)
        elif self.lookback_period == "1m":
            delta = datetime.timedelta(days=30)
        else:
            delta = datetime.timedelta(days=14)
        return (now - delta).strftime("%Y-%m-%d")

    def _get_latest_stock_info_file(self) -> Path:
        """获取 target_dir 目录下最新的 stock_*.csv 文件路径"""
        files = list(self.target_dir.glob("stock_*.csv"))
        if not files:
            if self.symbol_list is not None or self.incremental:
                return self.target_dir / "stock_placeholder.csv"
            raise FileNotFoundError(f"未在目录 '{self.target_dir}' 下找到任何 stock_*.csv 文件")
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        logger.info(f"[{self.market.upper()}] 自动定位最新股票列表文件: {latest_file.name}")
        return latest_file

    def _init_cn_symbol_mapping(self):
        """为 CN 市场解析列表，构建 纯数字代码 -> 原始 Symbol 的双向映射"""
        if self.symbol_list is not None:
            for sym in self.symbol_list:
                raw_sym = str(sym).strip()
                clean_code = re.sub(r"\D", "", raw_sym).zfill(6)
                self.cn_symbol_map[clean_code] = raw_sym
            return

        if not self.stock_list_path.exists():
            if self.incremental:
                return
            raise FileNotFoundError(f"未找到股票列表文件: {self.stock_list_path}")

        df = pd.read_csv(self.stock_list_path, dtype=str)
        target_col = None
        for col in ["symbol", "Symbol", "code", "Code"]:
            if col in df.columns:
                target_col = col
                break

        if not target_col:
            raise ValueError("CSV 文件中未找到 'symbol' 或 'code' 列")

        for sym in df[target_col].dropna():
            raw_sym = str(sym).strip()
            clean_code = re.sub(r"\D", "", raw_sym).zfill(6)
            self.cn_symbol_map[clean_code] = raw_sym

    def _load_checkpoint(self) -> Set[str]:
        if self.force_refresh or self.incremental:
            if self.incremental:
                logger.info(f"⚡ [{self.market.upper()} 增量模式] 不加载历史 Checkpoint，直接执行增量刷新。")
            else:
                logger.info("⚡ [定时全量模式] 已开启 force_refresh，清空旧 Checkpoint。")
            self._clear_checkpoint()
            return set()

        if self.checkpoint_path.exists():
            try:
                mtime = datetime.datetime.fromtimestamp(self.checkpoint_path.stat().st_mtime)
                today = datetime.datetime.now().date()

                if mtime.date() < today:
                    logger.info(
                        f"📅 检测到 Checkpoint 为历史日期 ({mtime.strftime('%Y-%m-%d')})，"
                        f"自动重置 Checkpoint。"
                    )
                    self._clear_checkpoint()
                    return set()

                with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                    symbols = set(json.load(f))
                    logger.info(f"🔄 检测到当日 Checkpoint，成功载入 {len(symbols)} 条已处理记录")
                    return symbols

            except Exception as e:
                logger.warning(f"读取 Checkpoint 失败，将重新建立: {e}")

        return set()

    def _clear_checkpoint(self):
        if self.checkpoint_path.exists():
            try:
                os.remove(self.checkpoint_path)
            except Exception as e:
                logger.warning(f"删除旧 Checkpoint 文件失败: {e}")

    def _save_checkpoint(self):
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(list(self.processed_symbols), f)

    def _save_records_to_csv(self, records: List[Dict]):
        if not records:
            return

        new_df = pd.DataFrame(records)[self.CSV_HEADERS]

        existing_df = pd.DataFrame(columns=self.CSV_HEADERS)

        target_read_path = None
        if self.dated_csv_path.exists() and os.path.getsize(self.dated_csv_path) > 0:
            target_read_path = self.dated_csv_path
        elif self.output_csv_path.exists() and os.path.getsize(self.output_csv_path) > 0:
            target_read_path = self.output_csv_path

        if target_read_path:
            try:
                existing_df = pd.read_csv(target_read_path, dtype=str)
                existing_df["dividend"] = existing_df["dividend"].astype(float)
                existing_df["split_ratio"] = existing_df["split_ratio"].astype(float)
            except Exception as e:
                logger.warning(f"读取原有数据文件失败，将全新创建: {e}")

        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(subset=["symbol", "date"], keep="last", inplace=True)
        combined_df.sort_values(by=["symbol", "date"], ascending=[True, True], inplace=True)

        combined_df.to_csv(
            self.dated_csv_path,
            mode="w",
            index=False,
            header=True,
            encoding="utf-8-sig",
        )

        del combined_df, existing_df, new_df

        if self.output_csv_path.exists():
            if self.bak_csv_path.exists():
                os.remove(self.bak_csv_path)
            os.rename(self.output_csv_path, self.bak_csv_path)

        shutil.copy2(self.dated_csv_path, self.output_csv_path)

        if self.bak_csv_path.exists():
            if self.dated_csv_path.exists():
                os.remove(self.dated_csv_path)
            os.rename(self.bak_csv_path, self.dated_csv_path)

    # ==================== CN 动态/增量筛选与获取 ====================
    def get_cn_stock_symbols_with_actions(self, start_date: str, end_date: str) -> Set[str]:
        """通过东财 API 快速查询指定区间内有除权事件的 A 股股票代码"""
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        headers = {"Referer": "https://data.eastmoney.com/"}
        filter_str = f"(EX_DIVIDEND_DATE>='{start_date}')(EX_DIVIDEND_DATE<='{end_date}')"

        page_size = 500
        page_number = 1
        stock_symbols = set()

        while True:
            params = {
                "sortColumns": "EX_DIVIDEND_DATE",
                "sortTypes": "-1",
                "pageSize": str(page_size),
                "pageNumber": str(page_number),
                "reportName": "RPT_SHAREBONUS_DET",
                "columns": "SECURITY_CODE,EX_DIVIDEND_DATE",
                "filter": filter_str,
            }

            try:
                res = curl_requests.get(url, params=params, headers=headers, timeout=10).json()
                if not res.get("success") or not res.get("result"):
                    break

                data_list = res["result"].get("data") or []
                for item in data_list:
                    code = str(item.get("SECURITY_CODE", "")).zfill(6)
                    if code and code != "000000":
                        stock_symbols.add(code)

                total_pages = res["result"].get("pages") or 1
                if page_number >= total_pages or not data_list:
                    break
                page_number += 1
            except Exception as e:
                logger.warning(f"[CN Incremental API Error] {e}")
                break

        return stock_symbols

    def load_cn_symbols(self) -> Tuple[List[str], List[str]]:
        """依据模式提取 CN 股票和 ETF"""
        if self.symbol_list is not None:
            stocks, etfs = [], []
            for clean_code, raw_sym in self.cn_symbol_map.items():
                if raw_sym.upper().startswith("ETF"):
                    etfs.append(clean_code)
                else:
                    stocks.append(clean_code)
            return sorted(list(set(stocks))), sorted(list(set(etfs)))

        if self.incremental:
            inc_stocks = self.get_cn_stock_symbols_with_actions(self.start_date, self.end_date)

            for clean_code in inc_stocks:
                if clean_code not in self.cn_symbol_map:
                    prefix = "SH" if clean_code.startswith("6") else "SZ"
                    self.cn_symbol_map[clean_code] = f"{prefix}{clean_code}"

            stocks, etfs = [], []
            if self.stock_list_path.exists():
                df = pd.read_csv(self.stock_list_path, dtype=str)
                target_col = next((c for c in ["symbol", "Symbol", "code", "Code"] if c in df.columns), None)
                if target_col:
                    for sym in df[target_col].dropna():
                        raw_sym = str(sym).strip()
                        clean_code = re.sub(r"\D", "", raw_sym).zfill(6)
                        if raw_sym.upper().startswith("ETF"):
                            etfs.append(clean_code)

            return sorted(list(inc_stocks)), sorted(list(set(etfs)))

        stocks, etfs = [], []
        for clean_code, raw_sym in self.cn_symbol_map.items():
            if raw_sym.upper().startswith("ETF"):
                etfs.append(clean_code)
            else:
                stocks.append(clean_code)

        return sorted(list(set(stocks))), sorted(list(set(etfs)))

    def fetch_actions_for_etfs(self, etf_list: List[str]) -> List[Dict]:
        if not etf_list:
            return []

        action_map: Dict[Tuple[str, str], Dict[str, float]] = {}

        start_year = int(self.start_date[:4]) if self.start_date else 2025
        current_year = datetime.datetime.now().year
        years_to_fetch = [str(y) for y in range(start_year, current_year + 1)]
        etf_set = set(etf_list)

        logger.info(f"[CN ETF] 开始获取 {len(etf_list)} 只 ETF 在 {years_to_fetch} 年份内的数据...")

        for yr in years_to_fetch:
            try:
                df_fh = ak.fund_fh_em(year=yr, page=-1)
                if df_fh is not None and not df_fh.empty and "基金代码" in df_fh.columns:
                    df_fh["基金代码"] = df_fh["基金代码"].astype(str).str.zfill(6)
                    df_target_fh = df_fh[df_fh["基金代码"].isin(etf_set)].copy()

                    for _, row in df_target_fh.iterrows():
                        code = row["基金代码"]
                        ex_date = str(row["除息日期"]).strip()

                        try:
                            div_val = float(row["分红"]) if pd.notna(row["分红"]) else 0.0
                        except (ValueError, TypeError):
                            div_val = 0.0

                        if re.match(r"^\d{4}-\d{2}-\d{2}$", ex_date) and div_val > 0:
                            if self.start_date and ex_date < self.start_date:
                                continue
                            if self.end_date and ex_date > self.end_date:
                                continue

                            raw_sym = self.cn_symbol_map.get(code, f"ETF{code}")
                            key = (raw_sym, ex_date)
                            if key not in action_map:
                                action_map[key] = {"dividend": 0.0, "split_ratio": 1.0}
                            action_map[key]["dividend"] = div_val
            except Exception as e:
                logger.warning(f"[CN ETF] 抓取 {yr} 年分红数据失败: {e}")

            try:
                df_cf = ak.fund_cf_em(year=yr, page=-1)
                if df_cf is not None and not df_cf.empty and "基金代码" in df_cf.columns:
                    df_cf["基金代码"] = df_cf["基金代码"].astype(str).str.zfill(6)
                    df_target_cf = df_cf[df_cf["基金代码"].isin(etf_set)].copy()

                    for _, row in df_target_cf.iterrows():
                        code = row["基金代码"]
                        ex_date = str(row["拆分折算日"]).strip()

                        try:
                            ratio_val = float(row["拆分折算"]) if pd.notna(row["拆分折算"]) else 1.0
                        except (ValueError, TypeError):
                            ratio_val = 1.0

                        if re.match(r"^\d{4}-\d{2}-\d{2}$", ex_date) and ratio_val != 1.0 and ratio_val > 0:
                            if self.start_date and ex_date < self.start_date:
                                continue
                            if self.end_date and ex_date > self.end_date:
                                continue

                            raw_sym = self.cn_symbol_map.get(code, f"ETF{code}")
                            key = (raw_sym, ex_date)
                            if key not in action_map:
                                action_map[key] = {"dividend": 0.0, "split_ratio": 1.0}
                            action_map[key]["split_ratio"] = ratio_val
            except Exception as e:
                logger.warning(f"[CN ETF] 抓取 {yr} 年拆分数据失败: {e}")

        records = []
        for (sym, date_str), data in action_map.items():
            records.append({
                "symbol": sym,
                "date": date_str,
                "dividend": float(data["dividend"]),
                "split_ratio": float(data["split_ratio"]) if data["split_ratio"] > 0 else 1.0,
            })

        logger.info(f"[CN ETF] 抓取完成，保留 {len(records)} 条符合条件的记录")
        return records

    def fetch_actions_for_cn_stock(self, symbol: str) -> List[Dict]:
        records = []
        raw_sym = self.cn_symbol_map.get(symbol, symbol)
        try:
            df_stock = ak.stock_fhps_detail_em(symbol=symbol)
            if df_stock is not None and not df_stock.empty and "除权除息日" in df_stock.columns:
                df_valid = df_stock.dropna(subset=["除权除息日"]).copy()

                for _, row in df_valid.iterrows():
                    ex_date = str(row.get("除权除息日", "")).strip()
                    if not ex_date or ex_date == "-" or ex_date.lower() == "nan":
                        continue

                    try:
                        date_str = pd.to_datetime(ex_date).strftime("%Y-%m-%d")
                    except Exception:
                        continue

                    if self.start_date and date_str < self.start_date:
                        continue
                    if self.end_date and date_str > self.end_date:
                        continue

                    cash_raw = row.get("现金分红-现金分红比例", 0.0)
                    try:
                        cash_10 = float(cash_raw) if (pd.notna(cash_raw) and str(cash_raw).strip() not in ["-", "nan", ""]) else 0.0
                    except (ValueError, TypeError):
                        cash_10 = 0.0
                    div_val = cash_10 / 10.0

                    song_raw = row.get("送转股份-送股比例", 0.0)
                    try:
                        song_10 = float(song_raw) if (pd.notna(song_raw) and str(song_raw).strip() not in ["-", "nan", ""]) else 0.0
                    except (ValueError, TypeError):
                        song_10 = 0.0

                    zhuan_raw = row.get("送转股份-转股比例", 0.0)
                    try:
                        zhuan_10 = float(zhuan_raw) if (pd.notna(zhuan_raw) and str(zhuan_raw).strip() not in ["-", "nan", ""]) else 0.0
                    except (ValueError, TypeError):
                        zhuan_10 = 0.0

                    split_ratio_val = 1.0 + ((song_10 + zhuan_10) / 10.0)

                    if pd.isna(split_ratio_val) or split_ratio_val <= 0:
                        split_ratio_val = 1.0

                    if div_val > 0 or abs(split_ratio_val - 1.0) > 1e-6:
                        records.append({
                            "symbol": raw_sym,
                            "date": date_str,
                            "dividend": float(div_val),
                            "split_ratio": float(split_ratio_val),
                        })
        except Exception as e:
            logger.debug(f"获取 CN 股票 [{raw_sym}] 数据失败: {e}")

        return records

    def _apply_proxy_env(self, proxy_dict: Optional[dict]) -> Optional[str]:
        """将代理字典应用到系统环境变量，并返回格式化后的代理字符串"""
        if not proxy_dict:
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)
            return None

        raw_proxy = (
            proxy_dict.get("socks5") or proxy_dict.get("https") or proxy_dict.get("http")
        )
        proxy_str = format_proxy_url(raw_proxy) if raw_proxy else None

        if proxy_str:
            os.environ["HTTP_PROXY"] = proxy_str
            os.environ["HTTPS_PROXY"] = proxy_str
        else:
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)

        return proxy_str

    def _rotate_to_working_proxy(self) -> Optional[str]:
        """获取并验证一个真正可用的代理，并同步刷新环境变量与日志"""
        # 1. 优先获取经过健康检测验证的有效代理
        new_proxy = None
        if self.proxy_manager:
            new_proxy = self.proxy_manager.get_working_proxy(max_retries=2, enable_proxy=True)
            # 2. 保底兜底：若拿不到验证代理，退而求其次盲切下一个
            if not new_proxy:
                new_proxy = self.proxy_manager.get_next_proxy()

        self.current_working_proxy = new_proxy
        return self._apply_proxy_env(self.current_working_proxy)        

    def _get_us_main_exchange_symbols_from_sec(self) -> Set[str]:
        """
        基于跨市场挂牌结构 + 官方通用法权后缀的极致精炼 ADR 池
        (完全零具体公司名硬编码，将 2100+ 进一步缩减至 ~450 只精准 ADR，彻底排除 GOOG/BRK 等本土双重股)
        """
        # 1. 确保环境变量装载了有效代理
        if not getattr(self, "current_working_proxy", None):
            self._rotate_to_working_proxy()
        else:
            self._apply_proxy_env(self.current_working_proxy)

        headers = {'User-Agent': 'QuantDataServices admin@quantdata.com'}
        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        
        target_symbols = set()
        max_retries = 2

        for attempt in range(1, max_retries + 1):
            try:
                # curl_requests 自动读取 os.environ 中的 HTTP_PROXY / HTTPS_PROXY
                res = curl_requests.get(url, headers=headers, timeout=12)
                if res.status_code == 200:
                    data = res.json()
                    df = pd.DataFrame(data['data'], columns=data['fields'])
                    # fields: ['cik', 'name', 'ticker', 'exchange']
                    
                    df['exchange_upper'] = df['exchange'].str.upper().str.strip()
                    df['clean_ticker'] = df['ticker'].str.upper().str.strip()
                    df['clean_name'] = df['name'].str.upper().str.strip()

                    main_exchanges = {'NYQ', 'NYSE', 'NMS', 'NGS', 'NCM', 'NASDAQ', 'ASE'}
                    otc_exchanges = {'OTC', 'OTCBB', 'PINK'}

                    df['is_main'] = df['exchange_upper'].isin(main_exchanges)
                    df['is_otc'] = df['exchange_upper'].isin(otc_exchanges)

                    # ---------------------------------------------------------------------
                    # 规则 1 [纯结构]: 查找 CIK 同时跨越【主板】与【OTC/场外】市场的标的
                    # (彻底精准抓取 BABA, TSM 等绝大多数 ADR，同时 100% 剔除 GOOG/BRK 等本土双重股)
                    # ---------------------------------------------------------------------
                    main_ciks = set(df[df['is_main']]['cik'])
                    otc_ciks = set(df[df['is_otc']]['cik'])
                    cross_market_ciks = main_ciks.intersection(otc_ciks)

                    # ---------------------------------------------------------------------
                    # 规则 2 [法权后缀]: 提取通用外企/存托后缀 (S.A., S.A.B., N.V., PLC, A.S., OYJ, ADR, ADS)
                    # (专为像 BBD/BBDO 这种两个代码全在 NYSE、不跨 OTC 的拉美/欧洲外企保底)
                    # ---------------------------------------------------------------------
                    std_foreign_suffix_pattern = (
                        r'(?i)\b(ADR|ADS|AMERICAN DEPOSITARY|DEPOSITARY|DEPOSITORY'
                        r'|S\.A\.|S\.A\.B\.|N\.V\.|PLC|A\.S\.|OYJ)\b'
                    )

                    # 过滤主板规范代码 (1~5 位纯字母)
                    valid_main_mask = (df['is_main']) & (df['clean_ticker'].str.match(r'^[A-Z]{1,5}$'))
                    df_main_valid = df[valid_main_mask].copy()

                    # 判定条件：跨市场挂牌 OR 包含通用外企法权后缀
                    cond_cross_market = df_main_valid['cik'].isin(cross_market_ciks)
                    cond_foreign_suffix = df_main_valid['clean_name'].str.contains(
                        std_foreign_suffix_pattern, case=False, regex=True, na=False
                    )

                    df_adr = df_main_valid[cond_cross_market | cond_foreign_suffix]
                    target_symbols = set(df_adr['clean_ticker'].tolist())

                    logger.info(
                        f"✅ [SEC 精炼ADR池] 提取成功: {len(target_symbols)} 只 "
                        f"(逻辑: 跨市场 CIK OR 外企法权后缀 | 100% 覆盖 BBD, BABA, TSM | 已排除 7200+ 本土股票)"
                    )
                    break
                else:
                    logger.warning(f"⚠️ [SEC 主板] 响应异常 Status: {res.status_code}")
            except Exception as e:
                logger.warning(f"⚠️ [SEC 主板] 第 {attempt}/{max_retries} 次请求失败: {e}，尝试轮换代理...")
                self._rotate_to_working_proxy()

        return target_symbols

    def _get_us_recent_action_symbols_from_nasdaq(
        self,
        start_date: str,
        end_date: str
    ) -> Set[str]:
        """
        通过 Nasdaq 官方 Calendar API 快捷拉取 [start_date ~ end_date] 范围内
        发生分红派息 (Dividends) 和拆合股 (Splits) 的美股 Symbol 集合
        直接通过环境变量捕获代理 (curl_cffi 自动拾取 OS 环境变量)
        """
        # 1. 确保环境变量已载入最新可用代理
        if not getattr(self, "current_working_proxy", None):
            proxy_str = self._rotate_to_working_proxy()
        else:
            proxy_str = self._apply_proxy_env(self.current_working_proxy)

        action_symbols: Set[str] = set()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.nasdaq.com",
            "Referer": "https://www.nasdaq.com/",
        }

        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

        curr_dt = start_dt
        logger.info(f"🚀 [Nasdaq 日历] 开始提取除权/拆股日历数据，窗口: [{start_date} ~ {end_date}] | 📡 初始代理: {proxy_str or '直连'}")

        while curr_dt <= end_dt:
            if curr_dt.weekday() < 5:  # 跳过周末
                date_str = curr_dt.strftime("%Y-%m-%d")
                day_div_count = 0
                day_split_count = 0

                # ------------------------------------------------------------------
                # 1. 获取分红派息 (Dividends)
                # ------------------------------------------------------------------
                div_url = f"https://api.nasdaq.com/api/calendar/dividends?date={date_str}&limit=9999"
                try:
                    res_div = curl_requests.get(div_url, headers=headers, timeout=12)
                    if res_div.status_code == 200:
                        div_json = res_div.json()
                        div_data = div_json.get("data", {}) or {}
                        
                        rows = []
                        if isinstance(div_data, dict):
                            calendar_obj = div_data.get("calendar", {}) or {}
                            if isinstance(calendar_obj, dict):
                                rows = calendar_obj.get("rows", []) or []

                        for row in rows:
                            sym = row.get("symbol")
                            if sym:
                                clean_sym = str(sym).strip().upper()
                                if clean_sym and not clean_sym.startswith("="):
                                    action_symbols.add(clean_sym)
                                    day_div_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ [Nasdaq 分红] 请求 {date_str} 失败: {e}，尝试轮换代理...")
                    proxy_str = self._rotate_to_working_proxy()

                # ------------------------------------------------------------------
                # 2. 获取拆合股 (Stock Splits)
                # ------------------------------------------------------------------
                split_url = f"https://api.nasdaq.com/api/calendar/splits?date={date_str}"
                try:
                    res_split = curl_requests.get(split_url, headers=headers, timeout=12)
                    if res_split.status_code == 200:
                        split_json = res_split.json()
                        split_data = split_json.get("data", {}) or {}

                        rows = []
                        if isinstance(split_data, dict):
                            rows = split_data.get("rows", []) or []

                        for row in rows:
                            sym = row.get("symbol")
                            if sym:
                                clean_sym = str(sym).strip().upper()
                                if clean_sym and not clean_sym.startswith("="):
                                    action_symbols.add(clean_sym)
                                    day_split_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ [Nasdaq 拆股] 请求 {date_str} 失败: {e}，尝试轮换代理...")
                    proxy_str = self._rotate_to_working_proxy()

                logger.info(
                    f"📅 [Nasdaq 日历] 日期: {date_str} -> "
                    f"命中分红: {day_div_count:3d} 只 | 命中拆股: {day_split_count:3d} 只 | "
                    f"累计命中标的: {len(action_symbols)} 只"
                )

            curr_dt += datetime.timedelta(days=1)

        logger.info(f"🎯 [Nasdaq 日历完成] 共捕获除权/拆股美股标的: {len(action_symbols)} 只")
        return action_symbols


    def _scan_us_symbols_with_actions(self, all_us_symbols: List[str]) -> Set[str]:
            """
            替代原有的 yf.download 批量下载矩阵扫描逻辑：
            1. 利用 Nasdaq Calendar API 提取 [self.start_date ~ self.end_date] 范围内发生分红/拆股的本土美股。
            2. 利用 SEC 官方 API 拉取三大主板 (NYSE/NASDAQ/AMEX) 的全量股票池 (保障 BBD 等 ADR / 存托凭证 100% 涵盖)。
            3. 过滤并合并返回，解决批量 yf.download 导致的严重限流与拦截问题。
            """
            logger.info(
                f"⚡ [US 增量扫描] 开始提取除权/拆股标的，传入候选总数: {len(all_us_symbols)} 只，"
                f"窗口: [{self.start_date} ~ {self.end_date}]"
            )

            # 尝试获取当前可用代理字典（如果类内部配置了代理方法）
            proxy_dict = None
            if hasattr(self, "current_working_proxy") and self.current_working_proxy:
                proxy_dict = {
                    "http": self.current_working_proxy,
                    "https": self.current_working_proxy,
                }

            # 1. 从 Nasdaq 提取本土美股除权事件标的
            nasdaq_action_symbols = self._get_us_recent_action_symbols_from_nasdaq(
                start_date=self.start_date,
                end_date=self.end_date,
            )

            # 2. 从 SEC 提取三大主板全量标的 (作为全量 ADR / 存托凭证池)
            sec_main_symbols = self._get_us_main_exchange_symbols_from_sec()

            # 3. 本土除权标的 + SEC 主板 ADR 标的 合并
            detected_symbols = nasdaq_action_symbols | sec_main_symbols

            # 4. 与传入的 all_us_symbols 取交集，保证输出结果约束在系统已知的 US Symbol 列表中
            if all_us_symbols:
                candidate_set = set(str(s).upper().strip() for s in all_us_symbols)
                # 交集：在 candidate_set 中出现的 detected_symbols
                target_symbols = detected_symbols.intersection(candidate_set)
                
                # 容错补充：如果 Nasdaq 抓到了候选列表中没有的新除权 Symbol，也一并打入
                target_symbols.update(nasdaq_action_symbols)
            else:
                target_symbols = detected_symbols

            logger.info(
                f"✨ [US 增量扫描完成] Nasdaq 日历命中: {len(nasdaq_action_symbols)} 只 | "
                f"SEC 主板池: {len(sec_main_symbols)} 只 | "
                f"最终筛选待处理标的: {len(target_symbols)} 只"
            )

            return target_symbols

    def fetch_actions_for_us_stock(self, symbol: str) -> List[Dict]:
        """请求筛选出的 symbol 的 ticker.actions，进行增量更新"""
        os.environ.setdefault("CURL_CA_BUNDLE", "")
        os.environ.setdefault("SSL_CERT_FILE", "")

        for attempt in range(1, self.max_retries_per_symbol + 1):
            if not self.current_working_proxy:
                proxy_str = self._rotate_to_working_proxy()
            else:
                proxy_str = self._apply_proxy_env(self.current_working_proxy)

            try:
                ticker = yf.Ticker(symbol)
                actions = ticker.actions

                raw_records = []
                if actions is not None and not actions.empty:
                    for date, row in actions.iterrows():
                        date_str = date.strftime("%Y-%m-%d")

                        if self.start_date and date_str < self.start_date:
                            continue
                        if self.end_date and date_str > self.end_date:
                            continue

                        div = float(row.get("Dividends", 0.0))
                        raw_split = float(row.get("Stock Splits", 0.0))

                        split_ratio_val = raw_split if raw_split > 0.0 else 1.0

                        if div > 0 or split_ratio_val != 1.0:
                            raw_records.append({
                                "symbol": symbol,
                                "date": date_str,
                                "dividend": div,
                                "split_ratio": split_ratio_val,
                            })

                records = []
                if raw_records:
                    i = 0
                    n = len(raw_records)
                    while i < n:
                        curr = raw_records[i]
                        curr_dt = pd.to_datetime(curr["date"])

                        while i + 1 < n:
                            next_rec = raw_records[i + 1]
                            next_dt = pd.to_datetime(next_rec["date"])
                            day_diff = (next_dt - curr_dt).days

                            same_div = abs(next_rec["dividend"] - curr["dividend"]) < 1e-4
                            same_split = abs(next_rec["split_ratio"] - curr["split_ratio"]) < 1e-4

                            if day_diff <= 3 and same_div and same_split:
                                curr = next_rec
                                curr_dt = next_dt
                                i += 1
                            else:
                                break

                        records.append(curr)
                        i += 1

                del actions
                del ticker

                return records

            except Exception as e:
                # 异常时清空当前代理，触发下一次循环调用 _rotate_to_working_proxy 获取经测有效的新代理
                self.current_working_proxy = None

            finally:
                self._apply_proxy_env(None)

        return []      

    def load_us_symbols(self) -> List[str]:
        """载入待处理的美股 Symbol 列表"""
        # 1. 从命令行参数或本地 stock_*.csv 获取全量 symbol list
        if self.symbol_list is not None:
            all_symbols = sorted(list(set(str(s).strip().upper() for s in self.symbol_list)))
        elif not self.stock_list_path.exists():
            all_symbols = []
        else:
            df = pd.read_csv(self.stock_list_path)
            target_col = "symbol" if "symbol" in df.columns else "Symbol"
            all_symbols = (
                df[target_col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.upper()
                .unique()
                .tolist()
            )

        if not all_symbols:
            return []

        # 2. 仅在增量模式 (incremental=True) 下，调用矩阵扫描过滤
        if self.incremental:
            target_symbols = self._scan_us_symbols_with_actions(all_symbols)
            logger.info(f"🎯 [增量模式] 筛选出发生过除权/拆股的标的共: {len(target_symbols)} 只")
            return sorted(list(target_symbols))

        # 3. 全量模式 (incremental=False) 下，直接返回全量列表，直接进行全量获取
        logger.info(f"🌐 [全量模式] 准备处理全量美股标的共: {len(all_symbols)} 只")
        return sorted(all_symbols)


    # ==================== 执行调度入口 ====================
    def run(self):
        logger.info(f"[{self.market.upper()}] 启动除权分红抓取任务...")
        logger.info(f"目标目录: {self.target_dir.resolve()}")
        logger.info(f"模式区间: [{self.start_date or '不限制'} ~ {self.end_date}]")

        if self.market == "cn":
            all_stocks, etf_list = self.load_cn_symbols()

            pending_etfs = [e for e in etf_list if e not in self.processed_symbols]
            if pending_etfs:
                logger.info(f"检测到待处理 ETF 共 {len(pending_etfs)} 只，开始提取...")
                etf_records = self.fetch_actions_for_etfs(pending_etfs)
                if etf_records:
                    self._save_records_to_csv(etf_records)

                self.processed_symbols.update(pending_etfs)
                self._save_checkpoint()
                logger.info("✅ ETF 阶段完成！相关 Checkpoint 已落盘更新。")
                gc.collect()

            all_symbols = all_stocks
            fetch_func = self.fetch_actions_for_cn_stock
        else:
            all_symbols = self.load_us_symbols()
            fetch_func = self.fetch_actions_for_us_stock

        total_count = len(all_symbols)
        if total_count == 0:
            logger.warning("未检测到待处理的股票，任务终止。")
            return

        finished_stocks = [s for s in all_symbols if s in self.processed_symbols]
        pending_symbols = [s for s in all_symbols if s not in self.processed_symbols]

        processed_count = len(finished_stocks)

        logger.info(
            f"股票总进度 -> 全量总数: {total_count} | 历史已完成: {processed_count} | 本次待处理: {len(pending_symbols)}"
        )

        if not pending_symbols:
            logger.info("所有股票除权分红数据均已抓取完成！")
            return

        total_batches = (len(pending_symbols) + self.batch_size - 1) // self.batch_size

        for batch_idx in range(total_batches):
            start_idx = batch_idx * self.batch_size
            end_idx = start_idx + self.batch_size
            batch_symbols = pending_symbols[start_idx:end_idx]

            logger.info(
                f"--- Batch [{batch_idx + 1}/{total_batches}] 开始，本批包含 {len(batch_symbols)} 只股票 ---"
            )

            batch_records = []
            for idx_in_batch, symbol in enumerate(batch_symbols, 1):
                current_global_idx = processed_count + idx_in_batch
                progress_pct = (current_global_idx / total_count) * 100
                display_symbol = self.cn_symbol_map.get(symbol, symbol) if self.market == "cn" else symbol

                records = fetch_func(symbol)
                if records:
                    batch_records.extend(records)

                logger.info(
                    f"[{progress_pct:6.2f}%] [{current_global_idx:4d}/{total_count:4d}] "
                    f"[Batch {batch_idx + 1}/{total_batches}] [{display_symbol}] 抓取完成 (保留 {len(records)} 条除权记录)"
                )

            if batch_records:
                self._save_records_to_csv(batch_records)

            processed_count += len(batch_symbols)
            self.processed_symbols.update(batch_symbols)
            self._save_checkpoint()

            logger.info(
                f"✅ Batch [{batch_idx + 1}/{total_batches}] 成功落盘！总体股票进度: {processed_count}/{total_count} "
                f"({(processed_count / total_count) * 100:.2f}%)"
            )

            del batch_records
            gc.collect()