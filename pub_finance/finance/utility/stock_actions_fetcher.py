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
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

import akshare as ak
import pandas as pd
from tqdm import tqdm
import yfinance as yf

# ==============================================================================
# 关键修复：彻底全局禁用 tqdm 进度条
# 解决 AkShare 内部调用 tqdm 导致的控制台闪屏刷新以及画面卡死在 "0%" 的问题
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

    - 屏蔽 AkShare 内部 tqdm 进度条，彻底解决终端闪屏与 0% 卡死
    - 修复无历史分红股票/新股触发的 'NoneType' object is not subscriptable 异常
    - 基于全量股票数精准实时计算全局进度 %
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
    ):
        self.market = market.lower()
        if self.market not in ["cn", "us"]:
            raise ValueError("market 参数必须为 'cn' 或 'us'")

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

        self.output_csv_path = self.target_dir / (output_filename or default_out)
        self.checkpoint_path = self.target_dir / (checkpoint_filename or default_ckpt)

        # 2. 参数与网络设置
        self.batch_size = batch_size
        self.max_retries_per_symbol = max_retries_per_symbol
        self.start_date = start_date

        if self.market == "us":
            self.proxy_manager = (
                proxy_manager or ProxyManager.create_overseas_manager()
            )
            self.current_working_proxy: Optional[Dict[str, str]] = None

        # 3. 映射表与断点记录
        self.cn_symbol_map: Dict[str, str] = {}  # 纯数字代码 -> 原始带前缀 Symbol (如 510300 -> ETF510300)
        self.processed_symbols: Set[str] = self._load_checkpoint()

        if self.market == "cn":
            self._init_cn_symbol_mapping()

    def _get_latest_stock_info_file(self) -> Path:
        """获取 target_dir 目录下最新的 stock_*.csv 文件路径"""
        files = list(self.target_dir.glob("stock_*.csv"))
        if not files:
            raise FileNotFoundError(f"未在目录 '{self.target_dir}' 下找到任何 stock_*.csv 文件")
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        logger.info(f"[{self.market.upper()}] 自动定位最新股票列表文件: {latest_file.name}")
        return latest_file

    def _init_cn_symbol_mapping(self):
        """为 CN 市场解析列表，构建 纯数字代码 -> 原始 Symbol 的双向映射"""
        if not self.stock_list_path.exists():
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
        """读取断点记录"""
        if self.checkpoint_path.exists():
            try:
                with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                    return set(json.load(f))
            except Exception as e:
                logger.warning(f"读取 Checkpoint 失败，将重新建立: {e}")
        return set()

    def _save_checkpoint(self):
        """保存断点记录"""
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(list(self.processed_symbols), f)

    def _save_records_to_csv(self, records: List[Dict]):
        """统一落盘逻辑，确保追加时严格符合规范 Header"""
        if not records:
            return

        df = pd.DataFrame(records)
        df = df[self.CSV_HEADERS]  # 强行按统一 Header 调整顺序

        file_exists = (
            self.output_csv_path.exists()
            and os.path.getsize(self.output_csv_path) > 0
        )

        df.to_csv(
            self.output_csv_path,
            mode="a",
            index=False,
            header=not file_exists,  # 仅文件不存在/为空时写 Header
            encoding="utf-8-sig",
        )
        del df

    # ==================== CN (AkShare) 处理逻辑 ====================
    def load_cn_symbols(self) -> Tuple[List[str], List[str]]:
        """依据前缀提取 CN 股票和 ETF"""
        stocks, etfs = [], []
        for clean_code, raw_sym in self.cn_symbol_map.items():
            if raw_sym.upper().startswith("ETF"):
                etfs.append(clean_code)
            elif raw_sym.upper().startswith(("SH", "SZ")):
                stocks.append(clean_code)

        return sorted(list(set(stocks))), sorted(list(set(etfs)))

    def fetch_actions_for_etfs(self, etf_list: List[str]) -> List[Dict]:
        """批量获取 ETF 分红与拆分数据"""
        if not etf_list:
            return []

        records = []
        start_year = int(self.start_date[:4]) if self.start_date else 2025
        current_year = datetime.datetime.now().year
        years_to_fetch = [str(y) for y in range(start_year, current_year + 1)]
        etf_set = set(etf_list)

        logger.info(f"[CN ETF] 开始获取 {len(etf_list)} 只 ETF 在 {years_to_fetch} 年份内的数据...")

        for yr in years_to_fetch:
            # 1. 抓取分红 (ak.fund_fh_em)
            try:
                df_fh = ak.fund_fh_em(year=yr, page=-1)
                if df_fh is not None and not df_fh.empty and "基金代码" in df_fh.columns:
                    df_fh["基金代码"] = df_fh["基金代码"].astype(str).str.zfill(6)
                    df_target_fh = df_fh[df_fh["基金代码"].isin(etf_set)].copy()

                    for _, row in df_target_fh.iterrows():
                        code = row["基金代码"]
                        ex_date = str(row["除息日期"]).strip()
                        div_val = float(row["分红"]) if row["分红"] else 0.0

                        if re.match(r"^\d{4}-\d{2}-\d{2}$", ex_date) and div_val > 0:
                            if self.start_date and ex_date < self.start_date:
                                continue
                            
                            raw_sym = self.cn_symbol_map.get(code, f"ETF{code}")
                            records.append({
                                "symbol": raw_sym,
                                "date": ex_date,
                                "dividend": div_val,
                                "split_ratio": 1.0,
                            })
            except Exception as e:
                logger.warning(f"[CN ETF] 抓取 {yr} 年分红数据失败: {e}")

            # 2. 抓取拆分折算 (ak.fund_cf_em)
            try:
                df_cf = ak.fund_cf_em(year=yr, page=-1)
                if df_cf is not None and not df_cf.empty and "基金代码" in df_cf.columns:
                    df_cf["基金代码"] = df_cf["基金代码"].astype(str).str.zfill(6)
                    df_target_cf = df_cf[df_cf["基金代码"].isin(etf_set)].copy()

                    for _, row in df_target_cf.iterrows():
                        code = row["基金代码"]
                        ex_date = str(row["拆分折算日"]).strip()
                        ratio_val = float(row["拆分折算"]) if row["拆分折算"] else 1.0

                        if re.match(r"^\d{4}-\d{2}-\d{2}$", ex_date) and ratio_val != 1.0:
                            if self.start_date and ex_date < self.start_date:
                                continue
                            
                            raw_sym = self.cn_symbol_map.get(code, f"ETF{code}")
                            records.append({
                                "symbol": raw_sym,
                                "date": ex_date,
                                "dividend": 0.0,
                                "split_ratio": ratio_val,
                            })
            except Exception as e:
                logger.warning(f"[CN ETF] 抓取 {yr} 年拆分数据失败: {e}")

        logger.info(f"[CN ETF] 抓取完成，保留 {len(records)} 条符合条件的记录")
        return records

    def fetch_actions_for_cn_stock(self, symbol: str) -> List[Dict]:
        """抓取单只 CN 股票的除权除息数据（优雅防崩溃处理）"""
        records = []
        raw_sym = self.cn_symbol_map.get(symbol, symbol)
        try:
            df_stock = ak.stock_fhps_detail_em(symbol=symbol)
            if df_stock is not None and not df_stock.empty and "除权除息日" in df_stock.columns:
                df_valid = df_stock.dropna(subset=["除权除息日"]).copy()

                for _, row in df_valid.iterrows():
                    ex_date = str(row.get("除权除息日", "")).strip()
                    if not ex_date or ex_date == "-":
                        continue

                    date_str = pd.to_datetime(ex_date).strftime("%Y-%m-%d")

                    if self.start_date and date_str < self.start_date:
                        continue

                    cash_10 = float(row.get("现金分红-现金分红比例", 0.0) or 0.0)
                    div_val = cash_10 / 10.0

                    song_10 = float(row.get("送转股份-送股比例", 0.0) or 0.0)
                    zhuan_10 = float(row.get("送转股份-转股比例", 0.0) or 0.0)
                    split_ratio_val = 1.0 + ((song_10 + zhuan_10) / 10.0)

                    if div_val > 0 or split_ratio_val != 1.0:
                        records.append({
                            "symbol": raw_sym,
                            "date": date_str,
                            "dividend": div_val,
                            "split_ratio": split_ratio_val,
                        })
        except (TypeError, KeyError, AttributeError):
            # 捕获次新股/未分红股票引发的 'NoneType' object is not subscriptable 异常，静默跳过
            pass
        except Exception as e:
            logger.debug(f"获取 CN 股票 [{raw_sym}] 数据失败: {e}")

        return records

    # ==================== US (yfinance) 处理逻辑 ====================
    def load_us_symbols(self) -> List[str]:
        """载入待处理的美股 Symbol 列表"""
        df = pd.read_csv(self.stock_list_path)
        target_col = "symbol" if "symbol" in df.columns else "Symbol"
        return (
            df[target_col]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .unique()
            .tolist()
        )

    def fetch_actions_for_us_stock(self, symbol: str) -> List[Dict]:
        """抓取美股数据：优先复用代理，带重试与校验控制"""
        os.environ.setdefault("CURL_CA_BUNDLE", "")
        os.environ.setdefault("SSL_CERT_FILE", "")

        for attempt in range(1, self.max_retries_per_symbol + 1):
            if not self.current_working_proxy:
                self.current_working_proxy = self.proxy_manager.get_working_proxy(
                    max_retries=2, enable_proxy=True
                )
                if not self.current_working_proxy:
                    self.current_working_proxy = self.proxy_manager.get_next_proxy()

            proxy_dict = self.current_working_proxy
            raw_proxy = (
                (proxy_dict.get("socks5") or proxy_dict.get("https") or proxy_dict.get("http"))
                if proxy_dict else None
            )
            proxy_str = format_proxy_url(raw_proxy) if raw_proxy else None

            if proxy_str:
                os.environ["HTTP_PROXY"] = proxy_str
                os.environ["HTTPS_PROXY"] = proxy_str
            else:
                os.environ.pop("HTTP_PROXY", None)
                os.environ.pop("HTTPS_PROXY", None)

            try:
                ticker = yf.Ticker(symbol)
                actions = ticker.actions

                records = []
                if actions is not None and not actions.empty:
                    for date, row in actions.iterrows():
                        date_str = date.strftime("%Y-%m-%d")

                        if self.start_date and date_str < self.start_date:
                            continue

                        div = float(row.get("Dividends", 0.0))
                        split = float(row.get("Stock Splits", 0.0))

                        if div != 0 or split != 0:
                            records.append({
                                "symbol": symbol,
                                "date": date_str,
                                "dividend": div,
                                "split_ratio": split,
                            })

                del actions
                del ticker

                if proxy_str and hasattr(self.proxy_manager, "mark_proxy_working"):
                    self.proxy_manager.mark_proxy_working(proxy_str)

                return records

            except Exception as e:
                if proxy_str and hasattr(self.proxy_manager, "mark_proxy_failed"):
                    self.proxy_manager.mark_proxy_failed(proxy_str)
                self.current_working_proxy = None

            finally:
                os.environ.pop("HTTP_PROXY", None)
                os.environ.pop("HTTPS_PROXY", None)

        return []

    # ==================== 执行调度入口 ====================
    def run(self):
        logger.info(f"[{self.market.upper()}] 启动除权分红抓取任务...")
        logger.info(f"目标目录: {self.target_dir.resolve()}")
        logger.info(f"起始筛选日期: {self.start_date or '不限制(全量历史)'}")

        if self.market == "cn":
            all_stocks, etf_list = self.load_cn_symbols()

            # 1. 判断未处理的 ETF 并落盘
            pending_etfs = [e for e in etf_list if e not in self.processed_symbols]
            if pending_etfs:
                logger.info(f"检测到未处理 ETF 共 {len(pending_etfs)} 只，开始提取...")
                etf_records = self.fetch_actions_for_etfs(pending_etfs)
                if etf_records:
                    self._save_records_to_csv(etf_records)

                self.processed_symbols.update(pending_etfs)
                self._save_checkpoint()
                logger.info("✅ ETF 阶段完成！相关 Checkpoint 已落盘更新。")
                gc.collect()
            else:
                logger.info("所有 ETF 均已在 Checkpoint 记录中，跳过 ETF 处理。")

            all_symbols = all_stocks
            fetch_func = self.fetch_actions_for_cn_stock
        else:
            all_symbols = self.load_us_symbols()
            fetch_func = self.fetch_actions_for_us_stock

        total_count = len(all_symbols)
        if total_count == 0:
            logger.warning("未检测到待处理的股票，任务终止。")
            return

        # 精确计算股票维度已处理和未处理的列表
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
                # 计算绝对真实的全局当前处理位置与百分比
                current_global_idx = processed_count + idx_in_batch
                progress_pct = (current_global_idx / total_count) * 100
                display_symbol = self.cn_symbol_map.get(symbol, symbol) if self.market == "cn" else symbol

                records = fetch_func(symbol)
                if records:
                    batch_records.extend(records)

                # 强行每处理一只打印一次全局百分比进度
                logger.info(
                    f"[{progress_pct:6.2f}%] [{current_global_idx:4d}/{total_count:4d}] "
                    f"[Batch {batch_idx + 1}/{total_batches}] [{display_symbol}] 抓取完成 (保留 {len(records)} 条除权记录)"
                )

            # 批次落盘 CSV
            if batch_records:
                self._save_records_to_csv(batch_records)

            # 更新计数和 Checkpoint
            processed_count += len(batch_symbols)
            self.processed_symbols.update(batch_symbols)
            self._save_checkpoint()

            logger.info(
                f"✅ Batch [{batch_idx + 1}/{total_batches}] 成功落盘！总体股票进度: {processed_count}/{total_count} "
                f"({(processed_count / total_count) * 100:.2f}%)"
            )

            del batch_records
            gc.collect()
