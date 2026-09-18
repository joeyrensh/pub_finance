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

    - 支持定时任务：跨天自动重置 Checkpoint 开启新一轮全量刷刷新，同天崩溃支持断点续传
    - 安全文件轮换：数据实时写入带日期文件，完成后原文件备份为 .bak 并顺次重命名
    - 统一中美股 split_ratio 语义：无拆分时均为 1.0，1拆N 时为 N.0
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
        symbol_list: Optional[List[str]] = None,  # 💡 新增：支持指定 symbol_list，默认 None
    ):
        self.market = market.lower()
        if self.market not in ["cn", "us"]:
            raise ValueError("market 参数必须为 'cn' 或 'us'")

        self.force_refresh = force_refresh
        self.symbol_list = symbol_list  # 💡 新增属性保存

        # 1. 目录及路径定位
        default_dir = "cnstockinfo" if self.market == "cn" else "usstockinfo"
        self.target_dir = target_dir or (FINANCE_ROOT / default_dir)
        os.makedirs(self.target_dir, exist_ok=True)

        # 若未提供 symbol_list，才强行要求 stock_list_path 文件存在
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

    def _get_latest_stock_info_file(self) -> Path:
        """获取 target_dir 目录下最新的 stock_*.csv 文件路径"""
        # 💡 若指定了 symbol_list 且文件不存在，返回虚拟路径避免报错
        files = list(self.target_dir.glob("stock_*.csv"))
        if not files:
            if self.symbol_list is not None:
                return self.target_dir / "stock_placeholder.csv"
            raise FileNotFoundError(f"未在目录 '{self.target_dir}' 下找到任何 stock_*.csv 文件")
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        logger.info(f"[{self.market.upper()}] 自动定位最新股票列表文件: {latest_file.name}")
        return latest_file

    def _init_cn_symbol_mapping(self):
        """为 CN 市场解析列表，构建 纯数字代码 -> 原始 Symbol 的双向映射"""
        # 💡 若指定了 symbol_list，直接从 symbol_list 构建映射
        if self.symbol_list is not None:
            for sym in self.symbol_list:
                raw_sym = str(sym).strip()
                clean_code = re.sub(r"\D", "", raw_sym).zfill(6)
                self.cn_symbol_map[clean_code] = raw_sym
            return

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
        if self.force_refresh:
            logger.info("⚡ [定时模式] 已开启 force_refresh，清空旧 Checkpoint，开启全新抓取周期。")
            self._clear_checkpoint()
            return set()

        if self.checkpoint_path.exists():
            try:
                mtime = datetime.datetime.fromtimestamp(self.checkpoint_path.stat().st_mtime)
                today = datetime.datetime.now().date()

                if mtime.date() < today:
                    logger.info(
                        f"📅 检测到 Checkpoint 为历史日期 ({mtime.strftime('%Y-%m-%d')})，"
                        f"判定为新一轮定时任务，自动重置 Checkpoint。"
                    )
                    self._clear_checkpoint()
                    return set()

                with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                    symbols = set(json.load(f))
                    logger.info(f"🔄 检测到当日 Checkpoint，成功载入 {len(symbols)} 条已处理记录 (断点续传模式)")
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

        # 1. 将本次批次新抓取到的记录转换为 DataFrame
        new_df = pd.DataFrame(records)[self.CSV_HEADERS]

        # 2. 读取已存在的历史 CSV 数据（优先从 output_csv_path 或 dated_csv_path 中读取）
        existing_df = pd.DataFrame(columns=self.CSV_HEADERS)
        
        target_read_path = None
        if self.dated_csv_path.exists() and os.path.getsize(self.dated_csv_path) > 0:
            target_read_path = self.dated_csv_path
        elif self.output_csv_path.exists() and os.path.getsize(self.output_csv_path) > 0:
            target_read_path = self.output_csv_path

        if target_read_path:
            try:
                existing_df = pd.read_csv(target_read_path, dtype=str)
                # 类型转换，确保数值列精度统一
                existing_df["dividend"] = existing_df["dividend"].astype(float)
                existing_df["split_ratio"] = existing_df["split_ratio"].astype(float)
            except Exception as e:
                logger.warning(f"读取原有数据文件失败，将全新创建: {e}")

        # 3. 合并新旧数据，并按 ['symbol', 'date'] 复合主键去重 (keep='last' 确保新抓取的数据覆盖旧数据)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(subset=["symbol", "date"], keep="last", inplace=True)
        
        # 4. 重新排序：按 symbol, date 字典序升序，保证文件可读性
        combined_df.sort_values(by=["symbol", "date"], ascending=[True, True], inplace=True)

        # 5. 安全覆盖落盘到带有日期标记的 CSV 中 (mode="w")
        combined_df.to_csv(
            self.dated_csv_path,
            mode="w",
            index=False,
            header=True,
            encoding="utf-8-sig",
        )
        
        del combined_df, existing_df, new_df

        # 6. 安全原子替换主文件逻辑 (保持原有 .bak 轮换逻辑)
        if self.output_csv_path.exists():
            if self.bak_csv_path.exists():
                os.remove(self.bak_csv_path)
            os.rename(self.output_csv_path, self.bak_csv_path)

        shutil.copy2(self.dated_csv_path, self.output_csv_path)

        if self.bak_csv_path.exists():
            if self.dated_csv_path.exists():
                os.remove(self.dated_csv_path)
            os.rename(self.bak_csv_path, self.dated_csv_path)

    # ==================== CN (AkShare) 处理逻辑 ====================
    def load_cn_symbols(self) -> Tuple[List[str], List[str]]:
        """依据前缀提取 CN 股票和 ETF"""
        stocks, etfs = [], []
        for clean_code, raw_sym in self.cn_symbol_map.items():
            if raw_sym.upper().startswith("ETF"):
                etfs.append(clean_code)
            elif raw_sym.upper().startswith(("SH", "SZ")):
                stocks.append(clean_code)
            else:  # 💡 若未带 SH/SZ 前缀，默认归为股票
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

    # ==================== US (yfinance) 处理逻辑 ====================
    def load_us_symbols(self) -> List[str]:
        """载入待处理的美股 Symbol 列表"""
        # 💡 若指定了 symbol_list，优先使用
        if self.symbol_list is not None:
            return sorted(list(set(str(s).strip().upper() for s in self.symbol_list)))

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
                        raw_split = float(row.get("Stock Splits", 0.0))

                        split_ratio_val = raw_split if raw_split > 0.0 else 1.0

                        if div > 0 or split_ratio_val != 1.0:
                            records.append({
                                "symbol": symbol,
                                "date": date_str,
                                "dividend": div,
                                "split_ratio": split_ratio_val,
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
            elif etf_list:
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

            # 批次落盘 CSV（安全轮换逻辑）
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