from collections import defaultdict
import gc
import itertools
import json
from pathlib import Path
import re
import sys
import pandas as pd
from curl_cffi import requests
from finance import FINANCE_ROOT
from finance.utility.emcookie_generation import CookieGeneration
from finance.utility.get_proxy import ProxyManager


class BatchSplitDividendDetector:

    def __init__(
        self,
        data_dir: Path,
        market: str = "us",  # "us" 或 "cn"
        gap_threshold: float = None,  # US 默认 50%, CN 默认 30%
        batch_size: int = 10,
        min_price: float = 1.0,  # 过滤最新交易日收盘价低于 1.0 的股票
        use_proxy: bool = True,
    ):
        """data_dir: 数据保存目录

        market: 市场类型 ("us" 或 "cn")
        gap_threshold: 异常跳空阈值
        batch_size: 扫描时每批读取处理的 Symbol 数量
        min_price: 忽略最新股价低于该值的股票 (默认 1.0)
        use_proxy: 是否开启代理
        """
        self.data_dir = Path(data_dir)
        self.market = market.lower()
        self.batch_size = batch_size
        self.min_price = min_price
        self.use_proxy = use_proxy

        # 缓存路径设置
        self.raw_cache_path = self.data_dir / "raw_suspicious_symbols.txt"
        self.final_csv_path = self.data_dir / "suspicious_symbols.csv"

        # 1. 自动设置跳空阈值
        if gap_threshold is not None:
            self.gap_threshold = gap_threshold
        else:
            self.gap_threshold = 0.50 if self.market == "us" else 0.30

        # 2. 网络请求与代理配置
        self.__url_history = (
            "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        )
        self.pm = ProxyManager()
        self.cg = CookieGeneration()
        self.cg.generate_em_cookies()
        self.proxy = None

        self.headers = {
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        }

        # 3. Dynamic Cookie & Counter 初始化
        self.cookie_path = FINANCE_ROOT / "utility" / "eastmoney_cookie.json"
        self._cookie_base = self.parse_cookie_string()
        self._counter = itertools.count(start=1)

        print(
            f"Initialized [{self.market.upper()} Market] Detector: "
            f"Gap Threshold = {self.gap_threshold * 100:.0f}%, "
            f"Min Price = {self.min_price}"
        )

    # ==================== Dynamic Cookie & Utility 方法 ====================

    def parse_cookie_string(self) -> dict:
        """从本地 json 文件解析基础 Cookie 数据"""
        if not self.cookie_path.exists():
            return {}
        try:
            with open(self.cookie_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            print(f"读取 Cookie 异常: {e}")
            return {}

    def get_dynamic_cookies(self) -> dict:
        """生成带计数器的动态 Cookie"""
        cookies = self._cookie_base.copy()
        count_val = next(self._counter)
        cookies["qgqp_b_id"] = (
            f"dynamic_{count_val}_{int(pd.Timestamp.now().timestamp())}"
        )
        return cookies

    def generate_ut_param(self) -> str:
        """生成 API 的 ut 参数"""
        return "fa5fd1943c7b386f172d6893dbfba10b"

    def _get_sorted_files(self) -> list[Path]:
        """按日期排序 csv 文件列表"""
        files = list(self.data_dir.glob("stock_*.csv"))
        files.sort(key=lambda x: x.stem)
        return files

    def get_base_symbols(self) -> list[str]:
        """提取完整 Symbol 列表，直接排除最新交易日收盘价 < min_price 的股票"""
        sorted_files = self._get_sorted_files()
        if not sorted_files:
            raise FileNotFoundError("未找到符合 stock_*.csv 格式的数据文件")

        latest_file = sorted_files[-1]
        print(f"正在从最新文件 [{latest_file.name}] 提取基准 Symbol 列表...")

        df_latest = pd.read_csv(latest_file, usecols=["symbol", "close"])

        # 1. 过滤 symbol 空行
        df_latest = df_latest.dropna(subset=["symbol"]).copy()

        # 2. 核心过滤：仅保留最新收盘价 >= min_price (1.0) 的股票
        valid_latest = df_latest[df_latest["close"] >= self.min_price]
        ignored_count = len(df_latest) - len(valid_latest)

        # 3. 转换为 str 并去重排序
        raw_symbols = valid_latest["symbol"].astype(str).str.strip().unique()
        symbol_list = sorted(list(raw_symbols))

        print(
            f"提取完成！基准 Symbol 总数: {len(symbol_list)} "
            f"(已忽略最新股价 < {self.min_price} 的股票 {ignored_count} 只)"
        )
        return symbol_list

    # ==================== Market Code 逻辑 ====================

    def _resolve_cn_mkt_code(self, symbol: str) -> int:
        """CN 市场 mkt_code 规则计算"""
        symbol_str = str(symbol).strip().upper()

        if symbol_str.startswith("ETF"):
            clean_symbol = re.sub(r"^ETF", "", symbol_str)
            return 1 if clean_symbol.startswith("5") else 0
        elif symbol_str.startswith("SH"):
            return 1
        elif symbol_str.startswith("SZ"):
            return 0

        clean_code = re.sub(r"^[^\d]+", "", symbol_str)
        return 1 if clean_code.startswith(("5", "6", "9")) else 0

    def _resolve_us_mkt_code_single(self, symbol: str) -> int:
        """US 市场 API 单线程检测 (轮询 105, 106, 107)，已包含 beg/end 补全"""
        symbol_val = str(symbol).strip()

        if self.proxy is None:
            self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)

        for mkt_code in ["105", "106", "107"]:
            params = {
                "secid": f"{mkt_code}.{symbol_val}",
                "ut": self.generate_ut_param(),
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56",
                "klt": "101",
                "fqt": "1",
                "beg": "20260101",  # 补全起始日期避免 rc:102
                "end": "20500101",  # 补全结束日期避免 rc:102
                "smplmt": "755",
                "lmt": "1",
            }

            for try_cnt in range(3):
                cookie_str = self.get_dynamic_cookies()
                try:
                    response = requests.get(
                        self.__url_history,
                        params=params,
                        proxies=self.proxy,
                        headers=self.headers,
                        cookies=cookie_str,
                        timeout=10,
                        impersonate="chrome120",
                    )

                    res = response.json()

                    # 1. 命中正确市场
                    if res.get("rc") == 0 and res.get("data"):
                        return int(mkt_code)

                    # 2. rc=100/102 或 data 为空，说明不属于该市场，跳出重试逻辑，换下一个 mkt_code
                    break

                except Exception as e:
                    print(
                        f"  [Symbol: {symbol} | MktCode: {mkt_code}] 网络/代理请求异常: {e}，第 {try_cnt + 1} 次重试..."
                    )
                    self.proxy = self.pm.get_working_proxy(
                        enable_proxy=self.use_proxy
                    )
                    continue

        return 105

    def resolve_mkt_codes(
        self, suspicious_symbols: list[str]
    ) -> list[dict[str, any]]:
        """批量获取所有可疑股票的 mkt_code"""
        if not suspicious_symbols:
            return []

        print(
            f"\n开始解析 {len(suspicious_symbols)} 只可疑股票的 mkt_code ({self.market.upper()} 市场)..."
        )
        resolved_results = []

        for idx, sym in enumerate(suspicious_symbols, 1):
            if self.market == "cn":
                mkt_code = self._resolve_cn_mkt_code(sym)
            else:
                mkt_code = self._resolve_us_mkt_code_single(sym)

            resolved_results.append({"symbol": sym, "mkt_code": mkt_code})
            print(
                f"  [{idx}/{len(suspicious_symbols)}] [MktCode 已确认] {sym} -> {mkt_code}"
            )

        resolved_results.sort(key=lambda x: str(x["symbol"]))
        return resolved_results

    # ==================== 两级缓存持久化逻辑 ====================

    def _load_final_csv_cache(self) -> list[dict[str, any]] | None:
        """第一级缓存：直接校验并读取 suspicious_symbols.csv 文件"""
        if not self.final_csv_path.exists():
            return None
        try:
            df = pd.read_csv(self.final_csv_path)
            if df.empty or "symbol" not in df.columns or "mkt_code" not in df.columns:
                return None
            
            result_list = df.to_dict(orient="records")
            print(
                f"[终极缓存命中] 检测到 '{self.final_csv_path.name}' 已存在且非空，"
                f"跳过扫描与 MktCode 获取，直接返回本地 {len(result_list)} 条数据！"
            )
            return result_list
        except Exception as e:
            print(f"读取最终缓存文件 {self.final_csv_path} 异常: {e}")
            return None

    def _save_raw_cache(self, raw_symbols: list[str]):
        """保存中间扫描的可疑 txt 列表"""
        try:
            with open(self.raw_cache_path, "w", encoding="utf-8") as f:
                for sym in raw_symbols:
                    f.write(f"{sym}\n")
            print(f"[缓存成功] 原始可疑列表已保存至: {self.raw_cache_path.resolve()}")
        except Exception as e:
            print(f"写入缓存文件 {self.raw_cache_path} 失败: {e}")

    def _load_raw_cache(self) -> list[str] | None:
        """第二级缓存：读取中间扫描的 txt 文件"""
        if not self.raw_cache_path.exists():
            return None
        try:
            with open(self.raw_cache_path, "r", encoding="utf-8") as f:
                raw_symbols = [line.strip() for line in f if line.strip()]
            print(f"[二级缓存命中] 成功读取已扫描的 {len(raw_symbols)} 只可疑股票，跳过历史 CSV 扫描，直接开始解析 MktCode！")
            return sorted(raw_symbols)
        except Exception as e:
            print(f"读取缓存文件 {self.raw_cache_path} 异常: {e}")
            return None

    # ==================== 主流程控制 ====================

    def scan_suspicious_symbols(self) -> list[dict[str, any]]:
        """主入口：具备双级缓存机制的自动扫描"""
        
        # 1. 第一级缓存检测：如果最终 csv 文件已存在且有内容，直接返回
        final_cached_list = self._load_final_csv_cache()
        if final_cached_list is not None:
            return final_cached_list

        # 2. 第二级缓存检测：尝试读取历史扫描出来的 raw_suspicious_symbols.txt
        raw_suspicious_list = self._load_raw_cache()

        # 3. 缓存均未命中，执行全量历史文件扫描
        if raw_suspicious_list is None:
            sorted_files = self._get_sorted_files()
            if not sorted_files:
                return []

            if len(sorted_files) > 1:
                scan_files = sorted_files[1:]
                print(
                    f"[性能优化] 已跳过最早的历史大文件 [{sorted_files[0].name}]，本次仅扫描后续 {len(scan_files)} 个增量文件。"
                )
            else:
                scan_files = sorted_files
                print(f"仅存在 1 个数据文件 [{scan_files[0].name}]，将对其进行扫描。")

            base_symbols = self.get_base_symbols()
            suspicious_symbols = set()
            total_batches = (
                len(base_symbols) + self.batch_size - 1
            ) // self.batch_size

            for i in range(0, len(base_symbols), self.batch_size):
                batch_symbols = set(base_symbols[i : i + self.batch_size])
                current_batch_num = i // self.batch_size + 1

                print(
                    f"\n--- 正在处理批次 [{current_batch_num}/{total_batches}] (包含 {len(batch_symbols)} 只股票) ---"
                )

                batch_data_map = {sym: [] for sym in batch_symbols}

                for file_path in scan_files:
                    try:
                        chunk_list = []
                        for chunk in pd.read_csv(
                            file_path,
                            usecols=[
                                "symbol",
                                "date",
                                "open",
                                "close",
                                "high",
                                "low",
                            ],
                            chunksize=50000,
                        ):
                            chunk["symbol"] = (
                                chunk["symbol"].astype(str).str.strip()
                            )
                            filtered_chunk = chunk[
                                chunk["symbol"].isin(batch_symbols)
                            ]
                            if not filtered_chunk.empty:
                                chunk_list.append(filtered_chunk)

                        if not chunk_list:
                            continue

                        file_df = pd.concat(chunk_list, ignore_index=True)

                        for sym, group in file_df.groupby("symbol"):
                            batch_data_map[sym].append(group)

                    except Exception as e:
                        print(f"读取文件 {file_path.name} 异常: {e}")

                # 判定跳空断层
                for sym, data_chunks in batch_data_map.items():
                    if not data_chunks:
                        continue

                    sym_df = pd.concat(data_chunks, ignore_index=True)
                    sym_df["date"] = pd.to_datetime(sym_df["date"])
                    sym_df = sym_df.sort_values("date").drop_duplicates(
                        subset=["date"]
                    )

                    if len(sym_df) < 2:
                        continue

                    sym_df["prev_close"] = sym_df["close"].shift(1)

                    valid_df = sym_df[
                        (sym_df["prev_close"] >= self.min_price)
                        & (sym_df["open"] >= self.min_price)
                    ].copy()

                    if valid_df.empty:
                        continue

                    valid_df["gap"] = (
                        valid_df["open"] - valid_df["prev_close"]
                    ) / valid_df["prev_close"]

                    split_down = (valid_df["gap"] < -self.gap_threshold) & (
                        valid_df["high"]
                        < valid_df["prev_close"] * (1 - self.gap_threshold)
                    )

                    split_up = (valid_df["gap"] > self.gap_threshold) & (
                        valid_df["low"]
                        > valid_df["prev_close"] * (1 + self.gap_threshold)
                    )

                    if (split_down | split_up).any():
                        suspicious_symbols.add(sym)
                        date_min = sym_df["date"].min().strftime("%Y-%m-%d")
                        date_max = sym_df["date"].max().strftime("%Y-%m-%d")
                        print(
                            f"  [锁定除权/拆股] 股票: {sym} (扫描区间: {date_min} ~ {date_max})"
                        )

                del batch_data_map
                gc.collect()

            raw_suspicious_list = sorted(list(suspicious_symbols))
            print(
                f"\n全量文件扫描完成！累计锁定 {len(raw_suspicious_list)} 只真正存在除权/拆股断层的股票。"
            )
            # 写入中间 txt 缓存
            self._save_raw_cache(raw_suspicious_list)

        # 4. 批量补充 mkt_code (单线程)
        final_symbol_list = self.resolve_mkt_codes(raw_suspicious_list)

        # 5. 写入最终 csv 文件 (生成一级缓存)
        self._save_to_csv(final_symbol_list)

        return final_symbol_list

    def _save_to_csv(self, symbol_list: list[dict[str, any]]):
        """覆盖保存包含 symbol 和 mkt_code 的 suspicious_symbols.csv 文件"""
        try:
            df = pd.DataFrame(symbol_list)
            df.to_csv(self.final_csv_path, index=False, encoding="utf-8")
            print(f"\n[成功] 结构化结果已写至最终文件: {self.final_csv_path.resolve()}")
        except Exception as e:
            print(f"保存文件 {self.final_csv_path} 失败: {e}")