import gc
import json
import logging
import os
from pathlib import Path
import random
import sys
import time
from typing import Dict, List, Optional, Set

import pandas as pd
import yfinance as yf

# ========== 1. 项目路径规范导入 ==========
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.em_stock_uti import EMWebCrawlerUti
from finance.utility.get_proxy import ProxyManager
from finance.utility.toolkit import ToolKit

# ========== 2. 配置日志 ==========
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 屏蔽 yfinance 内部冗余的报错日志
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


def format_proxy_url(proxy_str: str) -> str:
    """统一代理 URL 格式，补齐协议前缀"""
    if not proxy_str:
        return ""
    if not proxy_str.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return f"http://{proxy_str}"
    return proxy_str


class USStockActionsFetcher:

    def __init__(
        self,
        target_dir: Optional[Path] = None,
        stock_filename: str = "stock_20260915.csv",
        output_filename: str = "us_stock_actions_history.csv",
        checkpoint_filename: str = "actions_fetch_checkpoint.json",
        proxy_manager: Optional[ProxyManager] = None,
        batch_size: int = 30,
        max_retries_per_symbol: int = 3,
        start_date: Optional[str] = "2025-01-01",  # 增加起始日期过滤参数 (格式: YYYY-MM-DD)
    ):
        """美股除权分红数据批量抓取器（支持日期过滤 + 代理持久复用版）"""
        self.target_dir = target_dir or (FINANCE_ROOT / "usstockinfo")
        os.makedirs(self.target_dir, exist_ok=True)

        self.stock_list_path = self.target_dir / stock_filename
        self.output_csv_path = self.target_dir / output_filename
        self.checkpoint_path = self.target_dir / checkpoint_filename

        self.proxy_manager = (
            proxy_manager or ProxyManager.create_overseas_manager()
        )
        self.batch_size = batch_size
        self.max_retries_per_symbol = max_retries_per_symbol
        self.start_date = start_date  # 过滤起始日期

        # 缓存当前已被证实可用的代理（跨股票复用）
        self.current_working_proxy: Optional[Dict[str, str]] = None

        self.processed_symbols: Set[str] = self._load_checkpoint()

    def _load_checkpoint(self) -> Set[str]:
        """读取断点记录"""
        if self.checkpoint_path.exists():
            try:
                with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                    return set(json.load(f))
            except Exception as e:
                logger.warning(
                    f"读取 Checkpoint 失败，将重新建立索引: {e}"
                )
        return set()

    def _save_checkpoint(self):
        """保存断点记录"""
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(list(self.processed_symbols), f)

    def load_symbols(self) -> List[str]:
        """从 CSV 载入待处理的美股 Symbol 列表"""
        if not self.stock_list_path.exists():
            raise FileNotFoundError(
                f"未找到股票列表文件: {self.stock_list_path}"
            )

        df = pd.read_csv(self.stock_list_path)
        target_col = "symbol" if "symbol" in df.columns else "Symbol"
        symbols = (
            df[target_col]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .unique()
            .tolist()
        )
        return symbols

    def fetch_actions_for_symbol(self, symbol: str) -> List[Dict]:
        """获取单只股票的数据：优先复用当前代理，带有日期过滤与重试控制"""
        # 关闭 SSL 校验环境变量
        os.environ.setdefault("CURL_CA_BUNDLE", "")
        os.environ.setdefault("SSL_CERT_FILE", "")

        for attempt in range(1, self.max_retries_per_symbol + 1):
            # 1. 如果没有已缓存的可用代理，则触发代理测试选择新代理
            if not self.current_working_proxy:
                self.current_working_proxy = (
                    self.proxy_manager.get_working_proxy(
                        max_retries=2, enable_proxy=True
                    )
                )

                # 兜底：若测试未找到可用代理，退化使用轮询代理
                if not self.current_working_proxy:
                    self.current_working_proxy = (
                        self.proxy_manager.get_next_proxy()
                    )

            proxy_dict = self.current_working_proxy
            raw_proxy = (
                (
                    proxy_dict.get("socks5")
                    or proxy_dict.get("https")
                    or proxy_dict.get("http")
                )
                if proxy_dict
                else None
            )
            proxy_str = format_proxy_url(raw_proxy) if raw_proxy else None

            # 设置环境变量
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

                        # 核心修改：如果设置了 start_date，跳过早于该日期的记录
                        if self.start_date and date_str < self.start_date:
                            continue

                        div = float(row.get("Dividends", 0.0))
                        split = float(row.get("Stock Splits", 0.0))

                        if div != 0 or split != 0:
                            records.append(
                                {
                                    "symbol": symbol,
                                    "date": date_str,
                                    "dividend": div,
                                    "split_ratio": split,
                                }
                            )

                del actions
                del ticker
                logger.info(
                    f"[{symbol}] 抓取成功 (保留 {len(records)} 条符合日期要求的记录, 尝试: {attempt}/{self.max_retries_per_symbol}, 代理: {proxy_str or '直连'})"
                )

                # 反馈成功状态给 ProxyManager
                if proxy_str and hasattr(self.proxy_manager, "mark_proxy_working"):
                    self.proxy_manager.mark_proxy_working(proxy_str)

                # 抓取成功！保留 self.current_working_proxy 给下一个股票继续复用
                return records

            except Exception as e:
                logger.warning(
                    f"[{symbol}] 代理 [{proxy_str}] 失败 (尝试: {attempt}/{self.max_retries_per_symbol}): {e} -> 弃用此代理..."
                )

                # 反馈失败状态给 ProxyManager 并置空当前代理
                if proxy_str and hasattr(self.proxy_manager, "mark_proxy_failed"):
                    self.proxy_manager.mark_proxy_failed(proxy_str)

                self.current_working_proxy = None

            finally:
                # 必须清理代理配置环境变量
                os.environ.pop("HTTP_PROXY", None)
                os.environ.pop("HTTPS_PROXY", None)

        logger.error(f"❌ [{symbol}] 在达到最大重试次数后仍然失败，跳过该股票")
        return []

    def run(self):
        all_symbols = self.load_symbols()
        pending_symbols = [
            s for s in all_symbols if s not in self.processed_symbols
        ]

        logger.info(f"目标目录: {self.target_dir.resolve()}")
        logger.info(f"起始筛选日期: {self.start_date or '不限制(全量历史)'}")
        logger.info(
            f"股票总数: {len(all_symbols)} | 已完成: {len(self.processed_symbols)} | 待处理: {len(pending_symbols)}"
        )

        if not pending_symbols:
            logger.info("所有股票除权分红数据均已抓取完成！")
            return

        total_batches = (
            len(pending_symbols) + self.batch_size - 1
        ) // self.batch_size

        for batch_idx in range(total_batches):
            start_idx = batch_idx * self.batch_size
            end_idx = start_idx + self.batch_size
            batch_symbols = pending_symbols[start_idx:end_idx]

            logger.info(
                f"--- 处理 Batch [{batch_idx + 1}/{total_batches}] (包含 {len(batch_symbols)} 只股票) ---"
            )

            batch_records = []
            for symbol in batch_symbols:
                records = self.fetch_actions_for_symbol(symbol)
                if records:
                    batch_records.extend(records)

            # 1. 增量追加落盘 CSV
            if batch_records:
                df_batch = pd.DataFrame(batch_records)
                file_exists = self.output_csv_path.exists()

                df_batch.to_csv(
                    self.output_csv_path,
                    mode="a",
                    index=False,
                    header=not file_exists,
                    encoding="utf-8-sig",
                )
                del df_batch

            # 2. 批次落盘成功后更新 Checkpoint
            self.processed_symbols.update(batch_symbols)
            self._save_checkpoint()

            logger.info(
                f"Batch [{batch_idx + 1}/{total_batches}] 成功落盘！已累计完成: {len(self.processed_symbols)}/{len(all_symbols)}"
            )

            # 3. 内存回收
            del batch_records
            gc.collect()


if __name__ == "__main__":
    overseas_proxy_mgr = ProxyManager.create_overseas_manager()

    fetcher = USStockActionsFetcher(
        target_dir=FINANCE_ROOT / "usstockinfo",
        stock_filename="stock_20260914.csv",
        output_filename="us_stock_actions_history.csv",
        checkpoint_filename="actions_fetch_checkpoint.json",
        proxy_manager=overseas_proxy_mgr,
        batch_size=30,  # 30只股票一批落盘
        max_retries_per_symbol=3,  # 单只股票最多重试 3 次，防止死循环
        start_date="2025-01-01",  # 仅保留 2025-01-01 及之后发生的 Actions 数据
    )

    fetcher.run()