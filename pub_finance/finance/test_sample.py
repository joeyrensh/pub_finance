import gc
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Dict, List, Optional, Set

import pandas as pd
import yfinance as yf

# 1. 项目路径规范导入
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.em_stock_uti import EMWebCrawlerUti
from finance.utility.get_proxy import ProxyManager
from finance.utility.toolkit import ToolKit

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 屏蔽 yfinance 内部冗余的报错日志（如 "possibly delisted"）
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


class USStockActionsFetcher:

    def __init__(
        self,
        target_dir: Optional[Path] = None,
        stock_filename: str = "stock_20260915.csv",
        output_filename: str = "us_stock_actions_history.csv",
        checkpoint_filename: str = "actions_fetch_checkpoint.json",
        proxy_manager: Optional[ProxyManager] = None,
        batch_size: int = 30,
    ):
        """美股除权分红数据批量抓取器（无限重试直到成功版）"""
        self.target_dir = target_dir or (FINANCE_ROOT / "usstockinfo")
        os.makedirs(self.target_dir, exist_ok=True)

        self.stock_list_path = self.target_dir / stock_filename
        self.output_csv_path = self.target_dir / output_filename
        self.checkpoint_path = self.target_dir / checkpoint_filename

        self.proxy_manager = (
            proxy_manager or ProxyManager.create_overseas_manager()
        )
        self.batch_size = batch_size

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
        """获取单只股票的数据：无上限重试，直到获取成功为止"""
        attempt = 0
        while True:
            attempt += 1

            # 1. 优先获取通过有效性验证的代理（避免卡死在死代理上）
            proxy_dict = self.proxy_manager.get_working_proxy(
                max_retries=2, enable_proxy=True
            )

            # 若 get_working_proxy 未找到可用代理，则退化轮询 get_next_proxy 避免死锁
            if not proxy_dict:
                proxy_dict = self.proxy_manager.get_next_proxy()

            proxy_str = (
                (proxy_dict.get("https") or proxy_dict.get("http"))
                if proxy_dict
                else None
            )

            # 设置全局环境变量与超时控制
            if proxy_str:
                os.environ["HTTP_PROXY"] = proxy_str
                os.environ["HTTPS_PROXY"] = proxy_str

            try:
                ticker = yf.Ticker(symbol)
                actions = ticker.actions

                # yfinance 请求正常完成
                records = []
                if actions is not None and not actions.empty:
                    for date, row in actions.iterrows():
                        div = float(row.get("Dividends", 0.0))
                        split = float(row.get("Stock Splits", 0.0))

                        if div != 0 or split != 0:
                            records.append(
                                {
                                    "symbol": symbol,
                                    "date": date.strftime("%Y-%m-%d"),
                                    "dividend": div,
                                    "split_ratio": split,
                                }
                            )

                # 清理临时变量并返回数据（即便是空数组列表 [], 也代表成功获取且无除权）
                del actions
                del ticker
                logger.info(
                    f"[{symbol}] 抓取成功 (尝试次数: {attempt}, Proxy: {proxy_str})"
                )
                return records

            except Exception as e:
                logger.warning(
                    f"[{symbol}] 第 {attempt} 次抓取失败 (Proxy: {proxy_str}) 错误: {e} -> 正在重试..."
                )
                time.sleep(1)  # 失败等待 1s 后继续死循环重试

            finally:
                # 必须清除代理配置环境变量
                os.environ.pop("HTTP_PROXY", None)
                os.environ.pop("HTTPS_PROXY", None)

    def run(self):
        all_symbols = self.load_symbols()
        pending_symbols = [
            s for s in all_symbols if s not in self.processed_symbols
        ]

        logger.info(f"目标目录: {self.target_dir.resolve()}")
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
                # 每只股票都会死循环重试直到成功
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

            time.sleep(0.5)


if __name__ == "__main__":
    overseas_proxy_mgr = ProxyManager.create_overseas_manager()

    fetcher = USStockActionsFetcher(
        target_dir=FINANCE_ROOT / "usstockinfo",
        stock_filename="stock_20260914.csv",
        output_filename="us_stock_actions_history.csv",
        checkpoint_filename="actions_fetch_checkpoint.json",
        proxy_manager=overseas_proxy_mgr,
        batch_size=30,  # 30只股票一批落盘
    )

    fetcher.run()