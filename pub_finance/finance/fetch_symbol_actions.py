#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from pathlib import Path
import sys
import time
import functools

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.get_proxy import ProxyManager
from finance.utility.stock_actions_fetcher import StockActionsFetcher
import logging


def main():
    # 配置基础日志输出到控制台
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )    
    # 示例 1: 运行 A 股 / ETF 除权分红抓取
    # print(">>> 启动 CN 市场除权抓取...")
    # fetcher_cn = StockActionsFetcher(
    #     market="cn",
    #     target_dir=FINANCE_ROOT / "cnstockinfo",
    #     output_filename="cn_stock_actions_history.csv",
    #     checkpoint_filename="actions_fetch_checkpoint.json",
    #     batch_size=100,  # 每 100 只股票追加落盘一次
    #     start_date="2025-01-01",
    # )
    # fetcher_cn.run()

    # 示例 2: 运行美股除权分红抓取
    print("\n>>> 启动 US 市场除权抓取...")
    overseas_proxy_mgr = ProxyManager.create_overseas_manager()
    fetcher_us = StockActionsFetcher(
        market="us",
        target_dir=FINANCE_ROOT / "usstockinfo",
        output_filename="us_stock_actions_history.csv",
        checkpoint_filename="actions_fetch_checkpoint.json",
        proxy_manager=overseas_proxy_mgr,
        batch_size=30,
        start_date="2025-01-01",
    )
    fetcher_us.run()


if __name__ == "__main__":
    main()