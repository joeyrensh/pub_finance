#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import argparse
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.get_proxy import ProxyManager
from finance.utility.stock_actions_fetcher import StockActionsFetcher


def parse_args():
    parser = argparse.ArgumentParser(description="中美股除权分红数据定时抓取入口脚本")
    
    # 命令行参数配置
    parser.add_argument(
        "--market",
        type=str,
        default="all",
        choices=["cn", "us", "all"],
        help="指定运行的市场类型: cn (A股), us (美股), all (两者都跑，默认: all)",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=None,
        help="批量落盘大小 (如果不传，CN 默认 100，US 默认 30)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2025-01-01",
        help="数据的起始日期 (格式: YYYY-MM-DD，默认: 2025-01-01)",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="是否强制清空 Checkpoint 重新抓取 (定时任务/手动全量重跑时加上此标记)",
    )

    return parser.parse_args()


def run_cn(batch_size: int, start_date: str, force_refresh: bool):
    print(">>> 启动 CN 市场除权抓取...")
    fetcher_cn = StockActionsFetcher(
        market="cn",
        target_dir=FINANCE_ROOT / "cnstockinfo",
        output_filename="cn_stock_actions_history.csv",
        checkpoint_filename="actions_fetch_checkpoint.json",
        batch_size=batch_size,
        start_date=start_date,
        force_refresh=force_refresh,
    )
    fetcher_cn.run()


def run_us(batch_size: int, start_date: str, force_refresh: bool):
    print("\n>>> 启动 US 市场除权抓取...")
    overseas_proxy_mgr = ProxyManager.create_overseas_manager()
    fetcher_us = StockActionsFetcher(
        market="us",
        target_dir=FINANCE_ROOT / "usstockinfo",
        output_filename="us_stock_actions_history.csv",
        checkpoint_filename="actions_fetch_checkpoint.json",
        proxy_manager=overseas_proxy_mgr,
        batch_size=batch_size,
        start_date=start_date,
        force_refresh=force_refresh,
    )
    fetcher_us.run()


def main():
    args = parse_args()

    # 配置基础日志输出到控制台
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    market = args.market.lower()

    # 根据选定的市场和指定的批次大小执行对应流程
    if market in ["cn", "all"]:
        batch_size_cn = args.batch if args.batch is not None else 100
        run_cn(
            batch_size=batch_size_cn,
            start_date=args.start_date,
            force_refresh=args.force_refresh,
        )

    if market in ["us", "all"]:
        batch_size_us = args.batch if args.batch is not None else 30
        run_us(
            batch_size=batch_size_us,
            start_date=args.start_date,
            force_refresh=args.force_refresh,
        )


if __name__ == "__main__":
    main()