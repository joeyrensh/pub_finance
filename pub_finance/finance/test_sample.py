#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import progressbar
from utility.toolkit import ToolKit
from utility.stock_analysis import StockProposal
import gc
from utility.em_stock_uti import EMWebCrawlerUti
from uscrawler.ak_incre_crawler import AKUSWebCrawler
from utility.backtrader_exec import BacktraderExec
import sys

# 主程序入口
if __name__ == "__main__":
    """美股交易日期 utc-4"""
    trade_date = ToolKit("get latest trade date").get_us_latest_trade_date(0)

    """ 非交易日程序终止运行 """
    if ToolKit("判断当天是否交易日").is_us_trade_date(trade_date):
        pass
    else:
        sys.exit()

    """ 定义程序显示的进度条 """
    widgets = [
        "doing task: ",
        progressbar.Percentage(),
        " ",
        progressbar.Bar(),
        " ",
        progressbar.ETA(),
    ]
    """ 创建进度条并开始运行 """
    pbar = progressbar.ProgressBar(maxval=100, widgets=widgets).start()

    """ 东方财经爬虫 """
    """ 爬取每日最新股票数据 """
    # em = EMWebCrawlerUti()
    # em.get_daily_stock_info("us", trade_date)

    # ak_daily_crawler = AKUSWebCrawler()
    # df_stock_daily = ak_daily_crawler.get_us_daily_stock_info_ak(trade_date)

    """ 执行bt相关策略 """

    def run_backtest_in_process(date, exec_func):
        """在独立进程中运行回测，确保内存完全释放。

        参数:
            date: 交易日期，传递给 exec_func
            exec_func: 可调用对象，签名为 exec_func(date)，返回 (cash, final_value)
        """
        import multiprocessing
        from multiprocessing import Queue

        def _worker(q, trade_date):
            try:
                cash, final_value = exec_func(trade_date)
                q.put((cash, final_value))
            except Exception as e:
                q.put(("error", str(e)))

        q = Queue()
        p = multiprocessing.Process(target=_worker, args=(q, date))
        p.start()
        p.join(timeout=3600)  # 1小时超时

        if p.is_alive():
            p.terminate()
            raise TimeoutError("Backtest timed out")

        result = q.get()
        if result[0] == "error":
            raise RuntimeError(result[1])
        return result[0], result[1]

    def run_backtest_and_send(market, trade_date, force_run=False):
        """
        运行指定市场的回测并发送邮件
        - market: 市场标识 ("cn", "cnetf", "cn_dynamic")
        - market: 市场标识 ("us", "us_special", "us_dynamic")
        - trade_date: 交易日期
        """
        cash, final_value = run_backtest_in_process(
            trade_date,
            lambda d: BacktraderExec(market, d).exec_btstrategy(force_run=force_run),
        )
        collected = gc.collect()
        print("Garbage collector: collected %d objects." % (collected))

        proposal = StockProposal(market, trade_date)
        if market == "cnetf":
            proposal.send_etf_btstrategy_by_email(cash, final_value)
        else:
            proposal.send_btstrategy_by_email(cash, final_value)

    # 美股主要策略执行
    run_backtest_and_send("us", trade_date, force_run=True)

    # 固定列表追踪
    run_backtest_and_send("us_special", trade_date, force_run=True)

    # 动态列表追踪
    run_backtest_and_send("us_dynamic", trade_date, force_run=True)

    """ 结束进度条 """
    pbar.finish()
