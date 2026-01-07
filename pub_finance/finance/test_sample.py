#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import progressbar
from utility.toolkit import ToolKit
from utility.stock_analysis import StockProposal
import gc
from utility.em_stock_uti import EMWebCrawlerUti
from uscrawler.ak_incre_crawler import AKUSWebCrawler
from utility.backtrader_exec import BacktraderExec

# 主程序入口
if __name__ == "__main__":
    """美股交易日期 utc-4"""
    trade_date = ToolKit("get latest trade date").get_us_latest_trade_date(1)

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

    """ 执行bt相关策略 """

    def run_backtest_in_process(date):
        """在独立进程中运行回测，确保内存完全释放"""
        import multiprocessing
        from multiprocessing import Queue

        def _worker(q, trade_date):
            try:
                cash, final_value = BacktraderExec(
                    "us_special", trade_date
                ).exec_btstrategy()
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

    # 主函数中替换原有调用
    # cash, final_value = exec_btstrategy(trade_date)
    cash, final_value = run_backtest_in_process(trade_date)

    collected = gc.collect()

    print("Garbage collector: collected %d objects." % (collected))

    """ 发送邮件 """
    StockProposal("us_special", trade_date).send_btstrategy_by_email(cash, final_value)

    """ 结束进度条 """
    pbar.finish()
