#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import progressbar
from pathlib import Path
import sys
import time
import functools
import multiprocessing
from multiprocessing import Queue

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.utility.toolkit import ToolKit
from finance.utility.stock_analysis_simplify import StockProposal
import gc
from finance.utility.em_stock_uti import EMWebCrawlerUti
from finance.uscrawler.ak_incre_crawler import AKUSWebCrawler
from finance.utility.backtrader_exec import BacktraderExec


# ------------------- 重试机制 -------------------
def retry_call(func, max_retries=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    通用重试函数
    :param func: 待执行的函数（无参）
    :param max_retries: 最大重试次数
    :param delay: 初始延迟（秒）
    :param backoff: 延迟倍数
    :param exceptions: 需重试的异常类型
    :return: 函数返回值
    """
    for attempt in range(max_retries):
        try:
            return func()
        except exceptions as e:
            if attempt == max_retries - 1:
                raise  # 最后一次失败则抛出异常
            wait = delay * (backoff**attempt)
            print(f"重试第 {attempt+1} 次，等待 {wait:.1f} 秒后重试，错误: {e}")
            time.sleep(wait)
    # 理论上不会执行到这里
    raise RuntimeError("重试失败")


# ------------------- 子进程执行器 -------------------
def run_in_subprocess(task_func, *args, timeout=3600):
    """
    在独立子进程中运行指定任务，任务结束后操作系统会自动回收该进程占用的所有内存与 Swap
    :param task_func: 目标执行函数
    :param args: 传给目标函数的参数
    :param timeout: 超时时间（秒）
    :return: 目标函数的返回值
    """
    def _worker(q, func, func_args):
        try:
            res = func(*func_args)
            q.put(("success", res))
        except Exception as e:
            import traceback
            q.put(("error", f"{e}\n{traceback.format_exc()}"))

    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=_worker, args=(q, task_func, args))
    p.start()
    p.join(timeout=timeout)

    if p.is_alive():
        p.terminate()
        p.join()
        raise TimeoutError(f"任务 {task_func.__name__} 执行超时 (限制: {timeout}s)")

    if q.empty():
        raise RuntimeError(f"任务 {task_func.__name__} 子进程异常退出，未返回结果")

    status, result = q.get()
    if status == "error":
        raise RuntimeError(f"子进程执行出错: {result}")

    return result


# 主程序入口
if __name__ == "__main__":
    """美股交易日期 utc-4"""
    trade_date = ToolKit("获取最新交易日").get_us_latest_trade_date(0)

    """ 非交易日程序终止运行 """
    if ToolKit("判断是否休市").is_us_trade_date(trade_date):
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
        "\n",
    ]
    """ 创建进度条并开始运行 """
    pbar = progressbar.ProgressBar(maxval=100, widgets=widgets).start()

    """ 东方财经爬虫 """
    """ 爬取每日最新股票数据 """
    # em = EMWebCrawlerUti()
    # em.get_daily_stock_info("us", trade_date)

    # ========== 1. 爬虫重试 ==========
    print("开始爬取美股日线数据...")
    ak_daily_crawler = AKUSWebCrawler()

    def crawl():
        return ak_daily_crawler.get_us_daily_stock_info_ak(trade_date)

    df_stock_daily = retry_call(crawl, max_retries=3, delay=2)
    print("爬取完成")

    """ 执行bt相关策略 """

    def run_backtest_in_process(date, exec_func):
        """在独立进程中运行回测，确保内存完全释放。

        参数:
            date: 交易日期，传递给 exec_func
            exec_func: 可调用对象，签名为 exec_func(date)，返回 (cash, final_value)
        """
        def _exec_wrapper(d):
            return exec_func(d)

        return run_in_subprocess(_exec_wrapper, date, timeout=3600)

    def _exec_spark_and_email(market, trade_date, cash, final_value):
        """在子进程中独立运行 Spark 分析并发送邮件"""
        proposal = StockProposal(market, trade_date)
        if market == "cnetf":
            proposal.send_etf_btstrategy_by_email(cash, final_value)
        else:
            proposal.send_btstrategy_by_email(cash, final_value)

    def run_backtest_and_send(market, trade_date, force_run=False):
        """
        运行指定市场的回测并发送邮件
        - market: 市场标识 ("cn", "cnetf", "cn_dynamic")
        - market: 市场标识 ("us", "us_special", "us_dynamic")
        - trade_date: 交易日期
        """
        # 1. 在独立子进程运行回测，跑完强行释放物理内存
        cash, final_value = run_backtest_in_process(
            trade_date,
            lambda d: BacktraderExec(market, d).exec_btstrategy(force_run=force_run),
        )
        collected = gc.collect()
        print("Garbage collector: collected %d objects." % (collected))

        # 2. 将 Spark 分析与邮件发送同样放入子进程，隔离 Spark 占用的内存与 Swap
        run_in_subprocess(_exec_spark_and_email, market, trade_date, cash, final_value)

    # ========== 2. 策略执行与邮件发送重试 ==========
    def retry_backtest_and_send(market, trade_date, force_run=False, max_retries=3):
        """带重试的 backtest 封装"""

        def do_task():
            run_backtest_and_send(market, trade_date, force_run)

        retry_call(do_task, max_retries=max_retries, delay=3)

    # 美股主要策略执行
    print("-----------美股主策略执行-----------")
    retry_backtest_and_send("us", trade_date)

    # # 固定列表追踪
    # print("-----------美股固定列表策略执行-----------")
    # retry_backtest_and_send("us_special", trade_date)

    # 动态列表追踪
    print("-----------美股动态列表策略执行-----------")
    retry_backtest_and_send("us_dynamic", trade_date)

    """ 结束进度条 """
    pbar.finish()