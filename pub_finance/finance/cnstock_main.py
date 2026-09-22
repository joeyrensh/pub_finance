#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import argparse
import functools
import gc
import multiprocessing
from multiprocessing import Queue
from pathlib import Path
import sys
import time
import traceback
import progressbar

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.cncrawler.ak_incre_crawler import AKCNWebCrawler
from finance.utility.backtrader_exec import BacktraderExec
from finance.utility.em_stock_uti import EMWebCrawlerUti
from finance.utility.stock_analysis_simplify import StockProposal
from finance.utility.toolkit import ToolKit


# ------------------- 重试机制 -------------------
def retry_call(
    func, max_retries=3, delay=1, backoff=2, exceptions=(Exception,)
):
  """通用重试函数

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
      print(
          f"重试第 {attempt+1} 次，等待 {wait:.1f} 秒后重试，错误: {e}"
      )
      time.sleep(wait)
  # 理论上不会执行到这里
  raise RuntimeError("重试失败")


# ------------------- 子进程 worker 顶层定义（解决 pickle 序列化问题） -------------------
def _subprocess_worker(q, func, func_args):
  """全局 worker 函数，确保支持 spawn 模式下的 pickle 序列化"""
  try:
    res = func(*func_args)
    q.put(("success", res))
  except Exception as e:
    q.put(("error", f"{e}\n{traceback.format_exc()}"))


def run_in_subprocess(task_func, *args, timeout=3600):
  """在独立子进程中运行指定任务，任务结束后操作系统会自动回收该进程占用的所有内存与 Swap

  :param task_func: 必须是顶层定义的函数（不能是局部函数或 lambda）
  :param args: 传给目标函数的参数
  :param timeout: 超时时间（秒）
  :return: 目标函数的返回值
  """
  ctx = multiprocessing.get_context("spawn")
  q = ctx.Queue()
  p = ctx.Process(target=_subprocess_worker, args=(q, task_func, args))
  p.start()
  p.join(timeout=timeout)

  if p.is_alive():
    p.terminate()
    p.join()
    raise TimeoutError(
        f"任务 {task_func.__name__} 执行超时 (限制: {timeout}s)"
    )

  if q.empty():
    raise RuntimeError(
        f"任务 {task_func.__name__} 子进程异常退出，未返回结果"
    )

  status, result = q.get()
  if status == "error":
    raise RuntimeError(f"子进程执行出错: {result}")

  return result


# ------------------- 业务任务顶层包装 -------------------
def _exec_backtest_task(market, trade_date, force_run):
  """在子进程中执行回测的顶层函数"""
  return BacktraderExec(market, trade_date).exec_btstrategy(
      force_run=force_run
  )


def _exec_spark_and_email(market, trade_date, cash, final_value):
  """在子进程中执行 Spark 分析与邮件发送的顶层函数"""
  proposal = StockProposal(market, trade_date)
  if market == "cnetf":
    proposal.send_etf_btstrategy_by_email(cash, final_value)
  else:
    proposal.send_btstrategy_by_email(cash, final_value)


# 主程序入口
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="A股策略回测及分析运行脚本")
  parser.add_argument(
      "--skip-crawl",
      action="store_true",
      default=False,
      help="是否跳过爬虫流程（默认不跳过）",
  )
  parser.add_argument(
      "--force-run",
      action="store_true",
      default=False,
      help="是否强制运行回测策略（默认不强制）",
  )
  parser.add_argument(
      "--trade-date-offset",
      type=int,
      default=0,
      help="交易日偏移量，0表示取当前交易日，1表示T-1，2表示T-2（默认0）",
  )
  args = parser.parse_args()

  """美股交易日期 utc+8"""
  trade_date = ToolKit("获取最新交易日").get_cn_latest_trade_date(
      args.trade_date_offset
  )

  """ 非交易日程序终止运行 """
  if ToolKit("判断是否休市").is_cn_trade_date(trade_date):
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
  # ========== 1. 爬虫流程（根据参数决定是否跳过） ==========
  if args.skip_crawl:
    print("已使用 --skip-crawl 参数，跳过爬虫流程。")
  else:
    print("开始爬取A股日线数据...")
    em = EMWebCrawlerUti()

    def crawl():
      return em.get_daily_stock_info("cn", trade_date)

    df_stock_daily = retry_call(crawl, max_retries=3, delay=2)
    print("爬取完成")

  # em = AKCNWebCrawler()
  # em.get_cn_daily_stock_info_ak(trade_date)

  # em = EMWebCrawlerUti()
  # em.get_daily_gz_info("cn", trade_date)

  """ 执行bt相关策略 """

  def run_backtest_and_send(market, trade_date, force_run=False):
    """运行指定市场的回测并发送邮件

    - market: 市场标识 ("cn", "cnetf", "cn_dynamic")
    - market: 市场标识 ("us", "us_special", "us_dynamic")
    - trade_date: 交易日期
    - force_run: 是否强制执行
    """
    # 1. 在独立子进程运行回测，结束即彻底回收物理内存
    cash, final_value = run_in_subprocess(
        _exec_backtest_task, market, trade_date, force_run, timeout=3600
    )
    collected = gc.collect()
    print("Garbage collector: collected %d objects." % (collected))

    # 2. 将 Spark 分析与邮件发送放入子进程，完全清理 Spark/JVM 占用的内存和 Swap
    run_in_subprocess(
        _exec_spark_and_email, market, trade_date, cash, final_value, timeout=3600
    )

  # ========== 2. 策略执行与邮件发送重试 ==========
  def retry_backtest_and_send(market, trade_date, force_run=False, max_retries=3):
    """带重试的 backtest 封装"""

    def do_task():
      run_backtest_and_send(market, trade_date, force_run=force_run)

    retry_call(do_task, max_retries=max_retries, delay=3)

  # A股主要策略执行
  print("-----------A股主策略执行-----------")
  retry_backtest_and_send("cn", trade_date, force_run=args.force_run)

  # ETF主要策略执行
  print("-----------A股ETF策略执行-----------")
  retry_backtest_and_send("cnetf", trade_date, force_run=args.force_run)

  # A股动态列表执行
  print("-----------A股动态列表策略执行-----------")
  retry_backtest_and_send("cn_dynamic", trade_date, force_run=args.force_run)

  """ 结束进度条 """
  pbar.finish()