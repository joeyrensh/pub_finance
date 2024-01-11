#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from utility.FileInfo import FileInfo
from tabulate import tabulate
import progressbar
from utility.ToolKit import ToolKit
from utility.MyEmail import MyEmail
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
import seaborn as sns
from backtraderref.BTStrategyVol import BTStrategyVol
import backtrader as bt
from utility.TickerInfo import TickerInfo
from cncrawler.EMCNWebCrawler import EMCNWebCrawler
from cncrawler.EMCNTickerCategoryCrawler import EMCNTickerCategoryCrawler
from backtraderref.BTPandasDataExt import BTPandasDataExt
from utility.StockProposal import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc


""" backtrader策略 """


def exec_btstrategy(date):
    """创建cerebro对象"""
    cerebro = bt.Cerebro(stdstats=False)
    """ 添加bt相关的策略 """
    cerebro.addstrategy(BTStrategyVol)

    # 回测时需要添加 TimeReturn 分析器
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="_TimeReturn")
    cerebro.addobserver(bt.observers.BuySell)

    """ 初始资金100M """
    cerebro.broker.setcash(2000000.0)
    """ 每手10股 """
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0.001, stocklike=True)
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, "cn").get_backtrader_data_feed()
    """ 循环初始化数据进入cerebro """
    for h in list:
        if h["symbol"][0] != "SH600563":
            continue
        print("正在初始化: ", h["symbol"][0])
        """ 历史数据最早不超过2021-01-01 """
        data = BTPandasDataExt(
            dataname=h, name=h["symbol"][0], fromdate=datetime(2023, 1, 1)
        )
        cerebro.adddata(data)
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)
    """ 起始资金池 """
    print("Starting Portfolio Value: %.2f" % cerebro.broker.getvalue())

    # 节约内存
    del list
    gc.collect()

    """ 运行cerebro """
    result = cerebro.run()

    """ 最终资金池 """
    print("当前现金持有: ", cerebro.broker.get_cash())
    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())


# 主程序入口
if __name__ == "__main__":
    """美股交易日期 utc+8"""
    trade_date = ToolKit("获取最新A股交易日期").get_cn_latest_trade_date(0)

    """ 东方财经爬虫 """
    """ 爬取每日最新股票数据 """
    em = EMCNWebCrawler()
    em.get_cn_daily_stock_info(trade_date)

    """ 执行bt相关策略 """
    exec_btstrategy(trade_date)
