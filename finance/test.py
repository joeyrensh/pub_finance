#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from utility.FileInfo import FileInfo
from usstrategy.UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from utility.ToolKit import ToolKit
from utility.MyEmail import MyEmail
from datetime import datetime, timedelta
import pandas as pd
import sys
import seaborn as sns
from backtraderref.BTStrategy import BTStrategy
import backtrader as bt
from utility.TickerInfo import TickerInfo
from uscrawler.EMWebCrawler import EMWebCrawler
from uscrawler.EMUsTickerCategoryCrawler import EMUsTickerCategoryCrawler
from backtraderref.BTPandasDataExt import BTPandasDataExt
from utility.StockProposal import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc

""" 执行策略 """


def exec_strategy(date):
    """小市值大波动策略-策略1"""
    us_strate = UsStrategy(date)
    df1 = us_strate.get_usstrategy1()
    print(tabulate(df1, headers="keys", tablefmt="pretty"))
    return df1


""" backtrader策略 """


def exec_btstrategy(date):
    """创建cerebro对象"""
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.broker.set_coc(True)
    """ 添加bt相关的策略 """
    cerebro.addstrategy(BTStrategy)

    # 回测时需要添加 TimeReturn 分析器
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="_TimeReturn")
    cerebro.addobserver(bt.observers.BuySell)
    """ 初始资金100M """
    cerebro.broker.setcash(2000000.0)
    """ 每手10股 """
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0.001, stocklike=True)
    """ 添加股票当日即历史数据 """
    stocklist = ["GEHC", "LNG"]
    list = TickerInfo(date, "us").get_backtrader_data_feed_testonly(stocklist)
    """ 循环初始化数据进入cerebro """
    for h in list:
        """ 历史数据最早不超过2021-01-01 """
        # fromdate=datetime(2023, 1, 1)
        data = BTPandasDataExt(
            dataname=h, name=h["symbol"][0], fromdate=datetime(2023, 1, 1), datetime=-1, timeframe=bt.TimeFrame.Days
        )
        cerebro.adddata(data, name=h["symbol"][0])
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
    """美股交易日期 utc-4"""
    trade_date = ToolKit("获取最新美股交易日期").get_us_latest_trade_date(1)

    """ 执行bt相关策略 """
    exec_btstrategy(trade_date)
