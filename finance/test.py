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
    # cerebro.broker.set_coc(True)
    """ 添加bt相关的策略 """
    cerebro.addstrategy(BTStrategyVol, trade_date=date)

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
    stocklist = ["SH603220", "SH601009", "SZ002807", "SH601169"]
    # stocklist = TickerInfo(date, "cn").get_stock_list()
    list = TickerInfo(date, "cn").get_backtrader_data_feed_testonly(stocklist)
    """ 循环初始化数据进入cerebro """
    for h in list:
        """历史数据最早不超过2021-01-01"""
        data = BTPandasDataExt(
            dataname=h,
            name=h["symbol"][0],
            fromdate=datetime(2023, 1, 1),
            datetime=-1,
            timeframe=bt.TimeFrame.Days,
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
    trade_date = ToolKit("get_latest_trade_date").get_cn_latest_trade_date(1)

    """ 非交易日程序终止运行 """
    if ToolKit("判断当天是否交易日").is_cn_trade_date(trade_date):
        pass
    else:
        sys.exit()

    """ 执行bt相关策略 """
    exec_btstrategy(trade_date)

    StockProposal("cn", trade_date).send_btstrategy_by_email()
