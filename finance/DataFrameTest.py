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
from backtraderref.BTStrategy import BTStrategy
import backtrader as bt
from utility.TickerInfo import TickerInfo
from uscrawler.EMWebCrawler import EMWebCrawler
from uscrawler.EMUsTickerCategoryCrawler import EMUsTickerCategoryCrawler
from backtraderref.BTPandasDataExt import BTPandasDataExt

""" backtrader策略 """

def exec_btstrategy(date):
    """ 创建cerebro对象 """
    cerebro = bt.Cerebro()
    """ 添加bt相关的策略 """
    cerebro.addstrategy(BTStrategy)
    """ 初始资金100M """
    cerebro.broker.setcash(100000.0)
    """ 每手10股 """
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0.001)
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, 'us').get_backtrader_data_feed_test()
    """ 循环初始化数据进入cerebro """
    for h in list:
        print("正在初始化: ", h['symbol'][0])
        """ 历史数据最早不超过2021-01-01 """
        data = BTPandasDataExt(
            dataname=h, name=h['symbol'][0], fromdate=datetime(2021, 1, 1))
        cerebro.adddata(data)
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)
    """ 起始资金池 """
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    """ 运行cerebro """
    cerebro.run()
    """ 最终资金池 """
    print('当前现金持有: ', cerebro.broker.get_cash())
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    """ 画图相关 """
    # cerebro.plot(iplot=True, subplot=True)


# 主程序入口
if __name__ == '__main__':
    """ 美股交易日期 utc-4 """
    trade_date = ToolKit('获取最新美股交易日期').get_us_latest_trade_date(0)

    """ 执行bt相关策略 """
    exec_btstrategy(trade_date)

