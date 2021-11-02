#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from backtrader.cerebro import OptReturn
import matplotlib.pyplot as plt
import matplotlib
from os import close
import backtrader as bt
from UsFileInfo import UsFileInfo
from UsTicker import UsTicker
from ToolKit import ToolKit
from backtrader.feeds import PandasData
from datetime import datetime
import pandas as pd
import time
from datetime import datetime, timedelta
from MyEmail import MyEmail
import seaborn as sns
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo
from BTUsStrategy import BTUsStrategy
from EMWebCrawler import EMWebCrawler
from UsTickerInfo import UsTickerInfo


if __name__ == '__main__':

    # cerebro = bt.Cerebro()
    # # Add a strategy
    # # cerebro.addstrategy(StrategyOne)
    # cerebro.addstrategy(BTUsStrategy)
    # cerebro.broker.setcash(1000000.0)
    # cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    # cerebro.broker.setcommission(commission=0.001)

    # # Add Data
    # trade_date = ToolKit('获取最新美股交易日期').get_latest_trade_date(1)
    # list = UsTicker(trade_date).get_backtrader_data_feed_test()
    # for h in list:
    #     data = bt.feeds.PandasData(
    #         dataname=h, name=h['symbol'][0], fromdate=datetime(2021, 1, 1))
    #     # Add a data
    #     cerebro.adddata(data)
    #     # 周数据
    #     # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)

    # print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # cerebro.run()

    # print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # # b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
    # # cerebro.plot(b)

    # em = EMWebCrawler()
    # em.get_us_daily_stock_info('20211101')
    # tickers = UsTickerInfo('20211101').get_usstock_list()
    # files = UsFileInfo('20211101').get_files_day_list
    # print(files)
    file = UsFileInfo('20211102')
    files = file.get_files_day_list
    print(files)