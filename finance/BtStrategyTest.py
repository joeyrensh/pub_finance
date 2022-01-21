#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from os import close
import backtrader as bt
from utility.ToolKit import ToolKit
from datetime import datetime
from datetime import datetime, timedelta
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo
from backtraderref.BTStrategyVol import BTStrategyVol
from utility.TickerInfo import TickerInfo
from backtraderref.BTPandasDataExt import BTPandasDataExt


if __name__ == "__main__":

    cerebro = bt.Cerebro()
    cerebro.addstrategy(BTStrategyVol)
    cerebro.broker.setcash(100000.0)
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)
    cerebro.broker.setcommission(commission=0.001)

    trade_date = ToolKit("获取最新A股交易日期").get_cn_latest_trade_date(1)
    list = TickerInfo(trade_date, "cn").get_backtrader_data_feed_test()
    for h in list:
        print("正在初始化: ", h["symbol"][0])
        data = BTPandasDataExt(
            dataname=h, name=h["symbol"][0], fromdate=datetime(2021, 1, 1)
        )
        cerebro.adddata(data)
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)

    print("Starting Portfolio Value: %.2f" % cerebro.broker.getvalue())

    cerebro.run()

    print("当前现金持有: ", cerebro.broker.get_cash())
    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())

    # b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
    # cerebro.plot(b)
