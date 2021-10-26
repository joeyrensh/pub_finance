#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import matplotlib.pyplot as plt
import matplotlib
from os import close
import backtrader as bt
from UsTicker import UsTicker
from ToolKit import ToolKit
from backtrader.feeds import PandasData
from datetime import datetime
import pandas as pd
import time


class TestStrategy(bt.Strategy):
    # 自定义均线的实践间隔，默认是5天
    params = (
        ('slowperiod', 26),
        ('fastperiod', 12),
        ('signalperiod', 9),
        ('shortperiod', 20),
        ('longperiod', 60)
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 存储特定股票的订单，key为股票代码
        self.orders = dict()
        # 存储不同的技术指标
        self.inds = dict()
        # 增加均线，简单移动平均线（SMA）又称“算术移动平均线”，是指对特定期间的收盘价进行简单平均化
        # self.sma = bt.indicators.SimpleMovingAverage(
        #     self.datas[0], period=self.params.maperiod)
        for i, d in enumerate(self.datas):
            self.orders[d._name] = None
            # 为每个股票初始化技术指标
            self.inds[d] = dict()
            # 判断是否为日线数据
            if i % 2 == 0:
                self.inds[d]['ma'] = bt.talib.MA(
                    d.close, timeperiod=self.params.shortperiod)
            # 周线数据
            else:
                self.inds[d]['macd_weekly'] = bt.talib.MACD(
                    d.close,
                    fastperiod=self.params.fastperiod,
                    slowperiod=self.params.slowperiod,
                    signalperiod=self.params.signalperiod)

    # 订单状态改变回调方法 be notified through notify_order(order) of any status change in an order

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                print('{} BUY {} EXECUTED, Price: {:.2f}'.format(
                    self.datetime.date(), order.data._name, order.executed.price))
            else:  # Sell
                self.orders[order.data._name] = None
                print('{} SELL {} EXECUTED, Price: {:.2f}'.format(
                    self.datetime.date(), order.data._name, order.executed.price))

    # 交易状态改变回调方法 be notified through notify_trade(trade) of any opening/updating/closing trade

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        # 每笔交易收益 毛利和净利
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        for i, d in enumerate(self.datas):
            pos = self.getposition(d)
            # 如果是周数据就跳过，在日数据中已经处理过了
            if i % 2 == 1:
                continue
            # self.log('当前代码: %s, 当前持仓:, %s' % (d._name,
            #          self.getposition(d).size))
            # 有持仓就不再买入
            if not len(pos):
                # 收盘价站上MA均线
                if d.close[0] >= self.inds[d]['ma'][0]:
                    pass
                else:
                    continue
                # 周MACD缩短
                if self.inds[self.datas[i+1]]['macd_weekly'][0] > self.inds[self.datas[i+1]]['macd_weekly'][-1]:
                    pass
                else:
                    continue
                # 今日收盘价高于昨日
                if d.close[0] > d.close[-1]:
                    pass
                else:
                    continue
                # 买入对应仓位
                # print('Stock: ', d._name, '当日收盘价: ', d.close[0], '上日收盘价: ', d.close[-1])
                # print('Stock: ', d._name, '当日MA: ', self.inds[d]['ma'][0])
                # print('当周MACD: ', self.inds[self.datas[i+1]]['macd_weekly'][0], '上周MACD:', self.inds[self.datas[i+1]]['macd_weekly'][-1])
                self.orders[d._name] = self.buy(data=d)
            elif not self.orders[d._name]:
                # 跌破均线即卖出
                if d.close[0] <= self.inds[d]['ma'][0]:
                    self.orders[d._name] = self.sell(data=d)


if __name__ == '__main__':

    cerebro = bt.Cerebro()
    # Add a strategy
    cerebro.addstrategy(TestStrategy)
    cerebro.broker.setcash(100000.0)
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    cerebro.broker.setcommission(commission=0.001)

    # Add Data
    trade_date = ToolKit('获取最新美股交易日期').get_latest_trade_date(1)
    list = UsTicker(trade_date).get_backtrader_data_feed()
    t = ToolKit('回测数据加载进度')
    x = 0
    for h in list:
        x = x + 1
        t.progress_bar(len(list), x)
        data = PandasData(dataname=h, name=h['symbol'][0])
        # Add a data
        cerebro.adddata(data)
        cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.plot(iplot=True, subplot=True)
