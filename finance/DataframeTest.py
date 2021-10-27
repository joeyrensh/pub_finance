#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from backtrader.cerebro import OptReturn
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
from datetime import datetime, timedelta


class StrategyOne(bt.Strategy):
    # 自定义均线的实践间隔，默认是5天
    params = (
        ('fastperiod', 12),
        ('slowperiod', 26),
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
        self.signals = dict()
        t = ToolKit('策略初始化')
        for i, d in enumerate(self.datas):
            t.progress_bar(len(self.datas), i)
            print(d._name)
            self.orders[d._name] = None
            # 为每个股票初始化技术指标
            self.inds[d] = dict()
            self.signals[d] = dict()
            # 日线MA20指标
            self.inds[d]['ma20'] = bt.indicators.SMA(
                d.close, period=self.params.shortperiod)
            # 日线MA60指标
            self.inds[d]['ma60'] = bt.indicators.SMA(
                d.close, period=self.params.longperiod)
            # 日线EMA20指标
            self.inds[d]['ema20'] = bt.indicators.EMA(
                d.close, period=self.params.shortperiod)
            # 日线EMA60指标
            self.inds[d]['ema60'] = bt.indicators.EMA(
                d.close, period=self.params.longperiod)
            # 日线MACD指标
            self.inds[d]['dif'] = bt.indicators.MACDHisto(
                d.close,
                period_me1=self.params.fastperiod,
                period_me2=self.params.slowperiod,
                period_signal=self.params.signalperiod).macd
            self.inds[d]['dea'] = bt.indicators.MACDHisto(
                d.close,
                period_me1=self.params.fastperiod,
                period_me2=self.params.slowperiod,
                period_signal=self.params.signalperiod).signal
            self.inds[d]['macd'] = bt.indicators.MACDHisto(
                d.close,
                period_me1=self.params.fastperiod,
                period_me2=self.params.slowperiod,
                period_signal=self.params.signalperiod).histo * 2
            # 取60日内最低价
            self.inds[d]['lowest60'] = bt.indicators.Lowest(
                d.close, period=self.params.longperiod)
            # 取60日内最高价
            self.inds[d]['highest60'] = bt.indicators.Highest(
                d.close, period=self.params.longperiod)
            # 生成交易信号
            # 看涨信号
            # 收盘价站上ma20均线和ema20均线，且ema20上穿ema60均线
            self.signals[d]['close_over_ema20'] = d.close > self.inds[d]['ema20']
            self.signals[d]['close_over_ma20'] = d.close > self.inds[d]['ma20']
            self.signals[d]['ema_signal'] = bt.And(self.signals[d]['close_over_ema20'],
                                                   self.signals[d]['close_over_ma20'],
                                                   bt.indicators.CrossUp(self.inds[d]['ema20'], self.inds[d]['ema60']) == 1)

            # dif上穿dea，且收盘价在ma20均线和ema20均线上方
            self.signals[d]['dif_signal'] = bt.And(self.signals[d]['close_over_ema20'],
                                                   self.signals[d]['close_over_ma20'],
                                                   bt.indicators.CrossUp(self.inds[d]['dif'], self.inds[d]['dea']) == 1)

            # 看跌信号
            # 收盘价跌破ma20均线
            self.signals[d]['close_crossdown_ma20'] = bt.indicators.CrossDown(
                d.close, self.inds[d]['ma20'])
            # MACD下穿0轴
            self.signals[d]['macd_crossdown_axis'] = bt.indicators.CrossDown(
                self.inds[d]['macd'], 0)

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
            # self.log('当前代码: %s, 当前持仓:, %s' % (d._name,
            #          self.getposition(d).size))
            # 有持仓就不再买入
            pos = self.getposition(d)
            if not len(pos):
                # 收盘价站上MA20均线和EMA20均线
                if self.signals[d]['ema_signal'][0] \
                        or self.signals[d]['dif_signal'][0]:
                    # 买入对应仓位
                    self.orders[d._name] = self.buy(data=d)
            else:
                # 跌破均线即卖出, macd下穿0轴即卖出
                if self.signals[d]['close_crossdown_ma20'] == 1 \
                        or self.signals[d]['macd_crossdown_axis'] == 1:
                    self.orders[d._name] = self.sell(data=d)


if __name__ == '__main__':

    cerebro = bt.Cerebro()
    # Add a strategy
    cerebro.addstrategy(StrategyOne)
    cerebro.broker.setcash(100000.0)
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    cerebro.broker.setcommission(commission=0.001)

    # Add Data
    trade_date = ToolKit('获取最新美股交易日期').get_latest_trade_date(1)
    list = UsTicker(trade_date).get_backtrader_data_feed()
    for h in list:
        data = PandasData(dataname=h, name=h['symbol'][0])
        # Add a data
        cerebro.adddata(data)
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # cerebro.plot(iplot=True, subplot=True)
