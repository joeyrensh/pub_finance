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
from MyEmail import MyEmail
import seaborn as sns


class BTUsStrategy(bt.Strategy):
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
        self.log_file = open('position_log.txt', 'w')  # 用于输出仓位信息
        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        # 存储不同的技术指标
        self.inds = dict()
        self.signals = dict()
        self.last_deal_date = dict()
        t = ToolKit('策略初始化')
        for i, d in enumerate(self.datas):
            t.progress_bar(len(self.datas), i)
            self.last_deal_date[d._name] = None
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

            # ma20均线在ma60均线上方，ema20在ema60上方，且收盘价上穿ema20
            self.signals[d]['ema20_over_ema60'] = self.inds[d]['ema20'] > self.inds[d]['ema60']
            self.signals[d]['ma20_over_ma60'] = self.inds[d]['ma20'] > self.inds[d]['ma60']
            self.signals[d]['close_crossup_ema20_signal'] = bt.And(self.signals[d]['ema20_over_ema60'],
                                                                   self.signals[d]['ma20_over_ma60'],
                                                                   bt.indicators.CrossUp(d.close, self.inds[d]['ema20']) == 1)

            # 上涨力度
            self.signals[d]['chg_ratio_signal'] = (
                d.close(0) - d.close(-1)) / d.close(-1)

            # 看跌信号
            # 收盘价跌破ma20均线
            self.signals[d]['close_crossdown_ma20'] = bt.indicators.CrossDown(
                d.close, self.inds[d]['ma20'])
            # MACD下穿0轴
            self.signals[d]['macd_crossdown_axis'] = bt.indicators.CrossDown(
                self.inds[d]['macd'], 0)

    # 订单状态改变回调方法 be notified through notify_order(order) of any status change in an order
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                print('{} BUY {} EXECUTED, Price: {:.2f}'.format(
                    self.datetime.date(), order.data._name, order.executed.price))
                self.last_deal_date[order.data._name] = self.datetime.date()
            else:  # Sell
                print('{} SELL {} EXECUTED, Price: {:.2f}'.format(
                    self.datetime.date(), order.data._name, order.executed.price))
                self.last_deal_date[order.data._name] = None
            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

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
            # self.log('ema_signal: %f, dif_signal: %f, close_cross_ema20: %f, ma20: %f, ma60: %f, chg_ratio: %f, close_crossdown_ma20: %f macd_crossdown_axis: %f'
            #             % (self.signals[d]['ema_signal'][0], self.signals[d]['dif_signal'][0],
            #             self.signals[d]['close_crossup_ema20_signal'][0], self.inds[d]['ma20'][0], self.inds[d]['ma60'][0],
            #             self.signals[d]['chg_ratio_signal'][0],
            #             self.signals[d]['close_crossdown_ma20'][0], self.signals[d]['macd_crossdown_axis'][0]
            #             ))
            pos = self.getposition(d)
            if not len(pos):
                # 收盘价站上MA20均线和EMA20均线
                # dif上穿dea
                # 收盘价上穿MA20
                # 上涨力度超过5%
                if (self.signals[d]['ema_signal'][0]
                    or self.signals[d]['dif_signal'][0]
                    or self.signals[d]['close_crossup_ema20_signal'][0]) \
                        and self.signals[d]['chg_ratio_signal'][0] > 0.05:
                    # 买入对应仓位
                    self.order = self.buy(data=d, exectype=bt.Order.Close)
            else:
                # 跌破均线即卖出, macd下穿0轴即卖出
                if self.signals[d]['close_crossdown_ma20'][0] == 1 \
                        or self.signals[d]['macd_crossdown_axis'][0] == 1:
                    self.order = self.close(data=d, exectype=bt.Order.Close)

    def stop(self):
        # 打印持仓
        list = []
        for i, d in enumerate(self.datas):
            pos = self.getposition(d)
            if len(pos) and pos.size > 0:
                # print('{}, 持仓:{}, 成本价:{}, 当前价:{}, 盈亏:{:.2f}'.format(
                #     d._name, pos.size, pos.price, pos.adjbase, pos.size * (pos.adjbase - pos.price)),
                #     file=self.log_file)
                dict = {'symbol': d._name,
                        'pos_size': pos.size,
                        'buy_date': self.last_deal_date[d._name],
                        'price': pos.price,
                        'adjbase': pos.adjbase,
                        'p&l': pos.size * (pos.adjbase - pos.price),
                        'p&l_ratio': (pos.adjbase - pos.price) / pos.price
                        }
                list.append(dict)
        df = pd.DataFrame(list)
        df.sort_values(by=['buy_date', 'p&l_ratio'],
                       ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_csv('./position_log.txt')
        # 发送邮件
        if not df.empty:
            cm = sns.color_palette("Blues", as_cmap=True)
            html = (
                df.style.hide_index()
                .format({"price": "{:.2f}",
                         "adjbase": "{:.2f}",
                         "p&l": "{:.2f}",
                         "p&l_ratio": "{:.2f}%"})
                .background_gradient(subset=['price', 'adjbase', 'p&l'], cmap=cm)
                .bar(subset=['p&l_ratio'], align='mid', color=['#5fba7d', '#d65f5f'])
                .set_table_styles([{
                    "selector": "thead",
                    "props": "background-color:purple;color:white;"
                }])
                .set_properties(subset=['price', 'adjbase', 'p&l', 'p&l_ratio'], **{'width': '15px'})
                .render()
            )
            subject = 'BT策略全美模拟盘'
            MyEmail(subject, html).send_email()
