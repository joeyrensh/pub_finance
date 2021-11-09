#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from os import close
import backtrader as bt
from ToolKit import ToolKit
from datetime import datetime
import pandas as pd
from datetime import datetime, timedelta
from MyEmail import MyEmail
import seaborn as sns


class BTUsStrategy(bt.Strategy):
    """ 
    自定义均线的时间间隔
    目前定义了MA/EMA/MACD三类指标的时间区间
    """
    params = (
        ('fastperiod', 12),
        ('slowperiod', 26),
        ('signalperiod', 9),
        ('shortperiod', 20),
        ('longperiod', 60)
    )

    def log(self, txt, dt=None):
        """
        backtrader自己的log函数
        打印执行的时间以及执行日志
        """
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        """
        将每日回测完的持仓数据存储文件
        方便后面打印输出
        """
        self.log_file = open('position_log.txt', 'w')
        """ backtrader一些常用属性的初始化 """
        self.order = dict()
        self.buyprice = None
        self.buycomm = None
        """ 
        技术指标的初始化，每个指标的key值是每个datafeed
        买入以及卖出信号的初始化，每个指标的key值是每个datafeed
        存储每一笔股票最后一笔订单的购买日期，方便追踪股票后期走势
        """
        self.inds = dict()
        self.signals = dict()
        self.last_deal_date = dict()
        """ 策略进度方法初始化 """
        t = ToolKit('策略初始化')
        """
        循环遍历每一个datafeed
        每一个datafeed包括每一支股票的o/c/h/l/v相关数据
        初始化indicators，signals
        """
        for i, d in enumerate(self.datas):
            """ 初始化最后一笔订单的购买日期，key是symbol """
            self.last_deal_date[d._name] = None
            """ 为每个股票初始化技术指标，key是symbol """
            self.order[d._name] = None
            self.inds[d] = dict()
            self.signals[d] = dict()
            """ 日线MA20指标 """
            self.inds[d]['ma20'] = bt.indicators.SMA(
                d.close, period=self.params.shortperiod)
            """ 日线MA60指标 """
            self.inds[d]['ma60'] = bt.indicators.SMA(
                d.close, period=self.params.longperiod)
            """ 日线EMA20指标 """
            self.inds[d]['ema20'] = bt.indicators.EMA(
                d.close, period=self.params.shortperiod)
            """ 日线EMA60指标 """
            self.inds[d]['ema60'] = bt.indicators.EMA(
                d.close, period=self.params.longperiod)
            """ 
            日线DIF值
            日线DEA值
            日线MACD值
            """
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
            """ 取60日内最低价 """
            self.inds[d]['lowest60'] = bt.indicators.Lowest(
                d.close, period=self.params.longperiod)
            """ 取60日内最高价 """
            self.inds[d]['highest60'] = bt.indicators.Highest(
                d.close, period=self.params.longperiod)
            """ 
            生成交易信号
            看涨信号 
            """
            """         
            双均线看涨信号：
            收盘价在ma20均线和ema20均线之上
            ema20上穿ema60均线
            """
            self.signals[d]['close_over_ema20'] = d.close > self.inds[d]['ema20']
            self.signals[d]['close_over_ma20'] = d.close > self.inds[d]['ma20']
            self.signals[d]['ema_signal'] = bt.And(self.signals[d]['close_over_ema20'],
                                                   self.signals[d]['close_over_ma20'],
                                                   bt.indicators.CrossUp(self.inds[d]['ema20'], self.inds[d]['ema60']) == 1)

            """ 
            dif上穿dea看涨信号：
            收盘价在ma20均线和ema20均线上方
            dif上穿dea 
            """
            self.signals[d]['dif_signal'] = bt.And(self.signals[d]['close_over_ema20'],
                                                   self.signals[d]['close_over_ma20'],
                                                   bt.indicators.CrossUp(self.inds[d]['dif'], self.inds[d]['dea']) == 1)

            """
            收盘价看涨信号：
            近期收盘价跌破了ma20均线，再次上穿ma20信号：
            ma20均线在ma60均线上方
            ema20在ema60上方
            收盘价上穿ma20
            """
            self.signals[d]['ema20_over_ema60'] = self.inds[d]['ema20'] > self.inds[d]['ema60']
            self.signals[d]['ma20_over_ma60'] = self.inds[d]['ma20'] > self.inds[d]['ma60']
            self.signals[d]['close_crossup_ma20_signal'] = bt.And(self.signals[d]['ema20_over_ema60'],
                                                                  self.signals[d]['ma20_over_ma60'],
                                                                  bt.indicators.CrossUp(d.close, self.inds[d]['ma20']) == 1)

            """
            看涨信号滞后一天
            满足看涨信号后，次日收盘价依然上涨，即买 
            """
            self.signals[d]['close_over_preclose'] = d.close(0) > d.close(-1)

            """
            生成交易信号
            看跌信号
            """
            """ 收盘价跌破ma20均线 """
            self.signals[d]['close_crossdown_ma20'] = bt.indicators.CrossDown(
                d.close, self.inds[d]['ma20'])
            """ dif下穿0轴 """
            self.signals[d]['dif_crossdown_axis'] = bt.indicators.CrossDown(
                self.inds[d]['dif'], 0)

            """ indicators以及signals初始化进度打印 """
            t.progress_bar(len(self.datas), i)

    """ 订单状态改变回调方法 """

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            """ Buy/Sell order submitted/accepted to/by broker - Nothing to do """
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                """ 订单购入成功 """
                print('{} BUY {} EXECUTED, Price: {:.2f}'.format(
                    self.datetime.date(), order.data._name, order.executed.price))
                self.last_deal_date[order.data._name] = self.datetime.date()
            else:
                """ 订单卖出成功 """
                print('{} SELL {} EXECUTED, Price: {:.2f}'.format(
                    self.datetime.date(), order.data._name, order.executed.price))
                self.last_deal_date[order.data._name] = None
            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            """ 由于仓位不足或者执行限价单等因素造成订单未成交 """
            self.log('%s Order Canceled/Margin/Rejected' % (order.data._name))
        self.order[order.data._name] = None

    """ 
    交易状态改变回调方法 
    be notified through notify_trade(trade) of any opening/updating/closing trade 
    """

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        """ 每笔交易收益 毛利和净利 """
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        for i, d in enumerate(self.datas):

            """
            self.log('当前代码: %s, 当前持仓:, %s' % 
            (d._name, self.getposition(d).size)) 
            """

            """         
            self.log('symbol: %s, ema_signal: %f, dif_signal: %f, close_cross_ma20: %f,'
                     'ma20: %f, ma60: %f, ema20: %f,ema60: %f,close: %f,chg_ratio: %f,'
                     'close_crossdown_ma20: %f macd_crossdown_axis: %f'
                     % (d._name, self.signals[d]['ema_signal'][0], self.signals[d]['dif_signal'][0],
                        self.signals[d]['close_crossup_ma20_signal'][0], self.inds[d]['ma20'][0],
                        self.inds[d]['ma60'][0], self.inds[d]['ema20'][0],
                        self.inds[d]['ema60'][0], d.close[0], self.signals[d]['chg_ratio_signal'][0],
                        self.signals[d]['close_crossdown_ma20'][0], self.signals[d]['dif_crossdown_axis'][0]
                        ))
            """
            pos = self.getposition(d)
            """ 如果没有仓位就判断是否买卖 """
            if not len(pos):
                """ 
                双均线交易信号:
                信号1: close > ema20 and close > ma20，ema20上穿ema60
                信号2: close > ema20 and close > ma20，dif上穿dea
                信号3: ema20/60 以及ma20/60呈多头排列，收盘价上穿ma20
                同时: 所有信号满足情况下，次日收盘价持续上涨 
                """
                if (self.signals[d]['ema_signal'][-1]
                        or self.signals[d]['dif_signal'][-1]
                        or self.signals[d]['close_crossup_ma20_signal'][-1])\
                        and self.signals[d]['close_over_preclose'][0]:
                    """ 买入对应仓位 """
                    self.order[d._name] = self.buy(data=d)
            else:
                """ 
                均线止损/止盈信号：
                信号1: 收盘价跌破ma20
                信号2: 日dif下穿0轴
                信号3: 已经亏损10% 
                """
                if self.signals[d]['close_crossdown_ma20'][0] == 1 \
                        or self.signals[d]['dif_crossdown_axis'][0] == 1\
                        or (d.close[0] - pos.price) / pos.price < -0.1:
                    self.order[d._name] = self.sell(data=d)

    def stop(self):
        list = []
        for i, d in enumerate(self.datas):
            pos = self.getposition(d)
            """ 截止当前，持仓仓位打印 """
            if len(pos) and pos.size > 0:
                """ 
                股票代码
                最后买入日期
                买入价
                当前价
                盈亏额
                盈亏比 
                """
                dict = {'symbol': d._name,
                        'buy_date': self.last_deal_date[d._name],
                        'price': pos.price,
                        'adjbase': pos.adjbase,
                        'p&l': pos.size * (pos.adjbase - pos.price),
                        'p&l_ratio': (pos.adjbase - pos.price) * 100 / pos.price
                        }
                """ 
                5天以上累计涨幅超过10个点
                5天以内累计涨幅超过5个点
                发送对应仓位到邮箱 
                """
                if dict['buy_date'] == None:
                    continue
                """ 取交易天数，过滤滞涨的股票 """
                lst_trade_date_str = ToolKit(
                    '获取最近交易日').get_latest_trade_date(0)
                lst_trade_date = datetime.strptime(
                    lst_trade_date_str, '%Y%m%d')
                lst_buy_date = datetime.strptime(
                    str(dict['buy_date']), '%Y-%m-%d')
                intervals = ToolKit('获取交易天数').get_trade_off_days(
                    lst_trade_date, lst_buy_date)
                print('当前交易天数：', intervals)
                if intervals > 5 and dict['p&l_ratio'] > 10:
                    pass
                elif intervals < 5 and dict['p&l_ratio'] > 5:
                    pass
                else:
                    continue
                list.append(dict)
        df = pd.DataFrame(list)
        if df.empty:
            return
        """ 匹配行业信息 """
        df_o = pd.read_csv('./usstockinfo/usindustry_em.csv',
                           usecols=[i for i in range(1, 3)])
        df_n = pd.merge(df, df_o, how='left', on='symbol')
        """ 按照买入日期以及盈亏比倒排 """
        df_n.sort_values(by=['buy_date', 'p&l_ratio'],
                         ascending=False, inplace=True)
        df_n.reset_index(drop=True, inplace=True)
        df_n.to_csv('./position_log.txt')
        """ 发送邮件 """
        if not df_n.empty:
            cm = sns.color_palette("Blues", as_cmap=True)
            html = (
                df_n.style.hide_index()
                .format({"price": "{:.2f}",
                         "adjbase": "{:.2f}",
                         "p&l": "{:.2f}",
                         "p&l_ratio": "{:.2f}%"})
                .background_gradient(subset=['price', 'adjbase', 'p&l'], cmap=cm)
                .bar(subset=['p&l_ratio'], align='mid', color=['#5fba7d', '#d65f5f'])
                # .set_table_styles([{
                #     "selector": "thead",
                #     "props": "background-color:purple;color:white;"
                # }])
                # .set_properties(subset=['price', 'adjbase', 'ma20', 'p&l', 'p&l_ratio'], **{'width': '15px'})
                # .set_properties(**{'max-width': '15px'})
                .render()
            )
            subject = 'BT策略全美模拟盘'
            MyEmail(subject, html).send_email()
        """ 按照行业板块聚合，统计最近成交率最高的行业 """
        df_s = df_n.groupby(by='industry').size().reset_index(name='count')
        df_s.sort_values(by=['count'],
                         ascending=False, inplace=True)
        df_s.reset_index(drop=True, inplace=True)
        """ 发送邮件 """
        if not df_s.empty:
            cm = sns.color_palette("Blues", as_cmap=True)
            html = (
                df_s.style.hide_index()
                .format({"count": "{:.0f}"})
                .background_gradient(subset=['count'], cmap=cm)
                .render()
            )
            subject = 'BT策略全美模拟盘统计'
            MyEmail(subject, html).send_email()
