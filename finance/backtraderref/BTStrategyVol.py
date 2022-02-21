#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from os import close
import backtrader as bt
from utility.ToolKit import ToolKit
from datetime import datetime
import pandas as pd
from datetime import datetime, timedelta
from utility.MyEmail import MyEmail
import seaborn as sns
from utility.FileInfo import FileInfo


class BTStrategyVol(bt.Strategy):
    """
    自定义均线的时间间隔
    目前定义了MA/EMA/MACD三类指标的时间区间
    自定义MAVOL三个指标区间
    """

    params = (
        ("fastperiod", 12),
        ("slowperiod", 26),
        ("signalperiod", 9),
        ("shortperiod", 20),
        ("longperiod", 60),
        ("volshortperiod", 10),
        ("volmidperiod", 20),
        ("vollongperiod", 60),
    )

    def log(self, txt, dt=None):
        """
        backtrader自己的log函数
        打印执行的时间以及执行日志
        """
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def __init__(self):
        """
        将每日回测完的持仓数据存储文件
        方便后面打印输出
        """
        if self.datas[0].market[0] == 1:
            trade_date = ToolKit("获取最新交易日期").get_us_latest_trade_date(0)
            file = FileInfo(trade_date, "us")
            """ 仓位文件地址 """
            file_position = file.get_file_name_position
            self.log_file = open(file_position, "w")
            """ 板块文件地址 """
            self.file_industry = file.get_file_name_industry
        elif self.datas[0].market[0] == 2:
            trade_date = ToolKit("获取最新交易日期").get_cn_latest_trade_date(0)
            file = FileInfo(trade_date, "cn")
            """ 仓位文件地址 """
            file_position = file.get_file_name_position
            self.log_file = open(file_position, "w")
            """ 板块文件地址 """
            self.file_industry = file.get_file_name_industry
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
        t = ToolKit("策略初始化")
        """
        循环遍历每一个datafeed
        每一个datafeed包括每一支股票的o/c/h/l/v相关数据
        初始化indicators，signals
        """
        for i, d in enumerate(self.datas):
            """初始化最后一笔订单的购买日期，key是symbol"""
            self.last_deal_date[d._name] = None
            """ 为每个股票初始化技术指标，key是symbol """
            self.order[d._name] = None
            self.inds[d] = dict()
            self.signals[d] = dict()
            """ 日线MA20指标 """
            self.inds[d]["mashort"] = bt.indicators.SMA(
                d.close, period=self.params.shortperiod
            )
            """ 日线MA60指标 """
            self.inds[d]["malong"] = bt.indicators.SMA(
                d.close, period=self.params.longperiod
            )
            """ 日线EMA20指标 """
            self.inds[d]["emashort"] = bt.indicators.EMA(
                d.close, period=self.params.shortperiod
            )
            """ 日线EMA60指标 """
            self.inds[d]["emalong"] = bt.indicators.EMA(
                d.close, period=self.params.longperiod
            )
            """
            MAVOL10、20、60成交量均线
            """
            self.inds[d]["mavolshort"] = bt.indicators.SMA(
                d.volume, period=self.params.volshortperiod
            )
            self.inds[d]["mavolmid"] = bt.indicators.SMA(
                d.volume, period=self.params.volmidperiod
            )
            self.inds[d]["mavollong"] = bt.indicators.SMA(
                d.volume, period=self.params.vollongperiod
            )
            """
            生成交易信号
            看涨信号
            """
            """
            信号1:
            成交量均线多头排列
            ema20上穿ema60或者ema20和ema60多头排列或者成交价上穿ema20
            """
            self.signals[d]["mavol_long_position"] = bt.And(
                self.inds[d]["mavolshort"] > self.inds[d]["mavolmid"],
                self.inds[d]["mavolmid"] > self.inds[d]["mavollong"],
            )

            self.signals[d]["close_over_ema"] = bt.And(
                d.close > self.inds[d]["emashort"],
                self.inds[d]["emashort"] > self.inds[d]["emalong"],
            )

            self.signals[d]["emashort_cross_emalong"] = bt.indicators.CrossUp(
                self.inds[d]["emashort"], self.inds[d]["emalong"]
            )

            self.signals[d]["close_cross_emashort"] = bt.indicators.CrossUp(
                d.close, self.inds[d]["emashort"]
            )

            self.signals[d]["signal1"] = bt.And(
                self.signals[d]["mavol_long_position"],
                bt.Or(
                    self.signals[d]["close_over_ema"],
                    self.signals[d]["emashort_cross_emalong"] == 1,
                    self.signals[d]["close_cross_emashort"] == 1,
                ),
            )
            """
            生成交易信号
            看跌信号
            """
            """ 收盘价跌破ma20均线 """
            self.signals[d]["close_crossdown_malong"] = bt.indicators.CrossDown(
                d.close, self.inds[d]["malong"]
            )

            """ indicators以及signals初始化进度打印 """
            t.progress_bar(len(self.datas), i)

    """ 订单状态改变回调方法 """

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            """Buy/Sell order submitted/accepted to/by broker - Nothing to do"""
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                """订单购入成功"""
                print(
                    "{}, Buy {} Executed, Price: {:.2f}".format(
                        self.datetime.date(), order.data._name, order.executed.price
                    )
                )
                self.last_deal_date[order.data._name] = self.datetime.date()
            elif order.issell():
                """订单卖出成功"""
                print(
                    "{} Sell {} Executed, Price: {:.2f}".format(
                        self.datetime.date(), order.data._name, order.executed.price
                    )
                )
                self.last_deal_date[order.data._name] = None
            elif order.alive():
                """returns bool if order is in status Partial or Accepted"""
                print(
                    "{} Partial {} Executed, Price: {:.2f}".format(
                        self.datetime.date(), order.data._name, order.executed.price
                    )
                )
                self.last_deal_date[order.data._name] = None
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            """由于仓位不足或者执行限价单等因素造成订单未成交"""
            if order.isbuy():
                self.log("Buy %s Order Canceled/Margin/Rejected" % (order.data._name))
            else:
                self.log("Sell %s Order Canceled/Margin/Rejected" % (order.data._name))
        self.order[order.data._name] = None

    """
    交易状态改变回调方法
    be notified through notify_trade(trade) of any opening/updating/closing trade
    """

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        """ 每笔交易收益 毛利和净利 """
        self.log("Operation Profit, Gross %.2f, Net %.2f" % (trade.pnl, trade.pnlcomm))

    def prenext(self):
        # call next() even when data is not available for all tickers
        self.next()

    def next(self):
        for i, d in enumerate(self.datas):
            if self.order[d._name]:
                continue
            """
            self.log('当前代码: %s, 当前持仓:, %s' %
            (d._name, self.getposition(d).size))
            """
            pos = self.getposition(d)
            """ 如果没有仓位就判断是否买卖 """
            if not len(pos):
                """成交量均线和K均线均多头"""
                if self.signals[d]["signal1"]:
                    """买入对应仓位"""
                    self.order[d._name] = self.buy(data=d, exectype=bt.Order.Market)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
            else:
                """
                均线止损/止盈信号：
                信号1: 收盘价跌破ma20
                信号2: 已经亏损20%
                """
                if (
                    self.signals[d]["close_crossdown_malong"][0] == 1
                    or (d.close[0] - pos.price) / pos.price < -0.2
                ):
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))

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
                dict = {
                    "symbol": d._name,
                    "buy_date": self.last_deal_date[d._name],
                    "price": pos.price,
                    "adjbase": pos.adjbase,
                    "p&l": pos.size * (pos.adjbase - pos.price),
                    "p&l_ratio": (pos.adjbase - pos.price) / pos.price,
                }
                """ 
                过滤累计涨幅不满足条件的
                5天内累计上涨10个点以上
                5天以上累计上15个点以上
                """
                if dict["buy_date"] == None:
                    continue
                t = ToolKit("最新交易日")
                if self.datas[0].market[0] == 1:
                    cur = datetime.strptime(t.get_us_latest_trade_date(0), "%Y%m%d")
                    bef = datetime.strptime(str(dict["buy_date"]), "%Y-%m-%d")
                    interval = t.get_us_trade_off_days(cur, bef)
                elif self.datas[0].market[0] == 2:
                    cur = datetime.strptime(t.get_cn_latest_trade_date(0), "%Y%m%d")
                    bef = datetime.strptime(str(dict["buy_date"]), "%Y-%m-%d")
                    interval = t.get_cn_trade_off_days(cur, bef)
                print(
                    "当前股票: %s, 交易天数: %s, 累计涨幅: %s"
                    % (dict["symbol"], interval, dict["p&l_ratio"])
                )
                # """ 少于10%的股票不展示 """
                # if dict["p&l_ratio"] < 0.10:
                #     continue
                list.append(dict)
        df = pd.DataFrame(list)
        if df.empty:
            return
        """ 匹配行业信息 """
        df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
        df_n = pd.merge(df, df_o, how="left", on="symbol")
        """ 按照买入日期以及盈亏比倒排 """
        df_n.sort_values(by=["buy_date", "p&l_ratio"], ascending=False, inplace=True)
        df_n.reset_index(drop=True, inplace=True)
        df_n.to_csv(self.log_file)
        self.log_file.close()
