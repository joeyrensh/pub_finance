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


class BTStrategy(bt.Strategy):
    """
    自定义均线的时间间隔
    目前定义了MA/EMA/MACD三类指标的时间区间
    """

    params = (
        ("fastperiod", 12),
        ("slowperiod", 26),
        ("signalperiod", 9),
        ("shortperiod", 20),
        ("longperiod", 60),
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
            self.inds[d]["ma20"] = bt.indicators.SMA(
                d.close, period=self.params.shortperiod
            )
            """ 日线MA60指标 """
            self.inds[d]["ma60"] = bt.indicators.SMA(
                d.close, period=self.params.longperiod
            )
            """ 日线EMA20指标 """
            self.inds[d]["ema20"] = bt.indicators.EMA(
                d.close, period=self.params.shortperiod
            )
            """ 日线EMA60指标 """
            self.inds[d]["ema60"] = bt.indicators.EMA(
                d.close, period=self.params.longperiod
            )
            """
            日线DIF值
            日线DEA值
            日线MACD值
            """
            self.inds[d]["dif"] = bt.indicators.MACDHisto(
                d.close,
                period_me1=self.params.fastperiod,
                period_me2=self.params.slowperiod,
                period_signal=self.params.signalperiod,
            ).macd
            self.inds[d]["dea"] = bt.indicators.MACDHisto(
                d.close,
                period_me1=self.params.fastperiod,
                period_me2=self.params.slowperiod,
                period_signal=self.params.signalperiod,
            ).signal
            self.inds[d]["macd"] = (
                bt.indicators.MACDHisto(
                    d.close,
                    period_me1=self.params.fastperiod,
                    period_me2=self.params.slowperiod,
                    period_signal=self.params.signalperiod,
                ).histo
                * 2
            )
            """ 取60日内最低价 """
            self.inds[d]["lowest60"] = bt.indicators.Lowest(
                d.close, period=self.params.longperiod
            )
            """ 取60日内最高价 """
            self.inds[d]["highest60"] = bt.indicators.Highest(
                d.close, period=self.params.longperiod
            )
            """ 取5日平均成交量 """
            self.inds[d]["mean_volume5"] = bt.indicators.Average(d.volume, period=5)
            """ 20均线乖离率 """
            self.inds[d]["bias_ma20"] = abs(
                (self.inds[d]["ema20"] - self.inds[d]["ma20"]) / self.inds[d]["ma20"]
            )
            """
            生成交易信号
            看涨信号
            """
            """
            双均线看涨信号：
            信号1:
            收盘价在ma20均线和ema20均线之上
            ema20上穿ema60均线
            """
            self.signals[d]["close_over_ema20"] = d.close > self.inds[d]["ema20"]
            self.signals[d]["close_over_ma20"] = d.close > self.inds[d]["ma20"]
            self.signals[d]["ema20_crossup_ema60"] = bt.And(
                self.signals[d]["close_over_ema20"],
                self.signals[d]["close_over_ma20"],
                bt.indicators.CrossUp(self.inds[d]["ema20"], self.inds[d]["ema60"])
                == 1,
            )

            """
            信号2:
            dif上穿dea看涨信号：
            收盘价在ma20均线和ema20均线上方
            dif上穿dea
            """
            self.signals[d]["high_over_ema20"] = d.high > self.inds[d]["ema20"]
            self.signals[d]["high_over_ma20"] = d.high > self.inds[d]["ma20"]
            self.signals[d]["dif_crossup_dea"] = bt.And(
                bt.Or(
                    self.signals[d]["close_over_ema20"],
                    self.signals[d]["high_over_ema20"],
                ),
                bt.Or(
                    self.signals[d]["close_over_ma20"],
                    self.signals[d]["high_over_ma20"],
                ),
                bt.indicators.CrossUp(self.inds[d]["dif"], self.inds[d]["dea"]) == 1,
            )

            """
            信号3：
            收盘价看涨信号：
            近期收盘价跌破了ma20均线，再次上穿ma20信号：
            ma20均线在ma60均线上方
            ema20在ema60上方
            收盘价上穿ma20
            """
            self.signals[d]["ema20_over_ema60"] = (
                self.inds[d]["ema20"] > self.inds[d]["ema60"]
            )
            self.signals[d]["ma20_over_ma60"] = (
                self.inds[d]["ma20"] > self.inds[d]["ma60"]
            )
            self.signals[d]["close_crossup_ma20"] = bt.And(
                self.signals[d]["ema20_over_ema60"],
                self.signals[d]["ma20_over_ma60"],
                bt.indicators.CrossUp(d.close, self.inds[d]["ma20"]) == 1,
            )

            """
            信号4:
            近5日均线密集，成交量突然放大，收盘价高于昨日
            """
            self.signals[d]["bias_ma20"] = bt.And(
                self.inds[d]["bias_ma20"](0) < 0.1,
                self.inds[d]["bias_ma20"](-1) < 0.1,
                self.inds[d]["bias_ma20"](-2) < 0.1,
                self.inds[d]["bias_ma20"](-3) < 0.1,
                self.inds[d]["bias_ma20"](-4) < 0.1,
            )
            self.signals[d]["volume_up2times"] = (
                d.volume > self.inds[d]["mean_volume5"] * 3
            )
            self.signals[d]["close_up"] = d.close(0) > d.close(-1) * 1.1
            self.signals[d]["volume_break_thr"] = bt.And(
                self.signals[d]["bias_ma20"],
                self.signals[d]["volume_up2times"],
                self.signals[d]["close_up"],
            )

            """
            看涨信号滞后一天
            满足看涨信号后，次日收盘价依然上涨，即买
            """
            self.signals[d]["close_over_preclose"] = d.close(0) > d.close(-1)

            """
            最近5个交易日，收盘价频繁穿越ma20均线
            震荡过大的股票进行过滤
            成交量超过过去5天成交量均值
            """
            self.signals[d]["close_crossover_ma20"] = bt.indicators.CrossOver(
                d.close, self.inds[d]["ma20"]
            )
            self.signals[d]["volume_over5"] = d.volume > self.inds[d]["mean_volume5"]
            """
            生成交易信号
            看跌信号
            """
            """ 收盘价跌破ma20均线 """
            self.signals[d]["close_crossdown_ma20"] = bt.indicators.CrossDown(
                d.close, self.inds[d]["ma20"]
            )
            """ dif下穿0轴 """
            self.signals[d]["dif_crossdown_axis"] = bt.indicators.CrossDown(
                self.inds[d]["dif"], 0
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
                """最近5个交易日收盘价频繁穿越ma20均线，不进行交易"""
                if (
                    self.signals[d]["close_crossover_ma20"][0] in [1, -1]
                    and self.signals[d]["close_crossover_ma20"][-1] in [1, -1]
                    and self.signals[d]["close_crossover_ma20"][-2] in [1, -1]
                    and self.signals[d]["close_crossover_ma20"][-3] in [1, -1]
                    and self.signals[d]["close_crossover_ma20"][-4] in [1, -1]
                ):
                    continue
                """
                双均线交易信号:
                信号1: close > ema20 and close > ma20，ema20上穿ema60
                信号2: close > ema20 and close > ma20，dif上穿dea
                信号3: ema20/60 以及ma20/60呈多头排列，收盘价上穿ma20
                同时: 所有信号满足情况下，成交量放大
                """
                if (
                    self.signals[d]["ema20_crossup_ema60"][0]
                    or self.signals[d]["dif_crossup_dea"][0]
                    or self.signals[d]["close_crossup_ma20"][0]
                    or self.signals[d]["volume_break_thr"][0]
                ):
                    # and self.signals[d]['volume_over5'][0]:
                    """买入对应仓位"""
                    self.order[d._name] = self.buy(data=d, exectype=bt.Order.Market)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
            else:
                """
                均线止损/止盈信号：
                信号1: 收盘价跌破ma20
                信号2: 日dif下穿0轴
                信号3: 已经亏损20%
                """
                if (
                    self.signals[d]["close_crossdown_ma20"][0] == 1
                    or self.signals[d]["dif_crossdown_axis"][0] == 1
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
                # """ 少于10%收益的股票不展示 """
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
