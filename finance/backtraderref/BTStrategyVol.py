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
        ("fastperiod", 10),
        ("slowperiod", 20),
        ("signalperiod", 7),
        ("shortperiod", 20),
        ("midperiod", 60),
        ("longperiod", 120),
        ("volshortperiod", 5),
        ("volmidperiod", 10),
        ("vollongperiod", 30),
    )

    def log(self, txt, dt=None):
        """
        backtrader自己的log函数
        打印执行的时间以及执行日志
        """
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def start(self):
        # trade_date = ToolKit("获取最新交易日期").get_cn_latest_trade_date(1)
        trade_date = self.trade_date
        file = FileInfo(trade_date, "cn")
        """ 仓位文件地址 """
        file_path_position = file.get_file_path_position
        self.file_path_position = open(file_path_position, "w")
        file_path_position_detail = file.get_file_path_position_detail
        self.file_path_position_detail = open(file_path_position_detail, "w")
        file_path_trade = file.get_file_path_trade
        self.file_path_trade = open(file_path_trade, "w")
        """ 板块文件地址 """
        self.file_industry = file.get_file_path_industry

    def __init__(self, trade_date):
        """
        将每日回测完的持仓数据存储文件
        方便后面打印输出
        """
        self.trade_date = trade_date
        """ backtrader一些常用属性的初始化 """
        self.trade = None
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
            self.inds[d._name] = dict()
            self.signals[d._name] = dict()

            """ 
            MA短中长周期指标 
            """
            self.inds[d._name]["mashort"] = bt.indicators.SMA(
                d.close, period=self.params.shortperiod
            )
            self.inds[d._name]["mamid"] = bt.indicators.SMA(
                d.close, period=self.params.midperiod
            )
            self.inds[d._name]["malong"] = bt.indicators.SMA(
                d.close, period=self.params.longperiod
            )

            """
            EMA短中长周期指标 
            """
            self.inds[d._name]["emashort"] = bt.indicators.EMA(
                d.close, period=self.params.shortperiod
            )
            self.inds[d._name]["emamid"] = bt.indicators.EMA(
                d.close, period=self.params.midperiod
            )
            self.inds[d._name]["emalong"] = bt.indicators.EMA(
                d.close, period=self.params.longperiod
            )

            """
            MAVOL5、10、30成交量均线
            """
            self.inds[d._name]["mavolshort"] = bt.indicators.EMA(
                d.volume, period=self.params.volshortperiod
            )
            self.inds[d._name]["mavolmid"] = bt.indicators.EMA(
                d.volume, period=self.params.volmidperiod
            )
            self.inds[d._name]["mavollong"] = bt.indicators.EMA(
                d.volume, period=self.params.vollongperiod
            )

            """
            日线DIF值
            日线DEA值
            日线MACD值
            """
            self.inds[d._name]["dif"] = bt.indicators.MACDHisto(
                d.close,
                period_me1=self.params.fastperiod,
                period_me2=self.params.slowperiod,
                period_signal=self.params.signalperiod,
            ).macd
            self.inds[d._name]["dea"] = bt.indicators.MACDHisto(
                d.close,
                period_me1=self.params.fastperiod,
                period_me2=self.params.slowperiod,
                period_signal=self.params.signalperiod,
            ).signal
            self.inds[d._name]["macd"] = (
                bt.indicators.MACDHisto(
                    d.close,
                    period_me1=self.params.fastperiod,
                    period_me2=self.params.slowperiod,
                    period_signal=self.params.signalperiod,
                ).histo
                * 2
            )

            """ 
            近x日内最低和最高收盘价 
            """
            self.inds[d._name]["lowest_close"] = bt.indicators.Lowest(
                d.close, period=self.params.shortperiod
            )
            self.inds[d._name]["highest_close"] = bt.indicators.Highest(
                d.close, period=self.params.shortperiod
            )

            """
            辅助指标：成交量放大
            """
            self.signals[d._name]["mavol_long_position"] = bt.And(
                self.inds[d._name]["mavolshort"] > self.inds[d._name]["mavolmid"],
                self.inds[d._name]["mavolshort"] > self.inds[d._name]["mavollong"],
            )
            """ 
            辅助指标：收盘价穿越MA
            """
            self.signals[d._name]["close_crossover_mashort"] = bt.indicators.CrossOver(
                d.close, self.inds[d._name]["mashort"]
            )
            """ 
            多头排列1，均线多头，价格上穿短期均线
            """
            self.signals[d._name]["close_over_ema"] = bt.And(
                d.close > self.inds[d._name]["emashort"],
                bt.Or(
                    self.inds[d._name]["emashort"] >= self.inds[d._name]["emamid"],
                    self.inds[d._name]["mashort"] >= self.inds[d._name]["mamid"],
                ),
            )
            """
            多头排列2, 价在线上，ema短期上穿中期均线
            """
            self.signals[d._name]["emashort_cross_emamid"] = bt.And(
                bt.indicators.CrossUp(
                    self.inds[d._name]["emashort"], self.inds[d._name]["emamid"]
                ),
                d.close >= self.inds[d._name]["emashort"],
            )
            """
            多头排列3，兜底策略，价在线上，均线多头 
            """
            self.signals[d._name]["long_position"] = bt.And(
                d.close > self.inds[d._name]["emashort"],
                self.inds[d._name]["emashort"] >= self.inds[d._name]["emamid"],
                self.inds[d._name]["mashort"] >= self.inds[d._name]["mamid"],
            )

            """
            MDCD上穿，价在线上
            """
            self.signals[d._name]["dea_crossup_0axis"] = bt.And(
                bt.Or(
                    d.close >= self.inds[d._name]["emashort"],
                    d.close >= self.inds[d._name]["mashort"],
                ),
                bt.indicators.CrossUp(self.inds[d._name]["dea"], 0) == 1,
            )

            """
            成交量突然放大，价格上穿短期均线
            """
            self.signals[d._name]["mavol_long_position"] = bt.And(
                self.inds[d._name]["mavolshort"] > self.inds[d._name]["mavolmid"],
                self.inds[d._name]["mavolshort"] > self.inds[d._name]["mavollong"],
                bt.indicators.CrossUp(d.close, self.inds[d._name]["emashort"]),
            )

            """
            生成交易信号
            """
            self.signals[d._name]["signalup"] = bt.And(
                bt.Or(
                    self.signals[d._name]["close_over_ema"] == 1,
                    self.signals[d._name]["emashort_cross_emamid"] == 1,
                    self.signals[d._name]["long_position"] == 1,
                    self.signals[d._name]["dea_crossup_0axis"] == 1,
                    self.signals[d._name]["mavol_long_position"] == 1,
                ),
                self.inds[d._name]["mamid"](0) > self.inds[d._name]["mamid"](-1),
                d.close > d.open,
                bt.Or(
                    self.inds[d._name]["lowest_close"](0)
                    > self.inds[d._name]["lowest_close"](-1),
                    self.inds[d._name]["highest_close"](0)
                    > self.inds[d._name]["highest_close"](-1),
                ),
            )

            """
            看跌信号
            """
            """ 收盘价跌破ma20均线 """
            self.signals[d._name]["close_crossdown_mashort"] = bt.indicators.CrossDown(
                d.close, self.inds[d._name]["mashort"]
            )
            """
            close未跌破MA60
            """
            self.signals[d._name]["close_over_mamid"] = (
                d.close > self.inds[d._name]["mamid"]
            )
            """ dif下穿0轴 """
            self.signals[d._name]["dif_crossdown_0axis"] = bt.indicators.CrossDown(
                self.inds[d._name]["dif"], 0
            )
            self.signals[d._name]["dea_crossdown_0axis"] = bt.indicators.CrossDown(
                self.inds[d._name]["dea"], 0
            )
            self.signals[d._name]["dif_crossdown_dea"] = bt.indicators.CrossDown(
                self.inds[d._name]["dif"], self.inds[d._name]["dea"]
            )

            """ indicators以及signals初始化进度打印 """
            t.progress_bar(len(self.datas), i)

    """ 订单状态改变回调方法 """

    def notify_order(self, order):
        list = []
        dict = {}
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
                dict = {
                    "symbol": order.data._name,
                    "trade_date": self.datetime.date(),
                    "trade_type": "buy",
                    "price": order.executed.price,
                    "size": order.executed.size,
                }
            elif order.issell():
                """订单卖出成功"""
                print(
                    "{} Sell {} Executed, Price: {:.2f}".format(
                        self.datetime.date(), order.data._name, order.executed.price
                    )
                )
                self.last_deal_date[order.data._name] = None
                dict = {
                    "symbol": order.data._name,
                    "trade_date": self.datetime.date(),
                    "trade_type": "sell",
                    "price": order.executed.price,
                    "size": order.executed.size,
                }
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
        if dict:
            list.append(dict)
            df = pd.DataFrame(list)
            df.to_csv(self.file_path_trade, header=False)

    """
    交易状态改变回调方法
    be notified through notify_trade(trade) of any opening/updating/closing trade
    """

    def notify_trade(self, trade):
        self.trade = trade
        if not trade.isclosed:
            return
        """ 每笔交易收益 毛利和净利 """
        self.log("Operation Profit, Gross %.2f, Net %.2f" % (trade.pnl, trade.pnlcomm))

    def prenext(self):
        # call next() even when data is not available for all tickers
        self.next()

    def next(self):
        list = []
        for i, d in enumerate(self.datas):
            if self.order[d._name]:
                continue
            """ 头部index不计算，有bug """
            if len(d) == 0:
                continue

            """            
            self.log('当前代码: %s, 当前持仓:, %s' %
            (d._name, self.getposition(d).size))
            """
            pos = self.getposition(d)
            """ 如果没有仓位就判断是否买卖 """
            if len(pos) == 0:
                """噪声处理"""
                # 最近20个交易日收盘价频繁穿越ma20均线，不进行交易
                if (
                    self.signals[d._name]["close_crossover_mashort"]
                    .get(ago=0, size=self.params.shortperiod)
                    .count(1)
                    + self.signals[d._name]["close_crossover_mashort"]
                    .get(ago=0, size=self.params.shortperiod)
                    .count(-1)
                    > 5
                ):
                    continue
                # 最近20个交易日，dea下穿0轴2次，不进行交易
                if (
                    self.signals[d._name]["dea_crossdown_0axis"]
                    .get(ago=0, size=self.params.shortperiod)
                    .count(1)
                    > 2
                ) or (
                    self.signals[d._name]["dif_crossdown_dea"]
                    .get(ago=0, size=self.params.shortperiod)
                    .count(1)
                    > 2
                ):
                    continue
                """成交量均线和K均线均多头"""
                if self.signals[d._name]["signalup"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d, exectype=bt.Order.Market)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
            else:
                """
                均线止损/止盈信号：
                信号1: 收盘价跌破ma20
                信号2: 日dif下穿0轴
                信号3: 已经亏损20%
                """
                dt = self.datas[0].datetime.date(0)
                dict = {
                    "symbol": d._name,
                    "date": dt.isoformat(),
                    "price": pos.price,
                    "adjbase": pos.adjbase,
                    "pnl": pos.size * (pos.adjbase - pos.price),
                }
                list.append(dict)
                if (
                    (
                        self.signals[d._name]["close_crossdown_mashort"][0] == 1
                        and self.signals[d._name]["close_over_mamid"][0] == 0
                    )
                    or (
                        self.signals[d._name]["dea_crossdown_0axis"][0] == 1
                        and self.inds[d._name]["dif"] <= 0
                    )
                    or (d.close[0] - pos.price) / pos.price < -0.2
                ):
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
        df = pd.DataFrame(list)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(self.file_path_position_detail, header=None)

    def stop(self):
        list = []
        # 记录仓位情况，并打印明细
        pos_share = 0
        pos_loss = 0
        pos_earn = 0
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
                pos_share = pos_share + pos.size * pos.adjbase
                if pos.adjbase - pos.price >= 0:
                    pos_earn = pos_earn + pos.size * (pos.adjbase - pos.price)
                else:
                    pos_loss = pos_loss + pos.size * (pos.adjbase - pos.price)
                list.append(dict)
        print("总持仓：%s, 浮盈：%s, 浮亏：%s" % (pos_share, pos_earn, pos_loss))
        df = pd.DataFrame(list)
        if df.empty:
            return
        """ 匹配行业信息 """
        df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
        df_n = pd.merge(df, df_o, how="left", on="symbol")
        """ 按照买入日期以及盈亏比倒排 """
        df_n.sort_values(
            by=["industry", "buy_date", "p&l_ratio"], ascending=False, inplace=True
        )
        df_n.reset_index(drop=True, inplace=True)
        df_n.to_csv(self.file_path_position)
        self.file_path_position.close()
        self.file_path_position_detail.close()
