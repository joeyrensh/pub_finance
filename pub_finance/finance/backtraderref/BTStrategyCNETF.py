#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import backtrader as bt
from utility.ToolKit import ToolKit
from datetime import datetime
import pandas as pd
from utility.FileInfo import FileInfo


class BTStrategyCNETF(bt.Strategy):
    """
    自定义均线的时间间隔
    目前定义了MA/EMA/MACD三类指标的时间区间
    自定义MAVOL三个指标区间
    """

    params = (
        ("fastperiod", 10),
        ("slowperiod", 22),
        ("signalperiod", 8),
        ("shortperiod", 20),
        ("midperiod", 60),
        # ("longperiod", 120),
        ("volshortperiod", 5),
        ("volmidperiod", 10),
        ("vollongperiod", 20),
        ("annualperiod", 250),
        ("availablecash", 500000),
        ("hlshortperiod", 5),
        ("hlmidperiod", 10),
        ("hllongperiod", 20),
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
        file_path_etf_position = file.get_file_path_etf_position
        self.file_path_etf_position = open(file_path_etf_position, "w")
        file_path_etf_position_detail = file.get_file_path_etf_position_detail
        self.file_path_etf_position_detail = open(file_path_etf_position_detail, "w")
        file_path_etf_trade = file.get_file_path_etf_trade
        self.file_path_etf_trade = open(file_path_etf_trade, "w")

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
        self.myorder = dict()
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

            # 跟踪订单交易策略
            self.myorder[d._name] = dict()

            """ 
            MA短中长周期指标 
            """
            self.inds[d._name]["mashort"] = bt.indicators.SMA(
                d.close, period=self.params.shortperiod
            )
            self.inds[d._name]["mamid"] = bt.indicators.SMA(
                d.close, period=self.params.midperiod
            )
            self.inds[d._name]["maannual"] = bt.indicators.SMA(
                d.close, period=self.params.annualperiod
            )
            # self.inds[d._name]["malong"] = bt.indicators.SMA(
            #     d.close, period=self.params.longperiod
            # )

            """
            EMA短中长周期指标 
            """
            self.inds[d._name]["emashort"] = bt.indicators.EMA(
                d.close, period=self.params.shortperiod
            )
            self.inds[d._name]["emamid"] = bt.indicators.EMA(
                d.close, period=self.params.midperiod
            )
            # self.inds[d._name]["emalong"] = bt.indicators.EMA(
            #     d.close, period=self.params.longperiod
            # )

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
            self.inds[d._name]["highest_short"] = bt.indicators.Highest(
                d.close, period=self.params.hlshortperiod
            )
            self.inds[d._name]["lowest_short"] = bt.indicators.Lowest(
                d.close, period=self.params.hlshortperiod
            )

            self.inds[d._name]["highest_mid"] = bt.indicators.Highest(
                d.close, period=self.params.hlmidperiod
            )
            self.inds[d._name]["lowest_mid"] = bt.indicators.Lowest(
                d.close, period=self.params.hlmidperiod
            )

            self.inds[d._name]["highest_long"] = bt.indicators.Highest(
                d.close, period=self.params.hllongperiod
            )
            self.inds[d._name]["lowest_long"] = bt.indicators.Lowest(
                d.close, period=self.params.hllongperiod
            )

            """
            辅助指标：低点上移且高点上移
            """
            self.signals[d._name]["higher_fast"] = bt.Or(
                self.inds[d._name]["highest_short"]
                > self.inds[d._name]["highest_short"](-5),
                self.inds[d._name]["lowest_short"]
                > self.inds[d._name]["lowest_short"](-5),
            )
            self.signals[d._name]["higher"] = bt.Or(
                self.inds[d._name]["highest_mid"]
                > self.inds[d._name]["highest_mid"](-10),
                self.inds[d._name]["lowest_mid"]
                > self.inds[d._name]["lowest_mid"](-10),
            )

            self.signals[d._name]["lower"] = bt.Or(
                self.inds[d._name]["lowest_short"]
                < self.inds[d._name]["lowest_short"](-5),
                self.inds[d._name]["lowest_mid"]
                < self.inds[d._name]["lowest_mid"](-10),
            )

            """
            辅助指标：成交量放大
            """
            self.signals[d._name]["mavol_long_position"] = bt.And(
                self.inds[d._name]["mavolshort"] > self.inds[d._name]["mavolmid"],
                self.inds[d._name]["mavolshort"] > self.inds[d._name]["mavollong"],
                d.volume > self.inds[d._name]["mavolshort"],
            )

            """ 
            辅助指标：乖离率判断
            """
            bias20 = (d.close - self.inds[d._name]["mashort"]) / self.inds[d._name][
                "mashort"
            ]
            self.signals[d._name]["reasonable_angle"] = bt.And(
                bias20 >= -0.1,
                bias20 <= 0.1,
            )

            self.signals[d._name]["steep_angle"] = bias20 >= 0.3

            """ 辅助指标：RSI """
            self.inds[d._name]["RSI"] = bt.indicators.RSI(
                d.close, period=self.params.fastperiod, safediv=True
            )

            """
            近20日均线密集，价格上穿短期均线
            """
            self.signals[d._name]["close_crossup_emashort"] = (
                bt.indicators.crossover.CrossUp(d.close, self.inds[d._name]["emashort"])
            )

            """
            多头排列1, 价格在均线上方，短期均线上穿中期均线
            """
            self.signals[d._name]["emashort_crossup_emamid"] = bt.And(
                d.close >= self.inds[d._name]["emashort"],
                d.close >= self.inds[d._name]["mashort"],
                bt.indicators.crossover.CrossUp(
                    self.inds[d._name]["emashort"], self.inds[d._name]["emamid"]
                )
                == 1,
            )
            """ 
            多头排列2, 均线多头排列，价格上穿短期均线
            """
            self.signals[d._name]["close_crossup_mashort"] = bt.And(
                # bt.Or(
                #     self.inds[d._name]["emashort"] >= self.inds[d._name]["emamid"],
                #     self.inds[d._name]["mashort"] >= self.inds[d._name]["mamid"],
                # ),
                bt.indicators.crossover.CrossUp(d.close, self.inds[d._name]["emashort"])
                == 1,
            )

            """
            多头排列3，兜底策略，价在线上，均线多头 
            """
            self.signals[d._name]["long_position"] = bt.And(
                d.close >= self.inds[d._name]["emashort"],
                self.inds[d._name]["emashort"] >= self.inds[d._name]["emamid"],
                self.inds[d._name]["mashort"] >= self.inds[d._name]["mamid"],
            )

            """
            MDCD上穿，价在线上
            """
            self.signals[d._name]["dif_crossup_dea"] = bt.And(
                bt.Or(
                    d.close >= self.inds[d._name]["emashort"],
                    d.close >= self.inds[d._name]["mashort"],
                ),
                bt.indicators.crossover.CrossUp(
                    self.inds[d._name]["dif"], self.inds[d._name]["dea"]
                )
                == 1,
            )

            """
            成交量突然放大，价格上穿短期均线
            """
            self.signals[d._name]["mavol_long_position"] = bt.And(
                self.inds[d._name]["mavolshort"] > self.inds[d._name]["mavolmid"],
                self.inds[d._name]["mavolshort"] > self.inds[d._name]["mavollong"],
                bt.indicators.crossover.CrossUp(
                    d.close, self.inds[d._name]["emashort"]
                ),
            )

            """
            穿越年线
            """
            self.signals[d._name]["closs_crossup_annualline"] = (
                bt.indicators.crossover.CrossUp(d.close, self.inds[d._name]["maannual"])
            )

            """
            看跌信号
            """
            """ 收盘价跌破ma20均线 """
            self.signals[d._name]["close_crossdown_mashort"] = (
                bt.indicators.crossover.CrossDown(
                    d.close, self.inds[d._name]["mashort"]
                )
            )
            """
            close未跌破MA60
            """
            self.signals[d._name]["close_over_mamid"] = (
                d.close > self.inds[d._name]["mamid"]
            )
            """ dif下穿0轴 """
            self.signals[d._name]["dif_crossdown_0axis"] = (
                bt.indicators.crossover.CrossDown(self.inds[d._name]["dif"], 0)
            )
            self.signals[d._name]["dea_crossdown_0axis"] = (
                bt.indicators.crossover.CrossDown(self.inds[d._name]["dea"], 0)
            )
            self.signals[d._name]["dif_crossdown_dea"] = (
                bt.indicators.crossover.CrossDown(
                    self.inds[d._name]["dif"], self.inds[d._name]["dea"]
                )
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
                    "{}, Buy {} Executed, Price: {:.2f} Size: {:.2f}".format(
                        bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
                    )
                )
                self.last_deal_date[order.data._name] = bt.num2date(
                    order.executed.dt
                ).strftime("%Y-%m-%d")
                dict = {
                    "symbol": order.data._name,
                    "trade_date": bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                    "trade_type": "buy",
                    "price": order.executed.price,
                    "size": order.executed.size,
                    "strategy": self.myorder[order.data._name]["strategy"],
                }
            elif order.issell():
                """订单卖出成功"""
                print(
                    "{} Sell {} Executed, Price: {:.2f} Size: {:.2f}".format(
                        bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
                    )
                )
                self.last_deal_date[order.data._name] = None
                dict = {
                    "symbol": order.data._name,
                    "trade_date": bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                    "trade_type": "sell",
                    "price": order.executed.price,
                    "size": order.executed.size,
                    "strategy": self.myorder[order.data._name]["strategy"],
                }
            elif order.alive():
                """returns bool if order is in status Partial or Accepted"""
                print(
                    "{} Partial {} Executed, Price: {:.2f} Size: {:.2f}".format(
                        bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
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
            df.to_csv(self.file_path_etf_trade, header=False)

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
        # 策略执行进度
        t = ToolKit("策略执行中")
        # 增加cash
        if (
            len(self) < self.data.buflen() - 1
            and self.broker.cash < self.params.availablecash
        ):
            self.broker.add_cash(self.params.availablecash - self.broker.cash)
            self.log(
                "cash is not enough %s, add cash %s"
                % (self.broker.cash, self.params.availablecash - self.broker.cash)
            )

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
            if not pos:
                # 小于100，取100，大于100，取100的倍数
                # buy_size = (lambda num: 100 if num < 100 else (num // 100) * 100)(
                #     self.getsizing(d)
                # )
                """噪声处理"""
                # 低于年线的不看
                # if d.close[0] < self.inds[d._name]["maannual"][0]:
                #     continue
                # # 最近20个交易日收盘价频繁穿越ma20均线，不进行交易
                # if (
                #     self.signals[d._name]["close_crossdown_mashort"]
                #     .get(ago=-1, size=self.params.shortperiod)
                #     .count(1)
                #     >= 4
                # ):
                #     continue
                # # 最近20个交易日，dea下穿0轴2次，不进行交易
                # if (
                #     self.signals[d._name]["dif_crossdown_dea"]
                #     .get(ago=-1, size=self.params.shortperiod)
                #     .count(1)
                #     >= 5
                # ):
                #     continue

                # 均线密集判断，短期ema与中期ema近20日内密集排列
                x1 = self.inds[d._name]["emashort"].get(
                    ago=-1, size=self.params.shortperiod
                )
                y1 = self.inds[d._name]["emamid"].get(
                    ago=-1, size=self.params.shortperiod
                )
                x2 = self.inds[d._name]["mashort"].get(
                    ago=-1, size=self.params.shortperiod
                )
                y2 = self.inds[d._name]["mamid"].get(
                    ago=-1, size=self.params.shortperiod
                )
                diff_array = [abs((x - y) * 100 / y) for x, y in zip(x1, y1) if y != 0]
                diff_array2 = [abs((x - y) * 100 / y) for x, y in zip(x2, y2) if y != 0]

                # 收盘价穿越均线
                if (
                    self.signals[d._name]["close_crossup_mashort"][0] == 1
                    and self.inds[d._name]["mashort"][0]
                    > self.inds[d._name]["mashort"][-1]
                    and d.close[0] > d.open[0]
                    and self.signals[d._name]["higher_fast"][0] == 1
                    and self.signals[d._name]["reasonable_angle"][0] == 1
                    and (
                        self.inds[d._name]["dif"][0] > 0
                        or self.inds[d._name]["dif"][0] > self.inds[d._name]["dea"][0]
                    )
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "收盘价上穿短期均线"

                # 短期均线穿越中期均线
                elif (
                    self.signals[d._name]["emashort_crossup_emamid"][0] == 1
                    and self.inds[d._name]["mashort"][0]
                    > self.inds[d._name]["mashort"][-1]
                    and d.close[0] > d.open[0]
                    and d.close[0] > d.close[-self.params.shortperiod]
                    and self.signals[d._name]["higher"][0] == 1
                    and self.signals[d._name]["reasonable_angle"][0] == 1
                    and (
                        self.inds[d._name]["dif"][0] > 0
                        or self.inds[d._name]["dif"][0] > self.inds[d._name]["dea"][0]
                    )
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "短期均线上穿中期均线"

                # 均线多头排列
                elif (
                    self.signals[d._name]["long_position"][0] == 1
                    and self.inds[d._name]["mamid"][0] > self.inds[d._name]["mamid"][-1]
                    and d.close[0] > d.open[0]
                    and d.close[0] > d.close[-self.params.shortperiod]
                    and self.signals[d._name]["higher"][0] == 1
                    and self.signals[d._name]["reasonable_angle"][0] == 1
                    and (
                        self.inds[d._name]["dif"][0] > 0
                        or self.inds[d._name]["dif"][0] > self.inds[d._name]["dea"][0]
                    )
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "多头排列"

                # dea上穿0轴
                elif (
                    self.signals[d._name]["dif_crossup_dea"][0] == 1
                    and self.inds[d._name]["mashort"][0]
                    > self.inds[d._name]["mashort"][-1]
                    and d.close[0] > d.open[0]
                    and d.close[0] > d.close[-self.params.shortperiod]
                    and self.signals[d._name]["higher"][0] == 1
                    and self.signals[d._name]["reasonable_angle"][0] == 1
                    and self.inds[d._name]["RSI"][0] >= 60
                    and (
                        self.inds[d._name]["dif"][0] > 0
                        or self.inds[d._name]["dif"][0] > self.inds[d._name]["dea"][0]
                    )
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "DIF上穿DEA-金叉"

                # 成交量突然放大
                elif (
                    self.signals[d._name]["mavol_long_position"][0] == 1
                    and self.inds[d._name]["mashort"][0]
                    > self.inds[d._name]["mashort"][-1]
                    and d.close[0] > d.open[0]
                    and d.close[0] > d.close[-self.params.shortperiod]
                    and self.signals[d._name]["higher_fast"][0] == 1
                    and self.signals[d._name]["reasonable_angle"][0] == 1
                    and (
                        self.inds[d._name]["dif"][0] > 0
                        or self.inds[d._name]["dif"][0] > self.inds[d._name]["dea"][0]
                    )
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "成交量放大"

                # 均线密集
                elif (
                    (
                        self.signals[d._name]["close_crossup_emashort"][0]
                        and sum(1 for value in diff_array if value < 2) > 5
                        and sum(1 for value in diff_array2 if value < 2) > 5
                    )
                    and self.inds[d._name]["mashort"][0]
                    > self.inds[d._name]["mashort"][-1]
                    and d.close[0] > d.open[0]
                    and d.close[0] > d.close[-self.params.shortperiod]
                    and self.signals[d._name]["higher_fast"][0] == 1
                    and self.signals[d._name]["reasonable_angle"][0] == 1
                    and (
                        self.inds[d._name]["dif"][0] > 0
                        or self.inds[d._name]["dif"][0] > self.inds[d._name]["dea"][0]
                    )
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "均线密集"

                # 穿越年线
                elif (
                    self.signals[d._name]["closs_crossup_annualline"][0] == 1
                    and self.inds[d._name]["mashort"][0]
                    > self.inds[d._name]["mashort"][-1]
                    and d.close[0] > d.open[0]
                    and d.close[0] > d.close[-self.params.shortperiod]
                    and self.signals[d._name]["higher"][0] == 1
                    and self.signals[d._name]["reasonable_angle"][0] == 1
                    and (
                        self.inds[d._name]["dif"][0] > 0
                        or self.inds[d._name]["dif"][0] > self.inds[d._name]["dea"][0]
                    )
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "收盘价上穿年线"

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

                # 收盘价跌破均线支撑
                if (
                    self.signals[d._name]["close_crossdown_mashort"][0] == 1
                    and self.signals[d._name]["close_over_mamid"][0] == 0
                    and self.signals[d._name]["lower"][0] == 1
                    and (
                        self.inds[d._name]["dif"][0] < 0
                        or self.inds[d._name]["dif"][0] < self.inds[d._name]["dea"][0]
                    )
                ):
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "收盘价跌破短期均线"

                # dif下穿0轴
                elif (
                    self.signals[d._name]["dif_crossdown_dea"][0] == 1
                    and self.signals[d._name]["lower"][0] == 1
                    and self.inds[d._name]["RSI"][0] <= 40
                ):
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "DIF下穿DEA-死叉"

                # 止损点
                elif (
                    d.close[0] < d.close[-self.params.shortperiod]
                    and d.close[0] < self.inds[d._name]["emamid"][0]
                    and self.inds[d._name]["emashort"][0]
                    < self.inds[d._name]["emashort"][-1]
                    and self.signals[d._name]["lower"][0] == 1
                    and (
                        self.inds[d._name]["dif"][0] < 0
                        or self.inds[d._name]["dif"][0] < self.inds[d._name]["dea"][0]
                    )
                ):
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "收盘价跌破短期抵扣价"

                # 止盈点
                elif (
                    self.signals[d._name]["steep_angle"][0] == 1
                    and self.signals[d._name]["higher"][0] == 1
                ):
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "乖离率过大"

        df = pd.DataFrame(list)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(self.file_path_etf_position_detail, header=None)

        # 最后一日剔除多余现金
        if (
            len(self) == self.data.buflen() - 1
            and self.broker.cash > self.params.availablecash
        ):
            self.broker.add_cash(self.params.availablecash - self.broker.cash)
            self.log(
                "cash is too much %s, add cash %s"
                % (self.broker.cash, self.params.availablecash - self.broker.cash)
            )
        t.progress_bar(self.data.buflen(), len(self))

    def stop(self):
        list = []
        # 记录仓位情况，并打印明细
        pos_share = 0
        pos_loss = 0
        pos_earn = 0
        for i, d in enumerate(self.datas):
            pos = self.getposition(d)
            """ 截止当前，持仓仓位打印 """
            if pos.size > 0:
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
                    "size": pos.size,
                    "p&l": pos.size * (pos.adjbase - pos.price),
                    "p&l_ratio": (pos.adjbase - pos.price) / pos.price,
                }
                """ 
                过滤累计涨幅不满足条件的
                5天内累计上涨10个点以上
                5天以上累计上15个点以上
                """
                if dict["buy_date"] is None:
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
        """ 按照买入日期以及盈亏比倒排 """
        df.sort_values(by=["buy_date", "p&l_ratio"], ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_csv(self.file_path_etf_position)
        self.file_path_etf_position.close()
        self.file_path_etf_position_detail.close()
