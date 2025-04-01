#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import backtrader as bt
from utility.toolkit import ToolKit
from datetime import datetime
import pandas as pd
from utility.fileinfo import FileInfo


class GlobalStrategy(bt.Strategy):
    """
    自定义均线的时间间隔
    目前定义了MA/EMA/MACD三类指标的时间区间
    """

    params = (
        ("fastperiod", 10),
        ("slowperiod", 20),
        ("signalperiod", 8),
        ("shortperiod", 20),
        ("midperiod", 60),
        # ("longperiod", 120),
        ("volshortperiod", 5),
        ("volmidperiod", 10),
        ("vollongperiod", 20),
        ("annualperiod", 240),
        ("availablecash", 4000000),
        ("restcash", 1000000),
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
        trade_date = self.trade_date
        market = self.market
        if self.market in ("cn", "us"):
            file = FileInfo(trade_date, market)
            """ 仓位文件地址 """
            file_path_position = file.get_file_path_position
            self.file_path_position = open(file_path_position, "w")
            file_path_position_detail = file.get_file_path_position_detail
            self.file_path_position_detail = open(file_path_position_detail, "w")
            file_path_trade = file.get_file_path_trade
            self.file_path_trade = open(file_path_trade, "w")
            """ 板块文件地址 """
            self.file_industry = file.get_file_path_industry
        elif self.market == "cnetf":
            file = FileInfo(trade_date, "cn")
            """ 仓位文件地址 """
            file_path_position = file.get_file_path_etf_position
            self.file_path_position = open(file_path_position, "w")
            file_path_position_detail = file.get_file_path_etf_position_detail
            self.file_path_position_detail = open(file_path_position_detail, "w")
            file_path_trade = file.get_file_path_etf_trade
            self.file_path_trade = open(file_path_trade, "w")

    def __init__(self, trade_date, market):
        """
        将每日回测完的持仓数据存储文件
        方便后面打印输出
        """

        """ backtrader一些常用属性的初始化 """
        self.trade_date = trade_date
        self.market = market
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
            """为每个股票初始化技术指标，key是symbol """
            self.order[d._name] = None
            self.inds[d._name] = dict()
            self.signals[d._name] = dict()

            # 跟踪订单交易策略
            self.myorder[d._name] = dict()

            """MA20/60/120指标 """
            self.inds[d._name]["short_term_ma"] = bt.indicators.SMA(
                d.close, period=self.params.shortperiod
            )
            self.inds[d._name]["mid_term_ma"] = bt.indicators.SMA(
                d.close, period=self.params.midperiod
            )
            self.inds[d._name]["annual_ma"] = bt.indicators.SMA(
                d.close, period=self.params.annualperiod
            )
            # self.inds[d._name]["long_term_ma"] = bt.indicators.SMA(
            #     d.close, period=self.params.longperiod
            # )

            """EMA20/60/120"""
            self.inds[d._name]["short_term_ema"] = bt.indicators.EMA(
                d.close, period=self.params.shortperiod
            )
            self.inds[d._name]["mid_term_ema"] = bt.indicators.EMA(
                d.close, period=self.params.midperiod
            )
            # self.inds[d._name]["long_term_ema"] = bt.indicators.EMA(
            #     d.close, period=self.params.longperiod
            # )
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
            MAVOL5、10、20成交量均线
            """
            self.inds[d._name]["short_term_mavol"] = bt.indicators.EMA(
                d.volume, period=self.params.volshortperiod
            )
            self.inds[d._name]["mid_term_mavol"] = bt.indicators.EMA(
                d.volume, period=self.params.volmidperiod
            )
            self.inds[d._name]["long_term_mavol"] = bt.indicators.EMA(
                d.volume, period=self.params.vollongperiod
            )

            """ 收盘价高点/低点 """
            self.inds[d._name]["short_term_highest"] = bt.indicators.Highest(
                d.close, period=self.params.hlshortperiod
            )
            self.inds[d._name]["mid_term_highest"] = bt.indicators.Highest(
                d.close, period=self.params.hlmidperiod
            )
            self.inds[d._name]["long_term_highest"] = bt.indicators.Highest(
                d.close, period=self.params.hllongperiod
            )
            self.inds[d._name]["short_term_lowest"] = bt.indicators.Lowest(
                d.close, period=self.params.hlshortperiod
            )
            self.inds[d._name]["mid_term_lowest"] = bt.indicators.Lowest(
                d.close, period=self.params.hlmidperiod
            )
            self.inds[d._name]["long_term_lowest"] = bt.indicators.Lowest(
                d.close, period=self.params.hllongperiod
            )

            """
            辅助指标：红三兵
            """
            self.signals[d._name]["red3_soldiers"] = bt.And(
                d.close > d.close(-1),
                d.close(-1) > d.close(-2),
                d.close > d.open,
                d.close(-1) > d.open(-1),
                d.close(-2) > d.open(-2),
                d.close > ((d.high - d.low) * 0.618 + d.low),
                d.close(-1) > ((d.high(-1) - d.low(-1)) * 0.618 + d.low(-1)),
                d.close(-2) > ((d.high(-2) - d.low(-2)) * 0.618 + d.low(-2)),
                d.volume > d.volume(-1),
                d.volume(-1) > d.volume(-2),
                d.volume > self.inds[d._name]["short_term_mavol"],
            )

            """
            辅助指标：上影线判定
            """
            self.signals[d._name]["upper_shadow"] = bt.And(
                d.close > ((d.high - d.low) * 0.618 + d.low), d.close > d.open
            )

            """
            辅助指标：高低点上移/下移
            """

            self.signals[d._name]["higher"] = bt.Or(
                bt.And(
                    self.inds[d._name]["short_term_lowest"]
                    > self.inds[d._name]["short_term_lowest"](-5),
                    self.inds[d._name]["short_term_lowest"]
                    >= self.inds[d._name]["mid_term_lowest"],
                    self.signals[d._name]["upper_shadow"] == 1,
                ),
                bt.And(
                    self.inds[d._name]["short_term_highest"]
                    > self.inds[d._name]["short_term_highest"](-5),
                    self.inds[d._name]["short_term_highest"]
                    >= self.inds[d._name]["mid_term_highest"],
                    self.signals[d._name]["upper_shadow"] == 1,
                ),
            )

            self.signals[d._name]["lower"] = bt.And(
                self.inds[d._name]["short_term_lowest"]
                < self.inds[d._name]["short_term_lowest"](-5),
                self.inds[d._name]["short_term_highest"]
                < self.inds[d._name]["short_term_highest"](-5),
            )

            """ 
            辅助指标：波动过大，避免频繁交易
            """
            self.signals[d._name]["close_crossdown_short_term_ma"] = (
                bt.indicators.crossover.CrossDown(
                    d.close, self.inds[d._name]["short_term_ma"]
                )
            )

            self.signals[d._name]["dif_crossdown_dea"] = (
                bt.indicators.crossover.CrossDown(
                    self.inds[d._name]["dif"], self.inds[d._name]["dea"]
                )
            )

            """
            辅助指标：收盘价上穿短期均线
            """
            self.signals[d._name]["close_crossup_short_term_ema"] = (
                bt.indicators.crossover.CrossUp(
                    d.close, self.inds[d._name]["short_term_ema"]
                )
            )

            """ 
            辅助指标：乖离率判断
            """
            self.signals[d._name]["deviant"] = (
                d.close - self.inds[d._name]["short_term_ma"]
            ) / self.inds[d._name]["short_term_ma"] <= 0.12

            """
            辅助指标：Dif和Dea的关系
            """
            self.signals[d._name]["golden_cross"] = bt.Or(
                bt.And(
                    bt.indicators.crossover.CrossUp(
                        self.inds[d._name]["dif"], self.inds[d._name]["dea"]
                    )
                    == 1,
                    self.inds[d._name]["dea"] > self.inds[d._name]["dea"](-1),
                ),
                bt.And(
                    self.inds[d._name]["dif"] > self.inds[d._name]["dea"](-1),
                    bt.indicators.crossover.CrossUp(self.inds[d._name]["dif"], 0) == 1,
                ),
                bt.And(
                    self.inds[d._name]["dif"] > self.inds[d._name]["dea"],
                    self.inds[d._name]["dif"] > self.inds[d._name]["dif"](-1),
                ),
            )
            self.signals[d._name]["death_cross"] = bt.Or(
                bt.And(
                    bt.indicators.crossover.CrossDown(
                        self.inds[d._name]["dif"], self.inds[d._name]["dea"]
                    )
                    == 1,
                    self.inds[d._name]["dea"] < self.inds[d._name]["dea"](-1),
                ),
                bt.And(
                    self.inds[d._name]["dif"] < self.inds[d._name]["dea"](-1),
                    bt.indicators.crossover.CrossDown(self.inds[d._name]["dif"], 0)
                    == 1,
                ),
                bt.And(
                    self.inds[d._name]["dif"] < self.inds[d._name]["dea"],
                    self.inds[d._name]["dif"] < self.inds[d._name]["dif"](-1),
                ),
            )

            """
            买入1: 均线上穿
            """
            self.signals[d._name]["ma_crossup"] = bt.Or(
                bt.And(
                    self.inds[d._name]["mid_term_ma"]
                    > self.inds[d._name]["mid_term_ma"](-1),
                    self.inds[d._name]["short_term_ema"]
                    > self.inds[d._name]["mid_term_ema"],
                    bt.indicators.crossover.CrossUp(
                        self.inds[d._name]["short_term_ma"],
                        self.inds[d._name]["mid_term_ma"],
                    )
                    == 1,
                    self.signals[d._name]["golden_cross"] == 1,
                    d.close > d.open,
                    self.signals[d._name]["deviant"] == 1,
                ),
                bt.And(
                    self.inds[d._name]["mid_term_ema"]
                    > self.inds[d._name]["mid_term_ema"](-1),
                    bt.indicators.crossover.CrossUp(
                        self.inds[d._name]["short_term_ema"],
                        self.inds[d._name]["mid_term_ema"],
                    )
                    == 1,
                    self.signals[d._name]["golden_cross"] == 1,
                    d.close > d.open,
                    self.signals[d._name]["deviant"] == 1,
                ),
            )

            """
            买入2: 收盘价走高
            """
            self.signals[d._name]["close_crossup"] = (
                bt.And(
                    self.signals[d._name]["higher"] == 1,
                    d.close > d.open,
                    self.signals[d._name]["deviant"] == 1,
                    self.inds[d._name]["short_term_ema"]
                    > self.inds[d._name]["short_term_ema"](-1),
                    self.inds[d._name]["short_term_mavol"]
                    > self.inds[d._name]["long_term_mavol"],
                    bt.Or(
                        bt.indicators.crossover.CrossUp(
                            d.close, self.inds[d._name]["short_term_ema"]
                        )
                        == 1,
                        bt.indicators.crossover.CrossUp(
                            d.close, self.inds[d._name]["short_term_ma"]
                        )
                        == 1,
                        self.signals[d._name]["golden_cross"] == 1,
                    ),
                ),
            )

            """ 
            买入3: 均线多头排列
            """
            self.signals[d._name]["long_position"] = bt.And(
                d.close > self.inds[d._name]["short_term_ema"],
                self.inds[d._name]["short_term_ema"]
                > self.inds[d._name]["mid_term_ema"],
                self.inds[d._name]["short_term_ma"] > self.inds[d._name]["mid_term_ma"],
                self.inds[d._name]["short_term_ema"]
                > self.inds[d._name]["short_term_ema"](-1),
                self.inds[d._name]["mid_term_ema"]
                > self.inds[d._name]["mid_term_ema"](-1),
                self.signals[d._name]["golden_cross"] == 1,
                self.signals[d._name]["deviant"] == 1,
            )

            """ 
            买入4: 成交量突然增加，价格走高
            """
            self.signals[d._name]["vol_increase"] = bt.And(
                self.inds[d._name]["short_term_mavol"]
                > self.inds[d._name]["mid_term_mavol"],
                self.inds[d._name]["short_term_mavol"]
                > self.inds[d._name]["long_term_mavol"],
                d.volume > self.inds[d._name]["short_term_mavol"] * 1.5,
                self.signals[d._name]["higher"] == 1,
                self.signals[d._name]["deviant"] == 1,
            )

            """
            买入5: 穿越年线
            """
            self.signals[d._name]["close_crossup_annualline"] = bt.And(
                bt.indicators.crossover.CrossUp(
                    d.close, self.inds[d._name]["annual_ma"]
                )
                == 1,
                bt.Or(
                    self.signals[d._name]["higher"] == 1,
                    self.signals[d._name]["golden_cross"] == 1,
                ),
                self.inds[d._name]["annual_ma"] > self.inds[d._name]["annual_ma"](-1),
                self.signals[d._name]["deviant"] == 1,
            )

            """
            卖出1: 均线破位
            """
            self.signals[d._name]["ma_crossdown"] = bt.Or(
                bt.And(
                    self.inds[d._name]["mid_term_ma"]
                    < self.inds[d._name]["mid_term_ma"](-1),
                    bt.indicators.crossover.CrossDown(
                        self.inds[d._name]["short_term_ma"],
                        self.inds[d._name]["mid_term_ma"],
                    )
                    == 1,
                    d.close < d.open,
                    self.signals[d._name]["death_cross"] == 1,
                ),
                bt.And(
                    self.inds[d._name]["mid_term_ema"]
                    < self.inds[d._name]["mid_term_ema"](-1),
                    bt.indicators.crossover.CrossDown(
                        self.inds[d._name]["short_term_ema"],
                        self.inds[d._name]["mid_term_ema"],
                    )
                    == 1,
                    d.close < d.open,
                    self.signals[d._name]["death_cross"] == 1,
                ),
            )
            """
            卖出2: 收盘价走低
            """
            self.signals[d._name]["close_crossdown"] = bt.And(
                self.inds[d._name]["mid_term_ma"]
                < self.inds[d._name]["mid_term_ma"](-1),
                d.close < d.open,
                self.signals[d._name]["lower"] == 1,
                bt.Or(
                    bt.indicators.crossover.CrossDown(
                        d.close, self.inds[d._name]["short_term_ema"]
                    )
                    == 1,
                    bt.indicators.crossover.CrossDown(
                        d.close, self.inds[d._name]["short_term_ma"]
                    )
                    == 1,
                    self.signals[d._name]["death_cross"] == 1,
                ),
            )
            """ 
            卖出3: 均线空头排列
            """
            self.signals[d._name]["short_position"] = bt.And(
                d.close < self.inds[d._name]["short_term_ema"],
                self.inds[d._name]["short_term_ema"]
                < self.inds[d._name]["mid_term_ema"],
                self.inds[d._name]["short_term_ma"] < self.inds[d._name]["mid_term_ma"],
                self.inds[d._name]["mid_term_ema"]
                < self.inds[d._name]["mid_term_ema"](-1),
                self.inds[d._name]["short_term_ema"]
                < self.inds[d._name]["short_term_ema"](-1),
                self.signals[d._name]["death_cross"] == 1,
            )
            """ 
            卖出5: 下穿年线
            """
            self.signals[d._name]["closs_crossdown_annualline"] = bt.And(
                bt.indicators.crossover.CrossDown(
                    d.close, self.inds[d._name]["annual_ma"]
                )
                == 1,
                bt.Or(
                    self.signals[d._name]["lower"] == 1,
                    self.signals[d._name]["death_cross"] == 1,
                ),
                self.inds[d._name]["annual_ma"] < self.inds[d._name]["annual_ma"](-1),
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
        print("current period:", len(self), "current date", self.datetime.date())
        self.next()

    def next(self):
        # 策略执行进度
        t = ToolKit("策略执行中")
        # # 增加cash
        # if (
        #     len(self) < self.data.buflen() - 1
        #     and self.broker.cash < self.params.availablecash
        # ):
        #     self.broker.add_cash(self.params.availablecash - self.broker.cash)
        #     self.log(
        #         "cash is not enough %s, add cash %s"
        #         % (self.broker.cash, self.params.availablecash - self.broker.cash)
        #     )
        list = []
        for i, d in enumerate(self.datas):
            if self.order[d._name]:
                continue
            """ 头部index不计算，有bug """
            if len(d) == 0:
                continue
            pos = self.getposition(d)

            """ 如果没有仓位就判断是否买卖 """
            if not pos:
                """噪声处理"""
                # # 均线密集判断，短期ema与中期ema近20日内密集排列
                # if (
                #     self.signals[d._name]["close_crossdown_mashort"]
                #     .get(ago=-1, size=self.params.shortperiod)
                #     .count(1)
                #     > 2
                # ):
                #     continue
                # # 最近20个交易日，dea下穿0轴2次，不进行交易
                # if (
                #     self.signals[d._name]["dif_crossdown_dea"]
                #     .get(ago=-1, size=self.params.shortperiod)
                #     .count(1)
                #     > 2
                # ):
                #     continue

                # 均线密集判断，短期ema与中期ema近20日内密集排列
                x1 = self.inds[d._name]["short_term_ema"].get(
                    ago=-1, size=self.params.fastperiod
                )
                y1 = self.inds[d._name]["mid_term_ema"].get(
                    ago=-1, size=self.params.fastperiod
                )
                x2 = self.inds[d._name]["short_term_ma"].get(
                    ago=-1, size=self.params.fastperiod
                )
                y2 = self.inds[d._name]["mid_term_ma"].get(
                    ago=-1, size=self.params.fastperiod
                )
                diff_array = [abs((x - y) * 100 / y) for x, y in zip(x1, y1) if y != 0]
                diff_array2 = [abs((x - y) * 100 / y) for x, y in zip(x2, y2) if y != 0]
                diff_array3 = [abs((x - y) * 100 / y) for x, y in zip(x1, x2) if y != 0]

                if self.signals[d._name]["ma_crossup"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "均线破线"
                elif self.signals[d._name]["vol_increase"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "成交放大"
                elif self.signals[d._name]["close_crossup_annualline"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "上穿年线"
                elif (
                    self.signals[d._name]["close_crossup_short_term_ema"][0] == 1
                    and sum(1 for value in diff_array if value < 2) >= 5
                    and sum(1 for value in diff_array2 if value < 2) >= 5
                    and sum(1 for value in diff_array3 if value < 2) >= 5
                    and self.signals[d._name]["deviant"][0] == 1
                ):
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "均线密集"
                elif self.signals[d._name]["long_position"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "均线多头"
                elif self.signals[d._name]["close_crossup"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "收盘价走高"
                elif (
                    self.signals[d._name]["red3_soldiers"][0] == 1
                    and self.signals[d._name]["deviant"][0] == 1
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.log("Buy %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "红三兵"
            else:
                dt = self.datas[0].datetime.date(0)
                dict = {
                    "symbol": d._name,
                    "date": dt.isoformat(),
                    "price": pos.price,
                    "adjbase": pos.adjbase,
                    "pnl": pos.size * (pos.adjbase - pos.price),
                }
                list.append(dict)

                if self.signals[d._name]["ma_crossdown"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "均线破位"
                elif self.signals[d._name]["closs_crossdown_annualline"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "下穿年线"
                elif self.signals[d._name]["short_position"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "均线空头"
                elif self.signals[d._name]["close_crossdown"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.log("Sell %s Created %.2f" % (d._name, d.close[0]))
                    self.myorder[d._name]["strategy"] = "收盘价走低"

        df = pd.DataFrame(list)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(self.file_path_position_detail, header=None)
        # # 最后一日剔除多余现金
        # if len(self) == self.data.buflen() - 1 and self.broker.cash > self.params.restcash:
        #     self.broker.add_cash(self.params.restcash - self.broker.cash)
        #     self.log(
        #         "cash is too much %s, add cash %s"
        #         % (self.broker.cash, self.params.restcash - self.broker.cash)
        #     )
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
                if self.market == "us":
                    cur = datetime.strptime(t.get_us_latest_trade_date(0), "%Y%m%d")
                    bef = datetime.strptime(str(dict["buy_date"]), "%Y-%m-%d")
                    interval = t.get_us_trade_off_days(cur, bef)
                elif self.market == "cn":
                    cur = datetime.strptime(t.get_cn_latest_trade_date(0), "%Y%m%d")
                    bef = datetime.strptime(str(dict["buy_date"]), "%Y-%m-%d")
                    interval = t.get_cn_trade_off_days(cur, bef)
                elif self.market == "cnetf":
                    cur = datetime.strptime(t.get_cn_latest_trade_date(0), "%Y%m%d")
                    bef = datetime.strptime(str(dict["buy_date"]), "%Y-%m-%d")
                    interval = t.get_cn_trade_off_days(cur, bef)
                print(
                    "当前股票: %s, 交易天数: %s, 累计涨幅: %s"
                    % (dict["symbol"], interval, dict["p&l_ratio"])
                )
                list.append(dict)
                pos_share = pos_share + pos.size * pos.adjbase
                if pos.adjbase - pos.price >= 0:
                    pos_earn = pos_earn + pos.size * (pos.adjbase - pos.price)
                else:
                    pos_loss = pos_loss + pos.size * (pos.adjbase - pos.price)
        print("总持仓：%s, 浮盈：%s, 浮亏：%s" % (pos_share, pos_earn, pos_loss))
        df = pd.DataFrame(list)
        if df.empty:
            return
        if self.market in ("cn", "us"):
            """ 
            匹配行业信息
            """
            df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
            df_n = pd.merge(df, df_o, how="left", on="symbol")
            """ 按照买入日期以及盈亏比倒排 """
            df_n.sort_values(
                by=["industry", "buy_date", "p&l_ratio"], ascending=False, inplace=True
            )
        elif self.market == "cnetf":
            df_n = df.sort_values(by=["buy_date", "p&l_ratio"], ascending=False)
        df_n.reset_index(drop=True, inplace=True)
        df_n.to_csv(self.file_path_position)
        self.file_path_position.close()
        self.file_path_position_detail.close()
