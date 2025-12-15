#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import backtrader as bt
from utility.toolkit import ToolKit
from datetime import datetime
import pandas as pd
from utility.fileinfo import FileInfo
import numpy as np


class GlobalStrategy(bt.Strategy):
    """
    自定义均线的时间间隔
    目前定义了MA/EMA/MACD三类指标的时间区间
    """

    params = (
        ("macd_fast_period", 10),
        ("macd_slow_period", 20),
        ("macd_signal_period", 8),
        ("ma_short_period", 20),
        ("ma_mid_period", 60),
        ("ma_long_period", 120),
        ("vol_short_period", 5),
        ("vol_mid_period", 10),
        ("vol_long_period", 20),
        ("annual_period", 240),
        ("availablecash", 4000000),
        ("restcash", 1000000),
        ("price_short_period", 5),
        ("price_mid_period", 10),
        ("price_long_period", 20),
        ("rf_window", 20),  # 夏普比率滚动计算窗口（天）
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
        self.daily_returns = {}  # 每只股票每日收益率序列
        self.sharpe_ratios = {}  # 每只股票当前夏普比率
        self.sortino_ratios = {}
        # 读取国债收益率
        file = FileInfo(trade_date, market)
        file_gz = file.get_file_path_gz
        cols = ["code", "name", "date", "new"]
        self.rf_rate = 0
        try:
            df_gz = pd.read_csv(file_gz, usecols=cols)
            df_gz = df_gz.dropna(subset=["new"])
            if not df_gz.empty:
                self.rf_rate = df_gz["new"].astype(float).iloc[-1] / 100.0
                print(f"✅ 读取国债收益率: {self.rf_rate:.2f}")
        except Exception as e:
            print(f"⚠️ 无法读取国债收益率{e}")
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
            self.daily_returns[d._name] = []  # 初始化每日收益率
            self.sharpe_ratios[d._name] = None
            self.sortino_ratios[d._name] = None

            """MA20/60/120指标 """
            try:
                self.inds[d._name]["sma_short"] = bt.indicators.SMA(
                    d.close, period=self.params.ma_short_period
                )
                self.inds[d._name]["sma_mid"] = bt.indicators.SMA(
                    d.close, period=self.params.ma_mid_period
                )
            except Exception as e:
                print(f"❌ 初始化失败-SMA指标: {d._name}, {e}")
            # 半年线
            try:
                if d.buflen() > self.params.ma_long_period:
                    self.inds[d._name]["sma_long"] = bt.indicators.SMA(
                        d.close, period=self.params.ma_long_period
                    )
                else:
                    # 数据不足时，创建一个始终为NaN的虚拟指标
                    self.inds[d._name]["sma_long"] = bt.LineNum(float("nan"))
                    print(
                        f"警告: {d._name} 数据长度({d.buflen()})小于SMA周期({self.params.ma_long_period})"
                    )
            except Exception as e:
                print(f"❌ 初始化失败-半年线指标: {d._name}, {e}")
            # 年线
            try:
                if d.buflen() > self.params.annual_period:
                    self.inds[d._name]["sma_annual"] = bt.indicators.SMA(
                        d.close, period=self.params.annual_period
                    )
                else:
                    # 数据不足时，创建一个始终为NaN的虚拟指标
                    self.inds[d._name]["sma_annual"] = bt.LineNum(float("nan"))
                    print(
                        f"警告: {d._name} 数据长度({d.buflen()})小于SMA周期({self.params.annual_period})"
                    )
            except Exception as e:
                print(f"❌ 初始化失败-年线指标: {d._name}, {e}")

            """EMA20/60/120"""
            try:
                self.inds[d._name]["ema_short"] = bt.indicators.EMA(
                    d.close, period=self.params.ma_short_period
                )
                self.inds[d._name]["ema_mid"] = bt.indicators.EMA(
                    d.close, period=self.params.ma_mid_period
                )
            except Exception as e:
                print(f"❌ 初始化失败-ema指标: {d._name}, {e}")

            """
            日线DIF值
            日线DEA值
            日线MACD值
            """
            try:
                self.inds[d._name]["dif"] = bt.indicators.MACDHisto(
                    d.close,
                    period_me1=self.params.macd_fast_period,
                    period_me2=self.params.macd_slow_period,
                    period_signal=self.params.macd_signal_period,
                ).macd
                self.inds[d._name]["dea"] = bt.indicators.MACDHisto(
                    d.close,
                    period_me1=self.params.macd_fast_period,
                    period_me2=self.params.macd_slow_period,
                    period_signal=self.params.macd_signal_period,
                ).signal
                self.inds[d._name]["macd"] = (
                    bt.indicators.MACDHisto(
                        d.close,
                        period_me1=self.params.macd_fast_period,
                        period_me2=self.params.macd_slow_period,
                        period_signal=self.params.macd_signal_period,
                    ).histo
                    * 2
                )
            except Exception as e:
                print(f"❌ 初始化失败-MACD指标: {d._name}, {e}")

            """
            MAVOL5、10、20成交量均线
            """
            try:
                self.inds[d._name]["emavol_short"] = bt.indicators.EMA(
                    d.volume, period=self.params.vol_short_period
                )
                self.inds[d._name]["emavol_long"] = bt.indicators.EMA(
                    d.volume, period=self.params.vol_long_period
                )
            except Exception as e:
                print(f"❌ 初始化失败-Vol指标: {d._name}, {e}")

            """ 收盘价高点/低点 """
            try:
                self.inds[d._name]["highest_short"] = bt.indicators.Highest(
                    d.close, period=self.params.price_short_period
                )
                self.inds[d._name]["highest_mid"] = bt.indicators.Highest(
                    d.close, period=self.params.price_mid_period
                )
                self.inds[d._name]["highest_long"] = bt.indicators.Highest(
                    d.close, period=self.params.price_long_period
                )
                self.inds[d._name]["lowest_short"] = bt.indicators.Lowest(
                    d.close, period=self.params.price_short_period
                )
                self.inds[d._name]["lowest_mid"] = bt.indicators.Lowest(
                    d.close, period=self.params.price_mid_period
                )
                self.inds[d._name]["lowest_long"] = bt.indicators.Lowest(
                    d.close, period=self.params.price_long_period
                )
            except Exception as e:
                print(f"❌ 初始化失败-高低点指标: {d._name}, {e}")

            """
            红三兵
            """
            try:
                self.signals[d._name]["red_three_soldiers"] = bt.And(
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
                    d.volume > self.inds[d._name]["emavol_short"],
                )
            except Exception as e:
                print(f"❌ 初始化失败-红三兵指标: {d._name}, {e}")

            """
            辅助指标1：上影线判定
            """
            try:
                self.signals[d._name]["upper_shadow"] = bt.And(
                    d.close > ((d.high - d.low) * 0.618 + d.low), d.close > d.open
                )
            except Exception as e:
                print(f"❌ 初始化失败-上影线指标: {d._name}, {e}")

            """
            高低点上移/下移
            """
            try:
                self.signals[d._name]["price_higher"] = bt.Or(
                    # 情况1：创新高模式
                    bt.And(
                        d.close > self.inds[d._name]["highest_short"](-1),
                        d.close > d.close(-5),
                        self.signals[d._name]["upper_shadow"] == 1,
                    ),
                    # 情况2：收敛上涨模式 - 低点上移
                    bt.And(
                        self.inds[d._name]["lowest_short"]
                        > self.inds[d._name]["lowest_short"](-5),
                        d.close > d.close(-5),
                        # 可选：添加波动率收敛条件
                        (
                            self.inds[d._name]["highest_short"]
                            - self.inds[d._name]["lowest_short"]
                        )
                        < (
                            self.inds[d._name]["highest_short"](-5)
                            - self.inds[d._name]["lowest_short"](-5)
                        ),
                    ),
                    # 情况3：高点上移但未创新高
                    bt.And(
                        self.inds[d._name]["highest_short"]
                        > self.inds[d._name]["highest_short"](-5),
                        d.close > d.close(-5),
                        self.inds[d._name]["lowest_short"]
                        >= self.inds[d._name]["lowest_short"](-5),
                    ),
                )

                self.signals[d._name]["price_lower"] = bt.Or(
                    # 情况1：创新低模式
                    bt.And(
                        d.close < self.inds[d._name]["lowest_short"](-1),
                        d.close < d.close(-5),
                    ),
                    # 情况2：高点下移模式
                    bt.And(
                        self.inds[d._name]["highest_short"]
                        < self.inds[d._name]["highest_short"](-5),
                        d.close < d.close(-5),
                        # 确认卖压增强
                        self.inds[d._name]["lowest_short"]
                        <= self.inds[d._name]["lowest_short"](-5),
                    ),
                    # 情况3：收敛下跌模式 - 高低点同步下移但幅度小
                    bt.And(
                        self.inds[d._name]["highest_short"]
                        < self.inds[d._name]["highest_short"](-5),
                        self.inds[d._name]["lowest_short"]
                        < self.inds[d._name]["lowest_short"](-5),
                        d.close < d.close(-5),
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败-价格上涨和下跌指标: {d._name}, {e}")

            """ 
            辅助指标2：波动过大，避免频繁交易
            """
            try:
                self.signals[d._name]["close_crossdown_sma"] = bt.And(
                    bt.indicators.crossover.CrossDown(
                        d.close, self.inds[d._name]["sma_short"]
                    ),
                    self.inds[d._name]["sma_short"]
                    < self.inds[d._name]["sma_short"](-1),
                )

                self.signals[d._name]["dif_crossdown"] = bt.Or(
                    bt.And(
                        bt.indicators.crossover.CrossDown(
                            self.inds[d._name]["dif"], self.inds[d._name]["dea"]
                        ),
                        self.inds[d._name]["dea"] < self.inds[d._name]["dea"](-1),
                    ),
                    bt.indicators.crossover.CrossDown(self.inds[d._name]["dif"], 0),
                )
            except Exception as e:
                print(f"❌ 初始化失败-辅助指标2: {d._name}, {e}")

            """
            辅助指标3：收盘价上穿短期均线
            """
            try:
                self.signals[d._name]["close_crossup_ema_short"] = (
                    bt.indicators.crossover.CrossUp(
                        d.close, self.inds[d._name]["ema_short"]
                    )
                )
            except Exception as e:
                print(f"❌ 初始化失败-辅助指标3: {d._name}, {e}")

            """ 
            辅助指标4：乖离率判断
            """
            try:
                self.signals[d._name]["deviant"] = (
                    d.close - self.inds[d._name]["sma_short"]
                ) / self.inds[d._name]["sma_short"] <= 0.12
            except Exception as e:
                print(f"❌ 初始化失败-辅助指标4: {d._name}, {e}")

            """
            辅助指标5：Dif和Dea的关系
            """
            try:
                self.signals[d._name]["golden_cross"] = bt.Or(
                    bt.And(
                        bt.indicators.crossover.CrossUp(
                            self.inds[d._name]["dif"], self.inds[d._name]["dea"]
                        )
                        == 1,
                        self.inds[d._name]["dea"] > self.inds[d._name]["dea"](-1),
                    ),
                    bt.And(
                        self.inds[d._name]["dea"] > self.inds[d._name]["dea"](-1),
                        bt.indicators.crossover.CrossUp(self.inds[d._name]["dif"], 0)
                        == 1,
                    ),
                    bt.And(
                        self.inds[d._name]["dif"] > self.inds[d._name]["dea"],
                        self.inds[d._name]["dif"] > self.inds[d._name]["dif"](-1),
                    ),
                    d.volume > self.inds[d._name]["emavol_short"] * 1.25,
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
                        self.inds[d._name]["dea"] < self.inds[d._name]["dea"](-1),
                        bt.indicators.crossover.CrossDown(self.inds[d._name]["dif"], 0)
                        == 1,
                    ),
                    bt.And(
                        self.inds[d._name]["dif"] < self.inds[d._name]["dea"],
                        self.inds[d._name]["dif"] < self.inds[d._name]["dif"](-1),
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败-辅助指标5: {d._name}, {e}")

            """
            买入1: 均线金叉
            """
            try:
                self.signals[d._name]["ma_crossover_bullish"] = bt.Or(
                    bt.And(
                        self.inds[d._name]["sma_mid"]
                        > self.inds[d._name]["sma_mid"](-1),
                        self.inds[d._name]["ema_short"] > self.inds[d._name]["ema_mid"],
                        bt.indicators.crossover.CrossUp(
                            self.inds[d._name]["sma_short"],
                            self.inds[d._name]["sma_mid"],
                        )
                        == 1,
                        self.signals[d._name]["golden_cross"] == 1,
                        d.close > d.open,
                        self.signals[d._name]["deviant"] == 1,
                    ),
                    bt.And(
                        self.inds[d._name]["ema_mid"]
                        > self.inds[d._name]["ema_mid"](-1),
                        bt.indicators.crossover.CrossUp(
                            self.inds[d._name]["ema_short"],
                            self.inds[d._name]["ema_mid"],
                        )
                        == 1,
                        self.signals[d._name]["golden_cross"] == 1,
                        d.close > d.open,
                        self.signals[d._name]["deviant"] == 1,
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败-买入1: {d._name}, {e}")

            """
            买入2: 收盘价连续上涨
            """
            try:
                self.signals[d._name]["close_rising"] = (
                    bt.And(
                        self.signals[d._name]["price_higher"] == 1,
                        d.close > d.open,
                        self.signals[d._name]["deviant"] == 1,
                        bt.Or(
                            bt.indicators.crossover.CrossUp(
                                d.close, self.inds[d._name]["ema_short"]
                            )
                            == 1,
                            bt.indicators.crossover.CrossUp(
                                d.close, self.inds[d._name]["sma_short"]
                            )
                            == 1,
                            self.signals[d._name]["golden_cross"] == 1,
                        ),
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败-买入2: {d._name}, {e}")
            """ 
            买入3: 均线多头排列
            """
            try:
                self.signals[d._name]["long_position"] = bt.And(
                    d.close > self.inds[d._name]["ema_short"],
                    self.inds[d._name]["ema_short"] > self.inds[d._name]["ema_mid"],
                    self.inds[d._name]["sma_short"] > self.inds[d._name]["sma_mid"],
                    self.inds[d._name]["ema_short"]
                    > self.inds[d._name]["ema_short"](-1),
                    self.inds[d._name]["ema_mid"] > self.inds[d._name]["ema_mid"](-1),
                    self.signals[d._name]["golden_cross"] == 1,
                    self.signals[d._name]["deviant"] == 1,
                )
            except Exception as e:
                print(f"❌ 初始化失败-买入3: {d._name}, {e}")

            """ 
            买入4: 成交量突然增加，价格走高
            """
            try:
                self.signals[d._name]["volume_spike"] = bt.And(
                    self.inds[d._name]["emavol_short"]
                    > self.inds[d._name]["emavol_long"],
                    d.volume > self.inds[d._name]["emavol_short"] * 1.5,
                    self.signals[d._name]["price_higher"] == 1,
                    self.signals[d._name]["deviant"] == 1,
                )
            except Exception as e:
                print(f"❌ 初始化失败-买入4: {d._name}, {e}")

            """
            买入5: 穿越年线
            """
            try:
                self.signals[d._name]["close_crossup_annualline"] = bt.And(
                    bt.indicators.crossover.CrossUp(
                        d.close, self.inds[d._name]["sma_annual"]
                    )
                    == 1,
                    bt.Or(
                        self.signals[d._name]["price_higher"] == 1,
                        self.signals[d._name]["golden_cross"] == 1,
                    ),
                    self.inds[d._name]["sma_annual"]
                    > self.inds[d._name]["sma_annual"](-1),
                    self.signals[d._name]["deviant"] == 1,
                )
            except Exception as e:
                print(f"❌ 初始化失败-买入5: {d._name}, {e}")

            """
            买入6: 穿越半年线
            """
            try:
                self.signals[d._name]["close_crossup_halfannualline"] = bt.And(
                    bt.indicators.crossover.CrossUp(
                        d.close, self.inds[d._name]["sma_long"]
                    )
                    == 1,
                    bt.Or(
                        self.signals[d._name]["price_higher"] == 1,
                        self.signals[d._name]["golden_cross"] == 1,
                    ),
                    self.inds[d._name]["sma_long"] > self.inds[d._name]["sma_long"](-1),
                    self.signals[d._name]["deviant"] == 1,
                )
            except Exception as e:
                print(f"❌ 初始化失败-买入6: {d._name}, {e}")

            """
            卖出1: 均线死叉
            """
            try:
                self.signals[d._name]["ma_crossover_bearish"] = bt.Or(
                    bt.And(
                        self.inds[d._name]["sma_mid"]
                        < self.inds[d._name]["sma_mid"](-1),
                        bt.indicators.crossover.CrossDown(
                            self.inds[d._name]["sma_short"],
                            self.inds[d._name]["sma_mid"],
                        )
                        == 1,
                        d.close < d.open,
                        self.signals[d._name]["death_cross"] == 1,
                    ),
                    bt.And(
                        self.inds[d._name]["ema_mid"]
                        < self.inds[d._name]["ema_mid"](-1),
                        bt.indicators.crossover.CrossDown(
                            self.inds[d._name]["ema_short"],
                            self.inds[d._name]["ema_mid"],
                        )
                        == 1,
                        d.close < d.open,
                        self.signals[d._name]["death_cross"] == 1,
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败-卖出1: {d._name}, {e}")
            """
            卖出2: 收盘价连续下跌
            """
            try:
                self.signals[d._name]["close_falling"] = bt.And(
                    self.inds[d._name]["sma_short"]
                    < self.inds[d._name]["sma_short"](-1),
                    d.close < d.open,
                    self.signals[d._name]["price_lower"] == 1,
                    bt.Or(
                        bt.indicators.crossover.CrossDown(
                            d.close, self.inds[d._name]["ema_short"]
                        )
                        == 1,
                        bt.indicators.crossover.CrossDown(
                            d.close, self.inds[d._name]["sma_short"]
                        )
                        == 1,
                        self.signals[d._name]["death_cross"] == 1,
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败-卖出2: {d._name}, {e}")
            """ 
            卖出3: 均线空头排列
            """
            try:
                self.signals[d._name]["short_position"] = bt.And(
                    d.close < self.inds[d._name]["ema_short"],
                    self.inds[d._name]["ema_short"] < self.inds[d._name]["ema_mid"],
                    self.inds[d._name]["sma_short"] < self.inds[d._name]["sma_mid"],
                    self.inds[d._name]["ema_mid"] < self.inds[d._name]["ema_mid"](-1),
                    self.inds[d._name]["ema_short"]
                    < self.inds[d._name]["ema_short"](-1),
                    self.signals[d._name]["death_cross"] == 1,
                )
            except Exception as e:
                print(f"❌ 初始化失败-卖出3: {d._name}, {e}")
            """ 
            卖出4: 跌破年线
            """
            try:
                self.signals[d._name]["closs_crossdown_annualline"] = bt.And(
                    bt.indicators.crossover.CrossDown(
                        d.close, self.inds[d._name]["sma_annual"]
                    )
                    == 1,
                    bt.Or(
                        self.signals[d._name]["price_lower"] == 1,
                        self.signals[d._name]["death_cross"] == 1,
                    ),
                    self.inds[d._name]["sma_annual"]
                    < self.inds[d._name]["sma_annual"](-1),
                )
            except Exception as e:
                print(f"❌ 初始化失败-卖出4: {d._name}, {e}")
            """
            卖出5: 跌破半年线
            """
            try:
                self.signals[d._name]["closs_crossdown_halfannualline"] = bt.And(
                    bt.indicators.crossover.CrossDown(
                        d.close, self.inds[d._name]["sma_long"]
                    )
                    == 1,
                    bt.Or(
                        self.signals[d._name]["price_lower"] == 1,
                        self.signals[d._name]["death_cross"] == 1,
                    ),
                    self.inds[d._name]["sma_long"] < self.inds[d._name]["sma_long"](-1),
                )
            except Exception as e:
                print(f"❌ 初始化失败-卖出5: {d._name}, {e}")
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
                    "{}, Buy {} Executed, Price: {:.2f} Size: {:.2f} SharpRatio: {} SortinoRatio: {}".format(
                        bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
                        self.sharpe_ratios[order.data._name],
                        self.sortino_ratios[order.data._name],
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
                    "{} Sell {} Executed, Price: {:.2f} Size: {:.2f} SharpRatio: {} SortinoRatio: {}".format(
                        bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
                        self.sharpe_ratios[order.data._name],
                        self.sortino_ratios[order.data._name],
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
            df = pd.DataFrame([dict])
            df.to_csv(self.file_path_trade, header=False, mode="a")

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
        list = []
        for i, d in enumerate(self.datas):
            if self.order[d._name]:
                continue
            """ 头部index不计算，有bug """
            # if len(d) == 0:
            #     continue
            if len(d) < 2:
                continue

            # 夏普比率计算
            if d._name not in self.daily_returns:
                self.daily_returns[d._name] = []
                self.sharpe_ratios[d._name] = 0
                self.sortino_ratios[d._name] = 0

            # ---- (1) 计算每日收益率 ----
            prev_close = d.close[-1]
            curr_close = d.close[0]
            if prev_close > 0:
                ret = curr_close / prev_close - 1
                self.daily_returns[d._name].append(ret)

                # 只保留最近 rf_window 天数据
                if len(self.daily_returns[d._name]) > self.params.rf_window:
                    self.daily_returns[d._name] = self.daily_returns[d._name][
                        -self.params.rf_window :
                    ]

                # ---- (2) 夏普比率计算 ----
                # 条件1: 有足够天数
                enough_data = len(self.daily_returns[d._name]) >= min(
                    self.params.rf_window, self.params.annual_period
                )
                # 条件2: 无风险利率有效
                valid_rf = self.rf_rate is not None and self.rf_rate > 0

                if enough_data and valid_rf:
                    # 日化无风险收益
                    rf_daily = self.rf_rate / self.params.annual_period

                    # 超额收益
                    excess_ret = np.array(self.daily_returns[d._name]) - rf_daily

                    mean_ret = np.mean(excess_ret)
                    std_ret = np.std(excess_ret, ddof=1)

                    if std_ret > 0:
                        sharpe = np.sqrt(self.params.annual_period) * mean_ret / std_ret
                        self.sharpe_ratios[d._name] = sharpe
                    else:
                        self.sharpe_ratios[d._name] = 0

                    # 只计算下行波动（只考虑负值）
                    downside_returns = excess_ret[excess_ret < 0]
                    if len(downside_returns) > 5:
                        downside_std = np.std(downside_returns, ddof=1)
                        mean_ret = np.mean(excess_ret)
                        if downside_std > 0:
                            sortino = (
                                np.sqrt(self.params.annual_period)
                                * mean_ret
                                / downside_std
                            )
                            self.sortino_ratios[d._name] = sortino
                        else:
                            self.sortino_ratios[d._name] = 0
                    else:
                        # 没有下行波动，定义索提诺比率为年化平均收益除以小数值1e-10防止除零
                        self.sortino_ratios[d._name] = self.sharpe_ratios[d._name]
                else:
                    # 数据不足或无风险利率为0 → 夏普比率置0
                    self.sharpe_ratios[d._name] = None
                    self.sortino_ratios[d._name] = None

            pos = self.getposition(d)
            """ 如果没有仓位就判断是否买卖 """
            if not pos:
                # is_invalid_sortino = (
                #     self.sortino_ratios[d._name] is not None
                #     and (
                #         self.sortino_ratios[d._name] < 0.3
                #         or self.sortino_ratios[d._name] > 5
                #     )
                # ) or (
                #     self.sharpe_ratios[d._name] is not None
                #     and (
                #         self.sharpe_ratios[d._name] < 0.5
                #         or self.sharpe_ratios[d._name] > 5
                #     )
                # )

                # if is_invalid_sortino:
                #     continue
                # 诱多风险判定
                r1 = self.signals[d._name]["close_crossdown_sma"].get(
                    ago=-1, size=self.params.ma_short_period
                )
                r2 = self.signals[d._name]["dif_crossdown"].get(
                    ago=-1, size=self.params.ma_short_period
                )
                if (
                    sum(1 for value in r1 if value == 1) >= 2
                    and sum(1 for value in r2 if value == 1) >= 2
                ):
                    continue

                # 均线密集判断，短期ema与中期ema近20日内密集排列
                x1 = self.inds[d._name]["ema_short"].get(
                    ago=-1, size=self.params.ma_short_period
                )
                y1 = self.inds[d._name]["ema_mid"].get(
                    ago=-1, size=self.params.ma_short_period
                )
                x2 = self.inds[d._name]["sma_short"].get(
                    ago=-1, size=self.params.ma_short_period
                )
                y2 = self.inds[d._name]["sma_mid"].get(
                    ago=-1, size=self.params.ma_short_period
                )
                diff_array = [abs((x - y) * 100 / y) for x, y in zip(x1, y1) if y > 1]
                diff_array2 = [abs((x - y) * 100 / y) for x, y in zip(x2, y2) if y > 1]
                diff_array3 = [abs((x - y) * 100 / y) for x, y in zip(x1, x2) if y > 1]
                if self.signals[d._name]["long_position"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.myorder[d._name]["strategy"] = "多头排列"
                elif self.signals[d._name]["ma_crossover_bullish"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.myorder[d._name]["strategy"] = "均线金叉"
                elif (
                    self.signals[d._name]["close_crossup_ema_short"][0] == 1
                    and sum(1 for value in diff_array if value < 2) >= 5
                    and sum(1 for value in diff_array2 if value < 2) >= 5
                    and sum(1 for value in diff_array3 if value < 2) >= 5
                    and self.signals[d._name]["deviant"][0] == 1
                ):
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.myorder[d._name]["strategy"] = "均线收敛"
                elif self.signals[d._name]["close_crossup_annualline"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.myorder[d._name]["strategy"] = "突破年线"
                elif self.signals[d._name]["close_crossup_halfannualline"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.myorder[d._name]["strategy"] = "突破半年线"
                elif self.signals[d._name]["close_rising"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.myorder[d._name]["strategy"] = "收盘价连续上涨"
                elif self.signals[d._name]["volume_spike"][0] == 1:
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.myorder[d._name]["strategy"] = "成交量放大"
                elif (
                    self.signals[d._name]["red_three_soldiers"][0] == 1
                    and self.signals[d._name]["deviant"][0] == 1
                ):
                    """买入对应仓位"""
                    self.broker.cancel(self.order[d._name])
                    self.order[d._name] = self.buy(data=d)
                    self.myorder[d._name]["strategy"] = "红三兵"
            else:
                dt = self.datas[0].datetime.date(0)
                dict = {
                    "symbol": d._name,
                    "date": dt.isoformat(),
                    "price": pos.price,
                    "adjbase": pos.adjbase,
                    "pnl": pos.size * (pos.adjbase - pos.price),
                    "volume": d.volume[0],
                    "sharpe_ratio": self.sharpe_ratios[d._name],
                    "sortino_ratio": self.sortino_ratios[d._name],
                }
                list.append(dict)

                if self.signals[d._name]["short_position"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.myorder[d._name]["strategy"] = "空头排列"
                elif self.signals[d._name]["ma_crossover_bearish"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.myorder[d._name]["strategy"] = "均线死叉"
                elif self.signals[d._name]["closs_crossdown_annualline"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.myorder[d._name]["strategy"] = "跌破年线"
                elif self.signals[d._name]["closs_crossdown_halfannualline"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.myorder[d._name]["strategy"] = "跌破半年线"
                elif self.signals[d._name]["close_falling"][0] == 1:
                    self.order[d._name] = self.close(data=d)
                    self.myorder[d._name]["strategy"] = "收盘价连续下跌"

        df = pd.DataFrame(list)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(self.file_path_position_detail, header=False, mode="a")
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
                list.append(dict)
                pos_share = pos_share + pos.size * pos.adjbase
                if pos.adjbase - pos.price >= 0:
                    pos_earn = pos_earn + pos.size * (pos.adjbase - pos.price)
                else:
                    pos_loss = pos_loss + pos.size * (pos.adjbase - pos.price)
        print("\n总持仓：%s, 浮盈：%s, 浮亏：%s" % (pos_share, pos_earn, pos_loss))
        df = pd.DataFrame(list)
        if df.empty:
            return
        if self.market in ("cn", "us", "us_special"):
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
        df_n.to_csv(self.file_path_position, header=True, mode="w")
        self.file_path_position.close()
        self.file_path_position_detail.close()
        self.file_path_trade.close()
