#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import backtrader as bt
from finance.utility.toolkit import ToolKit
from datetime import datetime
import pandas as pd
from finance.utility.fileinfo import FileInfo
import numpy as np
from finance.backtraderref.nanindicator import NaNIndicator


class GlobalStrategy(bt.Strategy):
    """
    优化版本 (v5.0) - 专注于消除重复的买卖逻辑
    - 统一信号检查方法
    - 统一买入执行方法
    - 统一卖出执行方法
    - 提取复杂信号逻辑（均线收敛）
    """

    # ===== 策略级别定义（数字越小级别越高）=====
    STRATEGY_LEVELS = {
        # 长线策略 (级别 1)
        "多头排列": 1,
        "突破年线": 1,
        "跌破年线": 1,
        "空头排列": 1,
        # 趋势策略 (级别 2)
        "均线金叉": 2,
        "均线死叉": 2,
        "突破半年线": 2,
        "跌破半年线": 2,
        "均线收敛": 2,
        # 短线策略 (级别 3)
        "连续上涨": 3,
        "成交量放大": 3,
        "红三兵": 3,
        "连续下跌": 3,
        # 止损止盈 (级别 0 - 最高优先级，任何时候都可以卖出)
        "移动止盈": 0,
        "低于买入价 15%": 0,
    }

    # 买入信号到卖出信号的映射
    BUY_TO_SELL_SIGNALS = {
        # 长线买入信号 → 对应的长线卖出信号
        "多头排列": "short_position",
        "突破年线": "closs_crossdown_annualline",
        # 趋势买入信号 → 对应的趋势卖出信号
        "均线金叉": "ma_crossover_bearish",
        "均线收敛": "ma_crossover_bearish",
        "突破半年线": "closs_crossdown_halfannualline",
        # 短线买入信号 → 对应的短线卖出信号
        "连续上涨": "close_falling",
        "成交量放大": "close_falling",
        "红三兵": "close_falling",
    }

    @staticmethod
    def get_strategy_level(strategy_name):
        """获取策略级别，未知策略默认返回 3（短线级别）"""
        return GlobalStrategy.STRATEGY_LEVELS.get(strategy_name, 3)

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
        backtrader 自己的 log 函数
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
        """ backtrader 一些常用属性的初始化 """
        self.trade_date = trade_date
        self.market = market
        self.trade = None
        self.order = dict()
        self.buyprice = None
        self.buycomm = None
        """
        技术指标的初始化，每个指标的 key 值是每个 datafeed
        买入以及卖出信号的初始化，每个指标的 key 值是每个 datafeed
        存储每一笔股票最后一笔订单的购买日期，方便追踪股票走势
        """
        self.inds = dict()
        self.signals = dict()
        self.last_deal_date = dict()
        self.myorder = dict()
        self.daily_returns = {}  # 每只股票每日收益率序列
        self.sharpe_ratios = {}  # 每只股票当前夏普比率
        self.sortino_ratios = {}
        self.max_drawdowns = {}  # 每只股票最大回撤率
        self.current_signal = (
            {}
        )  # 记录当前满足的最高级别买入信号（动态更新，用于卖出决策）
        self.peak_price = {}  # 持仓期间最高价（用于移动止盈）
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
                print(f"✅ 读取国债收益率：{self.rf_rate:.2f}")
        except Exception as e:
            print(f"⚠️ 无法读取国债收益率{e}")
        """ 策略进度方法初始化 """
        t = ToolKit("策略初始化")
        """
        循环遍历每一个 datafeed
        每一个 datafeed 包括每一支股票的 o/c/h/l/v 相关数据
        初始化 indicators，signals
        """
        for i, d in enumerate(self.datas):
            """初始化最后一笔订单的购买日期，key 是 symbol"""
            self.last_deal_date[d._name] = None
            """为每个股票初始化技术指标，key 是 symbol """
            self.order[d._name] = None
            self.inds[d._name] = dict()
            self.signals[d._name] = dict()

            # 跟踪订单交易策略
            self.myorder[d._name] = dict()
            self.daily_returns[d._name] = []  # 初始化每日收益率
            self.sharpe_ratios[d._name] = None
            self.sortino_ratios[d._name] = None
            self.max_drawdowns[d._name] = 0
            self.peak_price[d._name] = None  # 初始化最高价
            self.current_signal[d._name] = None  # 初始化当前信号

            """MA20/60/120 指标 """
            try:
                self.inds[d._name]["sma_short"] = bt.indicators.SMA(
                    d.close, period=self.params.ma_short_period
                )
                self.inds[d._name]["sma_mid"] = bt.indicators.SMA(
                    d.close, period=self.params.ma_mid_period
                )
            except Exception as e:
                print(f"❌ 初始化失败-SMA 指标：{d._name}, {e}")
            # 半年线
            try:
                if d.buflen() > self.params.ma_long_period:
                    self.inds[d._name]["sma_long"] = bt.indicators.SMA(
                        d.close, period=self.params.ma_long_period
                    )
                else:
                    # 数据不足时，创建一个始终为 NaN 的虚拟指标
                    self.inds[d._name]["sma_long"] = NaNIndicator(data=d.close)
                    print(
                        f"警告：{d._name} 数据长度 ({d.buflen()}) 小于 SMA 周期 ({self.params.ma_long_period})"
                    )
            except Exception as e:
                print(f"❌ 初始化失败 - 半年线指标：{d._name}, {e}")
            # 年线
            try:
                if d.buflen() > self.params.annual_period:
                    self.inds[d._name]["sma_annual"] = bt.indicators.SMA(
                        d.close, period=self.params.annual_period
                    )
                else:
                    # 数据不足时，创建一个始终为 NaN 的虚拟指标
                    self.inds[d._name]["sma_annual"] = NaNIndicator(data=d.close)

                    print(
                        f"警告：{d._name} 数据长度 ({d.buflen()}) 小于 SMA 周期 ({self.params.annual_period})"
                    )
            except Exception as e:
                print(f"❌ 初始化失败 - 年线指标：{d._name}, {e}")

            """EMA20/60/120"""
            try:
                self.inds[d._name]["ema_short"] = bt.indicators.EMA(
                    d.close, period=self.params.ma_short_period
                )
                self.inds[d._name]["ema_mid"] = bt.indicators.EMA(
                    d.close, period=self.params.ma_mid_period
                )
            except Exception as e:
                print(f"❌ 初始化失败-ema 指标：{d._name}, {e}")

            """
            日线 DIF 值
            日线 DEA 值
            日线 MACD 值
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
                print(f"❌ 初始化失败-MACD 指标：{d._name}, {e}")

            """
            MAVOL5、10、20 成交量均线
            """
            try:
                self.inds[d._name]["emavol_short"] = bt.indicators.EMA(
                    d.volume, period=self.params.vol_short_period
                )
                self.inds[d._name]["emavol_long"] = bt.indicators.EMA(
                    d.volume, period=self.params.vol_long_period
                )
            except Exception as e:
                print(f"❌ 初始化失败-Vol 指标：{d._name}, {e}")

            """ 收盘价高点/低点 """
            try:
                self.inds[d._name]["highest_short"] = bt.indicators.Highest(
                    d.close, period=self.params.price_short_period
                )
                self.inds[d._name]["lowest_short"] = bt.indicators.Lowest(
                    d.close, period=self.params.price_short_period
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 高低点指标：{d._name}, {e}")

            """
            红三兵（优化版：放宽到 50% 分位）
            """
            try:
                self.signals[d._name]["red_three_soldiers"] = bt.And(
                    d.close > d.close(-1),
                    d.close(-1) > d.close(-2),
                    d.close > d.open,
                    d.close(-1) > d.open(-1),
                    d.close(-2) > d.open(-2),
                    # 放宽：从 61.8% 改为 50%
                    d.close > ((d.high - d.low) * 0.50 + d.low),
                    d.close(-1) > ((d.high(-1) - d.low(-1)) * 0.50 + d.low(-1)),
                    d.close(-2) > ((d.high(-2) - d.low(-2)) * 0.50 + d.low(-2)),
                    d.volume > d.volume(-1),
                    d.volume(-1) > d.volume(-2),
                    d.volume > self.inds[d._name]["emavol_short"],
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 红三兵指标：{d._name}, {e}")

            """
            辅助指标 1：上影线判定
            """
            try:
                self.signals[d._name]["upper_shadow"] = bt.And(
                    d.close > ((d.high - d.low) * 0.618 + d.low), d.close > d.open
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 上影线指标：{d._name}, {e}")

            """
            高低点上移/下移
            """
            try:
                self.signals[d._name]["price_higher"] = bt.Or(
                    # 情况 1：创新高
                    bt.And(
                        d.close >= self.inds[d._name]["highest_short"],
                        d.close > self.inds[d._name]["highest_short"](-5),
                        self.signals[d._name]["upper_shadow"] == 1,
                    ),
                    # 情况 2：低点上移 - 收敛上涨
                    bt.And(
                        self.inds[d._name]["lowest_short"]
                        > self.inds[d._name]["lowest_short"](-5),
                        d.close > d.close(-5),
                        (
                            self.inds[d._name]["highest_short"]
                            - self.inds[d._name]["lowest_short"]
                        )
                        < (
                            self.inds[d._name]["highest_short"](-5)
                            - self.inds[d._name]["lowest_short"](-5)
                        ),
                    ),
                    # 情况 3：高点上移
                    bt.And(
                        self.inds[d._name]["highest_short"]
                        > self.inds[d._name]["highest_short"](-5),
                        d.close > d.close(-5),
                        self.inds[d._name]["lowest_short"]
                        >= self.inds[d._name]["lowest_short"](-5),
                    ),
                )

                self.signals[d._name]["price_lower"] = bt.Or(
                    # 情况 1：创新低
                    bt.And(
                        d.close <= self.inds[d._name]["lowest_short"],
                        d.close < self.inds[d._name]["lowest_short"](-5),
                    ),
                    # 情况 2：低点下移
                    bt.And(
                        self.inds[d._name]["highest_short"]
                        < self.inds[d._name]["highest_short"](-5),
                        d.close < d.close(-5),
                        self.inds[d._name]["lowest_short"]
                        <= self.inds[d._name]["lowest_short"](-5),
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 价格上涨和下跌指标：{d._name}, {e}")

            """
            辅助指标 2：Dif 和 Dea 的关系
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
                    bt.indicators.crossover.CrossUp(self.inds[d._name]["dif"], 0) == 1,
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
                    bt.indicators.crossover.CrossDown(self.inds[d._name]["dif"], 0)
                    == 1,
                    bt.And(
                        self.inds[d._name]["dif"] < self.inds[d._name]["dea"],
                        self.inds[d._name]["dif"] < self.inds[d._name]["dif"](-1),
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 辅助指标 2: {d._name}, {e}")

            """
            辅助指标 3：收盘价上穿短期均线
            """
            try:
                self.signals[d._name]["close_crossup_ema_short"] = (
                    bt.indicators.crossover.CrossUp(
                        d.close, self.inds[d._name]["ema_short"]
                    )
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 辅助指标 3: {d._name}, {e}")

            """ 
            辅助指标 4：乖离率判断（优化版：收紧到 5%）
            """
            try:
                # 收紧：从 20% 改为 5%，同时增加下限 -10% 防止超跌
                self.signals[d._name]["deviant"] = bt.And(
                    (d.close - self.inds[d._name]["sma_short"])
                    / self.inds[d._name]["sma_short"]
                    <= 0.10,
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 辅助指标 4: {d._name}, {e}")

            """ 
            辅助指标 5：波动过大，避免频繁交易
            """
            try:
                self.signals[d._name]["close_crossdown_sma"] = bt.And(
                    bt.indicators.crossover.CrossDown(
                        d.close, self.inds[d._name]["sma_short"]
                    ),
                    self.inds[d._name]["sma_short"]
                    < self.inds[d._name]["sma_short"](-1),
                    self.signals[d._name]["death_cross"] == 1,
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 辅助指标 5: {d._name}, {e}")

            """
            辅助指标 6：判断是否资金净流入
            """
            try:
                # 市场因子
                factor = 100 if self.market.startswith("cn") else 1
                # 方法 A: 每日净流入 = (close - open) * volume * factor
                daily_net_inflow = (d.close - d.open) * d.volume * factor

                self.inds[d._name]["is_net_inflow_mid"] = bt.indicators.SMA(
                    daily_net_inflow, period=self.params.vol_mid_period
                )
                self.inds[d._name]["is_net_inflow_long"] = bt.indicators.SMA(
                    daily_net_inflow, period=self.params.vol_long_period
                )

            except Exception as e:
                print(f"❌ 初始化失败 - 辅助指标 7: {d._name}, {e}")

            """
            买入 1: 均线金叉
            """
            try:
                self.signals[d._name]["ma_crossover_bullish"] = bt.Or(
                    bt.And(
                        self.inds[d._name]["sma_mid"]
                        > self.inds[d._name]["sma_mid"](-1),
                        bt.indicators.crossover.CrossUp(
                            self.inds[d._name]["sma_short"],
                            self.inds[d._name]["sma_mid"],
                        )
                        == 1,
                        self.signals[d._name]["golden_cross"] == 1,
                        d.close > d.open,
                        self.signals[d._name]["deviant"] == 1,
                        self.inds[d._name]["is_net_inflow_long"] > 0,
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
                        self.inds[d._name]["is_net_inflow_long"] > 0,
                    ),
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 买入 1: {d._name}, {e}")

            """
            买入 2: 收盘价连续上涨
            """
            try:
                self.signals[d._name]["close_rising"] = bt.And(
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
                    self.inds[d._name]["is_net_inflow_mid"] > 0,
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 买入 2: {d._name}, {e}")
            """ 
            买入 3: 均线多头排列
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
                    self.inds[d._name]["is_net_inflow_long"] > 0,
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 买入 3: {d._name}, {e}")

            """ 
            买入 4: 成交量突然增加，价格走高
            """
            try:
                self.signals[d._name]["volume_breakout"] = bt.And(
                    self.inds[d._name]["emavol_short"]
                    > self.inds[d._name]["emavol_long"],
                    d.volume > self.inds[d._name]["emavol_short"] * 1.5,
                    self.signals[d._name]["price_higher"] == 1,
                    self.signals[d._name]["deviant"] == 1,
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 买入 4: {d._name}, {e}")

            """
            买入 5: 穿越年线
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
                    self.inds[d._name]["is_net_inflow_long"] > 0,
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 买入 5: {d._name}, {e}")

            """
            买入 6: 穿越半年线
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
                    self.inds[d._name]["is_net_inflow_long"] > 0,
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 买入 6: {d._name}, {e}")

            """
            卖出 1: 均线死叉
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
                print(f"❌ 初始化失败 - 卖出 1: {d._name}, {e}")
            """
            卖出 2: 收盘价连续下跌
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
                print(f"❌ 初始化失败 - 卖出 2: {d._name}, {e}")
            """ 
            卖出 3: 均线空头排列
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
                print(f"❌ 初始化失败 - 卖出 3: {d._name}, {e}")
            """ 
            卖出 4: 跌破年线
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
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 卖出 4: {d._name}, {e}")
            """
            卖出 5: 跌破半年线
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
                )
            except Exception as e:
                print(f"❌ 初始化失败 - 卖出 5: {d._name}, {e}")
            """ indicators 以及 signals 初始化进度打印 """
            t.progress_bar(len(self.datas), i)

    # ===== 新增：统一信号检查方法 =====
    def check_signal(self, symbol, signal_name):
        """统一检查信号状态，避免重复 [0] == 1"""
        return self.signals[symbol][signal_name][0] == 1

    # ===== 新增：均线收敛检查方法 =====
    def check_convergence_signal(self, symbol):
        """检查均线收敛信号（复杂逻辑提取）"""
        # 计算均线密集程度（近 20 日）
        x1 = self.inds[symbol]["ema_short"].get(
            ago=-1, size=self.params.ma_short_period
        )
        y1 = self.inds[symbol]["ema_mid"].get(ago=-1, size=self.params.ma_short_period)
        x2 = self.inds[symbol]["sma_short"].get(
            ago=-1, size=self.params.ma_short_period
        )
        y2 = self.inds[symbol]["sma_mid"].get(ago=-1, size=self.params.ma_short_period)
        diff_array = [abs((x - y) * 100 / y) for x, y in zip(x1, y1) if y > 1]
        diff_array2 = [abs((x - y) * 100 / y) for x, y in zip(x2, y2) if y > 1]
        diff_array3 = [abs((x - y) * 100 / y) for x, y in zip(x1, x2) if y > 1]

        return (
            sum(1 for value in diff_array if value < 2) >= 5
            and sum(1 for value in diff_array2 if value < 2) >= 5
            and sum(1 for value in diff_array3 if value < 2) >= 5
            and self.check_signal(symbol, "deviant")
            and self.inds[symbol]["is_net_inflow_long"] > 0
        )

    # ===== 新增：统一买入执行方法 =====
    def execute_buy(self, data, strategy_name, level):
        """统一买入执行逻辑"""
        symbol = data._name
        self.broker.cancel(self.order[symbol])
        self.order[symbol] = self.buy(data=data)
        self.myorder[symbol]["strategy"] = strategy_name
        self.current_signal[symbol] = (level, strategy_name, "initial")

    # ===== 新增：统一卖出执行方法 =====
    def execute_sell(self, data, strategy_name):
        """统一卖出执行逻辑"""
        symbol = data._name
        self.order[symbol] = self.close(data=data)
        self.myorder[symbol]["strategy"] = strategy_name
        self.peak_price[symbol] = None
        self.current_signal[symbol] = None

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
                    "{}, Buy {} Executed, Price: {:.2f} Size: {:.2f} SharpRatio: {} SortinoRatio: {} MaxDrawDown: {}".format(
                        bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
                        self.sharpe_ratios[order.data._name],
                        self.sortino_ratios[order.data._name],
                        self.max_drawdowns[order.data._name],
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
                    "{} Sell {} Executed, Price: {:.2f} Size: {:.2f} SharpRatio: {} SortinoRatio: {} MaxDrawDown: {}".format(
                        bt.num2date(order.executed.dt).strftime("%Y-%m-%d"),
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
                        self.sharpe_ratios[order.data._name],
                        self.sortino_ratios[order.data._name],
                        self.max_drawdowns[order.data._name],
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
            """ 头部 index 不计算，有 bug """
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
                # 条件 1: 有足够天数
                enough_data = len(self.daily_returns[d._name]) >= min(
                    self.params.rf_window, self.params.annual_period
                )
                # 条件 2: 无风险利率有效
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
                        # 没有下行波动，定义索提诺比率为年化平均收益除以小数值 1e-10 防止除零
                        self.sortino_ratios[d._name] = self.sharpe_ratios[d._name]

                    # ---- (3) 近 rf_window 日 Max Drawdown 计算 ----
                    if d._name not in self.max_drawdowns:
                        self.max_drawdowns[d._name] = 0.0

                    rets = np.array(self.daily_returns[d._name])

                    # 构造累计收益曲线（从 1 开始）
                    equity = np.cumprod(1.0 + rets)

                    # 历史最高点
                    peak = np.maximum.accumulate(equity)

                    # 回撤序列
                    drawdowns = equity / peak - 1.0

                    # 最大回撤（负值）
                    max_dd = np.min(drawdowns)

                    self.max_drawdowns[d._name] = max_dd

                else:
                    # 数据不足或无风险利率为 0 → 夏普比率置 0
                    self.sharpe_ratios[d._name] = None
                    self.sortino_ratios[d._name] = None
                    self.max_drawdowns[d._name] = None

            pos = self.getposition(d)
            """ 如果没有仓位就判断是否买卖 """
            if not pos:
                # ===== 优化后的信号优先级（使用统一方法）=====
                # 优先级 1: 多头排列（最高质量 - 多周期共振）
                if self.check_signal(d._name, "long_position"):
                    self.execute_buy(d, "多头排列", 1)

                # 优先级 2: 突破年线（长期趋势反转）
                elif self.check_signal(d._name, "close_crossup_annualline"):
                    self.execute_buy(d, "突破年线", 1)

                # 优先级 3: 均线金叉（中期趋势反转）
                elif self.check_signal(d._name, "ma_crossover_bullish"):
                    self.execute_buy(d, "均线金叉", 2)

                # 优先级 4: 均线收敛（趋势启动前 - 均线密集 + 突破）
                elif self.check_signal(
                    d._name, "close_crossup_ema_short"
                ) and self.check_convergence_signal(d._name):
                    self.execute_buy(d, "均线收敛", 2)

                # 优先级 5: 突破半年线（中期突破）
                elif self.check_signal(d._name, "close_crossup_halfannualline"):
                    self.execute_buy(d, "突破半年线", 2)

                # 优先级 6: 连续上涨（短期动量）
                elif self.check_signal(d._name, "close_rising"):
                    self.execute_buy(d, "连续上涨", 3)

                # 优先级 7: 成交量放大（量能确认）
                elif self.check_signal(d._name, "volume_breakout"):
                    self.execute_buy(d, "成交量放大", 3)

                # 优先级 8: 红三兵（短期形态 - 已放宽条件）
                elif self.check_signal(
                    d._name, "red_three_soldiers"
                ) and self.check_signal(d._name, "deviant"):
                    self.execute_buy(d, "红三兵", 3)
            else:
                # ===== 持仓期间：每日检查并更新当前满足的最高级别买入信号 =====
                # 检查所有买入信号，找到当前满足的最高级别（数字越小级别越高）
                # 注意：只升级不降级，符合投资逻辑（仓位级别只升不降）
                current_level, current_strategy, current_status = (
                    *self.current_signal.get(d._name, (None, None))[:2],
                    "initial",
                )
                self.current_signal[d._name] = (
                    current_level,
                    current_strategy,
                    current_status,
                )

                # 检查长线信号 (级别 1)
                if current_level >= 1:
                    if self.check_signal(d._name, "long_position"):
                        self.current_signal[d._name] = (1, "多头排列", "updated")
                    elif self.check_signal(d._name, "close_crossup_annualline"):
                        self.current_signal[d._name] = (1, "突破年线", "updated")

                # 检查趋势信号 (级别 2)
                if current_level >= 2:
                    if self.check_signal(d._name, "ma_crossover_bullish"):
                        self.current_signal[d._name] = (2, "均线金叉", "updated")
                    elif self.check_signal(
                        d._name, "close_crossup_ema_short"
                    ) and self.check_convergence_signal(d._name):
                        self.current_signal[d._name] = (2, "均线收敛", "updated")
                    elif self.check_signal(d._name, "close_crossup_halfannualline"):
                        self.current_signal[d._name] = (2, "突破半年线", "updated")

                # 检查短线信号 (级别 3)
                if current_level == 3:
                    if self.check_signal(d._name, "close_rising"):
                        self.current_signal[d._name] = (3, "连续上涨", "updated")
                    elif self.check_signal(d._name, "volume_breakout"):
                        self.current_signal[d._name] = (3, "成交量放大", "updated")
                    elif self.check_signal(
                        d._name, "red_three_soldiers"
                    ) and self.check_signal(d._name, "deviant"):
                        self.current_signal[d._name] = (3, "红三兵", "updated")

                # 更新当前满足的最高级别买入信号
                current_level, current_strategy, current_status = (
                    self.current_signal.get(d._name, (None, None, None))
                )

                # ===== 持仓数据处理 =====
                if len(d) <= d.buflen() - 1:
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
                        "max_drawdown": self.max_drawdowns[d._name],
                    }
                    list.append(dict)

                # 更新持仓期间最高价（用于移动止盈）
                if self.peak_price[d._name] is None:
                    self.peak_price[d._name] = d.close[0]
                else:
                    self.peak_price[d._name] = max(self.peak_price[d._name], d.close[0])

                # ===== 卖出逻辑（基于当前级别，检查该级别所有卖出信号）=====
                # 如果当前最高级别信号不再满足，检查是否需要卖出
                if current_status == "updated":
                    continue  # 升级后不立即检查卖出信号，等下一周期再检查，避免过度交易
                if current_level == 1:  # 长线信号不满足，检查长线卖出信号
                    if self.check_signal(d._name, "short_position"):
                        self.execute_sell(d, "空头排列")
                    elif self.check_signal(d._name, "ma_crossover_bearish"):
                        self.execute_sell(d, "均线死叉")
                    elif self.check_signal(d._name, "closs_crossdown_annualline"):
                        self.execute_sell(d, "跌破年线")
                    elif self.check_signal(d._name, "closs_crossdown_halfannualline"):
                        self.execute_sell(d, "跌破半年线")
                elif current_level == 2:  # 趋势信号不满足，检查趋势卖出信号
                    if self.check_signal(d._name, "ma_crossover_bearish"):
                        self.execute_sell(d, "均线死叉")
                    elif self.check_signal(d._name, "closs_crossdown_halfannualline"):
                        self.execute_sell(d, "跌破半年线")
                elif current_level == 3:  # 短线信号不满足，检查短线级别卖出信号
                    if self.check_signal(d._name, "close_falling"):
                        self.execute_sell(d, "连续下跌")
                # 止盈逻辑：如果价格从峰值回落超过一定比例（如 20%），且回落幅度超过买入价的一定比例（如 50%），则止盈卖出
                if self.peak_price[d._name] is not None:
                    if (
                        self.peak_price[d._name] >= pos.price * 1.2
                        and pos.adjbase
                        <= pos.price + (self.peak_price[d._name] - pos.price) * 0.5
                    ):
                        self.execute_sell(d, "移动止盈")
                    # 兜底止损（只在未卖出时检查）
                    elif d.close[0] <= pos.price * 0.85:
                        self.execute_sell(d, "低于买入价15%")

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
                5 天内累计上涨 10 个点以上
                5 天以上累计上 15 个点以上
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
        try:
            if df.empty:
                return  # 没有持仓数据，直接退出（但 finally 会执行关闭）
            if self.market in (
                "cn",
                "us",
                "us_special",
                "us_dynamic",
                "cn_dynamic",
                "us_backtest",
                "cn_backtest",
            ):
                df_o = pd.read_csv(self.file_industry, usecols=[i for i in range(1, 3)])
                df_n = pd.merge(df, df_o, how="left", on="symbol")
                df_n.sort_values(
                    by=["industry", "buy_date", "p&l_ratio"],
                    ascending=False,
                    inplace=True,
                )
            elif self.market == "cnetf":
                df_n = df.sort_values(by=["buy_date", "p&l_ratio"], ascending=False)
            df_n.reset_index(drop=True, inplace=True)
            df_n.to_csv(self.file_path_position, header=True, mode="w")
        finally:
            # 无论是否出现异常或提前 return，都会关闭文件
            self.file_path_position.close()
            self.file_path_position_detail.close()
            self.file_path_trade.close()
