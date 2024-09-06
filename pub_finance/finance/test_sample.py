#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import progressbar
from utility.ToolKit import ToolKit
from datetime import datetime
import pandas as pd
import sys
from backtraderref.BTStrategyV2 import BTStrategyV2
import backtrader as bt
from utility.TickerInfo import TickerInfo
from cncrawler.EMCNWebCrawler import EMCNWebCrawler
from backtraderref.BTPandasDataExt import BTPandasDataExt
from utility.StockProposal import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc
from backtraderref.FixAmountCN import FixedAmount


""" backtrader策略 """


def exec_btstrategy(date):
    """创建cerebro对象"""
    cerebro = bt.Cerebro(stdstats=False, maxcpus=0)
    # cerebro.broker.set_coc(True)
    """ 添加bt相关的策略 """
    cerebro.addstrategy(BTStrategyV2, trade_date=date, market="cn")

    # 回测时需要添加 TimeReturn 分析器
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="_TimeReturn", fund=True)
    # cerebro.addobserver(bt.observers.BuySell)

    """ 初始资金100M """
    cerebro.broker.setcash(1000000.0)
    cerebro.broker.set_coc(True)  # 设置以当日收盘价成交
    """ 每手10股 """
    # cerebro.addsizer(bt.sizers.FixedSize, stake=100)
    # cerebro.addsizer(bt.sizers.PercentSizerInt, percents=0.5)
    cerebro.addsizer(FixedAmount, amount=10000)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0, stocklike=True)
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, "cn").get_backtrader_data_feed()
    """ 循环初始化数据进入cerebro """
    for h in list:
        """历史数据最早不超过2021-01-01"""
        data = BTPandasDataExt(
            dataname=h,
            name=h["symbol"][0],
            fromdate=datetime(2023, 1, 1),
            todate=datetime.strptime(date, "%Y%m%d"),
            datetime=-1,
            timeframe=bt.TimeFrame.Days,
        )
        cerebro.adddata(data)
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)
    """ 起始资金池 """
    print("\nStarting Portfolio Value: %.2f" % cerebro.broker.getvalue())

    # 节约内存
    del list
    gc.collect()

    """ 运行cerebro """
    result = cerebro.run()

    """ 最终资金池 """
    print("\n当前现金持有: ", cerebro.broker.get_cash())
    print("\nFinal Portfolio Value: %.2f" % cerebro.broker.getvalue())

    """ 画图相关 """
    # cerebro.plot(iplot=True, subplot=True)
    # 提取收益序列
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="_AnnualReturn")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="_DrawDown")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="_SharpeRatio")
    cerebro.plot(style="candle", volume=True, save=True, filename="./images/test.png")

    # plt.savefig("./images/CNTRdraw_dark.png", transparent=True)

    return round(cerebro.broker.get_cash(), 2), round(cerebro.broker.getvalue(), 2)


# 主程序入口
if __name__ == "__main__":
    """美股交易日期 utc+8"""
    trade_date = ToolKit("get_latest_trade_date").get_cn_latest_trade_date(0)

    """ 非交易日程序终止运行 """
    if ToolKit("判断当天是否交易日").is_cn_trade_date(trade_date):
        pass
    else:
        sys.exit()

    """ 定义程序显示的进度条 """
    widgets = [
        "doing task: ",
        progressbar.Percentage(),
        " ",
        progressbar.Bar(),
        " ",
        progressbar.ETA(),
    ]
    """ 创建进度条并开始运行 """
    pbar = progressbar.ProgressBar(maxval=100, widgets=widgets).start()

    print("trade_date is :", trade_date)

    """ 东方财经爬虫 """
    """ 爬取每日最新股票数据 """
    # em = EMCNWebCrawler()
    # em.get_cn_daily_stock_info(trade_date)

    """ 执行bt相关策略 """
    cash, final_value = exec_btstrategy(trade_date)

    collected = gc.collect()

    print("Garbage collector: collected %d objects." % (collected))

    """ 发送邮件 """
    StockProposal("cn", trade_date).send_btstrategy_by_email(712627.0, 2979999.0)

    """ 结束进度条 """
    pbar.finish()
