#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from utility.FileInfo import FileInfo
from tabulate import tabulate
import progressbar
from utility.ToolKit import ToolKit
from utility.MyEmail import MyEmail
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
import seaborn as sns
from backtraderref.BTStrategyVol import BTStrategyVol
import backtrader as bt
from utility.TickerInfo import TickerInfo
from cncrawler.EMCNWebCrawler import EMCNWebCrawler
from cncrawler.EMCNTickerCategoryCrawler import EMCNTickerCategoryCrawler
from backtraderref.BTPandasDataExt import BTPandasDataExt
from utility.StockProposal import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc


""" backtrader策略 """


def exec_btstrategy(date):
    """创建cerebro对象"""
    cerebro = bt.Cerebro(stdstats=False)
    """ 添加bt相关的策略 """
    cerebro.addstrategy(BTStrategyVol)

    # 回测时需要添加 TimeReturn 分析器
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="_TimeReturn")
    cerebro.addobserver(bt.observers.BuySell)
        
    """ 初始资金100M """
    cerebro.broker.setcash(2000000.0)
    """ 每手10股 """
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0.001, stocklike=True)
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, "cn").get_backtrader_data_feed()
    """ 循环初始化数据进入cerebro """
    for h in list:
        print("正在初始化: ", h["symbol"][0])
        """ 历史数据最早不超过2021-01-01 """
        data = BTPandasDataExt(
            dataname=h, name=h["symbol"][0], fromdate=datetime(2023, 1, 1)
        )
        cerebro.adddata(data)
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)
    """ 起始资金池 """
    print("Starting Portfolio Value: %.2f" % cerebro.broker.getvalue())



    # 节约内存
    del list
    gc.collect()

    """ 运行cerebro """
    result = cerebro.run()
    


    """ 最终资金池 """
    print("当前现金持有: ", cerebro.broker.get_cash())
    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())

    """ 画图相关 """
    # 提取收益序列
    pnl = pd.Series(result[0].analyzers._TimeReturn.get_analysis())

    # 计算累计收益
    cumulative = (pnl + 1).cumprod()

    # 计算回撤序列
    max_return = cumulative.cummax()

    drawdown = (cumulative - max_return) / max_return

    # 按年统计收益指标
    perf_stats_year = (
        (pnl)
        .groupby(pnl.index.to_period("y"))
        .apply(lambda data: pf.timeseries.perf_stats(data))
        .unstack()
    )

    # 统计所有时间段的收益指标
    perf_stats_all = pf.timeseries.perf_stats((pnl)).to_frame(name="all")

    perf_stats = pd.concat([perf_stats_year, perf_stats_all.T], axis=0)

    perf_stats_ = round(perf_stats, 4).reset_index()

    # 绘制图形

    plt.rcParams["axes.unicode_minus"] = False  # 用来正常显示负号

    # 导入设置坐标轴的模块
    sns.set()
    # plt.style.use("seaborn")
    # plt.style.use('dark_background')

    fig, (ax0, ax1) = plt.subplots(
        2, 1, gridspec_kw={"height_ratios": [1.5, 4]}, figsize=(20, 8)
    )

    cols_names = [
        "date",
        "Annual\nreturn",
        "Cumulative\nreturns",
        "Annual\nvolatility",
        "Sharpe\nratio",
        "Calmar\nratio",
        "Stability",
        "Max\ndrawdown",
        "Omega\nratio",
        "Sortino\nratio",
        "Skew",
        "Kurtosis",
        "Tail\nratio",
        "Daily value\nat risk",
    ]

    # 绘制表格
    ax0.set_axis_off()
    # 除去坐标轴
    table = ax0.table(
        cellText=perf_stats_.values,
        bbox=(0, 0, 1, 1),
        # 设置表格位置， (x0, y0, width, height)
        rowLoc="right",
        # 行标题居中
        cellLoc="right",
        colLabels=cols_names,
        # 设置列标题
        colLoc="right",
        # 列标题居中
        edges="open",  # 不显示表格边框
    )

    table.set_fontsize(13)

    # 绘制累计收益曲线
    ax2 = ax1.twinx()

    ax1.yaxis.set_ticks_position("right")
    # 将回撤曲线的 y 轴移至右侧
    ax2.yaxis.set_ticks_position("left")
    # 将累计收益曲线的 y 轴移至左侧
    # 绘制回撤曲线
    drawdown.plot.area(
        ax=ax1, label="drawdown (right)", rot=0, alpha=0.3, fontsize=13, grid=False, color='red'
    )

    # 绘制累计收益曲线
    cumulative_line = (cumulative).plot(
        ax=ax2,
        lw=2.0,
        label="cumret (left)",
        rot=0,
        fontsize=13,
        grid=True,
        color='blue'
    )
    ax2.set_facecolor('lightgray')

    # 不然 x 轴留有空白
    ax2.set_xbound(lower=cumulative.index.min(), upper=cumulative.index.max())

    # 主轴定位器：每 5 个月显示一个日期：根据具体天数来做排版
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(100))

    # 同时绘制双轴的图例
    h1, l1 = ax1.get_legend_handles_labels()

    h2, l2 = ax2.get_legend_handles_labels()

    plt.legend(h1 + h2, l1 + l2, fontsize=12, loc="upper left", ncol=1)

    fig.tight_layout()
    plt.savefig("CNTRdraw.png")


# 主程序入口
if __name__ == "__main__":
    """美股交易日期 utc+8"""
    trade_date = ToolKit("获取最新A股交易日期").get_cn_latest_trade_date(0)

    """ 非交易日程序终止运行 """
    if ToolKit("判断当天是否交易日").is_cn_trade_date():
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
    em = EMCNWebCrawler()
    em.get_cn_daily_stock_info(trade_date)

    """ 执行bt相关策略 """
    exec_btstrategy(trade_date)

    collected = gc.collect()

    print("Garbage collector: collected %d objects." % (collected))

    """ 发送邮件 """
    StockProposal("cn", trade_date).send_btstrategy_by_email()

    """ 结束进度条 """
    pbar.finish()
