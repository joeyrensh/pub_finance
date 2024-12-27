#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import progressbar
from utility.toolkit import ToolKit
from datetime import datetime
import pandas as pd
import sys
from backtraderref.globalstrategyv2 import GlobalStrategy
import backtrader as bt
from utility.tickerinfo import TickerInfo
from cncrawler.eastmoney_incre_download import EMCNWebCrawler
from backtraderref.pandasdata_ext import BTPandasDataExt
from utility.stock_analysis import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc
from backtraderref.cnfixedamount import FixedAmount


""" backtrader策略 """


def exec_btstrategy(date):
    """创建cerebro对象"""
    cerebro = bt.Cerebro(stdstats=False, maxcpus=0)
    # cerebro.broker.set_coc(True)
    """ 添加bt相关的策略 """
    cerebro.addstrategy(GlobalStrategy, trade_date=date, market="cnetf")

    # 回测时需要添加 TimeReturn 分析器
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="_TimeReturn", fund=False)
    # cerebro.addobserver(bt.observers.BuySell)
    cerebro.broker.set_coc(True)  # 设置以当日收盘价成交
    """ 每手10股 """
    # cerebro.addsizer(bt.sizers.FixedSize, stake=100)
    # cerebro.addsizer(bt.sizers.PercentSizerInt, percents=2)
    cerebro.addsizer(FixedAmount, amount=10000)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0, stocklike=True)
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, "cn").get_etf_backtrader_data_feed()
    """ 初始资金100M """
    start_cash = len(list) * 20000
    cerebro.broker.setcash(start_cash)
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
    del data
    gc.collect()

    """ 运行cerebro """
    result = cerebro.run()

    """ 最终资金池 """
    print("\n当前现金持有: ", cerebro.broker.get_cash())
    print("\nFinal Portfolio Value: %.2f" % cerebro.broker.getvalue())

    """ 画图相关 """
    # cerebro.plot(iplot=True, subplot=True)
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
        .groupby(pnl.index.to_period("Y"))
        .apply(lambda data: pf.timeseries.perf_stats(data))
        .unstack()
    )

    # 统计所有时间段的收益指标
    perf_stats_all = pf.timeseries.perf_stats((pnl)).to_frame(name="all")

    perf_stats = pd.concat([perf_stats_year, perf_stats_all.T], axis=0)

    perf_stats = perf_stats.drop(
        columns=[
            "Annual volatility",
            "Sortino ratio",
            "Tail ratio",
            "Sharpe ratio",
            "Calmar ratio",
            "Stability",
            "Omega ratio",
            "Skew",
            "Kurtosis",
        ]
    )

    perf_stats_ = perf_stats.reset_index()
    perf_stats_[perf_stats_.columns[1:]] = perf_stats_[perf_stats_.columns[1:]].apply(
        lambda x: x.map(lambda y: f"{y*100:.2f}%")
    )

    # 绘制图形

    plt.rcParams["axes.unicode_minus"] = False  # 用来正常显示负号

    # 导入设置坐标轴的模块
    # sns.set()
    # plt.style.use("seaborn")
    # plt.style.use('dark_background')

    fig, (ax0, ax1) = plt.subplots(
        1, 2, gridspec_kw={"width_ratios": [1, 4]}, figsize=(20, 8)
    )

    cols_names = [
        "Date",
        "AnnualR",
        "CumR",
        "MaxDD",
        "DailyRisk",
    ]

    # 绘制表格
    ax0.set_axis_off()
    # 除去坐标轴
    # 设置新的列标题
    perf_stats_transposed = perf_stats_.T

    table = ax0.table(
        cellText=perf_stats_transposed.values,
        bbox=(0, 0, 1, 1),
        # 设置表格位置， (x0, y0, width, height)
        rowLoc="left",
        # 行标题居中
        cellLoc="left",
        # colLabels=perf_stats_transposed.columns,
        rowLabels=cols_names,
        # 设置列标题
        colLoc="left",
        # 列标题居中
        edges="open",  # 不显示表格边框
    )

    # # Access the cells and modify font properties
    # for cell in table.get_celld().values():
    #     cell.set_fontsize(18)  # Adjust the font size as per your preference

    # Set the font size of the table title
    table.auto_set_font_size(False)
    table.set_fontsize(20)

    # 绘制累计收益曲线
    ax2 = ax1.twinx()

    ax1.yaxis.set_ticks_position("right")
    # 将回撤曲线的 y 轴移至右侧
    ax2.yaxis.set_ticks_position("left")
    # 将累计收益曲线的 y 轴移至左侧
    # 绘制回撤曲线
    drawdown.plot.area(
        ax=ax1,
        label="drawdown (right)",
        rot=0,
        alpha=0.7,
        fontsize=20,
        grid=False,
        color="green",
    )

    # 绘制累计收益曲线
    (cumulative).plot(
        ax=ax2,
        lw=3.0,
        label="cumret (left)",
        rot=0,
        fontsize=20,
        grid=True,
        color="#FF4136",
    )
    ax2.set_facecolor("none")

    # 不然 x 轴留有空白
    ax2.set_xbound(lower=cumulative.index.min(), upper=cumulative.index.max())

    # 主轴定位器：每 5 个月显示一个日期：根据具体天数来做排版
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(120))
    # 同时绘制双轴的图例
    h1, l1 = ax1.get_legend_handles_labels()

    h2, l2 = ax2.get_legend_handles_labels()

    plt.legend(h1 + h2, l1 + l2, fontsize=20, loc="upper left", ncol=1)
    # Set the font color of the table cells to white
    # for key, cell in table.get_celld().items():
    #     cell.PAD = 0.1  # 设置单元格边距
    for cell in table.get_celld().values():
        cell.set_text_props(color="white")
    # Set the font color of the trend graph
    ax1.tick_params(axis="x", colors="white")
    for label in ax1.get_xticklabels():
        label.set_color("white")
    ax2.yaxis.label.set_color("white")
    ax2.tick_params(axis="y", colors="white")
    ax1.yaxis.label.set_color("white")
    ax1.tick_params(axis="y", colors="white")
    ax2.spines["right"].set_color("white")
    fig.tight_layout()
    plt.savefig("./images/performance_report.png", transparent=True)

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
    em = EMCNWebCrawler()
    em.get_cn_daily_stock_info(trade_date)

    """ 执行bt相关策略 """
    cash, final_value = exec_btstrategy(trade_date)

    collected = gc.collect()

    print("Garbage collector: collected %d objects." % (collected))

    """ 发送邮件 """
    StockProposal("cn", trade_date).send_etf_btstrategy_by_email(cash, final_value)

    """ 结束进度条 """
    pbar.finish()
