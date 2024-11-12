#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from tabulate import tabulate
import progressbar
from utility.toolkit import ToolKit
from datetime import datetime
import pandas as pd
import sys
from backtraderref.globalstrategyv2 import GlobalStrategy
import backtrader as bt
from utility.tickerinfo import TickerInfo
from uscrawler.eastmoney_incre_crawler import EMWebCrawler
from backtraderref.pandasdata_ext import BTPandasDataExt
from utility.stock_analysis import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc
from backtraderref.usfixedamount import FixedAmount

""" 执行策略 """
""" backtrader策略 """


def exec_btstrategy(date):
    """创建cerebro对象"""
    cerebro = bt.Cerebro(stdstats=False, maxcpus=0)
    # cerebro.broker.set_coc(True)
    """ 添加bt相关的策略 """
    cerebro.addstrategy(GlobalStrategy, trade_date=date, market="us")

    # 回测时需要添加 TimeReturn 分析器
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="_TimeReturn", fund=False)
    # cerebro.addobserver(bt.observers.BuySell)
    """ 每手10股 """
    # cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    # cerebro.addsizer(bt.sizers.PercentSizerInt, percents=0.5)
    cerebro.addsizer(FixedAmount, amount=10000)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0, stocklike=True)
    cerebro.broker.set_coc(True)  # 设置以当日收盘价成交
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, "us").get_backtrader_data_feed()
    """ 初始资金100M """
    start_cash = len(list) * 10000
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
        cerebro.adddata(data, name=h["symbol"][0])
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

    perf_stats = perf_stats.drop(columns=["Sortino ratio", "Tail ratio"])

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
        2, 1, gridspec_kw={"height_ratios": [1, 2]}, figsize=(20, 10)
    )

    """ 
    年度回报率 (Annual return)：衡量投资组合或股票在一年内的收益率。它通常以百分比表示，计算方法是将期末价值减去期初价值，再除以期初价值，并乘以100。

    累积回报率 (Cumulative returns)：衡量投资组合或股票在一段时间内的总收益率。它表示从投资开始到目前为止的总回报，可以用于评估长期投资的表现。

    年度波动率 (Annual volatility)：衡量股票或投资组合价格波动的程度。它是标准差的年化值，标准差衡量价格变动相对于其平均值的离散程度。较高的波动率意味着价格变动幅度较大。

    夏普比率 (Sharpe ratio)：衡量投资组合或股票每承担一单位风险所获得的超额回报。它是超额回报与波动率的比率，用于评估风险调整后的回报。

    卡尔马比率 (Calmar ratio)：衡量投资组合或股票的风险调整回报率。它是年度回报率与最大回撤之比，用于评估投资组合的风险收益特征。

    稳定性 (Stability)：衡量股票或投资组合价格的稳定性。较高的稳定性意味着价格波动较小。

    最大回撤 (Max drawdown)：衡量投资组合或股票价格从峰值到谷底的最大跌幅。它用于评估投资组合的风险承受能力和潜在损失。

    Omega比率 (Omega ratio)：衡量投资组合或股票正收益和负收益之间的比率。它将正收益的比例与负收益的比例进行比较，用于评估投资组合的收益分布特征。

    Sortino比率 (Sortino ratio)：类似于夏普比率，但只考虑下行风险，即价格下跌的风险。它是超额回报与下行波动率的比率，用于评估投资组合的风险调整后的回报。

    偏度 (Skew)：衡量股票或投资组合收益分布的偏斜程度。正偏度表示收益分布偏向较高的收益，负偏度表示偏向较低的收益。

    峰度 (Kurtosis)：衡量股票或投资组合收益分布的尖峰程度。它衡量收益分布相对于正态分布的尖峰或扁平程度。

    尾部比率 (Tail ratio)：衡量股票或投资组合收益分布的尾部风险。它是正尾部与负尾部之比，用于评估收益分布的不对称性和尾部风险。

    日风险价值 (Daily value at risk)：衡量股票或投资组合在一天内可能面临的最大损失。它是在给定置信水平下的损失金额，用于评估投资组合的风险暴露。 
    """

    cols_names = [
        "date",
        "Annual\nreturn",
        "Cum\nreturns",
        "Annual\nvolatility",
        "Sharpe\nratio",
        "Calmar\nratio",
        "Stability",
        "Max\ndrawdown",
        "Omega\nratio",
        # "Sortino\nratio",
        "Skew",
        "Kurtosis",
        # "Tail\nratio",
        "Daily value\nat risk",
    ]

    # 绘制表格
    ax0.set_axis_off()
    # 除去坐标轴
    table = ax0.table(
        cellText=perf_stats_.values,
        bbox=(0, 0, 1, 1),
        # 设置表格位置， (x0, y0, width, height)
        rowLoc="left",
        # 行标题居中
        cellLoc="left",
        colLabels=cols_names,
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
        alpha=1,
        fontsize=20,
        grid=False,
        color="#0b9e0b",
    )

    # 绘制累计收益曲线
    (cumulative).plot(
        ax=ax2,
        lw=4.0,
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
    for cell in table.get_celld().values():
        cell.set_text_props(color="black")
    # Set the font color of the trend graph
    ax1.tick_params(axis="x", colors="black")
    for label in ax1.get_xticklabels():
        label.set_color("black")
    ax2.yaxis.label.set_color("black")
    ax2.tick_params(axis="y", colors="black")
    ax1.yaxis.label.set_color("black")
    ax1.tick_params(axis="y", colors="black")
    ax2.spines["right"].set_color("black")
    fig.tight_layout()
    plt.savefig("./images/us_tr_light.png", transparent=True)
    # Set the font color of the table cells to white
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
    plt.savefig("./images/us_tr_dark.png", transparent=True)

    return round(cerebro.broker.get_cash(), 2), round(cerebro.broker.getvalue(), 2)


# 主程序入口
if __name__ == "__main__":
    """美股交易日期 utc-4"""
    trade_date = ToolKit("get latest trade date").get_us_latest_trade_date(0)

    """ 非交易日程序终止运行 """
    if ToolKit("判断当天是否交易日").is_us_trade_date(trade_date):
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

    """ 东方财经爬虫 """
    """ 爬取每日最新股票数据 """
    # em = EMWebCrawler()
    # em.get_us_daily_stock_info(trade_date)

    # """ 执行策略 """
    # df = exec_strategy(trade_date)
    # """ 发送邮件 """
    # if not df.empty:
    #     StockProposal("us", trade_date).send_strategy_df_by_email(df)

    """ 执行bt相关策略 """
    # cash, final_value = exec_btstrategy(trade_date)

    collected = gc.collect()

    print("Garbage collector: collected %d objects." % (collected))

    """ 发送邮件 """
    StockProposal("us", trade_date).send_btstrategy_by_email(3337908.65, 11876002.6)

    """ 结束进度条 """
    pbar.finish()


# df_trade = pd.read_csv(
#     "./usstockinfo/trade_20241023.csv",
#     header=None,
# )
# df_trade.columns = ["index", "symbol", "date", "action", "base", "volume", "strategy"]

# # 按symbol分组，按date倒序排序，然后保留每个分组中date最大的记录
# trade_result = df_trade.sort_values(
#     by=["symbol", "date"], ascending=[True, False]
# ).drop_duplicates(subset="symbol", keep="first")
# trade_result = trade_result[trade_result["action"] == "buy"]

# std_trade_result = trade_result.groupby(["strategy"])["symbol"].count()


# df_pos = pd.read_csv(
#     "./usstockinfo/position_20241023.csv",
# )

# result = df_pos[df_pos["p&l"] > 0].count()
# print(std_trade_result)
