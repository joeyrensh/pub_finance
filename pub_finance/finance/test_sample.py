#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import progressbar
from utility.toolkit import ToolKit
from datetime import datetime
import pandas as pd
import sys
from backtraderref.globalstrategyv3 import GlobalStrategy
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
from matplotlib import rcParams


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
    # perf_stats_all = pf.timeseries.perf_stats((pnl)).to_frame(name="All")

    # perf_stats = pd.concat([perf_stats_year, perf_stats_all.T], axis=0)

    perf_stats = pd.concat([perf_stats_year], axis=0)

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
        lambda x: x.map(lambda y: f"{y * 100:.2f}%")
    )

    # 绘制图形

    # ----------------------------
    # 绘图部分优化
    # ----------------------------

    def configure_theme(theme="light"):
        """统一配置主题颜色和样式"""
        theme_config = {
            "light": {
                "text": "#333333",
                "background": "white",
                "grid": "#333333",
                "cumret": "#2A5CAA",  # 深蓝
                "drawdown": "#D9534F",  # 红色
                "table_edge": "#333333",
                "table_header": "#F5F5F5",
            },
            "dark": {
                "text": "#FFFFFF",
                "background": "black",
                "grid": "#FFFFFF",
                "cumret": "#4C8BF5",  # 亮蓝
                "drawdown": "#FF6B6B",  # 亮红
                "table_edge": "#FFFFFF",
                "table_header": "#404040",
            },
        }
        colors = theme_config[theme]

        # 全局样式设置
        rcParams.update(
            {
                "font.size": 14,
                "axes.labelcolor": colors["text"],
                "axes.edgecolor": colors["text"],
                "xtick.color": colors["text"],
                "ytick.color": colors["text"],
                "grid.color": colors["grid"],
                "grid.linestyle": "-",
                "grid.alpha": 1,
                "grid.linewidth": 1.5,
                "figure.facecolor": colors["background"],
                "savefig.transparent": True,
            }
        )
        return colors

    def plot_chart(theme="light"):
        """绘制图表（含表格和曲线）"""
        cols_names = [
            "Date",
            "AnnualR",
            "CumR",
            "MaxDD",
            "DailyRisk",
        ]
        plt.rcParams["axes.unicode_minus"] = False  # 用来正常显示负号
        colors = configure_theme(theme)

        # 创建画布和子图
        fig, (ax_table, ax_chart) = plt.subplots(
            1,
            2,
            gridspec_kw={"width_ratios": [1, 4]},
            figsize=(20, 10),
            facecolor=colors["background"],
        )
        fig.subplots_adjust(wspace=0.1)

        # ----------------------------
        # 绘制表格
        # ----------------------------
        ax_table.axis("off")
        table = ax_table.table(
            cellText=perf_stats_.T.values,
            rowLabels=cols_names,
            bbox=[0, 0, 1, 1],
            cellLoc="center",
            edges="horizontal",
            # colColours=[colors["table_header"]] * len(cols_names),
        )
        # 统一单元格样式
        for cell in table.get_celld().values():
            cell.set_text_props(color=colors["text"], fontsize=18)
            cell.set_edgecolor(colors["table_edge"])
            cell.set_linewidth(1)

        # ----------------------------
        # 绘制双轴曲线
        # ----------------------------
        # 累计收益曲线（左轴）
        ax_chart.plot(
            cumulative.index,
            cumulative.values,
            color=colors["cumret"],
            label="Cumulative Return",
            linewidth=2.5,
        )
        ax_chart.set_ylabel("Cumulative Return", color=colors["cumret"])
        ax_chart.tick_params(axis="y", colors=colors["cumret"])
        ax_chart.grid(True, alpha=0.4)

        # 回撤曲线（右轴）
        ax_drawdown = ax_chart.twinx()
        ax_drawdown.plot(
            drawdown.index,
            drawdown.values,
            color=colors["drawdown"],
            label="Drawdown",
            linewidth=2,
            alpha=0.8,
        )
        ax_drawdown.set_ylabel("Drawdown", color=colors["drawdown"])
        ax_drawdown.tick_params(axis="y", colors=colors["drawdown"])
        ax_drawdown.grid(False)

        # ----------------------------
        # 图表美化
        # ----------------------------
        # X轴日期格式化
        ax_chart.xaxis.set_major_locator(ticker.AutoLocator())

        # 图例合并
        lines, labels = ax_chart.get_legend_handles_labels()
        lines2, labels2 = ax_drawdown.get_legend_handles_labels()
        ax_chart.legend(
            lines + lines2,
            labels + labels2,
            loc="upper left",
            frameon=False,
            fontsize=10,
        )

        # 隐藏冗余边框
        for spine in ax_chart.spines.values():
            spine.set_visible(False)
        for spine in ax_drawdown.spines.values():
            spine.set_visible(False)

        # 保存图片
        plt.savefig(
            f"./dashreport/assets/images/cnetf_tr_{theme}.png",
            dpi=600,
            bbox_inches="tight",
        )
        plt.close()

    # 生成两种主题图表
    plot_chart(theme="light")
    plot_chart(theme="dark")

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
