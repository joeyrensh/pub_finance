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
from backtraderref.pandasdata_ext import BTPandasDataExt
from utility.stock_analysis import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc
from backtraderref.usfixedamount import FixedAmount
from matplotlib import rcParams
from utility.em_stock_uti import EMWebCrawlerUti

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
                "cumret": "#D9534F",  # 深蓝
                "drawdown": "#009900",  # 红色
                "table_edge": "#333333",
                "table_header": "#F5F5F5",
                "legend_text": "#333333",
            },
            "dark": {
                "text": "#ffffffc5",
                "background": "black",
                "grid": "#FFFFFF",
                "cumret": "#FF6B6B",  # 亮蓝
                "drawdown": "#009900",  # 亮红
                "table_edge": "#FFFFFF",
                "table_header": "#404040",
                "legend_text": "#ffffffc5",
            },
        }
        colors = theme_config[theme]

        # 全局样式设置
        rcParams.update(
            {
                "font.size": 20,
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
                "svg.fonttype": "none",
            }
        )
        return colors

    def plot_chart(theme="light"):
        """绘制图表（含表格和曲线）"""
        cols_names = [
            "Year",
            "Ann.R",
            "Cum.R",
            "Mx.DD",
            "D.Rsk",
        ]
        plt.rcParams["axes.unicode_minus"] = False  # 用来正常显示负号
        colors = configure_theme(theme)

        # 创建画布和子图
        fig, (ax_table, ax_chart) = plt.subplots(
            1,
            2,
            gridspec_kw={"width_ratios": [1, 3]},
            figsize=(18, 10),
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
            cell.set_text_props(color=colors["text"])
            cell.set_edgecolor(colors["table_edge"])
            cell.set_linewidth(1)

        # ----------------------------
        # 绘制双轴曲线
        # ----------------------------
        # 先绘制面积图（关键点1：先创建右轴）
        ax_drawdown = ax_chart.twinx()

        # 设置图层优先级（关键点2：强制主轴在上层）
        ax_chart.set_zorder(ax_drawdown.get_zorder() + 1)  # 主轴提升到上方
        ax_chart.patch.set_visible(False)  # 隐藏主轴背景避免遮挡

        # 绘制面积图（关键点3：使用 zorder 控制层级）
        ax_drawdown.fill_between(
            drawdown.index,
            drawdown.values,
            y2=0,
            color=colors["drawdown"],
            alpha=0.4,  # 适当降低透明度
            zorder=2,  # 设置较低层级
            edgecolor=colors["drawdown"],
            linewidth=1,
            label="Drawdown",
        )

        # 最后绘制折线图（自然覆盖在面积图上）
        ax_chart.plot(
            cumulative.index,
            cumulative.values,
            color=colors["cumret"],
            label="Cumulative Return",
            linewidth=2,
            zorder=3,  # 设置更高层级
            marker="o",
            markersize=4,
            markerfacecolor=colors["cumret"],
            markeredgecolor=colors["cumret"],
        )
        ax_chart.grid(True, alpha=0.4)
        ax_drawdown.grid(False)

        # ----------------------------
        # 图表美化
        # ----------------------------
        # X轴日期格式化
        ax_chart.xaxis.set_major_locator(ticker.AutoLocator())

        # 图例合并
        lines, labels = ax_chart.get_legend_handles_labels()
        lines2, labels2 = ax_drawdown.get_legend_handles_labels()
        legend = ax_chart.legend(
            lines + lines2,
            labels + labels2,
            loc="upper left",
            frameon=False,
            # fontsize=16,
        )
        for text in legend.get_texts():
            text.set_color(colors["legend_text"])

        # 隐藏冗余边框
        for spine in ax_chart.spines.values():
            spine.set_visible(False)
        for spine in ax_drawdown.spines.values():
            spine.set_visible(False)

        # 调整 y 轴刻度标签和轴位置
        ax_chart.tick_params(axis="x", rotation=-30)  # 将 x 轴刻度标签旋转 45 度
        ax_chart.tick_params(axis="y", labelright=False, labelleft=True, direction="in")
        ax_drawdown.tick_params(
            axis="y", labelright=True, labelleft=False, direction="in"
        )
        ax_drawdown.yaxis.set_label_coords(0.99, 0.5)  # 标签向左平移
        # 调整 y 轴 spines 的位置
        ax_chart.spines["left"].set_position(("axes", 0.02))  # 左侧 y 轴靠近图表
        ax_drawdown.spines["right"].set_position(("axes", 0.98))  # 右侧 y 轴靠近图表

        # 保存图片
        plt.savefig(
            f"./dashreport/assets/images/us_tr_{theme}.svg",
            format="svg",
            bbox_inches="tight",  # 保持边界紧凑
            transparent=True,  # 保持背景透明
            pad_inches=0.2,
        )
        plt.close()

    # 生成两种主题图表
    plot_chart(theme="light")
    plot_chart(theme="dark")

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
    em = EMWebCrawlerUti()
    em.get_daily_stock_info("us", trade_date)

    """ 执行bt相关策略 """
    cash, final_value = exec_btstrategy(trade_date)

    collected = gc.collect()

    print("Garbage collector: collected %d objects." % (collected))

    """ 发送邮件 """
    StockProposal("us", trade_date).send_btstrategy_by_email(cash, final_value)

    """ 结束进度条 """
    pbar.finish()
