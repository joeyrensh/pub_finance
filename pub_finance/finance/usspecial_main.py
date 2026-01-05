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
import matplotlib.colors as mcolors
from utility.em_stock_uti import EMWebCrawlerUti
import numpy as np

""" 执行策略 """
""" backtrader策略 """


def exec_btstrategy(date):
    """创建cerebro对象"""
    cerebro = bt.Cerebro(stdstats=False, maxcpus=0)
    # cerebro.broker.set_coc(True)
    """ 添加bt相关的策略 """
    cerebro.addstrategy(GlobalStrategy, trade_date=date, market="us_special")

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
    list = TickerInfo(date, "us_special").get_special_us_backtrader_data_feed()
    """ 初始资金100M """
    start_cash = len(list) * 10000
    cerebro.broker.setcash(start_cash)
    """ 循环初始化数据进入cerebro """
    for h in list:
        """历史数据最早不超过2021-01-01"""
        data = BTPandasDataExt(
            dataname=h,
            name=h["symbol"][0],
            fromdate=datetime(2024, 1, 1),
            # todate=datetime.strptime(date, "%Y%m%d"),
            datetime=-1,
            timeframe=bt.TimeFrame.Days,
        )
        cerebro.adddata(data, name=h["symbol"][0])
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)
    """ 起始资金池 """
    print("\nStarting Portfolio Value: %.2f" % cerebro.broker.getvalue())

    # 节约内存
    list = None
    data = None
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

    # 去掉最后一个虚拟 bar
    pnl = pnl.iloc[:-1]

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
                "drawdown": "#3d9970",  # 红色
                "table_edge": "#333333",
                "table_header": "#F5F5F5",
                "legend_text": "#333333",
            },
            "dark": {
                "text": "#ffffffc5",
                "background": "black",
                "grid": "#FFFFFF",
                "cumret": "#FF6B6B",  # 亮蓝
                "drawdown": "#3d9970",  # 亮红
                "table_edge": "#FFFFFF",
                "table_header": "#404040",
                "legend_text": "#ffffffc5",
            },
        }
        colors = theme_config[theme]

        # 全局样式设置
        rcParams.update(
            {
                "font.size": 30,
                "axes.labelcolor": colors["text"],
                "axes.edgecolor": colors["text"],
                "xtick.color": colors["text"],
                "ytick.color": colors["text"],
                "grid.color": colors["grid"],
                "grid.linestyle": "-",
                "grid.alpha": 0.2,
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
            figsize=(20, 12.5),
            facecolor=colors["background"],
        )

        perf_stats_display = perf_stats_.copy()
        if len(perf_stats_display) > 2:
            perf_stats_display = perf_stats_display.iloc[-2:]

        def shorten_percent(text: str) -> str:
            """
            输入：带百分号的字符串 "1565.10%"
            输出：超过1000%的用 K，超过1_000_000%用 M，否则保留原样
            """
            try:
                # 提取数字部分
                v = float(text.strip().replace("%", ""))
            except:
                # 如果不能转数字，原样返回
                return text

            a = abs(v)
            if a >= 1000:
                return f"{v/1_000:.2f}K%"
            else:
                return f"{v:.2f}%"

        cellText = [
            [shorten_percent(x) for x in row] for row in perf_stats_display.T.values
        ]

        # ----------------------------
        # 绘制表格
        # ----------------------------
        ax_table.axis("off")
        table = ax_table.table(
            cellText=cellText,
            rowLabels=cols_names,
            bbox=[0, 0, 1, 1],
            cellLoc="center",
            edges="horizontal",
            # colColours=[colors["table_header"]] * len(cols_names),
        )
        table.auto_set_font_size(False)
        table.auto_set_column_width(range(len(perf_stats_.T.columns)))
        # 统一单元格样式
        for (row, col), cell in table.get_celld().items():
            # 跳过表头（row==0），只处理第二列（col==1）
            if col == 1 and row != 0:
                try:
                    # 获取单元格的原始数值（去掉百分号等）
                    val_str = (
                        str(cell.get_text().get_text())
                        .replace("%", "")
                        .replace(",", "")
                    )
                    val = float(val_str)
                    if val < 0:
                        cell.set_text_props(color="#0d876d")
                    elif val >= 0:
                        cell.set_text_props(color="#e01c3a")
                except Exception:
                    pass  # 非数字或转换失败时跳过
            else:
                cell.set_text_props(color=colors["text"])
            cell.set_edgecolor(mcolors.to_rgba(colors["table_edge"], alpha=0.2))
            cell.set_linewidth(1)

        # ----------------------------
        # 绘制双轴曲线
        # ----------------------------
        # 先绘制面积图（关键点1：先创建右轴）
        ax_drawdown = ax_chart.twinx()

        # 设置图层优先级（关键点2：强制主轴在上层）
        ax_chart.set_zorder(ax_drawdown.get_zorder() + 1)  # 主轴提升到上方
        ax_chart.patch.set_visible(False)  # 隐藏主轴背景避免遮挡

        # ========== CUMULATIVE RETURN 图 ========== #
        ax_chart.plot(
            cumulative.index,
            cumulative.values,
            color=colors["cumret"],
            label="Cumulative Return",
            linewidth=2,
            zorder=3,
            marker="o",
            markersize=4,
            markerfacecolor=colors["cumret"],
            markeredgecolor=colors["cumret"],
        )
        ax_chart.grid(True, alpha=0.25)
        # 最大收益点标注（非必须但非常专业）
        cum_max_idx = cumulative.idxmax()
        cum_max_val = cumulative.max()
        ax_chart.scatter(
            cum_max_idx, cum_max_val, color=colors["cumret"], s=55, zorder=4
        )
        ax_chart.text(
            cum_max_idx,
            cum_max_val,
            f" Max: {cum_max_val:.2f}",
            color=colors["text"],
            ha="right",
            va="bottom",
        )
        # ========== DRAWDOWN 图（最小改动，但增强可读性） ========== # 区域图：更专业的 fill_between
        ax_drawdown.fill_between(
            drawdown.index,
            drawdown.values,
            0,
            color=colors["drawdown"],
            alpha=0.35,
            zorder=1,
            label="Drawdown",
        )
        # 使用 step plot 强化 Drawdown 的阶段感（最小新增）
        ax_drawdown.step(
            drawdown.index,
            drawdown.values,
            where="post",
            color=colors["drawdown"],
            linewidth=1.5,
            alpha=0.9,
            zorder=2,
        )
        # 最大回撤水平线
        max_dd = drawdown.min()
        max_dd_idx = drawdown.idxmin()
        ax_drawdown.axhline(
            max_dd, linestyle="--", color=colors["drawdown"], linewidth=2, zorder=3
        )
        # 最大回撤点标注
        ax_drawdown.text(
            max_dd_idx,
            max_dd,
            f"Max DD: {max_dd:.2%}",
            color=colors["text"],
            ha="right",
            va="bottom",
        )

        # --- 设置排除窗口（天数） ---
        end_date = drawdown.index.max()

        # ===============================
        #   参数化定义：两个回撤窗口
        # ===============================
        window_A_days = 120  # 例如：120天窗口
        window_B_days = 30  # 例如：90天窗口

        # ===============================
        #   计算两个窗口的最大回撤点
        # ===============================

        def get_window_dd(drawdown, days, end_date):
            """返回给定窗口大小的最大回撤点 idx 与 val"""
            window_start = end_date - pd.Timedelta(days=days)
            dd_window = drawdown[drawdown.index >= window_start]

            if len(dd_window) > 0:
                idx = dd_window.idxmin()
                val = dd_window.loc[idx]
                return idx, val
            return None, None

        # 获取两个窗口的最大回撤
        dd_A_idx, dd_A_val = get_window_dd(drawdown, window_A_days, end_date)
        dd_B_idx, dd_B_val = get_window_dd(drawdown, window_B_days, end_date)

        # ===============================
        #   参数化窗口名称（用于文字标签）
        # ===============================
        label_A = f"{window_A_days}D"
        label_B = f"{window_B_days}D"

        dd_candidates = []

        # --- 全局最大和窗口内检测是否会重叠 ---
        if dd_A_idx is not None and dd_A_idx != max_dd_idx:
            dd_candidates.append((label_A, dd_A_idx, dd_A_val))
        if dd_B_idx is not None and dd_B_idx != max_dd_idx and dd_B_idx != dd_A_idx:
            dd_candidates.append((label_B, dd_B_idx, dd_B_val))

        # ===============================
        # 汇总回撤点
        # ===============================
        dd_points = {"max": max_dd}
        if dd_A_val is not None:
            dd_points[label_A] = dd_A_val
        if dd_B_val is not None:
            dd_points[label_B] = dd_B_val

        dd_vals = [v for _, v in dd_points.items()]
        dd_range = max(dd_vals) - min(dd_vals)
        dd_range = max(dd_range, 1e-6)
        # ===============================
        # 默认：120D / 30D 都向上
        # ===============================
        offset_sign = {
            label_A: 1,
            label_B: 1,
        }

        # ===============================
        # 是否需要特殊处理（非常接近）
        # ===============================
        if dd_A_val is not None and dd_B_val is not None:
            gap_AB = abs(dd_A_val - dd_B_val)

            # 判定“非常接近”
            if gap_AB < dd_range * 0.08:
                # 找出更深的那个
                deeper_label, deeper_val = (
                    (label_A, dd_A_val) if dd_A_val < dd_B_val else (label_B, dd_B_val)
                )

                # 假设更深的向下
                test_offset = min(gap_AB * 0.6, dd_range * 0.15)
                test_val = deeper_val - test_offset

                # 检查是否会贴近 max DD
                if abs(test_val - max_dd) > dd_range * 0.08:
                    # 可以下移
                    offset_sign[deeper_label] = -1

        ylim = ax_drawdown.get_ylim()
        y_top_threshold = ylim[1] - (ylim[1] - ylim[0]) * 0.05  # 顶部5%区域
        for label, idx, val in dd_candidates:
            ax_drawdown.scatter(idx, val, color=colors["drawdown"], s=55, zorder=4)
            min_dist = min(abs(val - v) for k, v in dd_points.items() if k != label)
            offset_mag = min(min_dist * 0.6, dd_range * 0.15)

            y_offset = offset_sign[label] * offset_mag
            # ====== 新增补丁: 避免接近右上角 ======
            if (val + y_offset) > y_top_threshold:
                y_offset = y_top_threshold - val - 0.01 * dd_range  # 微调向下
                va_pos = "top"
            else:
                va_pos = "bottom" if y_offset >= 0 else "top"
            # 应用偏移
            ax_drawdown.text(
                idx,
                val + y_offset,  # 根据你的Y轴范围调整系数
                f"{label} Max DD: {val:.2%}",
                color=colors["text"],
                ha="right",
                va=va_pos,  # 根据偏移方向调整文本锚点
            )

            ax_drawdown.axhline(
                val,
                linestyle="--",
                color=colors["drawdown"],
                linewidth=1.5,  # 稍细一些
                alpha=0.7,  # 降低透明度
                zorder=3,
            )

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
        ax_chart.tick_params(axis="x", rotation=-20)  # 将 x 轴刻度标签旋转 45 度
        ax_chart.tick_params(axis="y", labelright=False, labelleft=True, direction="in")
        ax_drawdown.tick_params(
            axis="y", labelright=True, labelleft=False, direction="in"
        )
        # 保证 y 轴刻度最多显示到小数点后两位
        fmt = ticker.FormatStrFormatter("%.2f")
        ax_chart.yaxis.set_major_formatter(fmt)
        ax_drawdown.yaxis.set_major_formatter(fmt)
        # ax_drawdown.yaxis.set_label_coords(0.99, 0.5)  # 标签向左平移
        # # 调整 y 轴 spines 的位置
        # ax_chart.spines["left"].set_position(("axes", 0.02))  # 左侧 y 轴靠近图表
        # ax_drawdown.spines["right"].set_position(("axes", 0.98))  # 右侧 y 轴靠近图表

        # 保存图片
        plt.subplots_adjust(left=0.075, right=0.94, top=1, bottom=0.1, wspace=0.1)
        plt.savefig(
            f"./dashreport/assets/images/us_special_tr_{theme}.svg",
            format="svg",
            # bbox_inches="tight",  # 保持边界紧凑
            bbox_inches=None,  # 保持边界紧凑
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

    """ 执行bt相关策略 """

    def run_backtest_in_process(date):
        """在独立进程中运行回测，确保内存完全释放"""
        import multiprocessing
        from multiprocessing import Queue

        def _worker(q, trade_date):
            try:
                cash, final_value = exec_btstrategy(trade_date)
                q.put((cash, final_value))
            except Exception as e:
                q.put(("error", str(e)))

        q = Queue()
        p = multiprocessing.Process(target=_worker, args=(q, date))
        p.start()
        p.join(timeout=3600)  # 1小时超时

        if p.is_alive():
            p.terminate()
            raise TimeoutError("Backtest timed out")

        result = q.get()
        if result[0] == "error":
            raise RuntimeError(result[1])
        return result[0], result[1]

    # 主函数中替换原有调用
    # cash, final_value = exec_btstrategy(trade_date)
    cash, final_value = run_backtest_in_process(trade_date)

    collected = gc.collect()

    print("Garbage collector: collected %d objects." % (collected))

    """ 发送邮件 """
    StockProposal("us_special", trade_date).send_btstrategy_by_email(cash, final_value)

    """ 结束进度条 """
    pbar.finish()
