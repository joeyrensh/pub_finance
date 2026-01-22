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
import matplotlib.dates as mdates
import warnings

warnings.filterwarnings(
    "ignore",
    message='Module "zipline.assets" not found; multipliers will not be applied to position notionals.',
)
import pyfolio as pf
import gc
from backtraderref.usfixedamount import FixedAmount as usFixedAmount
from backtraderref.cnfixedamount import FixedAmount as cnFixedAmount
from matplotlib import rcParams
import matplotlib.colors as mcolors
from utility.em_stock_uti import EMWebCrawlerUti
import numpy as np
import os
import pickle


class BacktraderExec:

    def __init__(self, market, trade_date):
        self.market = market
        self.trade_date = trade_date

    def run_strategy(self):
        """运行 backtrader 策略并返回 pnl, cash, total_value 。同时覆盖缓存文件。"""
        # 创建cerebro对象
        cerebro = bt.Cerebro(stdstats=False, maxcpus=0)
        # cerebro.broker.set_coc(True)
        # 添加bt相关的策略
        cerebro.addstrategy(
            GlobalStrategy, trade_date=self.trade_date, market=self.market
        )
        # 回测时需要添加 TimeReturn 分析器
        cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="_TimeReturn", fund=False)
        # 每手10000块/美金
        if self.market in ("cn", "cnetf", "cn_dynamic"):
            cerebro.addsizer(cnFixedAmount, amount=10000)
        elif self.market in ("us", "us_special", "us_dynamic"):
            cerebro.addsizer(usFixedAmount, amount=10000)
        # 费率千分之一
        cerebro.broker.setcommission(commission=0, stocklike=True)
        cerebro.broker.set_coc(True)  # 设置以当日收盘价成交
        # 添加股票当日即历史数据
        if self.market in ("cn", "us"):
            list = TickerInfo(self.trade_date, self.market).get_backtrader_data_feed()
        elif self.market == "cnetf":
            list = TickerInfo(
                self.trade_date, self.market
            ).get_etf_backtrader_data_feed()
        elif self.market == "us_special":
            list = TickerInfo(
                self.trade_date, self.market
            ).get_special_us_backtrader_data_feed()
        elif self.market in ("cn_dynamic", "us_dynamic"):
            list = TickerInfo(
                self.trade_date, self.market
            ).get_dynamic_backtrader_data_feed()
        # 初始资金100M
        start_cash = len(list) * 10000
        cerebro.broker.setcash(start_cash)
        # 循环初始化数据进入cerebro
        for h in list:
            # 历史数据最早不超过2021-01-01
            data = BTPandasDataExt(
                dataname=h,
                name=h["symbol"][0],
                fromdate=datetime(2024, 1, 1),
                # todate=datetime.strptime(date, "%Y%m%d"),
                datetime=-1,
                timeframe=bt.TimeFrame.Days,
            )
            cerebro.adddata(data, name=h["symbol"][0])
        # 起始资金池
        print("\nStarting Portfolio Value: %.2f" % cerebro.broker.getvalue())

        # 节约内存
        list = None
        data = None
        gc.collect()

        # 运行cerebro
        result = cerebro.run()
        # 最终资金池
        print("\n当前现金持有: ", cerebro.broker.get_cash())
        print("\nFinal Portfolio Value: %.2f" % cerebro.broker.getvalue())

        # 提取收益序列
        pnl = pd.Series(result[0].analyzers._TimeReturn.get_analysis())

        # 去掉最后一个虚拟 bar
        pnl = pnl.iloc[:-1]

        cash = round(cerebro.broker.get_cash(), 2)
        total_value = round(cerebro.broker.getvalue(), 2)

        # 保存缓存（覆盖）
        cache_dir = "./cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, f"pnl_{self.market}_{self.trade_date}.pkl")
        try:
            with open(cache_path, "wb") as f:
                pickle.dump((pnl, cash, total_value), f)
        except Exception:
            pass

        return pnl, cash, total_value

    def plot_from_pnl(self, pnl, cash, total_value):
        """根据 pnl 进行绘图和统计（无返回值）"""
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
        perf_stats_[perf_stats_.columns[1:]] = perf_stats_[
            perf_stats_.columns[1:]
        ].apply(lambda x: x.map(lambda y: f"{y * 100:.2f}%"))

        # ----------------------------
        # 绘图部分优化
        # ----------------------------

        def configure_theme(theme="light"):
            """统一配置主题颜色和样式"""
            theme_config = {
                "light": {
                    "text": "#333333",
                    "background": "none",
                    "grid": "#333333",
                    "cumret": "#e01c3a",  # 深蓝
                    "drawdown": "#0d876d",  # 红色
                    "table_edge": "#333333",
                    "table_header": "#F5F5F5",
                    "legend_text": "#333333",
                },
                "dark": {
                    "text": "#ffffffc5",
                    "background": "none",
                    "grid": "#FFFFFF",
                    "cumret": "#e01c3a",  # 亮蓝
                    "drawdown": "#0d876d",  # 亮红
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
                "YEAR",
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
                gridspec_kw={"width_ratios": [1, 2.5]},
                figsize=(20, 12.5),
                facecolor=colors["background"],
            )

            perf_stats_display = perf_stats_.copy()
            if len(perf_stats_display) > 2:
                perf_stats_display = perf_stats_display.iloc[-2:]

            def shorten_percent(text: str) -> str:
                try:
                    v = float(text.strip().replace("%", ""))
                except:
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
                # bbox=[0, 0, 1, 1],
                bbox=[0.03, 0.05, 1, 1],
                cellLoc="center",
                edges="horizontal",
            )
            table.auto_set_font_size(False)
            table.auto_set_column_width(range(len(perf_stats_.T.columns)))
            for (row, col), cell in table.get_celld().items():
                if col == 1 and row != 0:
                    try:
                        val_str = (
                            str(cell.get_text().get_text())
                            .replace("%", "")
                            .replace("K", "")
                            .replace(",", "")
                        )
                        val = float(val_str)
                        if val < 0:
                            cell.set_text_props(color="#0d876d")
                        elif val >= 0:
                            cell.set_text_props(color="#e01c3a")
                        else:
                            cell.set_text_props(color=colors["text"])
                    except Exception:
                        pass
                else:
                    cell.set_text_props(color=colors["text"])
                cell.set_edgecolor(mcolors.to_rgba(colors["table_edge"], alpha=0.2))
                cell.set_linewidth(1)
                cell.PAD = 0.15

            # ----------------------------
            # 绘制双轴曲线
            # ----------------------------
            ax_drawdown = ax_chart.twinx()
            ax_chart.set_zorder(ax_drawdown.get_zorder() + 1)
            ax_chart.patch.set_visible(False)

            ax_chart.plot(
                cumulative.index,
                cumulative.values,
                color=colors["cumret"],
                label="Cumulative Return",
                linewidth=2.5,
                zorder=3,
                marker="o",
                markersize=4,
                markerfacecolor=colors["cumret"],
                markeredgecolor=colors["cumret"],
            )
            ax_chart.grid(True, alpha=0.25)
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

            ax_drawdown.fill_between(
                drawdown.index,
                drawdown.values,
                0,
                color=colors["drawdown"],
                alpha=0.35,
                zorder=1,
                label="Drawdown",
            )
            ax_drawdown.step(
                drawdown.index,
                drawdown.values,
                where="post",
                color=colors["drawdown"],
                linewidth=1,
                alpha=0.9,
                zorder=2,
            )
            max_dd = drawdown.min()
            max_dd_idx = drawdown.idxmin()
            ax_drawdown.axhline(
                max_dd, linestyle="--", color=colors["drawdown"], linewidth=2, zorder=3
            )
            ax_chart.text(
                max_dd_idx,
                max_dd,
                f"Max DD: {max_dd:.2%}",
                color=colors["text"],
                ha="right",
                va="bottom",
                transform=ax_drawdown.transData,
                zorder=900,
            )

            end_date = drawdown.index.max()
            window_A_days = 120
            window_B_days = 30

            def get_window_dd(drawdown, days, end_date):
                window_start = end_date - pd.Timedelta(days=days)
                dd_window = drawdown[drawdown.index >= window_start]
                if len(dd_window) > 0:
                    idx = dd_window.idxmin()
                    val = dd_window.loc[idx]
                    return idx, val
                return None, None

            dd_A_idx, dd_A_val = get_window_dd(drawdown, window_A_days, end_date)
            dd_B_idx, dd_B_val = get_window_dd(drawdown, window_B_days, end_date)

            label_A = f"{window_A_days}D"
            label_B = f"{window_B_days}D"
            dd_candidates = []
            if dd_A_idx is not None and dd_A_idx != max_dd_idx:
                dd_candidates.append((label_A, dd_A_idx, dd_A_val))
            if dd_B_idx is not None and dd_B_idx != max_dd_idx and dd_B_idx != dd_A_idx:
                dd_candidates.append((label_B, dd_B_idx, dd_B_val))

            dd_points = {"max": max_dd}
            if dd_A_val is not None:
                dd_points[label_A] = dd_A_val
            if dd_B_val is not None:
                dd_points[label_B] = dd_B_val

            dd_vals = [v for _, v in dd_points.items()]
            dd_range = max(dd_vals) - min(dd_vals)
            # dd_range = max(dd_range, 1e-6)
            offset_sign = {label_A: 0.2, label_B: 0.2}

            if dd_A_val is not None and dd_B_val is not None:
                gap_AB = abs(dd_A_val - dd_B_val)
                if gap_AB < dd_range * 0.08:
                    deeper_label, deeper_val = (
                        (label_A, dd_A_val)
                        if dd_A_val < dd_B_val
                        else (label_B, dd_B_val)
                    )
                    test_offset = min(gap_AB * 0.6, dd_range * 0.15)
                    test_val = deeper_val - test_offset
                    if abs(test_val - max_dd) > dd_range * 0.08:
                        offset_sign[deeper_label] = -1

            ylim = ax_drawdown.get_ylim()
            y_top_threshold = ylim[1] - (ylim[1] - ylim[0]) * 0.05
            for label, idx, val in dd_candidates:
                ax_drawdown.scatter(idx, val, color=colors["drawdown"], s=55, zorder=4)
                min_dist = min(abs(val - v) for k, v in dd_points.items() if k != label)
                offset_mag = min(min_dist * 0.2, dd_range * 0.15)
                y_offset = offset_sign[label] * offset_mag
                if (val + y_offset) > y_top_threshold:
                    y_offset = y_top_threshold - val - 0.01 * dd_range
                    va_pos = "top"
                else:
                    va_pos = "bottom" if y_offset >= 0 else "top"
                ax_chart.text(
                    idx,
                    val + y_offset,
                    f"{label} Max DD: {val:.2%}",
                    color=colors["text"],
                    ha="right",
                    va=va_pos,
                    transform=ax_drawdown.transData,
                    zorder=1000,
                )
                ax_drawdown.axhline(
                    val,
                    linestyle="--",
                    color=colors["drawdown"],
                    linewidth=1.5,
                    alpha=0.7,
                    zorder=4,
                )

            ax_drawdown.grid(False)
            # ax_chart.xaxis.set_major_locator(ticker.AutoLocator())
            # ax_chart.xaxis.set_major_locator(ticker.MaxNLocator(base=8))

            # 获取x轴范围
            x_min, x_max = ax_chart.get_xlim()
            x_max_adjusted = x_max - 10  # 这里减去10，可以根据需要调整
            # 生成等距刻度，包含端点且避免重复（之前的写法会重复首尾）
            ticks = np.linspace(x_min, x_max_adjusted, 8).tolist()
            # 设置x轴刻度
            ax_chart.set_xticks(ticks)
            # 将刻度标签格式化为月份，显示为 "yyyy-mm"
            ax_chart.xaxis.set_major_locator(ticker.FixedLocator(ticks))
            ax_chart.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

            lines, labels = ax_chart.get_legend_handles_labels()
            lines2, labels2 = ax_drawdown.get_legend_handles_labels()
            legend = ax_chart.legend(
                lines + lines2, labels + labels2, loc="upper left", frameon=False
            )
            for text in legend.get_texts():
                text.set_color(colors["legend_text"])
            for spine in ax_chart.spines.values():
                spine.set_visible(False)
            for spine in ax_drawdown.spines.values():
                spine.set_visible(False)
            # 将 x 轴刻度标签与图表靠得更近：通过减少 pad（标签与刻度线的距离）
            # 并把子图底部边距稍微减小以节省垂直空间
            ax_chart.tick_params(
                axis="x", rotation=-20, colors=colors["legend_text"], pad=-15
            )
            ax_chart.tick_params(
                axis="y", labelright=False, labelleft=True, direction="in"
            )
            ax_drawdown.tick_params(
                axis="y", labelright=True, labelleft=False, direction="in"
            )
            fmt = ticker.FormatStrFormatter("%.2f")
            ax_chart.yaxis.set_major_formatter(fmt)
            ax_drawdown.yaxis.set_major_formatter(fmt)
            plt.subplots_adjust(left=0.075, right=0.94, top=1, bottom=0.07, wspace=0.1)
            out_path = f"./dashreport/assets/images/{self.market}_tr_{theme}.svg"
            plt.savefig(
                out_path,
                format="svg",
                bbox_inches=None,
                transparent=True,
                pad_inches=0.2,
            )
            plt.close()

        # 生成两种主题图表
        plot_chart(theme="light")
        plot_chart(theme="dark")

    def exec_btstrategy(self, force_run=False):
        """执行器：优先读取缓存（当 force_run=False 且缓存存在），否则运行策略并生成缓存，最后调用绘图函数。返回 (cash, total_value)"""
        cache_path = os.path.join("./cache", f"pnl_{self.market}_{self.trade_date}.pkl")
        if (not force_run) and os.path.exists(cache_path):
            try:
                with open(cache_path, "rb") as f:
                    pnl, cash, total_value = pickle.load(f)
            except Exception:
                pnl, cash, total_value = self.run_strategy()
        else:
            pnl, cash, total_value = self.run_strategy()

        # 调用绘图（不返回值）
        try:
            self.plot_from_pnl(pnl, cash, total_value)
        except Exception:
            pass

        return cash, total_value
