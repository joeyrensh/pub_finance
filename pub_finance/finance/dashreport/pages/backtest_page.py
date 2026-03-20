#!/usr/bin/env python3
"""回测分析页面 - 集成到 dashreport"""
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# 导入 dashreport 组件
from finance.dashreport.chart_builder import ChartBuilder
from finance.dashreport.utils import make_dash_format_table


class BacktestPage:
    def __init__(self, app):
        self.app = app
        self.cb = ChartBuilder()
        self.register_callbacks()

    def create_layout(self):
        """创建页面布局"""
        return html.Div(
            [
                html.Div(
                    [html.H6("回测分析", className="subtitle padded")], className="row"
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("市场", className="control-label"),
                                dcc.RadioItems(
                                    id="backtest-market",
                                    options=[
                                        {"label": "A股", "value": "cn"},
                                        {"label": "美股", "value": "us"},
                                    ],
                                    value="cn",
                                    inline=True,
                                    className="radio-group",
                                ),
                            ],
                            className="three columns",
                        ),
                        html.Div(
                            [
                                html.Label("回测日期", className="control-label"),
                                dcc.Input(
                                    id="backtest-date",
                                    type="text",
                                    value=datetime.now().strftime("%Y%m%d"),
                                    className="input-field",
                                ),
                            ],
                            className="three columns",
                        ),
                        html.Div(
                            [
                                html.Label("股票代码", className="control-label"),
                                dcc.Input(
                                    id="backtest-stocks",
                                    type="text",
                                    value="SZ002077,SZ002119",
                                    className="input-field wide",
                                ),
                            ],
                            className="four columns",
                        ),
                        html.Div(
                            [
                                html.Button(
                                    "执行回测",
                                    id="backtest-run",
                                    n_clicks=0,
                                    className="button-primary",
                                )
                            ],
                            className="two columns",
                        ),
                    ],
                    className="row control-panel",
                ),
                html.Div(id="backtest-status", className="status-bar"),
                html.Div(
                    [
                        html.Div(
                            [html.Div(id="backtest-kpi-return", className="kpi-card")],
                            className="three columns",
                        ),
                        html.Div(
                            [html.Div(id="backtest-kpi-cash", className="kpi-card")],
                            className="three columns",
                        ),
                        html.Div(
                            [html.Div(id="backtest-kpi-total", className="kpi-card")],
                            className="three columns",
                        ),
                        html.Div(
                            [html.Div(id="backtest-kpi-trades", className="kpi-card")],
                            className="three columns",
                        ),
                    ],
                    className="row",
                ),
                html.Div(
                    [
                        html.H6("收益与回撤", className="subtitle padded"),
                        html.Div(
                            [
                                dcc.Loading(
                                    id="loading-backtest-pnl",
                                    type="circle",
                                    children=dcc.Graph(id="backtest-pnl-chart"),
                                )
                            ],
                            className="chart-container",
                        ),
                    ],
                    className="row product",
                ),
                html.Div(
                    [
                        html.H6("K线图与买卖点", className="subtitle padded"),
                        html.Div(
                            [
                                dcc.Loading(
                                    id="loading-backtest-kline",
                                    type="circle",
                                    children=html.Div(id="backtest-kline-charts"),
                                )
                            ],
                            className="chart-container",
                        ),
                    ],
                    className="row product",
                ),
                html.Div(
                    [
                        html.H6("交易记录", className="subtitle padded"),
                        html.Div(
                            [
                                dcc.Loading(
                                    id="loading-backtest-table",
                                    type="circle",
                                    children=html.Div(id="backtest-trade-table"),
                                )
                            ],
                            className="table-container",
                        ),
                    ],
                    className="row product",
                ),
                dcc.Store(id="backtest-data"),
            ],
            className="page-content",
        )

    def register_callbacks(self):
        """注册回调函数"""

        @self.app.callback(
            Output("backtest-data", "data"),
            Output("backtest-status", "children"),
            Output("backtest-kpi-return", "children"),
            Output("backtest-kpi-cash", "children"),
            Output("backtest-kpi-total", "children"),
            Output("backtest-kpi-trades", "children"),
            Input("backtest-run", "n_clicks"),
            State("backtest-stocks", "value"),
            State("backtest-date", "value"),
            State("backtest-market", "value"),
            prevent_initial_call=True,
        )
        def run_backtest(n_clicks, stocks, date, market):
            if not stocks:
                return None, "请输入股票代码", "", "", "", ""

            stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
            if not stock_list:
                return None, "请输入有效的股票代码", "", "", "", ""

            try:
                from finance.utility.backtrader_exec import BacktraderExec

                executor = BacktraderExec(
                    market=f"{market}_backtest",
                    trade_date=date,
                    test=True,
                    stocklist=stock_list,
                )
                pnl, cash, total_value = executor.run_strategy()

                log_file = f"/home/ubuntu/pub_finance/finance/{market}stockinfo/{market}_backtest_trade_logs.csv"
                trades = []
                if os.path.exists(log_file):
                    df = pd.read_csv(
                        log_file,
                        header=None,
                        names=["i", "s", "d", "t", "p", "v", "st"],
                    )
                    for _, r in df.iterrows():
                        trades.append(
                            {
                                "symbol": r["s"],
                                "date": r["d"],
                                "type": r["t"],
                                "price": r["p"],
                                "volume": r["v"],
                                "strategy": r.get("st", ""),
                            }
                        )

                from finance.utility.tickerinfo import TickerInfo

                hist_data = (
                    TickerInfo(
                        date, f"{market}_backtest"
                    ).get_backtrader_data_feed_testonly(stock_list)
                    or []
                )

                returns = (
                    ((1 + pnl).cumprod().iloc[-1] - 1) * 100
                    if pnl is not None and len(pnl) > 0
                    else 0
                )

                data = {
                    "pnl": pnl,
                    "trades": trades,
                    "hist": (
                        [x.to_dict("records") for x in hist_data] if hist_data else []
                    ),
                    "stocks": stock_list,
                    "returns": returns,
                    "cash": cash,
                    "total": total_value,
                }

                status = f"回测完成 | 收益率：{returns:+.2f}%"

                return (
                    data,
                    status,
                    f"{returns:+.2f}%",
                    f"{cash:,.0f}",
                    f"{total_value:,.0f}",
                    f"{len(trades)}",
                )

            except Exception as e:
                import traceback

                traceback.print_exc()
                return None, f"错误：{e}", "", "", "", ""

        @self.app.callback(
            Output("backtest-pnl-chart", "figure"), Input("backtest-data", "data")
        )
        def update_pnl_chart(data):
            if not data or data.get("pnl") is None:
                return go.Figure()
            pnl = data["pnl"]
            if len(pnl) == 0:
                return go.Figure()
            try:
                fig = self.cb.annual_return(page="backtest", pnl=pnl, theme="light")
                return fig
            except:
                return self._create_pnl_chart_fallback(pnl)

        @self.app.callback(
            Output("backtest-kline-charts", "children"), Input("backtest-data", "data")
        )
        def update_kline_charts(data):
            if not data or not data.get("hist"):
                return html.Div("请执行回测")
            charts = []
            trades = data.get("trades", [])
            # 竖排显示 - 每个股票占满一行（twelve columns）
            for i, hist_df in enumerate(data["hist"][:6]):
                symbol = data["stocks"][i] if i < len(data["stocks"]) else f"S{i}"
                df = pd.DataFrame(hist_df)
                if df.empty:
                    continue
                fig = self._create_kline_chart(df, trades, symbol)
                charts.append(
                    html.Div(
                        [dcc.Graph(figure=fig, config={"displayModeBar": False})],
                        className="twelve columns",
                    )
                )
            return html.Div(charts, className="row")

        @self.app.callback(
            Output("backtest-trade-table", "children"), Input("backtest-data", "data")
        )
        def update_trade_table(data):
            if not data or not data.get("trades"):
                return html.Div("暂无交易记录")
            trades = data["trades"]
            try:
                df = pd.DataFrame(trades)
                if len(df) > 0:
                    return make_dash_format_table(
                        df, {}, "cn", datetime.now().strftime("%Y%m%d")
                    )
            except:
                pass
            return self._create_trade_table_fallback(trades)

    def _create_pnl_chart_fallback(self, pnl):
        pnl = pnl.sort_index()
        dates = pd.to_datetime(pnl.index)
        cumret = (1 + pnl).cumprod()
        drawdown = (cumret - cumret.cummax()) / cumret.cummax() * 100
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            row_heights=[0.65, 0.35],
            vertical_spacing=0.08,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=(cumret - 1) * 100,
                name="累计收益",
                line=dict(color="#ff4444", width=2),
                fill="tozeroy",
                fillcolor="rgba(255,68,68,0.1)",
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=drawdown,
                name="回撤",
                line=dict(color="#0d876d", width=1.5),
                fill="tozeroy",
                fillcolor="rgba(13,135,109,0.2)",
            ),
            row=2,
            col=1,
        )
        fig.update_layout(
            height=450, margin=dict(l=60, r=40, t=50, b=40), hovermode="x unified"
        )
        return fig

    def _create_kline_chart(self, df, trades, symbol):
        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")

        # 只保留最近 360 天的数据
        end_date = df["datetime"].max()
        start_date = end_date - timedelta(days=360)
        df = df[df["datetime"] >= start_date].copy()

        if df.empty:
            return go.Figure()

        min_p, max_p = df["low"].min() * 0.97, df["high"].max() * 1.03
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            row_heights=[0.75, 0.25],
            vertical_spacing=0.02,
        )

        # K 线图
        fig.add_trace(
            go.Candlestick(
                x=df["datetime"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name=symbol,
                increasing_line_color="#ef5350",
                decreasing_line_color="#26a69a",
            ),
            row=1,
            col=1,
        )

        # 买卖点信号 - 美化版本（竖线 + 圆形标记）
        symbol_trades = [x for x in trades if x.get("symbol") == symbol]
        for t in symbol_trades:
            try:
                td = pd.to_datetime(t["date"])
                tp = t["price"]
                tt = t["type"]

                # 买入信号 - 红色，卖出信号 - 绿色
                if tt == "buy":
                    line_color = "#ff4444"
                    marker_color = "#ff4444"
                else:
                    line_color = "#0d876d"
                    marker_color = "#0d876d"

                # 竖线贯穿整个价格区间
                fig.add_trace(
                    go.Scatter(
                        x=[td, td],
                        y=[min_p, max_p],
                        mode="lines",
                        line=dict(color=line_color, width=1.5),
                        opacity=0.6,
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

                # 圆形标记在价格位置（带白边）
                fig.add_trace(
                    go.Scatter(
                        x=[td],
                        y=[tp],
                        mode="markers",
                        marker=dict(
                            symbol="circle",
                            size=12,
                            color=marker_color,
                            line=dict(color="white", width=2),
                        ),
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )
            except:
                pass

        # 成交量 - 加深颜色（opacity=1.0）
        colors = [
            "#ef5350" if df["close"].iloc[i] >= df["open"].iloc[i] else "#26a69a"
            for i in range(len(df))
        ]
        fig.add_trace(
            go.Bar(x=df["datetime"], y=df["volume"], marker_color=colors, opacity=1.0),
            row=2,
            col=1,
        )

        fig.update_layout(
            title=dict(text=f"{symbol}", x=0.5),
            height=500,
            xaxis_rangeslider_visible=False,
            margin=dict(l=50, r=50, t=50, b=50),
        )
        return fig

    def _create_trade_table_fallback(self, trades):
        rows = [
            html.Tr(
                [
                    html.Td(t.get("symbol", "-")),
                    html.Td(t["date"]),
                    html.Td(t["type"]),
                    html.Td(f"{t['price']:.2f}"),
                    html.Td(f"{int(t['volume']):,}"),
                    html.Td(t.get("strategy", "-")),
                ]
            )
            for t in trades[:50]
        ]
        return html.Table(
            [
                html.Thead(
                    html.Tr(
                        [
                            html.Th("股票"),
                            html.Th("日期"),
                            html.Th("类型"),
                            html.Th("价格"),
                            html.Th("数量"),
                            html.Th("策略"),
                        ]
                    )
                ),
                html.Tbody(rows),
            ],
            className="dash-table",
        )


def create_backtest_layout(app):
    page = BacktestPage(app)
    return page.get_layout()
