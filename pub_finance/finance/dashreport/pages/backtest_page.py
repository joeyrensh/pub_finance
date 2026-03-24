#!/usr/bin/env python3
"""回测分析页面 - 优化版"""
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from pathlib import Path
from datetime import datetime

from finance.dashreport.chart_builder import ChartBuilder
from finance.dashreport.utils import Header, make_dash_format_table
from finance.utility.toolkit import ToolKit
from finance.utility.backtrader_exec import BacktraderExec
from finance.utility.tickerinfo import TickerInfo
from finance.paths import FINANCE_ROOT
import time
import os


def run_bt(stocks, dt, m):
    return BacktraderExec(
        f"{m}_backtest", dt, test=True, stocklist=stocks
    ).run_strategy()


def load_logs(m):
    f = FINANCE_ROOT / (
        "cnstockinfo/cn_backtest_trade_logs.csv"
        if m == "cn"
        else "usstockinfo/us_backtest_trade_logs.csv"
    )
    logs = []
    if f.exists():
        df = pd.read_csv(f, header=None, names=["i", "s", "d", "t", "p", "v", "st"])
        for _, r in df.iterrows():
            logs.append(
                {
                    "symbol": r["s"],
                    "date": r["d"],
                    "type": "买入" if r["t"] == "buy" else "卖出",
                    "price": r["p"],
                    "volume": r["v"],
                    "strategy": r.get("st", ""),
                }
            )
    return logs


def load_hist(stocks, dt, m):
    return (
        TickerInfo(dt, f"{m}_backtest").get_backtrader_data_feed_testonly(stocks) or []
    )


def get_default_date(market="cn"):
    try:
        return (
            ToolKit.get_us_latest_trade_date(0)
            if market == "us"
            else ToolKit.get_cn_latest_trade_date(0)
        )
    except:
        return datetime.now().strftime("%Y%m%d")


class BacktestPage:
    def __init__(self, app, market="cn"):
        self.app = app
        self.market = market
        self.cb = ChartBuilder()
        self.register_callbacks()

    def register_callbacks(self):
        @self.app.callback(
            Output("backtest-date", "date"),  # 改为 date
            Input("backtest-market", "value"),
            prevent_initial_call=False,
        )
        def init_date(market):
            default = get_default_date(market or "cn")
            if default:
                # 转换为 YYYY-MM-DD 格式
                return datetime.strptime(default, "%Y%m%d").strftime("%Y-%m-%d")
            return None

        # 回测 callback（增强：按钮锁定 + 进度条）
        @self.app.callback(
            Output("backtest-data", "data"),
            Output("backtest-status", "children"),
            Input("backtest-run", "n_clicks"),
            State("backtest-market", "value"),
            State("backtest-date", "date"),  # 注意这里是 date
            State("backtest-stocks", "value"),
            prevent_initial_call=True,
            running=[
                (Output("backtest-run", "disabled"), True, False),
                (Output("backtest-run", "children"), "回测中…", "执行回测"),
                (
                    Output("backtest-progress", "style"),
                    {"display": "block"},
                    {"display": "none"},
                ),
            ],
        )
        def run_backtest(n_clicks, market, date, stocks):
            if not n_clicks or not stocks:
                return None, "请输入股票代码"
            stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
            if not stock_list:
                return None, "请输入有效的股票代码"
            try:
                # 将 YYYY-MM-DD 转换为 YYYYMMDD
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                date_str = date_obj.strftime("%Y%m%d")
                pnl, c, tv = run_bt(stock_list, date_str, market)
                tr = load_logs(market)
                h = load_hist(stock_list, date_str, market)
                pnl_data = (
                    {
                        "index": [str(x) for x in pnl.index.tolist()],
                        "values": pnl.values.tolist(),
                    }
                    if pnl is not None and len(pnl) > 0
                    else {}
                )
                returns = (
                    ((1 + pnl).cumprod().iloc[-1] - 1) * 100
                    if pnl is not None and len(pnl) > 0
                    else 0
                )
                data = {
                    "pnl": pnl_data,
                    "tr": tr,
                    "h": [x.to_dict("records") for x in h] if h else [],
                    "s": stock_list,
                    "c": c,
                    "tv": tv,
                    "returns": returns,
                    "market": market,  # 保存市场信息，供表格使用
                }
                status = f"回测完成 | 收益率：{returns:+.2f}% | 现金：{c:,.0f} | 总值：{tv:,.0f}"
                return data, status
            except Exception as e:
                import traceback

                traceback.print_exc()
                return None, f"错误：{e}"

        @self.app.callback(
            Output("backtest-pnl-chart", "children"),
            Input("backtest-data", "data"),
            Input("current-theme", "data"),
            Input("client-width", "data"),
            prevent_initial_call=True,  # 添加
        )
        def up_pnl(d, theme, client_width):
            if not d or not d.get("pnl") or not d["pnl"].get("index"):
                return html.Div(
                    "请执行回测",
                    style={"color": "#999", "padding": "60px", "textAlign": "center"},
                )
            pnl = pd.Series(d["pnl"]["values"], index=pd.to_datetime(d["pnl"]["index"]))
            return dcc.Graph(
                figure=self.cb.annual_return(
                    page="backtest",
                    pnl=pnl,
                    theme=theme,
                    client_width=client_width or 1440,
                ),
                config={
                    "displayModeBar": False,
                    "doubleClick": False,  # 禁用双击缩放
                    "responsive": True,
                },
                style={
                    "margin": 0,
                    "padding": 0,
                    "width": "100%",
                    "height": "100%",
                    "aspectRatio": 1.6,
                },
                mathjax=True,
            )

        @self.app.callback(
            Output("backtest-kline-charts", "children"),
            Input("backtest-data", "data"),
            Input("current-theme", "data"),
            Input("client-width", "data"),
            prevent_initial_call=True,  # 添加
        )
        def uk(d, theme, client_width):
            if not d or not d.get("h"):
                return html.Div(
                    "请执行回测",
                    style={"color": "#999", "padding": "60px", "textAlign": "center"},
                )
            stocks = d.get("s", [])
            histories = d.get("h", [])
            tr = d.get("tr", [])
            charts = []
            width = client_width or 1440
            for i in range(min(6, len(histories))):
                stock_data = pd.DataFrame(histories[i])
                symbol = stocks[i] if i < len(stocks) else f"S{i}"
                fig = self.cb.kl_fig(
                    stock_data, tr, symbol, theme=theme, client_width=width
                )
                charts.append(
                    html.Div(
                        dcc.Graph(
                            figure=fig,
                            config={
                                "displayModeBar": False,
                                "scrollZoom": False,  # 禁用滚轮缩放
                                "doubleClick": False,  # 禁用双击缩放
                                "responsive": True,
                            },
                        ),
                        style={
                            "width": "100%",
                            "height": "auto",
                            "marginBottom": "20px",
                        },
                    )
                )
            return html.Div(
                charts, style={"display": "block", "width": "100%", "height": "auto"}
            )

        # ========== 修正后的交易记录回调 ==========
        @self.app.callback(
            Output("backtest-trade-table", "children"),
            Input("backtest-data", "data"),  # 依赖回测数据
            Input("current-theme", "data"),  # 主题
            Input("client-width", "data"),  # 宽度（可保留，未使用）
            prevent_initial_call=True,  # 避免空数据时触发
        )
        def update_trade_table(data, theme, client_width):
            """使用 make_dash_format_table 格式化交易记录表格"""
            if not data or not data.get("tr"):
                return html.Div(
                    "暂无交易记录",
                    style={"color": "#999", "padding": "60px", "textAlign": "center"},
                )
            tr = data.get("tr", [])
            df = pd.DataFrame(tr)
            if df.empty:
                return html.Div(
                    "暂无交易记录",
                    style={"color": "#999", "padding": "60px", "textAlign": "center"},
                )
            df = df.rename(
                columns={
                    "symbol": "SYMBOL",
                    "date": "DATE",
                    "type": "TYPE",
                    "price": "PRICE",
                    "volume": "VOLUME",
                    "strategy": "STRATEGY",
                }
            )
            cols_format = {
                "DATE": ("date", "format"),
                "PRICE": ("float",),
                "VOLUME": ("float",),
            }
            df = df.groupby("SYMBOL", group_keys=False).apply(
                lambda x: x.sort_values("DATE", ascending=False)
            )
            # 从回测数据中获取市场，默认为 cn
            market = data.get("market", "cn")
            trade_date = get_default_date(market)
            return make_dash_format_table(df, cols_format, market, trade_date)

    def build_control_panel(self):
        # 计算默认日期
        default_date_str = get_default_date("cn")
        default_date = (
            datetime.strptime(default_date_str, "%Y%m%d").date()
            if default_date_str
            else None
        )

        return dbc.Container(
            [
                html.H6("回测分析", className="subtitle padded"),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Label("市场"),
                                dcc.RadioItems(
                                    id="backtest-market",
                                    options=[
                                        {"label": "A 股", "value": "cn"},
                                        {"label": "美股", "value": "us"},
                                    ],
                                    value="cn",
                                    inline=True,
                                ),
                            ],
                            xs=12,
                            sm=6,
                            md=3,
                            lg=2,
                        ),
                        dbc.Col(
                            [
                                html.Label("回测日期"),
                                dcc.DatePickerSingle(
                                    id="backtest-date",
                                    date=default_date,
                                    display_format="YYYY-MM-DD",
                                    placeholder="选择日期",
                                    className="custom-date-picker kpi-label",
                                    style={"width": "100%"},
                                ),
                            ],
                            xs=12,
                            sm=6,
                            md=3,
                            lg=3,
                        ),
                        dbc.Col(
                            [
                                html.Label("股票代码"),
                                dcc.Input(
                                    id="backtest-stocks",
                                    type="text",
                                    value="SZ002077,SZ002119",
                                    className="kpi-label",
                                    style={"width": "100%"},
                                ),
                            ],
                            xs=12,
                            sm=12,
                            md=4,
                            lg=5,
                        ),
                        dbc.Col(
                            [
                                html.Label(" "),
                                html.Button(
                                    "执行回测",
                                    id="backtest-run",
                                    n_clicks=0,
                                    className="btn btn-primary kpi-label",
                                    style={"width": "100%"},
                                ),
                            ],
                            xs=12,
                            sm=6,
                            md=2,
                            lg=2,
                        ),
                    ],
                    className="kpi-label",
                ),
                html.Div(
                    id="backtest-status",
                    style={"marginTop": "10px", "color": "#666"},
                ),
            ],
            fluid=True,
        )

    def build_annual_return_card(self):
        return html.Div(
            [
                html.H6(["收益与回撤 ‹"], className="subtitle padded"),
                html.Div(
                    className="chart-container",
                    children=[
                        dcc.Loading(
                            id="loading-backtest-pnl",
                            type="circle",
                            delay_hide=1000,
                            style={
                                "width": "100%",
                                "height": "100%",
                                "position": "relative",
                                "display": "flex",
                                "justifyContent": "center",
                                "alignItems": "center",
                            },
                            parent_style={
                                "width": "100%",
                                "height": "100%",
                                "display": "flex",
                                "justifyContent": "center",
                                "alignItems": "center",
                            },
                            color="#119DFF",
                            fullscreen=False,
                            children=[
                                html.Div(
                                    id="backtest-pnl-chart",
                                    style={
                                        "width": "100%",
                                        "height": "100%",
                                        "display": "flex",
                                        "justifyContent": "center",
                                        "alignItems": "center",
                                    },
                                    children=html.Div(
                                        "请执行回测",
                                        style={
                                            "color": "#999",
                                            "textAlign": "center",
                                            "padding": "40px",
                                        },
                                    ),
                                )
                            ],
                        )
                    ],
                ),
            ],
            className="twelve columns",
        )

    def build_kline_card(self):
        return html.Div(
            [
                html.H6(["K 线图与买卖点 ‹"], className="subtitle padded"),
                html.Div(
                    className="chart-container",
                    children=[
                        dcc.Loading(
                            id="loading-backtest-kline",
                            type="circle",
                            delay_hide=1000,
                            color="#119DFF",
                            fullscreen=False,
                            children=[
                                html.Div(
                                    id="backtest-kline-charts",
                                    style={
                                        "margin": 0,
                                        "padding": 0,
                                        "width": "100%",
                                        "height": "100%",
                                        "overflowY": "auto",
                                    },
                                )
                            ],
                        )
                    ],
                    style={"display": "block", "width": "100%"},
                ),
            ],
            className="twelve columns",
        )

    def build_trade_log_card(self):
        return html.Div(
            className="row",
            children=[
                html.Div(
                    className="twelve columns",
                    children=[
                        html.H6("交易记录", className="subtitle padded"),
                        html.Div(
                            [
                                dcc.Loading(
                                    id="loading-backtest-table",
                                    type="circle",
                                    delay_hide=1000,
                                    color="#119DFF",
                                    children=html.Div(id="backtest-trade-table"),
                                )
                            ],
                            className="table",
                            style={
                                "overflow-x": "auto",
                                "max-height": 400,
                                "overflow-y": "auto",
                            },
                        ),
                    ],
                )
            ],
        )

    def get_layout(self):
        return html.Div(
            [
                # 添加数据存储组件（关键！）
                dcc.Store(id="backtest-data", data=None),  # 初始为空，回测后填充
                Header(self.app),
                html.Div(
                    [
                        self.build_control_panel(),
                        self.build_annual_return_card(),
                        self.build_kline_card(),
                        self.build_trade_log_card(),
                    ],
                    className="sub_page",
                ),
            ],
            className="page",
        )
