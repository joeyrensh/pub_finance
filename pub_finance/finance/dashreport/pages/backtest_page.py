#!/usr/bin/env python3
"""回测分析页面 - 优化版"""
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import dash
import pandas as pd
from datetime import datetime

from finance.dashreport.chart_builder import ChartBuilder
from finance.dashreport.utils import Header, make_dash_format_table
from finance.utility.toolkit import ToolKit
from finance.utility.backtrader_exec import BacktraderExec
from finance.utility.tickerinfo import TickerInfo
from finance.paths import FINANCE_ROOT
from threading import Lock

BACKTEST_LOCK = Lock()


def run_bt(stocks, dt, m):
    return BacktraderExec(
        f"{m}_backtest", dt, test=True, stocklist=stocks
    ).run_strategy()


def load_logs(stocks, dt, m):
    f = FINANCE_ROOT / (
        "cnstockinfo/cn_backtest_trade_logs.csv"
        if m == "cn"
        else "usstockinfo/us_backtest_trade_logs.csv"
    )
    logs = []
    if f.exists():
        df_logs = pd.read_csv(
            f, header=None, names=["i", "s", "d", "t", "p", "v", "st"]
        )
    else:
        df_logs = pd.DataFrame(columns=["i", "s", "d", "t", "p", "v", "st"])
    # -----------------------------
    # 读取行业信息，只取相关股票
    f_industry = FINANCE_ROOT / (
        "cnstockinfo/industry.csv" if m == "cn" else "usstockinfo/industry.csv"
    )
    if f_industry.exists():
        df_industry = pd.read_csv(
            f_industry,
            usecols=["symbol", "industry"],
            dtype=str,
        )
        df_industry = df_industry[df_industry["symbol"].isin(stocks)]
    else:
        df_industry = pd.DataFrame(columns=["symbol", "industry"])

    # -----------------------------
    # 读取最新股票信息，只取相关股票
    f_latest_info = FINANCE_ROOT / (
        f"cnstockinfo/stock_{dt}.csv" if m == "cn" else f"usstockinfo/stock_{dt}.csv"
    )
    if f_latest_info.exists():
        df_latest = pd.read_csv(
            f_latest_info,
            usecols=["symbol", "name", "total_value", "pe"],
            dtype=str,
        )
        df_latest = df_latest[df_latest["symbol"].isin(stocks)]
    else:
        df_latest = pd.DataFrame(columns=["symbol", "name", "total_value", "pe"])

    # -----------------------------
    # --- 新增字段合并 ---
    if not df_logs.empty:
        # 先把 logs 转成 DataFrame
        df_logs = df_logs.rename(
            columns={
                "s": "symbol",
                "d": "date",
                "t": "type",
                "p": "price",
                "v": "volume",
                "st": "strategy",
            }
        )
        df_logs["type"] = (
            df_logs["type"].map({"buy": "买入", "sell": "卖出"}).fillna(df_logs["type"])
        )

        # 左连接行业信息和最新信息
        df_logs = df_logs.merge(df_industry, on="symbol", how="left")
        df_logs = df_logs.merge(df_latest, on="symbol", how="left")

        # 处理缺失值
        df_logs["industry"] = df_logs["industry"].fillna("-")
        # total_value 转为亿，非数值或缺失不报错
        df_logs["total_value"] = (
            pd.to_numeric(df_logs["total_value"], errors="coerce") / 100000000
        ).round(2)
        df_logs["total_value"] = df_logs["total_value"].fillna("-")
        df_logs["pe"] = (
            pd.to_numeric(df_logs["pe"], errors="coerce").round(2).fillna("-")
        )
        logs = df_logs.to_dict("records")

    # -----------------------------
    # 新增持仓明细加载
    pos_path = FINANCE_ROOT / (
        "cnstockinfo/cn_backtest_position_detail.csv"
        if m == "cn"
        else "usstockinfo/us_backtest_position_detail.csv"
    )
    if pos_path.exists():
        df_pos = pd.read_csv(
            pos_path, header=None, names=[f"col{i}" for i in range(1, 12)]
        )
        df_pos = df_pos.rename(
            columns={"col2": "symbol", "col3": "date", "col11": "strategy"}
        )[["symbol", "date", "strategy"]]
        df_pos["date"] = pd.to_datetime(df_pos["date"])
        # 排序并去除完全重复的行（同一天同一策略）
        df_pos = df_pos.sort_values(["symbol", "date"]).drop_duplicates(
            subset=["symbol", "date", "strategy"], keep="first"
        )
    else:
        df_pos = pd.DataFrame(columns=["symbol", "date", "strategy"])

    return logs, df_pos


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

        @self.app.callback(
            [
                Output("backtest-stocks", "value"),
                Output("backtest-full-list", "data"),
                Output("backtest-page", "data"),
                Output("backtest-refresh", "children"),
            ],
            Input("backtest-market", "value"),
            prevent_initial_call=False,
        )
        def load_stock_list(market):
            """市场切换时：加载完整股票列表，重置页码，显示前三支，并更新按钮文本"""
            file_path = FINANCE_ROOT / (
                "cnstockinfo/dynamic_list.csv"
                if market == "cn"
                else "usstockinfo/dynamic_list.csv"
            )
            symbols = []
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path, usecols=["symbol"], dtype=str)
                    symbols = df["symbol"].dropna().astype(str).str.strip().tolist()
                    symbols = [s for s in symbols if s]
                except Exception as e:
                    print(f"读取股票列表失败: {e}")
                    symbols = []
            if not symbols:
                # 默认股票列表（回退）
                if market == "cn":
                    symbols = ["SZ002077", "SZ002119"]
                else:
                    symbols = ["AAPL", "MSFT", "GOOGL"]

            total = len(symbols)
            page = 0
            start = page * 3
            end = min(start + 3, total)
            if start >= total:  # 边界处理
                start = 0
                end = min(3, total)
            current_stocks = symbols[start:end]
            button_text = f"{start+1}-{end} / {total}"

            return ",".join(current_stocks), symbols, page, button_text

        @self.app.callback(
            [
                Output("backtest-stocks", "value", allow_duplicate=True),
                Output("backtest-page", "data", allow_duplicate=True),
                Output("backtest-refresh", "children", allow_duplicate=True),
            ],
            Input("backtest-refresh", "n_clicks"),
            State("backtest-full-list", "data"),
            State("backtest-page", "data"),
            prevent_initial_call=True,
        )
        def refresh_stocks(n_clicks, full_list, current_page):
            """刷新按钮：滚动到下一组股票，循环"""
            if not full_list:
                return "", 0, "0-0"
            total = len(full_list)
            max_page = (total - 1) // 3
            next_page = (current_page + 1) % (max_page + 1) if max_page >= 0 else 0
            start = next_page * 3
            end = min(start + 3, total)
            current_stocks = full_list[start:end]
            button_text = f"{start+1}-{end} / {total}"
            return ",".join(current_stocks), next_page, button_text

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
                    Output("backtest-run", "className"),
                    "btn btn-primary kpi-label progress-btn running",
                    "btn btn-primary kpi-label progress-btn",
                ),
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
            if not BACKTEST_LOCK.acquire(blocking=False):
                return None, "⚠️ 系统正在执行回测，请稍后"
            stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
            if not stock_list:
                return None, "请输入有效的股票代码"
            try:
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                date_str = date_obj.strftime("%Y%m%d")
                pnl, c, tv = run_bt(stock_list, date_str, market)
                tr, pos_detail_df = load_logs(stock_list, date_str, market)

                # ----- 修复历史数据顺序：逐股票获取 -----
                h = []
                for sym in stock_list:
                    hist_data = load_hist([sym], date_str, market)
                    if hist_data and len(hist_data) > 0:
                        h.append(hist_data[0])
                    else:
                        h.append(pd.DataFrame())  # 空DataFrame

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
                    "pos_detail": (
                        pos_detail_df.to_dict("records")
                        if not pos_detail_df.empty
                        else []
                    ),
                    "h": [x.to_dict("records") if not x.empty else [] for x in h],
                    "s": stock_list,
                    "c": c,
                    "tv": tv,
                    "returns": returns,
                    "market": market,
                }
                status = f"回测完成 | 收益率：{returns:+.2f}% | 现金：{c:,.0f} | 总值：{tv:,.0f}"
                return data, status
            except Exception as e:
                import traceback

                traceback.print_exc()
                return None, f"错误：{e}"
            finally:
                BACKTEST_LOCK.release()

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
            pos_detail = d.get("pos_detail", [])  # 获取持仓明细

            charts = []
            width = client_width or 1440
            for i in range(min(6, len(histories))):
                stock_data = pd.DataFrame(histories[i])
                symbol = stocks[i] if i < len(stocks) else f"S{i}"
                fig = self.cb.kl_fig(
                    stock_data,
                    tr,
                    pos_detail=pos_detail,
                    symbol=symbol,
                    theme=theme,
                    client_width=width,
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
                            "marginBottom": "30px",
                            "padding": 0,
                            "width": "100%",
                            "height": "100%",
                            "aspectRatio": 1.6,
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
            # --- 字段重命名 & 顺序 ---
            rename_map = {
                "symbol": "SYMBOL",
                "industry": "INDUSTRY",
                "name": "NAME",
                "total_value": "TOTAL VALUE",
                "pe": "PE",
                "date": "DATE",
                "type": "TYPE",
                "price": "PRICE",
                "volume": "VOLUME",
                "strategy": "STRATEGY",
            }
            df = df.rename(columns=rename_map)

            # 保留指定字段顺序，剔除其它字段
            df = df[[v for k, v in rename_map.items() if v in df.columns]]
            cols_format = {
                "DATE": ("date", "format"),
                "PRICE": ("float",),
                "VOLUME": ("float",),
                "INDUSTRY": ("text",),
                "PE": ("text",),
            }
            df = (
                df.sort_values(["SYMBOL", "DATE"], ascending=[True, False])
                .groupby("SYMBOL", as_index=False)
                .head(2)
            )
            # 从回测数据中获取市场，默认为 cn
            market = data.get("market", "cn")
            trade_date = get_default_date(market)
            return make_dash_format_table(df, cols_format, market, trade_date)

    def build_control_panel(self):
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
                        # 市场选择列（不变）
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
                        # 回测日期列（不变）
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
                                html.Div(
                                    [
                                        dcc.Input(
                                            id="backtest-stocks",
                                            type="text",
                                            value="SZ002077,SZ002119",
                                            className="kpi-label",
                                            style={
                                                "flex": "1",
                                                "marginRight": "12px",  # 输入框右侧间隙
                                                "minWidth": "30%",  # 最小宽度为父容器宽度的 30%
                                            },
                                        ),
                                        html.Button(
                                            "刷新",
                                            id="backtest-refresh",
                                            n_clicks=0,
                                            className="kpi-label btn btn-secondary",
                                            style={
                                                "flex": "0 0 auto",
                                                "minWidth": "20%",  # 按钮固定最小宽度
                                            },
                                        ),
                                    ],
                                    style={
                                        "display": "flex",
                                        "alignItems": "flex-start",  # 让子元素高度一致
                                        "width": "100%",
                                    },
                                ),
                            ],
                            xs=12,
                            sm=12,
                            md=4,
                            lg=5,
                        ),
                        # 回测按钮列（单独一行，保持不变）
                        dbc.Col(
                            [
                                html.Label(" "),
                                html.Button(
                                    "执行回测",
                                    id="backtest-run",
                                    n_clicks=0,
                                    className="btn btn-primary kpi-label progress-btn",
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
                            color="#119DFF",
                            fullscreen=False,
                            children=[
                                html.Div(
                                    id="backtest-pnl-chart",
                                    style={
                                        "margin": 0,
                                        "padding": 0,
                                        "width": "100%",
                                        "height": "100%",
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
                # 新增两个 Store
                dcc.Store(id="backtest-full-list", data=[]),
                dcc.Store(id="backtest-page", data=0),
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
