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
            [
                Input("backtest-market", "value"),
                Input("backtest-date", "date"),
            ],
            prevent_initial_call=False,
        )
        def load_stock_list(market, date_str):
            """市场或日期变化时：加载完整股票列表，若存在对应日期的 stock 文件则按 PE、总市值排序，否则保持原顺序"""
            # 1. 读取所有股票代码
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

            # 2. 尝试根据日期读取股票信息文件（PE、总市值）
            stock_file_exists = False
            if date_str:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
                    stock_file = FINANCE_ROOT / (
                        f"cnstockinfo/stock_{dt}.csv"
                        if market == "cn"
                        else f"usstockinfo/stock_{dt}.csv"
                    )
                    if stock_file.exists():
                        stock_file_exists = True
                        df_stock = pd.read_csv(
                            stock_file,
                            usecols=["symbol", "pe", "total_value"],
                            dtype=str,
                        )
                        pe_dict = {}
                        tv_dict = {}
                        for _, row in df_stock.iterrows():
                            sym = row["symbol"]
                            pe = row.get("pe")
                            tv = row.get("total_value")
                            if pe and pe != "nan":
                                try:
                                    pe_dict[sym] = float(pe)
                                except:
                                    pass
                            if tv and tv != "nan":
                                try:
                                    tv_dict[sym] = float(tv)
                                except:
                                    pass

                        # 排序函数：PE 升序，总市值升序，缺失值排最后
                        def sort_key(symbol):
                            pe = pe_dict.get(symbol)
                            tv = tv_dict.get(symbol)
                            pe_rank = (
                                pe if (pe is not None and pe > 0) else float("inf")
                            )
                            tv_rank = (
                                tv if (tv is not None and tv > 0) else float("inf")
                            )
                            return (pe_rank, tv_rank)

                        symbols.sort(key=sort_key)
                        print(
                            f"已按 {stock_file} 中的 PE/总市值排序，共 {len(pe_dict)} 条数据"
                        )
                except Exception as e:
                    print(f"读取股票信息文件失败: {e}")

            if not stock_file_exists:
                print(f"未找到对应日期的股票文件，保持 dynamic_list 原始顺序")

            # 3. 分页（每页3个）
            total = len(symbols)
            page = 0
            start = page * 3
            end = min(start + 3, total)
            if start >= total:
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
            [
                Input("backtest-refresh", "n_clicks"),
                Input("backtest-next", "n_clicks"),
                Input("backtest-prev", "n_clicks"),
            ],
            State("backtest-full-list", "data"),
            State("backtest-page", "data"),
            prevent_initial_call=True,
        )
        def refresh_stocks(n_clicks, next_clicks, prev_clicks, full_list, current_page):
            ctx = dash.callback_context
            trigger = (
                ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None
            )
            if not full_list:
                return "", 0, "0-0"

            total = len(full_list)
            page_size = 3
            max_page = (total - 1) // page_size

            if trigger == "backtest-next":
                next_page = min(current_page + 1, max_page)

            elif trigger == "backtest-prev":
                next_page = max(current_page - 1, 0)

            else:
                next_page = current_page  # 中间按钮不动

            start = next_page * page_size
            end = min(start + page_size, total)

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
            # === 注入 stock_list 顺序 ===
            stock_list = data.get("s", [])
            symbol_order = {sym: i for i, sym in enumerate(stock_list)}
            df["_order"] = df["SYMBOL"].map(symbol_order)

            # === 排序 + 分组 ===
            df = (
                df.sort_values(["_order", "DATE"], ascending=[True, False])
                .groupby("SYMBOL", sort=False, as_index=False)
                .head(2)
            )

            df = df.drop(columns=["_order"])
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
                # 第一行：市场 + 回测日期（强制同行，横向滚动）
                html.Div(
                    [
                        html.Div(
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
                            style={
                                "display": "inline-flex",
                                "alignItems": "baseline",
                                "align-content": "flex-end",
                                "flex-direction": "row",
                                "marginRight": "20px",
                                "flexShrink": 0,
                            },
                        ),
                        html.Div(
                            [
                                html.Label("回测日期"),
                                dcc.DatePickerSingle(
                                    id="backtest-date",
                                    date=default_date,
                                    display_format="YYYY-MM-DD",
                                    placeholder="选择日期",
                                    className="custom-date-picker kpi-label",
                                    style={
                                        "width": "auto",
                                        "minWidth": "160px",
                                        "margin-left": "5px",
                                        "zIndex": 1050,
                                    },
                                ),
                            ],
                            style={
                                "display": "inline-flex",
                                "alignItems": "center",
                                "flexShrink": 0,
                            },
                        ),
                    ],
                    style={
                        "flexWrap": "nowrap",
                        # "overflowX": "auto",
                    },
                    className="kpi-label",
                ),
                # 第二行：股票代码区域 + 回测按钮（原样保留）
                dbc.Row(
                    [
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
                                                "marginRight": "12px",
                                                "minWidth": "30%",
                                            },
                                        ),
                                        html.Div(
                                            [
                                                html.Button(
                                                    "◀",
                                                    id="backtest-prev",
                                                    n_clicks=0,
                                                    className="btn btn-light kpi-label btn-left",
                                                    style={
                                                        "background": "transparent",
                                                        "border": "none",
                                                        "cursor": "pointer",
                                                        "color": "#666",
                                                    },
                                                ),
                                                html.Button(
                                                    "刷新",
                                                    id="backtest-refresh",
                                                    n_clicks=0,
                                                    className="btn btn-secondary kpi-label btn-refresh",
                                                    style={"marginRight": "5px"},
                                                ),
                                                html.Button(
                                                    "▶",
                                                    id="backtest-next",
                                                    n_clicks=0,
                                                    className="btn btn-light kpi-label btn-right",
                                                    style={
                                                        "background": "transparent",
                                                        "border": "none",
                                                        "cursor": "pointer",
                                                        "color": "#666",
                                                    },
                                                ),
                                            ],
                                            style={
                                                "display": "flex",
                                                "flex": "0 0 auto",
                                            },
                                        ),
                                    ],
                                    style={
                                        "display": "flex",
                                        "alignItems": "flex-start",
                                        "width": "100%",
                                        "flexWrap": "nowrap",
                                    },
                                ),
                            ],
                            xs=12,
                            sm=8,
                            md=8,
                            lg=9,
                        ),
                        dbc.Col(
                            [
                                html.Label(" "),
                                html.Button(
                                    "执行回测",
                                    id="backtest-run",
                                    n_clicks=0,
                                    className="btn btn-primary kpi-label progress-btn btn-backtest",
                                    style={"width": "100%"},
                                ),
                            ],
                            xs=12,
                            sm=4,
                            md=4,
                            lg=3,
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
