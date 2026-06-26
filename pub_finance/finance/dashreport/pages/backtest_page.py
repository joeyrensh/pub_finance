#!/usr/bin/env python3
"""回测分析页面 - 异步轮询版（单任务，30秒轮询，页面刷新状态恢复）"""

from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import dash
import pandas as pd
from datetime import datetime
import multiprocessing
from multiprocessing import Manager
import uuid

from finance.dashreport.chart_builder import ChartBuilder
from finance.dashreport.utils import Header, make_dash_format_table
from finance.utility.toolkit import ToolKit
from finance.utility.backtrader_exec import BacktraderExec
from finance.utility.tickerinfo import TickerInfo
from finance import FINANCE_ROOT
from finance.dashreport.data_loader import ReportDataLoader
from threading import Lock

BACKTEST_LOCK = Lock()

# 固定任务ID (单任务模式)
SINGLE_TASK_ID = "SINGLE_BACKTEST_TASK"

# 跨进程任务状态存储 (使用 Manager)
_manager = Manager()
task_state = _manager.dict()
task_state["status"] = "idle"  # idle, running, done, failed
task_state["result"] = None
task_state["error"] = None


# ---------- 耗时任务函数 ----------
def run_bt_task(stock_list, date_str, market):
    """在子进程中执行回测并更新 task_state"""
    try:
        pnl, c, tv = run_bt(stock_list, date_str, market)
        tr, pos_detail_df = load_logs(stock_list, date_str, market)

        hist_data = load_hist(stock_list, date_str, market)

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
                pos_detail_df.to_dict("records") if not pos_detail_df.empty else []
            ),
            "h": [
                x.iloc[:-1].to_dict("records") if len(x) > 1 else [] for x in hist_data
            ],
            "s": stock_list,
            "c": c,
            "tv": tv,
            "returns": returns,
            "market": market,
        }
        task_state["status"] = "done"
        task_state["result"] = data
    except Exception as e:
        import traceback

        traceback.print_exc()
        task_state["status"] = "failed"
        task_state["error"] = str(e)


# ---------- 原有业务函数（保持不变）----------
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
    # 行业信息
    f_industry = FINANCE_ROOT / (
        "cnstockinfo/industry.csv" if m == "cn" else "usstockinfo/industry.csv"
    )
    if f_industry.exists():
        df_industry = pd.read_csv(f_industry, usecols=["symbol", "industry"], dtype=str)
        df_industry = df_industry[df_industry["symbol"].isin(stocks)]
    else:
        df_industry = pd.DataFrame(columns=["symbol", "industry"])
    # 最新股票信息
    f_latest_info = FINANCE_ROOT / (
        f"cnstockinfo/stock_{dt}.csv" if m == "cn" else f"usstockinfo/stock_{dt}.csv"
    )
    if f_latest_info.exists():
        df_latest = pd.read_csv(
            f_latest_info, usecols=["symbol", "name", "total_value", "pe"], dtype=str
        )
        df_latest = df_latest[df_latest["symbol"].isin(stocks)]
    else:
        df_latest = pd.DataFrame(columns=["symbol", "name", "total_value", "pe"])
    if not df_logs.empty:
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
        df_logs = df_logs.merge(df_industry, on="symbol", how="left")
        df_logs = df_logs.merge(df_latest, on="symbol", how="left")
        df_logs["industry"] = df_logs["industry"].fillna("-")
        df_logs["total_value"] = (
            pd.to_numeric(df_logs["total_value"], errors="coerce") / 100000000
        ).round(2)
        df_logs["total_value"] = df_logs["total_value"].fillna("-")
        df_logs["pe"] = (
            pd.to_numeric(df_logs["pe"], errors="coerce").round(2).fillna("-")
        )
        logs = df_logs.to_dict("records")
    # 持仓明细
    pos_path = FINANCE_ROOT / (
        "cnstockinfo/cn_backtest_position_detail.csv"
        if m == "cn"
        else "usstockinfo/us_backtest_position_detail.csv"
    )
    if pos_path.exists():
        df_pos = pd.read_csv(
            pos_path, header=None, names=[f"col{i}" for i in range(1, 13)]
        )
        df_pos = df_pos.rename(
            columns={"col2": "symbol", "col3": "date", "col12": "strategy"}
        )[["symbol", "date", "strategy"]]
        df_pos["date"] = pd.to_datetime(df_pos["date"])
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


def get_end_date_from_prefix(prefix: str) -> str:
    data = ReportDataLoader.load(prefix=prefix, datasets=("overall",))
    df_overall = data.get("overall")
    if df_overall is not None and not df_overall.empty:
        end_date = df_overall.at[0, "end_date"]
        if isinstance(end_date, pd.Timestamp):
            return end_date.strftime("%Y%m%d")
        else:
            return str(end_date).replace("-", "")
    return datetime.now().strftime("%Y%m%d")


def get_default_date(market="cn"):
    try:
        return get_end_date_from_prefix(market)
    except Exception:
        try:
            if market == "us":
                return ToolKit.get_us_latest_trade_date(0)
            else:
                return ToolKit.get_cn_latest_trade_date(0)
        except:
            return datetime.now().strftime("%Y%m%d")


class BacktestPage:
    def __init__(self, app, market="cn"):
        self.app = app
        self.market = market
        self.cb = ChartBuilder()
        self.register_callbacks()

    def register_callbacks(self):
        # ---------- 1. 初始化日期 ----------
        @self.app.callback(
            Output("backtest-date", "date"),
            Input("backtest-market", "value"),
            prevent_initial_call=False,
        )
        def init_date(market):
            default = get_default_date(market or "cn")
            if default:
                return datetime.strptime(default, "%Y%m%d").strftime("%Y-%m-%d")
            return None

        # ---------- 2. 加载股票列表 ----------
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
                symbols = (
                    ["SZ002077", "SZ002119"]
                    if market == "cn"
                    else ["AAPL", "MSFT", "GOOGL"]
                )
            # 排序（按PE、市值）
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

                        def sort_key(symbol):
                            pe = pe_dict.get(symbol)
                            tv = tv_dict.get(symbol)
                            return (
                                pe if (pe is not None and pe > 0) else float("inf"),
                                tv if (tv is not None and tv > 0) else float("inf"),
                            )

                        symbols.sort(key=sort_key)
                        print(
                            f"已按 {stock_file} 中的 PE/总市值排序，共 {len(pe_dict)} 条数据"
                        )
                except Exception as e:
                    print(f"读取股票信息文件失败: {e}")
            if not stock_file_exists:
                print(f"未找到对应日期的股票文件，保持 dynamic_list 原始顺序")
            # 分页每页3个
            total = len(symbols)
            start = 0
            end = min(3, total)
            current_stocks = symbols[start:end]
            button_text = f"{1}-{end} / {total}"
            return ",".join(current_stocks), symbols, 0, button_text

        # ---------- 3. 分页翻页 ----------
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
            max_page = (total - 1) // page_size if total > 0 else 0
            if trigger == "backtest-next":
                next_page = 0 if current_page == max_page else current_page + 1
            elif trigger == "backtest-prev":
                next_page = max_page if current_page == 0 else current_page - 1
            else:
                next_page = current_page
            start = next_page * page_size
            end = min(start + page_size, total)
            if start >= total:
                start = 0
                end = min(page_size, total)
                next_page = 0
            current_stocks = full_list[start:end]
            button_text = f"{start + 1}-{end} / {total}"
            return ",".join(current_stocks), next_page, button_text

        # ---------- 4. 页面刷新恢复状态（单次触发）----------
        @self.app.callback(
            [
                Output("backtest-task-id", "data"),
                Output("backtest-run", "disabled"),
                Output("backtest-run", "children"),
                Output("backtest-run", "className"),
                Output("backtest-progress", "style"),
                Output("backtest-status", "children"),
                Output("backtest-poll-interval", "disabled"),
                Output("backtest-data", "data"),
            ],
            Input("backtest-recover-trigger", "n_intervals"),
            prevent_initial_call=False,
        )
        def recover_state(n):
            status = task_state.get("status", "idle")
            # 锁清理：如果任务不在运行中，但锁被意外占用，则强制释放
            if status != "running" and BACKTEST_LOCK.locked():
                BACKTEST_LOCK.release()
                print("WARNING: 检测到残留锁，已强制释放")
            if status == "running":
                # 任务仍在运行中
                return (
                    SINGLE_TASK_ID,
                    True,
                    "回测中…",
                    "btn btn-primary kpi-label progress-btn running",
                    {"display": "block"},
                    "回测任务已提交，后台运行中...",
                    False,
                    None,
                )
            elif status == "done":
                result = task_state.get("result")
                if result:
                    returns = result.get("returns", 0)
                    status_text = f"回测完成 | 收益率：{returns:+.2f}% | 现金：{result['c']:,.0f} | 总值：{result['tv']:,.0f}"
                    # 任务已完成，重置状态为 idle（便于下次提交）
                    task_state["status"] = "idle"
                    return (
                        None,
                        False,
                        "执行回测",
                        "btn btn-primary kpi-label progress-btn",
                        {"display": "none"},
                        status_text,
                        True,
                        result,
                    )
            elif status == "failed":
                error = task_state.get("error", "未知错误")
                task_state["status"] = "idle"
                return (
                    None,
                    False,
                    "执行回测",
                    "btn btn-primary kpi-label progress-btn",
                    {"display": "none"},
                    f"回测失败: {error}",
                    True,
                    None,
                )
            # idle 或其他
            return (
                None,
                False,
                "执行回测",
                "btn btn-primary kpi-label progress-btn",
                {"display": "none"},
                "",
                True,
                None,
            )

        # ---------- 5. 提交回测任务 ----------
        @self.app.callback(
            [
                Output("backtest-task-id", "data", allow_duplicate=True),
                Output("backtest-run", "disabled", allow_duplicate=True),
                Output("backtest-run", "children", allow_duplicate=True),
                Output("backtest-run", "className", allow_duplicate=True),
                Output("backtest-progress", "style", allow_duplicate=True),
                Output("backtest-status", "children", allow_duplicate=True),
                Output("backtest-poll-interval", "disabled", allow_duplicate=True),
            ],
            Input("backtest-run", "n_clicks"),
            [
                State("backtest-market", "value"),
                State("backtest-date", "date"),
                State("backtest-stocks", "value"),
            ],
            prevent_initial_call=True,
        )
        def submit_backtest(n_clicks, market, date, stocks):
            if not n_clicks or not stocks:
                return (
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                )
            # 防御：如果锁被占用但任务状态不是 running，强制释放锁
            if BACKTEST_LOCK.locked() and task_state.get("status") != "running":
                BACKTEST_LOCK.release()
                print("WARNING: 提交前清理残留锁")
            if not BACKTEST_LOCK.acquire(blocking=False):
                return (
                    None,
                    False,
                    "执行回测",
                    "btn btn-primary kpi-label progress-btn",
                    {"display": "none"},
                    "⚠️ 系统正在执行回测，请稍后",
                    True,
                )
            stock_list = [s.strip().upper() for s in stocks.split(",") if s.strip()]
            if not stock_list:
                BACKTEST_LOCK.release()
                return (
                    None,
                    False,
                    "执行回测",
                    "btn btn-primary kpi-label progress-btn",
                    {"display": "none"},
                    "请输入有效的股票代码",
                    True,
                )
            try:
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                date_str = date_obj.strftime("%Y%m%d")
                # 重置任务状态
                task_state["status"] = "running"
                task_state["result"] = None
                task_state["error"] = None
                # 启动子进程
                p = multiprocessing.Process(
                    target=run_bt_task, args=(stock_list, date_str, market)
                )
                p.start()
                # 注意：锁不释放，等待任务完成后释放
                return (
                    SINGLE_TASK_ID,
                    True,
                    "回测中…",
                    "btn btn-primary kpi-label progress-btn running",
                    {"display": "block"},
                    "回测任务已提交，后台运行中...",
                    False,
                )
            except Exception as e:
                BACKTEST_LOCK.release()
                print(f"提交任务失败: {e}")
                return (
                    None,
                    False,
                    "执行回测",
                    "btn btn-primary kpi-label progress-btn",
                    {"display": "none"},
                    f"提交失败: {e}",
                    True,
                )

        # ---------- 6. 轮询任务状态（30秒）----------
        @self.app.callback(
            [
                Output("backtest-data", "data", allow_duplicate=True),
                Output("backtest-status", "children", allow_duplicate=True),
                Output("backtest-run", "disabled", allow_duplicate=True),
                Output("backtest-run", "children", allow_duplicate=True),
                Output("backtest-run", "className", allow_duplicate=True),
                Output("backtest-progress", "style", allow_duplicate=True),
                Output("backtest-poll-interval", "disabled", allow_duplicate=True),
                Output("backtest-task-id", "data", allow_duplicate=True),
            ],
            Input("backtest-poll-interval", "n_intervals"),
            State("backtest-task-id", "data"),
            prevent_initial_call=True,
        )
        def poll_backtest_result(n_intervals, stored_task_id):
            if stored_task_id != SINGLE_TASK_ID:
                return (
                    None,
                    "",
                    False,
                    "执行回测",
                    "btn btn-primary kpi-label progress-btn",
                    {"display": "none"},
                    True,
                    dash.no_update,
                )
            status = task_state.get("status", "idle")
            if status == "running":
                # 任务未完成：无需更新任何前端组件（提示文本保持不变）
                return [dash.no_update] * 8
            elif status == "done":
                result = task_state.get("result")
                if result:
                    returns = result.get("returns", 0)
                    status_text = f"回测完成 | 收益率：{returns:+.2f}% | 现金：{result['c']:,.0f} | 总值：{result['tv']:,.0f}"
                    BACKTEST_LOCK.release()
                    task_state["status"] = "idle"
                    return (
                        result,
                        status_text,
                        False,
                        "执行回测",
                        "btn btn-primary kpi-label progress-btn",
                        {"display": "none"},
                        True,
                        None,
                    )
            elif status == "failed":
                error = task_state.get("error", "未知错误")
                BACKTEST_LOCK.release()
                task_state["status"] = "idle"
                return (
                    None,
                    f"回测失败: {error}",
                    False,
                    "执行回测",
                    "btn btn-primary kpi-label progress-btn",
                    {"display": "none"},
                    True,
                    None,
                )
            # 其他情况，禁用轮询
            return (
                None,
                "",
                False,
                "执行回测",
                "btn btn-primary kpi-label progress-btn",
                {"display": "none"},
                True,
                dash.no_update,
            )

        # ---------- 7. 收益与回撤图表 ----------
        @self.app.callback(
            Output("backtest-pnl-chart", "children"),
            Input("backtest-data", "data"),
            Input("current-theme", "data"),
            Input("client-width", "data"),
            prevent_initial_call=True,
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
                    "doubleClick": False,
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
            prevent_initial_call=True,
        )
        def uk(d, theme, client_width):
            if not d or not d.get("h"):
                return html.Div(
                    "请执行回测",
                    style={"color": "#999", "padding": "60px", "textAlign": "center"},
                )
            stocks = d.get("s", [])
            histories = d.get("h", [])
            all_tr = d.get("tr", [])  # 所有交易记录
            all_pos = d.get("pos_detail", [])  # 所有持仓明细
            charts = []
            width = client_width or 1440

            for i in range(min(6, len(histories))):
                stock_data = pd.DataFrame(histories[i])
                symbol = stocks[i] if i < len(stocks) else f"S{i}"
                if not stock_data.empty and "datetime" in stock_data.columns:
                    stock_data["datetime"] = pd.to_datetime(stock_data["datetime"])
                    stock_data = stock_data.sort_values("datetime")
                    cutoff = stock_data["datetime"].max() - pd.Timedelta(days=360)
                    stock_data = stock_data[stock_data["datetime"] >= cutoff]

                # 预先过滤当前股票的交易记录和持仓明细
                filtered_tr = [t for t in all_tr if t.get("symbol") == symbol]
                filtered_pos = [p for p in all_pos if p.get("symbol") == symbol]
                # 判断是否是最后一张图表
                is_last = i == min(6, len(histories)) - 1
                base_style = {
                    "margin": 0,
                    "padding": 0,
                    "width": "100%",
                    "height": "100%",
                    "aspectRatio": 1.6,
                }
                # 最后一个额外增加底部margin 5px
                if is_last:
                    base_style["marginBottom"] = "10px"

                fig = self.cb.kl_fig(
                    his=stock_data,
                    trades=filtered_tr,
                    pos_detail=filtered_pos,
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
                                "scrollZoom": False,
                                "doubleClick": False,
                                "responsive": True,
                            },
                        ),
                        style=base_style,
                    )
                )
            return html.Div(
                charts, style={"display": "block", "width": "100%", "height": "auto"}
            )

        # ---------- 9. 交易记录 ----------
        @self.app.callback(
            Output("backtest-trade-table", "children"),
            Input("backtest-data", "data"),
            Input("current-theme", "data"),
            Input("client-width", "data"),
            prevent_initial_call=True,
        )
        def update_trade_table(data, theme, client_width):
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
            df = df[[v for k, v in rename_map.items() if v in df.columns]]
            cols_format = {
                "DATE": ("date", "format"),
                "PRICE": ("float",),
                "VOLUME": ("float",),
                "INDUSTRY": ("text",),
                "PE": ("text",),
                "TYPE": ("text", "format"),
            }
            stock_list = data.get("s", [])
            symbol_order = {sym: i for i, sym in enumerate(stock_list)}
            df["_order"] = df["SYMBOL"].map(symbol_order)
            df = (
                df.sort_values(["_order", "DATE"], ascending=[True, False])
                .groupby("SYMBOL", sort=False, as_index=False)
                .head(1)
            )
            df = df.drop(columns=["_order"])
            market = data.get("market", "cn")
            trade_date = get_default_date(market)
            return make_dash_format_table(df, cols_format, market, trade_date)

    # ---------- 布局构建 ----------
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
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("市场"),
                                dcc.RadioItems(
                                    id="backtest-market",
                                    options=[
                                        {"label": "A股", "value": "cn"},
                                        {"label": "美股", "value": "us"},
                                    ],
                                    value="cn",
                                    inline=True,
                                    className="custom-date-picker kpi-label",
                                ),
                            ],
                            style={
                                "display": "inline-flex",
                                "alignItems": "baseline",
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
                                        "marginLeft": "5px",
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
                    style={"flexWrap": "nowrap"},
                    className="kpi-label",
                ),
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
                                            className="kpi-label custom-input",
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
                                                    className="btn btn-light kpi-label nav-btn left",
                                                ),
                                                html.Button(
                                                    "刷新",
                                                    id="backtest-refresh",
                                                    n_clicks=0,
                                                    className="btn btn-secondary kpi-label nav-btn middle",
                                                ),
                                                html.Button(
                                                    "▶",
                                                    id="backtest-next",
                                                    n_clicks=0,
                                                    className="btn btn-light kpi-label nav-btn right",
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
                            style={"marginTop": "-10px"},
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
                    id="backtest-status", style={"marginTop": "10px", "color": "#666"}
                ),
                html.Div(
                    id="backtest-progress", style={"display": "none"}
                ),  # 隐藏进度条容器
                html.Div(
                    [
                        dcc.Store(
                            id="backtest-task-id", storage_type="local", data=None
                        ),
                        dcc.Interval(
                            id="backtest-poll-interval", interval=5000, disabled=True
                        ),
                        dcc.Interval(
                            id="backtest-recover-trigger",
                            interval=100,
                            n_intervals=0,
                            max_intervals=1,
                        ),
                    ],
                    style={"display": "none"},
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
                html.H6(["K线图与买卖点 ‹"], className="subtitle padded"),
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
                            className="table_custom",
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
                dcc.Store(id="backtest-data", data=None),
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
