# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, MATCH
from dash import clientside_callback
from finance.dashreport.pages import slogans
from finance.dashreport.pages.page import Page as page_creater
from finance.dashreport.pages.chart_callback import ChartCallback
from finance.dashreport.pages.table_callback import TableCallback
from finance.dashreport.pages.kpi_callback import KpiCallback
from flask import Flask
from flask_compress import Compress
import configparser
from flask import session
import os
from datetime import timedelta
from pathlib import Path


server = Flask(__name__)
Compress(
    server,
)
server.config["COMPRESS_LEVEL"] = 6
server.config["COMPRESS_MIN_SIZE"] = 1000

# 读取配置文件
config = configparser.ConfigParser()
BASE_DIR = Path(__file__).resolve().parent
config.read(BASE_DIR / "login.ini")

VALID_USERNAME = config["credentials"]["username"]
VALID_PASSWORD = config["credentials"]["password"]

# 设置flask session的密钥和过期时间
server.secret_key = os.urandom(24)
server.permanent_session_lifetime = timedelta(minutes=1440)

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    meta_tags=[
        {
            "name": "viewport",
            "content": "width=device-width, initial-scale=1.0, user-scalable=no, maximum-scale=1.0",
        }
    ],
    server=server,
    serve_locally=False,
)
app.title = "Financial Report"

# 列出应用中允许的页面路径，用于登录后验证并决定是否重定向
ALLOWED_PATHS = {
    "/dash-financial-report/overview",
    "/dash-financial-report/cn-stock-performance",
    "/dash-financial-report/us-stock-performance",
    "/dash-financial-report/us-special-stock-performance",
    "/dash-financial-report/cn-dynamic-stock-performance",
    "/dash-financial-report/us-dynamic-stock-performance",
    "/dash-financial-report/slogans",
    "/dash-financial-report/full-view",
}

# Describe the layout/ UI of the app
app.layout = html.Div(
    children=[
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="auth-checked", data=False),  # 标记是否已检查登录
        dcc.Store(id="current-theme", data="light"),
        dcc.Store(id="client-width", data=1440),
        # 用于触发客户端事件
        html.Button(id="client-event", style={"display": "none"}),
        # dcc.Interval(
        #     id="theme-poller",
        #     interval=1000,  # 1 秒
        #     n_intervals=0,
        # ),
        html.Div(
            id="login-page",
            style={"display": "none"},
            children=[
                html.Form(
                    className="login-box",
                    children=[
                        html.H2("Login"),
                        dcc.Input(
                            id="username",
                            type="text",
                            placeholder="Username",
                            className="input-box",
                            name="username",
                            autoComplete="username",
                        ),
                        dcc.Input(
                            id="password",
                            type="password",
                            placeholder="Password",
                            className="input-box",
                            name="password",
                            autoComplete="current-password",
                        ),
                        html.Button(
                            "Login",
                            id="login-button",
                            className="login-button",
                            type="button",
                        ),
                        html.Div(id="output-state"),
                        html.Div(
                            className="备案号",
                            children=[
                                html.A(
                                    "沪ICP备2024089333号",
                                    href="https://beian.miit.gov.cn/",
                                    target="_blank",
                                )
                            ],
                        ),
                    ],
                )
            ],
            className="background",
        ),
        html.Div(
            id="main-page",
            style={"display": "none"},
            children=[
                html.Div(id="page-content"),
            ],
        ),
    ],
)
# ===== 1. 注册chart回调 =====
chart_callback = ChartCallback()
chart_callback.setup_callback(app)
app.chart_callback = chart_callback

# ===== 注册 table callback =====
table_callback = TableCallback()
table_callback.setup_callback(app)
app.table_callback = table_callback

# ===== 注册 KPI 回调 =====
kpi_callback = KpiCallback()
kpi_callback.setup_callback(app)
app.kpi_callback = kpi_callback
# ======================================================

app.clientside_callback(
    """
    function(n, currentTheme, currentWidth) {
        const isDark = window.matchMedia &&
            window.matchMedia('(prefers-color-scheme: dark)').matches;

        const newTheme = isDark ? 'dark' : 'light';
        const width = window.innerWidth || document.documentElement.clientWidth;

        let themeOut = window.dash_clientside.no_update;
        let widthOut = window.dash_clientside.no_update;

        if (newTheme !== currentTheme) {
            console.log('Theme changed:', currentTheme, '->', newTheme);
            themeOut = newTheme;
        }

        if (width !== currentWidth) {
            console.log('Width changed:', currentWidth, '->', width);
            widthOut = width;
        }

        return [themeOut, widthOut];
    }
    """,
    [
        Output("current-theme", "data"),
        Output("client-width", "data"),
    ],
    # Input("theme-poller", "n_intervals"),
    Input("client-event", "n_clicks"),
    [
        State("current-theme", "data"),
        State("client-width", "data"),
    ],
)


@app.callback(
    [
        Output("output-state", "children"),
        Output("login-page", "style"),
        Output("main-page", "style"),
        Output("auth-checked", "data"),
        Output("url", "pathname"),
    ],
    [
        Input("login-button", "n_clicks"),
        State("username", "value"),
        State("password", "value"),
        State("auth-checked", "data"),
        State("url", "pathname"),
    ],
)
def handle_login(n_clicks, username, password, auth_checked, current_pathname):
    # 页面首次加载或刷新时，n_clicks is None
    if n_clicks is None and not auth_checked:
        if session.get("logged_in"):
            return (
                "",
                {"display": "none"},
                {"display": "block"},
                True,
                dash.no_update,
            )
        else:
            # 未登录，显示登录页面，不修改 URL
            return (
                "",
                {"display": "flex"},
                {"display": "none"},
                True,
                dash.no_update,
            )

    # 登录按钮被点击
    if username == VALID_USERNAME and password == VALID_PASSWORD:
        session.permanent = True
        session["logged_in"] = True
        # 登录成功：仅在当前 pathname 合法时保留，否则不修改 URL（不强制重定向）
        target = (
            current_pathname if current_pathname in ALLOWED_PATHS else dash.no_update
        )
        return (
            "",
            {"display": "none"},
            {"display": "block"},
            True,
            target,
        )
    else:
        session["logged_in"] = False
        return (
            html.Div("Invalid username or password", style={"color": "red"}),
            {"display": "flex"},
            {"display": "none"},
            True,
            dash.no_update,
        )


@app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname")],
)
def update_page_content(pathname):
    if pathname == "/dash-financial-report/overview":
        return page_creater(
            app,
            "cn",
            show_charts=[
                "annual_return",
                "heatmap",
                "strategy",
                "trade",
                "pnl_trend",
                "industry_position",
                "industry_profit",
            ],
            show_tables=["category", "detail", "cn_etf", "detail_short"],
        ).get_layout()
    elif pathname == "/dash-financial-report/cn-stock-performance":
        return page_creater(
            app,
            "cn",
            show_charts=[
                "annual_return",
                "heatmap",
                "strategy",
                "trade",
                "pnl_trend",
                "industry_position",
                "industry_profit",
            ],
            show_tables=["category", "detail", "cn_etf", "detail_short"],
        ).get_layout()
    elif pathname == "/dash-financial-report/us-stock-performance":
        return page_creater(
            app,
            "us",
            show_charts=[
                "annual_return",
                "heatmap",
                "strategy",
                "trade",
                "pnl_trend",
                "industry_position",
                "industry_profit",
            ],
            show_tables=["category", "detail", "detail_short"],
        ).get_layout()
    elif pathname == "/dash-financial-report/us-special-stock-performance":
        return page_creater(
            app,
            "us_special",
            show_charts=[
                "annual_return",
                "heatmap",
                "strategy",
                "trade",
                "pnl_trend",
                "industry_position",
                "industry_profit",
            ],
            show_tables=["category", "detail", "detail_short"],
        ).get_layout()
    elif pathname == "/dash-financial-report/cn-dynamic-stock-performance":
        return page_creater(
            app,
            "cn_dynamic",
            show_charts=[
                "annual_return",
                "heatmap",
                "strategy",
                "trade",
                "pnl_trend",
                "industry_position",
                "industry_profit",
            ],
            show_tables=["category", "detail"],
        ).get_layout()
    elif pathname == "/dash-financial-report/us-dynamic-stock-performance":
        return page_creater(
            app,
            "us_dynamic",
            show_charts=[
                "annual_return",
                "heatmap",
                "strategy",
                "trade",
                "pnl_trend",
                "industry_position",
                "industry_profit",
            ],
            show_tables=["category", "detail"],
        ).get_layout()
    elif pathname == "/dash-financial-report/slogans":
        return slogans.create_layout(app)
    elif pathname == "/dash-financial-report/full-view":
        return [
            page_creater(
                app,
                "cn",
                show_charts=[
                    "annual_return",
                    "heatmap",
                    "strategy",
                    "trade",
                    "pnl_trend",
                    "industry_position",
                    "industry_profit",
                ],
                show_tables=["category", "detail", "cn_etf", "detail_short"],
            ).get_layout(),
            page_creater(
                app,
                "us",
                show_charts=[
                    "annual_return",
                    "heatmap",
                    "strategy",
                    "trade",
                    "pnl_trend",
                    "industry_position",
                    "industry_profit",
                ],
                show_tables=["category", "detail", "detail_short"],
            ).get_layout(),
            slogans.create_layout(app),
        ]
    else:
        return (
            page_creater(
                app,
                "cn",
                show_charts=[
                    "annual_return",
                    "heatmap",
                    "strategy",
                    "trade",
                    "pnl_trend",
                    "industry_position",
                    "industry_profit",
                ],
                show_tables=["category", "detail", "cn_etf", "detail_short"],
            ).get_layout(),
        )


@app.callback(
    Output({"type": "collapsible", "page": MATCH, "index": MATCH}, "style"),
    Output({"type": "collapse-btn", "page": MATCH, "index": MATCH}, "children"),
    Input({"type": "collapse-btn", "page": MATCH, "index": MATCH}, "n_clicks"),
    State({"type": "collapsible", "page": MATCH, "index": MATCH}, "style"),
    State({"type": "collapse-btn", "page": MATCH, "index": MATCH}, "children"),
    prevent_initial_call=True,
)
def toggle_collapse(n_clicks, current_style, btn_content):
    # 获取当前显示状态
    current_display = current_style.get("display", "block")

    # 切换显示状态
    new_display = "none" if current_display == "block" else "block"

    # 解析原始标题文本（去掉箭头）
    original_text = btn_content["props"]["children"][0].rstrip(" ‹›")
    new_arrow = "‹" if new_display == "block" else "›"

    # 构建新的标题元素
    new_title = html.H6([f"{original_text} {new_arrow}"], className="subtitle padded")

    return {"display": new_display}, new_title


@app.callback(
    Output(
        {"type": "auto-table-count", "table": MATCH},
        "children",
    ),
    Input(
        {"type": "auto-table", "table": MATCH},
        "derived_virtual_indices",
    ),
)
def update_row_count(indices):
    if indices is None:
        return "Total 0 Rows"
    return f"Total {len(indices)} Rows"


if __name__ == "__main__":
    app.run_server(
        host="0.0.0.0",
        port=80,
        debug=False,
        dev_tools_hot_reload=False,  # 禁用热重载，减少资源占用
        dev_tools_ui=False,  # 禁用调试面板
        processes=1,  # 核心：单进程，避免全局数据多进程重复加载
        threaded=True,  # 开启多线程，处理并发请求（单进程下不影响全局数据）
    )
    # app.run_server()
