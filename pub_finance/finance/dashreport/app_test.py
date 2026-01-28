# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, MATCH
from dash import clientside_callback
from finance.dashreport.pages import (
    cnstock_performance,
    slogans,
    usstock_performance_test as usstock_performance,
    usspecialstock_performance,
    cndynamicstock_performance,
    usdynamicstock_performance,
)
from finance.dashreport.pages.chart_callback import ChartCallback
from flask import Flask
from flask_compress import Compress
import configparser
from flask import session
import os
from datetime import timedelta
from pathlib import Path
from finance.dashreport.chart_builder import ChartBuilder


server = Flask(__name__)
Compress(server)

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
)
app.title = "Financial Report"

# Describe the layout/ UI of the app
app.layout = html.Div(
    children=[
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="current-theme", data="light"),
        dcc.Store(id="auth-checked", data=False),  # 标记是否已检查登录
        dcc.Store(id="client-width"),
        dcc.Interval(
            id="theme-poller",
            interval=1000,  # 1 秒
            n_intervals=0,
        ),
        html.Div(
            id="loading-mask",
            children=[
                dcc.Loading(
                    id="init-loading",
                    type="dot",
                    fullscreen=True,
                    color="#119DFF",
                    children=[],
                )
            ],
            style={"display": "block"},
        ),
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
                dcc.Loading(
                    id="loading",
                    type="dot",
                    fullscreen=False,
                    color="#119DFF",
                    style={"zIndex": "1000"},
                    children=[
                        html.Div(id="page-content"),
                    ],
                    className="loading-dot",
                ),
            ],
        ),
    ],
)
# 1. 创建实例
chart_callback = ChartCallback()

# 2. 设置回调（只调用一次）
chart_callback.setup_callback(app)

# 3. 存储到app中供页面使用
app.chart_callback = chart_callback
# ======================================================

app.clientside_callback(
    """
    function(n, currentTheme) {
        const isDark = window.matchMedia &&
            window.matchMedia('(prefers-color-scheme: dark)').matches;

        const newTheme = isDark ? 'dark' : 'light';

        if (newTheme === currentTheme) {
            return window.dash_clientside.no_update;
        }

        console.log('🎨 Theme changed:', currentTheme, '->', newTheme);
        return newTheme;
    }
    """,
    Output("current-theme", "data"),
    Input("theme-poller", "n_intervals"),
    State("current-theme", "data"),
)

app.clientside_callback(
    """
    function(_) {
        return window.innerWidth || document.documentElement.clientWidth;
    }
    """,
    output=dash.Output("client-width", "data"),
    inputs=[dash.Input("calendar-chart", "id")],
)


@app.callback(
    [
        Output("output-state", "children"),
        Output("login-page", "style"),
        Output("main-page", "style"),
        Output("loading-mask", "style"),
        Output("auth-checked", "data"),
    ],
    [
        Input("login-button", "n_clicks"),
        State("username", "value"),
        State("password", "value"),
        State("auth-checked", "data"),
    ],
)
def handle_login(n_clicks, username, password, auth_checked):
    # 页面首次加载或刷新时，n_clicks is None
    if n_clicks is None and not auth_checked:
        if session.get("logged_in"):
            # 已登录，显示主页面
            return (
                "",
                {"display": "none"},
                {"display": "block"},
                {"display": "none"},
                True,
            )
        else:
            # 未登录，显示登录页面
            return (
                "",
                {"display": "flex"},
                {"display": "none"},
                {"display": "none"},
                True,
            )

    # 登录按钮被点击
    if username == VALID_USERNAME and password == VALID_PASSWORD:
        session.permanent = True
        session["logged_in"] = True
        return "", {"display": "none"}, {"display": "block"}, {"display": "none"}, True
    else:
        session["logged_in"] = False
        return (
            html.Div("Invalid username or password", style={"color": "red"}),
            {"display": "flex"},
            {"display": "none"},
            {"display": "none"},
            True,
        )


@app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname")],
)
def update_page_content(pathname):
    if pathname == "/dash-financial-report/overview":
        # return overview.create_layout(app)
        return cnstock_performance.create_layout(app)
    elif pathname == "/dash-financial-report/cn-stock-performance":
        return cnstock_performance.create_layout(app)
    elif pathname == "/dash-financial-report/us-stock-performance":
        return usstock_performance.create_layout(app)
    elif pathname == "/dash-financial-report/us-special-stock-performance":
        return usspecialstock_performance.create_layout(app)
    elif pathname == "/dash-financial-report/cn-dynamic-stock-performance":
        return cndynamicstock_performance.create_layout(app)
    elif pathname == "/dash-financial-report/us-dynamic-stock-performance":
        return usdynamicstock_performance.create_layout(app)
    elif pathname == "/dash-financial-report/slogans":
        return slogans.create_layout(app)
    elif pathname == "/dash-financial-report/full-view":
        return [
            cnstock_performance.create_layout(app),
            usstock_performance.create_layout(app),
            # overview.create_layout(app),
            slogans.create_layout(app),
        ]
    else:
        return (cnstock_performance.create_layout(app),)


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


@app.callback(
    Output("calendar-chart", "figure"),
    Input("client-width", "data"),
)
def update_chart(client_width):
    if not client_width:
        client_width = 1440  # fallback

    fig = ChartBuilder.calendar_heatmap(
        df=df,
        theme="light",
        fig_width=client_width,  # 👈 关键
    )
    return fig


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=80, debug=True)
    # app.run_server()
