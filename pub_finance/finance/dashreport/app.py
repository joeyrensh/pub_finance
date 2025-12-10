# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, MATCH
from pages import (
    cnstock_performance,
    slogans,
    usstock_performance,
    usspecialstock_performance,
)

from flask import Flask
from flask_compress import Compress
import configparser
from flask import session
import os
from datetime import timedelta

server = Flask(__name__)
Compress(server)

# 读取配置文件
config = configparser.ConfigParser()
config.read("login.ini")

VALID_USERNAME = config["credentials"]["username"]
VALID_PASSWORD = config["credentials"]["password"]

# 设置flask session的密钥和过期时间
server.secret_key = os.urandom(24)
server.permanent_session_lifetime = timedelta(minutes=60)

app = dash.Dash(
    __name__,
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
        dcc.Store(id="auth-checked", data=False),  # 标记是否已检查登录
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
                html.Div(
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
                            type="submit",
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


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=80, debug=True)
    # app.run_server()
