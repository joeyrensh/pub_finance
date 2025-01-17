# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from pages import cnstock_performance, news_reviews, usstock_performance
from pages import (
    overview,
)
from flask import Flask
from flask_compress import Compress
from urllib.parse import urlparse, parse_qs


server = Flask(__name__)
Compress(server)

VALID_USERNAME_PASSWORD_PAIRS = {"admin": "123"}
# theme = "dark"
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
        html.Div(
            id="login-page",
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
                        ),
                        dcc.Input(
                            id="password",
                            type="password",
                            placeholder="Password",
                            className="input-box",
                        ),
                        html.Button(
                            "Login", id="login-button", className="login-button"
                        ),
                        html.Div(id="output-state"),
                        # 添加备案号链接
                        html.Div(
                            className="备案号",  # 可以自定义一个类名用于样式
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
            style={
                "display": "block",
            },
        ),
    ],
)


@app.callback(
    [
        Output("output-state", "children"),
        Output("login-page", "style"),
        Output("main-page", "style"),
    ],
    [
        Input("login-button", "n_clicks"),
        State("username", "value"),
        State("password", "value"),
    ],
)
def handle_login(n_clicks, username, password):
    if n_clicks is None:
        return "", {"display": "flex"}, {"display": "none"}

    if (
        username in VALID_USERNAME_PASSWORD_PAIRS
        and password == VALID_USERNAME_PASSWORD_PAIRS[username]
    ):
        return "", {"display": "none"}, {"display": "block"}
    else:
        return (
            html.Div("Invalid username or password", style={"color": "red"}),
            {"display": "flex"},
            {"display": "none"},
        )


@app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname"), State("url", "search")],
)
def update_page_content(pathname, search):
    # 解析查询字符串以获取 theme 参数
    parsed_url = urlparse(search)
    query_params = parse_qs(parsed_url.query)
    theme = query_params.get("theme", ["dark"])[
        0
    ]  # 如果没有指定 theme，默认为 'default'
    if pathname == "/dash-financial-report/overview":
        # return overview.create_layout(app)
        return cnstock_performance.create_layout(app, theme)
    elif pathname == "/dash-financial-report/cn-stock-performance":
        return cnstock_performance.create_layout(app, theme)
    elif pathname == "/dash-financial-report/us-stock-performance":
        return usstock_performance.create_layout(app, theme)
    elif pathname == "/dash-financial-report/news-and-reviews":
        return news_reviews.create_layout(app, theme)
    elif pathname == "/dash-financial-report/full-view":
        return [
            cnstock_performance.create_layout(app, theme),
            usstock_performance.create_layout(app, theme),
            # overview.create_layout(app),
            news_reviews.create_layout(app, theme),
        ]
    else:
        return (cnstock_performance.create_layout(app, theme),)


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=80, debug=True)
    # app.run_server()
