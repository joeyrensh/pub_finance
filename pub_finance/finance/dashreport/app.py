# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from pages import (
    overview,
    cnStockPerformance,
    usStockPerformance,
    newsReviews,
)
from flask import Flask
from flask_compress import Compress


server = Flask(__name__)
Compress(server)

VALID_USERNAME_PASSWORD_PAIRS = {"admin": "123"}

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
                    fullscreen=True,
                    color="#119DFF",
                    style={"zIndex": "1000"},
                    children=[
                        html.Div(id="page-content"),
                    ],
                ),
            ],
            style={"display": "none"},
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
    [
        Input("url", "pathname"),
    ],
)
def update_page_content(pathname):
    if pathname == "/dash-financial-report/overview":
        return overview.create_layout(app)
    elif pathname == "/dash-financial-report/cn-stock-performance":
        return cnStockPerformance.create_layout(app)
    elif pathname == "/dash-financial-report/us-stock-performance":
        return usStockPerformance.create_layout(app)
    elif pathname == "/dash-financial-report/news-and-reviews":
        return newsReviews.create_layout(app)
    elif pathname == "/dash-financial-report/full-view":
        return [
            overview.create_layout(app),
            cnStockPerformance.create_layout(app),
            usStockPerformance.create_layout(app),
            newsReviews.create_layout(app),
        ]
    else:
        return overview.create_layout(app)


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=80, debug=True)
    # app.run_server()
