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
        dcc.Store(
            id="theme-store", storage_type="session"
        ),  # 使用session存储以确保在页面刷新时保持主题
        html.Div(id="page-content"),
        # 添加 JavaScript 代码来检测系统的主题模式
        html.Script("""
            (function() {
                function detectTheme() {
                    const theme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
                    const themeStore = document.querySelector('#theme-store');
                    themeStore.setAttribute('data', theme);
                    themeStore.dispatchEvent(new Event('storage', { bubbles: true }));
                }
                detectTheme();
                window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', detectTheme);
            })();
        """),
    ],
)


@app.callback(Output("theme-store", "data"), [Input("theme-store", "data")])
def update_theme_store(theme):
    print("theme: ", theme)
    return theme


@app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname"), Input("theme-store", "data")],
)
def update_page_content(pathname, theme):
    print("pathname: ", pathname)
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
