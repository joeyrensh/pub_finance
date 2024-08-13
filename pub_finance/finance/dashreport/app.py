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
    meta_tags=[{"name": "viewport", "content": "width=device-width"}],
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
            # style={"display": "none"},
        ),
    ],
)


@app.callback(
    [
        Output("output-state", "children"),
        Output("url", "pathname"),
        Output("login-page", "style"),
        Output("main-page", "style"),
        Output("page-content", "children"),
    ],
    [
        Input("login-button", "n_clicks"),
        Input("url", "pathname"),
        State("username", "value"),
        State("password", "value"),
    ],
)
def update_output(n_clicks, pathname, username, password):
    ctx = dash.callback_context

    if not ctx.triggered:
        return "", dash.no_update, {}, {"display": "none"}, html.Div("Please log in.")

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if trigger_id == "login-button":
        if n_clicks is not None and (
            username in VALID_USERNAME_PASSWORD_PAIRS
            and password == VALID_USERNAME_PASSWORD_PAIRS[username]
        ):
            return (
                "",
                pathname,
                {"display": "none"},
                {"display": "block"},
                dash.no_update,
            )
        else:
            return (
                html.Div("Invalid username or password", style={"color": "red"}),
                dash.no_update,
                {"display": "flex"},
                {"display": "none"},
                html.Div("Please log in."),
            )
    is_main_page_loading = ctx.states.get("loading.loading_state") == "loading"

    if pathname == "/dash-financial-report/overview":
        return (
            "",
            dash.no_update,
            {},
            {"display": "none"},
            overview.create_layout(app),
        )
    elif pathname == "/dash-financial-report/cn-stock-performance":
        return (
            "",
            dash.no_update,
            {},
            {"display": "none"},
            cnStockPerformance.create_layout(app),
        )
    elif pathname == "/dash-financial-report/us-stock-performance":
        return (
            "",
            dash.no_update,
            {},
            {"display": "none"},
            usStockPerformance.create_layout(app),
        )
    elif pathname == "/dash-financial-report/news-and-reviews":
        return (
            "",
            dash.no_update,
            {},
            {"display": "none"},
            newsReviews.create_layout(app),
        )
    elif pathname == "/dash-financial-report/full-view":
        return (
            "",
            dash.no_update,
            {},
            {"display": "none"},
            html.Div(
                [
                    overview.create_layout(app),
                    cnStockPerformance.create_layout(app),
                    usStockPerformance.create_layout(app),
                    newsReviews.create_layout(app),
                ]
            ),
        )
    else:
        return (
            "",
            dash.no_update,
            {},
            {"display": "none"},
            overview.create_layout(app),
        )


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=80, debug=True)
    # app.run_server()
