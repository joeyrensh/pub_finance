# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
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

app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width"}],
    server=server,
)
app.title = "Financial Report"
# server = app.server

# Describe the layout/ UI of the app
app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        dcc.Loading(
            id="loading",
            type="dot",
            fullscreen=True,
            color="#119DFF",
            style={"zIndex": "1000"},
            children=[html.Div(id="page-content")],
        ),
    ]
)


# Update page
@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def display_page(pathname):
    if pathname == "/dash-financial-report/cn-stock-performance":
        return cnStockPerformance.create_layout(app)
    elif pathname == "/dash-financial-report/us-stock-performance":
        return usStockPerformance.create_layout(app)
    elif pathname == "/dash-financial-report/news-and-reviews":
        return newsReviews.create_layout(app)
    elif pathname == "/dash-financial-report/full-view":
        return (
            overview.create_layout(app),
            cnStockPerformance.create_layout(app),
            usStockPerformance.create_layout(app),
            newsReviews.create_layout(app),
        )
    else:
        return overview.create_layout(app)


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)
    # app.run_server()
