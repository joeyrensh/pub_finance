import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import numpy as np

from utils import Header, make_dash_table

import pandas as pd
import pathlib

# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../data").resolve()


df_fund_facts = pd.read_csv(DATA_PATH.joinpath("df_fund_facts.csv"))
df_price_perf = pd.read_csv(DATA_PATH.joinpath("df_price_perf.csv"))


def create_layout(app):
    # Page layouts
    return html.Div(
        [
            html.Div([Header(app)]),
            # page 1
            html.Div(
                [
                    # Row 3
                    # html.Div(
                    #     [
                    #         html.Div(
                    #             [
                    #                 html.H6(
                    #                     "Risk Potential", className="subtitle padded"
                    #                 ),
                    #                 html.Img(
                    #                     src=app.get_asset_url("risk_reward.png"),
                    #                     className="risk-reward",
                    #                 ),
                    #             ],
                    #             className="six columns",
                    #         ),
                    #     ]
                    # )
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                         "Risk Potential", className="subtitle padded"
                                    ),
                                    dcc.Graph(
                                        id="graph-1",
                                        figure={
                                            "data": [
                                                go.Bar(
                                                    x=np.linspace(1, 5, 5),
                                                    y=np.full(5, 1),
                                                    marker={
                                                        "color": np.linspace(1, 5, 5), "colorscale": "redor"
                                                    },
                                                    name="Calibre Index Fund",
                                                ),
                                                go.Scatter(
                                                    x=[0.6, 0.6, 5.4, 0.6],
                                                    y=[0, 1, 1, 0],
                                                    fill="toself",
                                                    fillcolor="white",
                                                    mode="none",
                                                ),
                                                go.Scatter(
                                                    x=[4],
                                                    y=[0.4],
                                                    text=[4],
                                                    mode="markers+text",
                                                    marker={"size": 80, "color": "white"},
                                                    textfont={"size": 20},
                                                ),
                                            ],
                                            "layout": go.Layout(
                                                autosize=False,
                                                bargap=0.35,
                                                font={"family": "Raleway", "size": 10},
                                                height=200,
                                                hovermode="closest",
                                                template="plotly_white",
                                                margin={
                                                    "r": 0,
                                                    "t": 20,
                                                    "b": 10,
                                                    "l": 10,
                                                },
                                                showlegend=False,
                                                title="",
                                                width=330,
                                                xaxis={"visible": False},
                                                yaxis={"visible": False},
                                            ),
                                        },
                                        config={"displayModeBar": False},
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                        className="row",
                        style={"margin-bottom": "35px"},
                    ),
                ]
            )
        ]
    )

                            