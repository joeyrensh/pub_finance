import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from utils import Header, make_dash_table
import pandas as pd
import pathlib
import base64

# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../data").resolve()

# 收益率曲线
IMAGE_PATH = PATH.joinpath("../../images").resolve()
with open(IMAGE_PATH.joinpath("CNTRdraw_light.png"), "rb") as f:
    image_data = f.read()
    encoded_image_trdraw = base64.b64encode(image_data).decode("utf-8")
with open(IMAGE_PATH.joinpath("cn_postion_byindustry_light.png"), "rb") as f:
    image_data = f.read()
    encoded_image_by_postion = base64.b64encode(image_data).decode("utf-8")
with open(IMAGE_PATH.joinpath("cn_postion_byp&l_light.png"), "rb") as f:
    image_data = f.read()
    encoded_image_by_pl = base64.b64encode(image_data).decode("utf-8")
with open(IMAGE_PATH.joinpath("cn_postion_bydate_light.png"), "rb") as f:
    image_data = f.read()
    encoded_image_by_positiondate = base64.b64encode(image_data).decode("utf-8")
with open(IMAGE_PATH.joinpath("cn_postion_byindustry&date_light.png"), "rb") as f:
    image_data = f.read()
    encoded_image_bypl_date = base64.b64encode(image_data).decode("utf-8")

df_current_prices = pd.read_csv(DATA_PATH.joinpath("df_current_prices.csv"))
df_hist_prices = pd.read_csv(DATA_PATH.joinpath("df_hist_prices.csv"))
df_avg_returns = pd.read_csv(DATA_PATH.joinpath("df_avg_returns.csv"))
df_after_tax = pd.read_csv(DATA_PATH.joinpath("df_after_tax.csv"))
df_recent_returns = pd.read_csv(DATA_PATH.joinpath("df_recent_returns.csv"))
df_graph = pd.read_csv(DATA_PATH.joinpath("df_graph.csv"))


def create_layout(app):
    return html.Div(
        [
            Header(app),
            # page 2
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        ["A股年化收益率分析"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_trdraw}",
                                        style={"width": "100%"},
                                    ),
                                ],
                                className="twelve columns",
                            )
                        ],
                        className="row ",
                    ),
                    # Row
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(["持仓占比"], className="subtitle padded"),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_postion}",
                                        style={"width": "80%"},
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.H6(
                                        ["盈利占比"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_pl}",
                                        style={"width": "80%"},
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                        className="row ",
                    ),
                    # Row 2
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(["持仓趋势"], className="subtitle padded"),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_positiondate}",
                                        style={"width": "100%"},
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.H6(
                                        ["盈利趋势"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_bypl_date}",
                                        style={"width": "100%"},
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                        className="row ",
                    ),
                    # Row 3
                    # Row 4
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        [
                                            "After-tax returns--updated quarterly as of 12/31/2017"
                                        ],
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Table(
                                                make_dash_table(df_after_tax),
                                                className="tiny-header",
                                            )
                                        ],
                                        style={"overflow-x": "auto"},
                                    ),
                                ],
                                className=" twelve columns",
                            )
                        ],
                        className="row ",
                    ),
                    # Row 5
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        ["Recent investment returns"],
                                        className="subtitle padded",
                                    ),
                                    html.Table(
                                        make_dash_table(df_recent_returns),
                                        className="tiny-header",
                                    ),
                                ],
                                className=" twelve columns",
                            )
                        ],
                        className="row ",
                    ),
                ],
                className="sub_page",
            ),
        ],
        className="page",
    )
