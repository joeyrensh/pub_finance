import dash_html_components as html
from utils import Header, make_dash_table
import pandas as pd
import pathlib
import base64


def create_layout(app):
    # get relative data folder
    PATH = pathlib.Path(__file__).parent

    # 收益率曲线
    IMAGE_PATH = PATH.joinpath("../../images").resolve()
    with open(IMAGE_PATH.joinpath("TRdraw_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_trdraw = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("us_postion_byindustry_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_by_postion = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("us_postion_byp&l_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_by_pl = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("us_postion_bydate_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_by_positiondate = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("us_postion_byindustry&p&l_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_bypl_date = base64.b64encode(image_data).decode("utf-8")
    # 板块数据
    df = pd.read_csv(
        IMAGE_PATH.joinpath("us_category.csv"), usecols=[i for i in range(1, 15)]
    )
    df = df[
        [
            "IND",
            "OPEN",
            "LRATIO",
            "L5 OPEN",
            "L5 CLOSE",
            "PROFIT",
            "PNL RATIO",
            "AVG TRANS",
            "AVG DAYS",
            "WIN RATE",
            "PROFIT TREND",
        ]
    ]
    df["LRATIO"] = df["LRATIO"].apply(lambda x: "{:.2%}".format(x))
    df["PROFIT"] = df["PROFIT"].apply(lambda x: "{:.0f}".format(x))
    df["PNL RATIO"] = df["PNL RATIO"].apply(lambda x: "{:.2%}".format(x))
    df["AVG TRANS"] = df["AVG TRANS"].apply(lambda x: "{:.2f}".format(x))
    df["AVG DAYS"] = df["AVG DAYS"].apply(lambda x: "{:.2f}".format(x))
    df["WIN RATE"] = df["WIN RATE"].apply(lambda x: "{:.2%}".format(x))

    # 持仓明细
    df_detail = pd.read_csv(
        IMAGE_PATH.joinpath("us_stockdetail.csv"), usecols=[i for i in range(1, 14)]
    )
    df_detail = df_detail[
        [
            "SYMBOL",
            "OPEN DATE",
            "BASE",
            "ADJBASE",
            "PNL",
            "PNL RATIO",
            "IND",
            "NAME",
            "AVG TRANS",
            "AVG DAYS",
            "WIN RATE",
            "TOTAL PNL RATIO",
            "Strategy",
        ]
    ]
    df_detail["BASE"] = df_detail["BASE"].apply(lambda x: "{:.2f}".format(x))
    df_detail["ADJBASE"] = df_detail["ADJBASE"].apply(lambda x: "{:.2f}".format(x))
    df_detail["PNL"] = df_detail["PNL"].apply(lambda x: "{:.2f}".format(x))
    df_detail["AVG TRANS"] = df_detail["AVG TRANS"].apply(lambda x: "{:.2f}".format(x))
    df_detail["AVG DAYS"] = df_detail["AVG DAYS"].apply(lambda x: "{:.2f}".format(x))
    df_detail["PNL RATIO"] = df_detail["PNL RATIO"].apply(lambda x: "{:.2%}".format(x))
    df_detail["WIN RATE"] = df_detail["WIN RATE"].apply(lambda x: "{:.2%}".format(x))
    df_detail["TOTAL PNL RATIO"] = df_detail["TOTAL PNL RATIO"].apply(
        lambda x: "{:.2%}".format(x)
    )
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
                                        ["美股年化收益率分析"],
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
                                    html.Br([]),
                                    html.H6(["持仓占比"], className="subtitle padded"),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_postion}",
                                        style={"width": "100%"},
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.Br([]),
                                    html.H6(
                                        ["盈利占比"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_pl}",
                                        style={"width": "100%"},
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
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        "美股板块分析",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                make_dash_table(df),
                                                className="gs-table-header",
                                                # className="tiny-header",
                                            )
                                        ],
                                        style={"overflow-x": "auto", "height": 400},
                                    ),
                                ],
                                className="twelve columns",
                                # dangerously_allow_html=True,
                            ),
                        ],
                        className="row",
                        style={"margin-bottom": "35px"},
                    ),
                    # Row 4
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        "美股持仓分析",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                make_dash_table(df_detail),
                                                className="gs-table-header",
                                                # className="tiny-header",
                                            )
                                        ],
                                        style={"overflow-x": "auto", "height": 400},
                                    ),
                                ],
                                className="twelve columns",
                                # dangerously_allow_html=True,
                            ),
                        ],
                        className="row",
                        style={"margin-bottom": "35px"},
                    ),
                ],
                className="sub_page",
            ),
        ],
        className="page",
    )