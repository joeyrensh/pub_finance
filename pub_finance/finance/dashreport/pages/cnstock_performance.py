import dash_html_components as html
from utils import Header, make_dash_format_table
import pandas as pd
import pathlib
import base64


def create_layout(app):
    # get relative data folder
    PATH = pathlib.Path(__file__).parent

    # 收益率曲线
    IMAGE_PATH = PATH.joinpath("../../images").resolve()
    DATA_PATH = PATH.joinpath("../../data").resolve()
    with open(IMAGE_PATH.joinpath("cn_tr_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_trdraw = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("cn_postion_byindustry_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_by_postion = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("cn_pl_byindustry_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_by_pl = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("cn_trade_trend_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_by_positiondate = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("cn_top_industry_pl_trend_light.png"), "rb") as f:
        image_data = f.read()
        encoded_image_bypl_date = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("cn_strategy_tracking.png"), "rb") as f:
        image_data = f.read()
        encoded_image_strategy = base64.b64encode(image_data).decode("utf-8")
    with open(IMAGE_PATH.joinpath("cn_industry_trend_heatmap.png"), "rb") as f:
        image_data = f.read()
        encoded_image_ind_trend = base64.b64encode(image_data).decode("utf-8")
    # Overall 信息
    df_overall = pd.read_csv(
        DATA_PATH.joinpath("cn_df_result.csv"),
        usecols=[i for i in range(1, 5)],
    )
    # 板块数据
    df = pd.read_csv(
        DATA_PATH.joinpath("cn_category.csv"), usecols=[i for i in range(1, 16)]
    )
    df["INDEX"] = df.index
    df = df[
        [
            "INDEX",
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
    ].copy()
    cols_format_category = {
        "LRATIO": ("ratio", "format"),
        "PROFIT": ("float", "format"),
        "PNL RATIO": ("ratio", "format"),
        "AVG TRANS": ("float",),
        "AVG DAYS": ("float",),
        "WIN RATE": ("ratio", "format"),
    }

    # 持仓明细
    df_detail = pd.read_csv(
        DATA_PATH.joinpath("cn_stockdetail.csv"), usecols=[i for i in range(1, 16)]
    )
    df_detail["INDEX"] = df_detail.index
    df_detail = df_detail[
        [
            "INDEX",
            "SYMBOL",
            "IND",
            "NAME",
            "EPR",
            "OPEN DATE",
            "BASE",
            "ADJBASE",
            "PNL",
            "PNL RATIO",
            "AVG TRANS",
            "AVG DAYS",
            "WIN RATE",
            "TOTAL PNL RATIO",
            "STRATEGY",
        ]
    ].copy()
    cols_format_detail = {
        "BASE": ("float",),
        "ADJBASE": ("float",),
        "PNL": ("float", "format"),
        "AVG TRANS": ("float",),
        "AVG DAYS": ("float",),
        "PNL RATIO": ("ratio", "format"),
        "WIN RATE": ("ratio", "format"),
        "TOTAL PNL RATIO": ("ratio", "format"),
        "OPEN DATE": ("date", "format"),
    }
    # 减仓明细
    df_detail_short = pd.read_csv(
        DATA_PATH.joinpath("cn_stockdetail_short.csv"),
        usecols=[i for i in range(1, 16)],
    )
    df_detail_short["INDEX"] = df_detail_short.index
    df_detail_short = df_detail_short[
        [
            "INDEX",
            "SYMBOL",
            "IND",
            "NAME",
            "EPR",
            "OPEN DATE",
            "CLOSE DATE",
            "BASE",
            "ADJBASE",
            "PNL",
            "PNL RATIO",
            "AVG DAYS",
            "WIN RATE",
            "TOTAL PNL RATIO",
            "STRATEGY",
        ]
    ].copy()
    cols_format_detail_short = {
        "BASE": ("float",),
        "ADJBASE": ("float",),
        "PNL": ("float", "format"),
        "PNL RATIO": ("ratio", "format"),
        "AVG DAYS": ("float",),
        "WIN RATE": ("ratio", "format"),
        "TOTAL PNL RATIO": ("ratio", "format"),
    }
    # ETF持仓明细
    df_etf = pd.read_csv(
        DATA_PATH.joinpath("cn_etf.csv"), usecols=[i for i in range(1, 13)]
    )
    df_etf["INDEX"] = df_etf.index
    df_etf = df_etf[
        [
            "INDEX",
            "SYMBOL",
            "NAME",
            "OPEN DATE",
            "BASE",
            "ADJBASE",
            "PNL",
            "PNL RATIO",
            "AVG TRANS",
            "AVG DAYS",
            "WIN RATE",
            "TOTAL PNL RATIO",
            "STRATEGY",
        ]
    ]
    cols_format_etf = {
        "BASE": ("float",),
        "ADJBASE": ("float",),
        "PNL": ("float", "format"),
        "AVG TRANS": ("float",),
        "AVG DAYS": ("float",),
        "PNL RATIO": ("ratio", "format"),
        "WIN RATE": ("ratio", "format"),
        "TOTAL PNL RATIO": ("ratio", "format"),
        "OPEN DATE": ("date", "format"),
    }
    return html.Div(
        [
            Header(app),
            # page 2
            html.Div(
                [
                    html.Div(
                        [
                            html.H6(
                                ["Market Trends and Index Summary"],
                                className="subtitle padded",
                            ),
                            html.Div(
                                [
                                    html.P(
                                        [
                                            html.Span(
                                                "海风卓越指数为",
                                                style={"color": "black"},
                                            ),
                                            html.Span(
                                                f"{int(round(df_overall.at[0, 'final_value'] / 1000 - df_overall.at[0, 'cash'] / 1000, 0))}, "
                                            ),
                                            html.Span(
                                                "最新回测所剩Cash为",
                                                style={"color": "black"},
                                            ),
                                            html.Span(f"{df_overall.at[0, 'cash']}, "),
                                            html.Span(
                                                "账户总资产为",
                                                style={"color": "black"},
                                            ),
                                            html.Span(
                                                f"{df_overall.at[0, 'final_value']}, "
                                            ),
                                            html.Span(
                                                "参与本次回测的股票数量为",
                                                style={"color": "black"},
                                            ),
                                            html.Span(
                                                f"{df_overall.at[0, 'stock_cnt']}, "
                                            ),
                                            html.Span(
                                                "数据更新至",
                                                style={"color": "black"},
                                            ),
                                            html.Span(
                                                f"{df_overall.at[0, 'end_date']}"
                                            ),
                                        ]
                                    ),
                                ],
                                className="product",
                            ),
                        ],
                        className="row",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    # html.Br([]),
                                    html.H6(
                                        ["Annual Return"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_trdraw}",
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                ],
                                className="twelve columns",
                            )
                        ],
                        className="row ",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    # html.Br([]),
                                    html.H6(
                                        ["Industries Tracking"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_ind_trend}",
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                ],
                                className="twelve columns",
                            )
                        ],
                        className="row ",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    # html.Br([]),
                                    html.H6(
                                        ["Strategy Tracking"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_strategy}",
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
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
                                    # html.Br([]),
                                    html.H6(
                                        ["Position Weight"], className="subtitle padded"
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_postion}",
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    # html.Br([]),
                                    html.H6(
                                        ["Earnings Weight"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_pl}",
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                    ),
                    # Row 2
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        ["Position Trend"], className="subtitle padded"
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_positiondate}",
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.H6(
                                        ["Earnings Trend"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_bypl_date}",
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                    ),
                    # Row 3
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        "Industries List",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                children=make_dash_format_table(
                                                    df, cols_format_category
                                                ),
                                            )
                                        ],
                                        style={
                                            "overflow-x": "auto",
                                            "max-height": 400,
                                            "overflow-y": "auto",
                                        },
                                        className="table",
                                    ),
                                ],
                                className="twelve columns",
                            ),
                        ],
                        className="row",
                    ),
                    # Row 4
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        "Position Holding",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                children=make_dash_format_table(
                                                    df_detail, cols_format_detail
                                                ),
                                            )
                                        ],
                                        style={
                                            "overflow-x": "auto",
                                            "max-height": 400,
                                            "overflow-y": "auto",
                                        },
                                        className="table",
                                    ),
                                ],
                                className="twelve columns",
                            ),
                        ],
                        className="row",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        "ETF Position Holding",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                children=make_dash_format_table(
                                                    df_etf, cols_format_etf
                                                ),
                                            )
                                        ],
                                        style={
                                            "overflow-x": "auto",
                                            "max-height": 300,
                                            "overflow-y": "auto",
                                        },
                                        className="table",
                                    ),
                                ],
                                className="twelve columns",
                            ),
                        ],
                        className="row",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        "Position Reduction",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                children=make_dash_format_table(
                                                    df_detail_short,
                                                    cols_format_detail_short,
                                                ),
                                            )
                                        ],
                                        style={
                                            "overflow-x": "auto",
                                            "max-height": 300,
                                            "overflow-y": "auto",
                                        },
                                        className="table",
                                    ),
                                ],
                                className="twelve columns",
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
