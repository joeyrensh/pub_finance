import dash_html_components as html
from utils import Header, make_dash_format_table
import pandas as pd
import pathlib
import base64

# 定义全局变量 df_detail
df_detail = None


def create_layout(app):
    # get relative data folder
    PATH = pathlib.Path(__file__).parent
    IMAGE_PATH = PATH.joinpath("../../images").resolve()
    DATA_PATH = PATH.joinpath("../../data").resolve()
    """ annual return """
    # dark mode
    # encoded_image_trdraw_dark = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_tr_dark.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_trdraw_dark = "/assets/images/us_tr_dark.svg"
    # light mode
    # encoded_image_trdraw = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_tr_light.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_trdraw = "/assets/images/us_tr_light.svg"

    """ position weight """
    # dark mode
    # encoded_image_by_postion_dark = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_postion_byindustry_dark.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_by_postion_dark = "/assets/images/us_postion_byindustry_dark.svg"
    # light mode
    # encoded_image_by_postion = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_postion_byindustry_light.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_by_postion = "/assets/images/us_postion_byindustry_light.svg"

    """ earnings weight """
    # dark mode
    # encoded_image_by_pl_dark = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_pl_byindustry_dark.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_by_pl_dark = "/assets/images/us_pl_byindustry_dark.svg"
    # light mode
    # encoded_image_by_pl = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_pl_byindustry_light.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_by_pl = "/assets/images/us_pl_byindustry_light.svg"

    """ position trend """
    # dark mode
    # encoded_image_by_positiondate_dark = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_trade_trend_dark.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_by_positiondate_dark = "/assets/images/us_trade_trend_dark.svg"
    # light mode
    # encoded_image_by_positiondate = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_trade_trend_light.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_by_positiondate = "/assets/images/us_trade_trend_light.svg"

    """ earnings trend """
    # dark mode
    # encoded_image_bypl_date_dark = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_top_industry_pl_trend_dark.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_bypl_date_dark = "/assets/images/us_top_industry_pl_trend_dark.svg"
    # light mode
    # encoded_image_bypl_date = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_top_industry_pl_trend_light.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_bypl_date = "/assets/images/us_top_industry_pl_trend_light.svg"

    """ strategy tracking """
    # dark mode
    # encoded_image_strategy_dark = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_strategy_tracking_dark.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_strategy_dark = "/assets/images/us_strategy_tracking_dark.svg"
    # light mode
    # encoded_image_strategy = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_strategy_tracking_light.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_strategy = "/assets/images/us_strategy_tracking_light.svg"

    """ industry trend """
    # dark mode
    # encoded_image_ind_trend_dark = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_industry_trend_heatmap_dark.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_ind_trend_dark = "/assets/images/us_industry_trend_heatmap_dark.svg"
    # light mode
    # encoded_image_ind_trend = base64.b64encode(
    #     IMAGE_PATH.joinpath("us_industry_trend_heatmap_light.png").read_bytes()
    # ).decode("utf-8")
    encoded_image_ind_trend = "/assets/images/us_industry_trend_heatmap_light.svg"

    # Overall 信息
    df_overall = pd.read_csv(
        DATA_PATH.joinpath("us_df_result.csv"),
        usecols=[i for i in range(1, 5)],
    )
    # 板块数据
    df = pd.read_csv(
        DATA_PATH.joinpath("us_category.csv"), usecols=[i for i in range(1, 16)]
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
        DATA_PATH.joinpath("us_stockdetail.csv"), usecols=[i for i in range(1, 17)]
    )
    df_detail["INDEX"] = df_detail.index
    df_detail = df_detail[
        [
            "INDEX",
            "SYMBOL",
            "IND",
            "NAME",
            "TOTAL VALUE",
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
        "TOTAL VALUE": ("float",),
    }
    # 减仓明细
    df_detail_short = pd.read_csv(
        DATA_PATH.joinpath("us_stockdetail_short.csv"),
        usecols=[i for i in range(1, 14)],
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
            "HIS DAYS",
            "STRATEGY",
        ]
    ].copy()
    cols_format_detail_short = {
        "BASE": ("float",),
        "ADJBASE": ("float",),
        "PNL": ("float", "format"),
        "PNL RATIO": ("ratio", "format"),
        "HIS DAYS": ("float",),
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
                                                "SWDI指数为",
                                                className="text_color",
                                            ),
                                            html.Span(
                                                f"{int(round(df_overall.at[0, 'final_value'] / 1000 - df_overall.at[0, 'cash'] / 1000, 0))}, ",
                                                className="number_color",
                                            ),
                                            html.Span(
                                                "最新回测所剩Cash为",
                                                className="text_color",
                                            ),
                                            html.Span(
                                                f"{df_overall.at[0, 'cash']}, ",
                                                className="number_color",
                                            ),
                                            html.Span(
                                                "账户总资产为",
                                                className="text_color",
                                            ),
                                            html.Span(
                                                f"{df_overall.at[0, 'final_value']}, ",
                                                className="number_color",
                                            ),
                                            html.Span(
                                                "参与本次回测的股票数量为",
                                                className="text_color",
                                            ),
                                            html.Span(
                                                f"{df_overall.at[0, 'stock_cnt']}, ",
                                                className="number_color",
                                            ),
                                            html.Span(
                                                "数据更新至",
                                                className="text_color",
                                            ),
                                            html.Span(
                                                f"{df_overall.at[0, 'end_date']}",
                                                className="number_color",
                                            ),
                                        ]
                                    ),
                                ],
                                className="product",
                            ),
                        ],
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
                                    html.Picture(
                                        [
                                            # 深色模式下的图片
                                            html.Source(
                                                # srcSet=f"data:image/png;base64,{encoded_image_trdraw_dark}",
                                                srcSet=encoded_image_trdraw_dark,
                                                media="(prefers-color-scheme: dark)",
                                                className="firstimg",
                                            ),
                                            html.Img(
                                                # src=f"data:image/png;base64,{encoded_image_trdraw}",
                                                src=encoded_image_trdraw,
                                                style={
                                                    "overflow-x": "auto",
                                                    "overflow-y": "auto",
                                                },
                                                className="firstimg",
                                            ),
                                        ],
                                    ),
                                ],
                                className="twelve columns",
                            )
                        ],
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
                                    html.Picture(
                                        [
                                            # 深色模式下的图片
                                            html.Source(
                                                # srcSet=f"data:image/png;base64,{encoded_image_ind_trend_dark}",
                                                srcSet=encoded_image_ind_trend_dark,
                                                media="(prefers-color-scheme: dark)",
                                            ),
                                            html.Img(
                                                # src=f"data:image/png;base64,{encoded_image_ind_trend}",
                                                src=encoded_image_ind_trend,
                                                style={
                                                    "overflow-x": "auto",
                                                    "width": "100%",
                                                    "overflow-y": "auto",
                                                },
                                            ),
                                        ]
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    # html.Br([]),
                                    html.H6(
                                        ["Strategy Tracking"],
                                        className="subtitle padded",
                                    ),
                                    html.Picture(
                                        [
                                            # 深色模式下的图片
                                            html.Source(
                                                # srcSet=f"data:image/png;base64,{encoded_image_strategy_dark}",
                                                srcSet=encoded_image_strategy_dark,
                                                media="(prefers-color-scheme: dark)",
                                            ),
                                            html.Img(
                                                # src=f"data:image/png;base64,{encoded_image_strategy}",
                                                src=encoded_image_strategy,
                                                style={
                                                    "overflow-x": "auto",
                                                    "width": "100%",
                                                    "overflow-y": "auto",
                                                },
                                            ),
                                        ]
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
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
                                    html.Picture(
                                        [
                                            # 深色模式下的图片
                                            html.Source(
                                                # srcSet=f"data:image/png;base64,{encoded_image_by_postion_dark}",
                                                srcSet=encoded_image_by_postion_dark,
                                                media="(prefers-color-scheme: dark)",
                                            ),
                                            html.Img(
                                                # src=f"data:image/png;base64,{encoded_image_by_postion}",
                                                src=encoded_image_by_postion,
                                                style={
                                                    "overflow-x": "auto",
                                                    "width": "100%",
                                                    "overflow-y": "auto",
                                                },
                                            ),
                                        ]
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
                                    html.Picture(
                                        [
                                            # 深色模式下的图片
                                            html.Source(
                                                # srcSet=f"data:image/png;base64,{encoded_image_by_pl_dark}",
                                                srcSet=encoded_image_by_pl_dark,
                                                media="(prefers-color-scheme: dark)",
                                            ),
                                            html.Img(
                                                # src=f"data:image/png;base64,{encoded_image_by_pl}",
                                                src=encoded_image_by_pl,
                                                style={
                                                    "overflow-x": "auto",
                                                    "width": "100%",
                                                    "overflow-y": "auto",
                                                },
                                            ),
                                        ]
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                        className="row",
                    ),
                    # Row 2
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        ["Position Trend"], className="subtitle padded"
                                    ),
                                    html.Picture(
                                        [
                                            # 深色模式下的图片
                                            html.Source(
                                                # srcSet=f"data:image/png;base64,{encoded_image_by_positiondate_dark}",
                                                srcSet=encoded_image_by_positiondate_dark,
                                                media="(prefers-color-scheme: dark)",
                                            ),
                                            html.Img(
                                                # src=f"data:image/png;base64,{encoded_image_by_positiondate}",
                                                src=encoded_image_by_positiondate,
                                                style={
                                                    "overflow-x": "auto",
                                                    "width": "100%",
                                                    "overflow-y": "auto",
                                                },
                                            ),
                                        ]
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
                                    html.Picture(
                                        [
                                            # 深色模式下的图片
                                            html.Source(
                                                # srcSet=f"data:image/png;base64,{encoded_image_bypl_date_dark}",
                                                srcSet=encoded_image_bypl_date_dark,
                                                media="(prefers-color-scheme: dark)",
                                            ),
                                            html.Img(
                                                # src=f"data:image/png;base64,{encoded_image_bypl_date}",
                                                src=encoded_image_bypl_date,
                                                style={
                                                    "overflow-x": "auto",
                                                    "width": "100%",
                                                    "overflow-y": "auto",
                                                },
                                            ),
                                        ]
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                        className="row",
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
                                        className="table",
                                        style={
                                            "overflow-x": "auto",
                                            "max-height": 400,
                                            "overflow-y": "auto",
                                        },
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
                                        className="table",
                                        style={
                                            "overflow-x": "auto",
                                            "max-height": 400,
                                            "overflow-y": "auto",
                                        },
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
                        style={"margin-bottom": "10px"},
                    ),
                ],
                className="sub_page",
            ),
        ],
        className="page",
    )
