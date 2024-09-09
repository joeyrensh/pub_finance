import dash_html_components as html
from utils import Header, make_dash_format_table
import pandas as pd
import pathlib
import base64
from dash import Dash, dcc
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio


# 定义全局变量 df_detail
df_detail = None


def create_layout(app):
    # get relative data folder
    PATH = pathlib.Path(__file__).parent

    IMAGE_PATH = PATH.joinpath("../../images").resolve()
    # TOP10行业
    # font_size = 12
    # pio.config.displayModeBar = False

    # df_top10ind = pd.read_csv(
    #     IMAGE_PATH.joinpath("us_top10ind.csv"),
    #     usecols=[i for i in range(1, 3)],
    # )
    # fig_top10ind = go.Figure(
    #     data=[
    #         go.Pie(
    #             labels=df_top10ind["industry"],
    #             values=df_top10ind["cnt"],
    #             pull=[0.1, 0.1, 0.1, 0.1, 0.1],
    #         )
    #     ]
    # )
    # fig_top10ind.update_traces(
    #     title="Top10 Position",
    #     textinfo="value+percent",
    #     textposition="inside",
    # )
    # fig_top10ind.update_layout(
    #     legend=dict(
    #         orientation="h",
    #         xanchor="auto",
    #         bgcolor="rgba(0,0,0,0)",
    #     ),
    #     margin=dict(t=0, b=0, l=20, r=20),  # 增加右侧边距
    #     autosize=True,
    #     plot_bgcolor="rgba(0,0,0,0)",
    #     paper_bgcolor="rgba(0,0,0,0)",
    # )
    # # TOP10盈利
    # df_top10pl = pd.read_csv(
    #     IMAGE_PATH.joinpath("us_top10pl.csv"),
    #     usecols=[i for i in range(1, 3)],
    # )
    # fig_top10pl = go.Figure(
    #     data=[
    #         go.Pie(
    #             labels=df_top10pl["industry"],
    #             values=df_top10pl["pl"],
    #             pull=[0.1, 0.1, 0.1, 0.1, 0.1],
    #         )
    #     ]
    # )
    # # fig1 = px.pie(df_top10pl, values="pl", names="industry", title="Top10 Profit")
    # fig_top10pl.update_traces(
    #     title="Top10 Profit",
    #     textinfo="value+percent",
    #     textposition="inside",
    # )
    # fig_top10pl.update_layout(
    #     legend=dict(
    #         orientation="h",
    #         xanchor="auto",
    #         bgcolor="rgba(0,0,0,0)",
    #     ),
    #     margin=dict(t=0, b=0, l=20, r=20),  # 增加右侧边距
    #     autosize=True,
    #     plot_bgcolor="rgba(0,0,0,0)",
    #     paper_bgcolor="rgba(0,0,0,0)",
    # )
    # # 交易明细
    # df_trade_detail = pd.read_csv(
    #     IMAGE_PATH.joinpath("us_trade_detail.csv"),
    #     usecols=[i for i in range(1, 5)],
    # )
    # fig_trade_detail = go.Figure()
    # fig_trade_detail.add_trace(
    #     go.Scatter(
    #         x=df_trade_detail["buy_date"],
    #         y=df_trade_detail["total_cnt"],
    #         mode="lines+markers",
    #         name="total stock",
    #         line=dict(color="red", width=3),
    #         yaxis="y",
    #     )
    # )
    # fig_trade_detail.add_trace(
    #     go.Bar(
    #         x=df_trade_detail["buy_date"],
    #         y=df_trade_detail["buy_cnt"],
    #         name="long",
    #         marker_color="red",
    #         yaxis="y2",
    #     )
    # )
    # fig_trade_detail.add_trace(
    #     go.Bar(
    #         x=df_trade_detail["buy_date"],
    #         y=df_trade_detail["sell_cnt"],
    #         name="short",
    #         marker_color="green",
    #         yaxis="y2",
    #     )
    # )
    # fig_trade_detail.update_layout(
    #     title={
    #         "text": "Last 60 days trade info",
    #         "xanchor": "left",
    #         "yanchor": "top",
    #         "font": dict(size=font_size, color="black"),
    #     },
    #     xaxis=dict(
    #         mirror=False,
    #         ticks="inside",
    #         showline=True,
    #         gridcolor="rgba(0, 0, 0, 0.5)",
    #         tickfont=dict(color="black", size=font_size),
    #     ),
    #     yaxis=dict(
    #         # title="Total Positions",
    #         # titlefont=dict(size=font_size, color="black"),
    #         side="left",
    #         mirror=False,
    #         ticks="inside",
    #         showline=True,
    #         gridcolor="rgba(0, 0, 0, 0.5)",
    #         tickfont=dict(color="black", size=font_size),
    #     ),
    #     yaxis2=dict(
    #         # title="Positions per day",
    #         # titlefont=dict(size=font_size, color="black"),
    #         side="right",
    #         overlaying="y",
    #         showgrid=False,
    #         ticks="inside",
    #         tickfont=dict(color="black", size=font_size),
    #     ),
    #     legend=dict(
    #         orientation="h",
    #         yanchor="bottom",
    #         y=-0.3,
    #         xanchor="center",
    #         x=0.5,
    #         font=dict(color="black", size=font_size),
    #     ),
    #     barmode="stack",
    #     autosize=True,
    #     plot_bgcolor="rgba(0,0,0,0)",
    #     paper_bgcolor="rgba(0,0,0,0)",
    #     margin=dict(l=0, r=0, t=20, b=0),  # 设置较小的边距
    # )
    # # 盈利趋势
    # df_pnltrend = pd.read_csv(
    #     IMAGE_PATH.joinpath("us_top5_pnltrend.csv"),
    #     usecols=[i for i in range(1, 4)],
    # )
    # fig_pnltrend = px.area(
    #     df_pnltrend,
    #     x="buy_date",
    #     y="pnl",
    #     color="industry",
    #     line_group="industry",
    #     text=None,
    # )
    # fig_pnltrend.update_layout(
    #     title={
    #         "text": "Last 60 days top5 pnl",
    #         "xanchor": "left",
    #         "yanchor": "top",
    #         "font": dict(size=font_size, color="black"),
    #     },
    #     xaxis=dict(
    #         mirror=False,
    #         ticks="inside",
    #         showline=True,
    #         gridcolor="rgba(0, 0, 0, 0.5)",
    #         tickfont=dict(color="black", size=font_size),
    #         # title="",
    #     ),
    #     yaxis=dict(
    #         mirror=False,
    #         ticks="outside",
    #         side="left",
    #         showline=True,
    #         gridcolor="rgba(0, 0, 0, 0.5)",
    #         tickfont=dict(color="black", size=font_size),
    #         # title="",
    #     ),
    #     legend_title_text="",
    #     legend=dict(
    #         orientation="h",
    #         yanchor="bottom",
    #         y=-0.3,
    #         xanchor="center",
    #         x=0.5,
    #         font=dict(color="black", size=font_size),
    #     ),
    #     xaxis_title="",  # 隐藏x轴标签
    #     yaxis_title="",  # 隐藏y轴标签
    #     autosize=True,
    #     plot_bgcolor="rgba(0, 0, 0, 0)",
    #     paper_bgcolor="rgba(0,0,0,0)",
    #     margin=dict(l=0, r=0, t=20, b=0),  # 设置较小的边距
    # )

    # 收益率曲线
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

    # Overall 信息
    df_overall = pd.read_csv(
        IMAGE_PATH.joinpath("us_df_result.csv"),
        usecols=[i for i in range(1, 4)],
    )
    # 板块数据
    df = pd.read_csv(
        IMAGE_PATH.joinpath("us_category.csv"), usecols=[i for i in range(1, 16)]
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
        IMAGE_PATH.joinpath("us_stockdetail.csv"), usecols=[i for i in range(1, 15)]
    )
    df_detail["INDEX"] = df_detail.index
    df_detail = df_detail[
        [
            "INDEX",
            "SYMBOL",
            "IND",
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
        IMAGE_PATH.joinpath("us_stockdetail_short.csv"),
        usecols=[i for i in range(1, 16)],
    )
    df_detail_short["INDEX"] = df_detail_short.index
    df_detail_short = df_detail_short[
        [
            "INDEX",
            "SYMBOL",
            "IND",
            "NAME",
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
                                    html.H5("美股主板全市场分析"),
                                    html.Br([]),
                                    html.P(
                                        [
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
                                                f"{df_overall.at[0, 'stock_cnt']}"
                                            ),
                                        ]
                                    ),
                                ],
                                className="product",
                            )
                        ],
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    # html.Br([]),
                                    html.H6(
                                        ["美股年化收益率分析"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_trdraw}",
                                        style={
                                            "overflow-x": "auto",
                                            "width": "100%",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                ],
                                className="twelve columns",
                            )
                        ],
                    ),
                    # Row
                    html.Div(
                        [
                            html.Div(
                                [
                                    # html.Br([]),
                                    html.H6(["持仓占比"], className="subtitle padded"),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_postion}",
                                        style={
                                            "overflow-x": "auto",
                                            "width": "100%",
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
                                        ["盈利占比"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_pl}",
                                        style={
                                            "overflow-x": "auto",
                                            "width": "100%",
                                            "overflow-y": "auto",
                                        },
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
                                    html.H6(["持仓趋势"], className="subtitle padded"),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_by_positiondate}",
                                        style={
                                            "overflow-x": "auto",
                                            "width": "100%",
                                            "overflow-y": "auto",
                                        },
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
                                        style={
                                            "overflow-x": "auto",
                                            "width": "100%",
                                            "overflow-y": "auto",
                                        },
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
                                        "美股板块分析",
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
                                        "美股持仓分析",
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
                                        "美股近5日减仓分析",
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
