import dash_html_components as html
from utils import Header, make_dash_format_table
import pandas as pd
import pathlib


def create_layout(app):
    # get relative data folder
    PATH = pathlib.Path(__file__).parent

    # 收益率曲线
    DATA_PATH = PATH.joinpath("../../data").resolve()
    prefix = "cn"

    """ annual return """
    # dark mode
    encoded_image_trdraw_dark = f"/assets/images/{prefix}_tr_dark.svg"

    # light mode
    encoded_image_trdraw = f"/assets/images/{prefix}_tr_light.svg"

    """ position weight """
    # dark mode
    encoded_image_by_postion_dark = (
        f"/assets/images/{prefix}_postion_byindustry_dark.svg"
    )

    # light mode
    encoded_image_by_postion = f"/assets/images/{prefix}_postion_byindustry_light.svg"

    """ earnings weight """
    # dark mode
    encoded_image_by_pl_dark = f"/assets/images/{prefix}_pl_byindustry_dark.svg"

    # light mode
    encoded_image_by_pl = f"/assets/images/{prefix}_pl_byindustry_light.svg"

    """ position trend """
    # dark mode
    encoded_image_by_positiondate_dark = f"/assets/images/{prefix}_trade_trend_dark.svg"
    # light mode
    encoded_image_by_positiondate = f"/assets/images/{prefix}_trade_trend_light.svg"

    """ earnings trend """
    # dark mode
    encoded_image_bypl_date_dark = (
        f"/assets/images/{prefix}_top_industry_pl_trend_dark.svg"
    )
    # light mode
    encoded_image_bypl_date = f"/assets/images/{prefix}_top_industry_pl_trend_light.svg"

    """ strategy tracking """
    # dark mode
    encoded_image_strategy_dark = f"/assets/images/{prefix}_strategy_tracking_dark.svg"
    # light mode
    encoded_image_strategy = f"/assets/images/{prefix}_strategy_tracking_light.svg"

    """ industry trend """
    # dark mode
    encoded_image_ind_trend_dark = (
        f"/assets/images/{prefix}_industry_trend_heatmap_dark.svg"
    )
    # light mode
    encoded_image_ind_trend = (
        f"/assets/images/{prefix}_industry_trend_heatmap_light.svg"
    )

    # Overall 信息
    df_overall = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_df_result.csv"),
        usecols=[i for i in range(1, 5)],
    )
    # 检查是否为NaN、None、0或空字符串
    stock_cnt_value = df_overall.at[0, "stock_cnt"]
    if (
        pd.isna(stock_cnt_value)
        or stock_cnt_value is None
        or stock_cnt_value == 0
        or stock_cnt_value == ""
    ):
        df_overall.at[0, "stock_cnt"] = 1
    # 板块数据
    df = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_category.csv"), usecols=[i for i in range(1, 16)]
    )
    df["IDX"] = df.index
    df = df[
        [
            "IDX",
            "IND",
            "OPEN",
            "LRATIO",
            "L5 OPEN",
            "L5 CLOSE",
            "ERP",
            "PROFIT",
            "PNL RATIO",
            "AVG TRANS",
            "AVG DAYS",
            "WIN RATE",
            "PROFIT TREND",
        ]
    ].copy()
    cols_format_category = {
        "IDX": ("float",),
        "OPEN": ("float",),
        "L5 OPEN": ("float",),
        "L5 CLOSE": ("float",),
        "LRATIO": ("ratio", "format"),
        "PROFIT": ("float", "format"),
        "PNL RATIO": ("ratio", "format"),
        "AVG TRANS": ("float",),
        "AVG DAYS": ("float",),
        "WIN RATE": ("ratio", "format"),
        "ERP": ("float",),
    }

    # 持仓明细
    df_detail = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_stockdetail.csv"),
        usecols=[i for i in range(1, 17)],
    )
    df_detail["IDX"] = df_detail.index
    df_detail = df_detail[
        [
            "IDX",
            "SYMBOL",
            "IND",
            "NAME",
            "TOTAL VALUE",
            "ERP",
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
        "IDX": ("float",),
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
        "ERP": ("float",),
    }
    # 减仓明细
    df_detail_short = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_stockdetail_short.csv"),
        usecols=[i for i in range(1, 15)],
    )
    df_detail_short["IDX"] = df_detail_short.index
    df_detail_short = df_detail_short[
        [
            "IDX",
            "SYMBOL",
            "IND",
            "NAME",
            "TOTAL VALUE",
            "ERP",
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
        "IDX": ("float",),
        "TOTAL VALUE": ("float",),
        "ERP": ("float",),
        "BASE": ("float",),
        "ADJBASE": ("float",),
        "PNL": ("float", "format"),
        "PNL RATIO": ("ratio", "format"),
        "HIS DAYS": ("float",),
    }
    # ETF持仓明细
    df_etf = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_etf.csv"), usecols=[i for i in range(1, 14)]
    )
    df_etf["IDX"] = df_etf.index
    df_etf = df_etf[
        [
            "IDX",
            "SYMBOL",
            "NAME",
            "TOTAL VALUE",
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
        "IDX": ("float",),
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
                                                f"{int(round(((df_overall.at[0, 'final_value'] - df_overall.at[0, 'cash']) - (df_overall.at[0, 'stock_cnt'] * 10000 - df_overall.at[0, 'cash'])) / df_overall.at[0, 'stock_cnt'], 0))}, ",
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
                        className="row",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    # 修改后的可点击标题
                                    html.Button(
                                        html.H6(
                                            ["Annual Return ▼"],  # 添加箭头指示符
                                            className="subtitle padded",
                                            id=f"{prefix}-annual-return-title",
                                        ),
                                        id={
                                            "type": "collapse-btn",
                                            "page": f"{prefix}",
                                            "index": 0,
                                        },
                                        style={
                                            "background": "none",
                                            "border": "none",
                                            "padding": "0",
                                            "cursor": "pointer",
                                            "width": "100%",
                                            "text-align": "left",
                                        },
                                    ),
                                    # 可折叠内容容器
                                    html.Div(
                                        className="chart-container",
                                        children=[
                                            # 浅色主题 SVG
                                            html.ObjectEl(
                                                data=encoded_image_trdraw,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-light",
                                                id=f"{prefix}-annual-return-light",
                                            ),
                                            # 深色主题 SVG
                                            html.ObjectEl(
                                                data=encoded_image_trdraw_dark,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-dark",
                                                style={"display": "none"},
                                                id=f"{prefix}-annual-return-dark",
                                            ),
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 0,
                                        },
                                        style={"display": "block"},  # 初始展开状态
                                    ),
                                ],
                                className="twelve columns",
                            )
                        ],
                        className="row",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Button(
                                        html.H6(
                                            ["Industries Tracking ▼"],
                                            className="subtitle padded",
                                            id=f"{prefix}-industries-tracking-title",
                                        ),
                                        id={
                                            "type": "collapse-btn",
                                            "page": f"{prefix}",
                                            "index": 1,
                                        },
                                        style={
                                            "background": "none",
                                            "border": "none",
                                            "padding": "0",
                                            "cursor": "pointer",
                                            "width": "100%",
                                            "text-align": "left",
                                        },
                                    ),
                                    html.Div(
                                        className="chart-container",
                                        children=[
                                            # 浅色主题 SVG（默认显示）
                                            html.ObjectEl(
                                                data=encoded_image_ind_trend,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-light",  # 添加专属类名
                                                id=f"{prefix}-ind-trend-light",
                                            ),
                                            # 深色主题 SVG（默认隐藏）
                                            html.ObjectEl(
                                                data=encoded_image_ind_trend_dark,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-dark",
                                                style={"display": "none"},  # 初始隐藏
                                                id=f"{prefix}-ind-trend-dark",
                                            ),
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 1,
                                        },
                                        style={"display": "block"},
                                    ),
                                ],
                                # className="twelve columns",
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        html.H6(
                                            ["Strategy Tracking ▼"],
                                            className="subtitle padded",
                                            id=f"{prefix}-strategy-tracking-title",
                                        ),
                                        id={
                                            "type": "collapse-btn",
                                            "page": f"{prefix}",
                                            "index": 2,
                                        },
                                        style={
                                            "background": "none",
                                            "border": "none",
                                            "padding": "0",
                                            "cursor": "pointer",
                                            "width": "100%",
                                            "text-align": "left",
                                        },
                                    ),
                                    html.Div(
                                        className="chart-container",
                                        children=[
                                            # 浅色主题 SVG（默认显示）
                                            html.ObjectEl(
                                                data=encoded_image_strategy,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-light",  # 添加专属类名
                                                id=f"{prefix}-strategy-light",
                                            ),
                                            # 深色主题 SVG（默认隐藏）
                                            html.ObjectEl(
                                                data=encoded_image_strategy_dark,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-dark",
                                                style={"display": "none"},  # 初始隐藏
                                                id=f"{prefix}-strategy-dark",
                                            ),
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 2,
                                        },
                                        style={"display": "block"},
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
                                    html.Button(
                                        html.H6(
                                            ["Position Trend ▼"],
                                            className="subtitle padded",
                                            id=f"{prefix}-position-trend-title",
                                        ),
                                        id={
                                            "type": "collapse-btn",
                                            "page": f"{prefix}",
                                            "index": 5,
                                        },
                                        style={
                                            "background": "none",
                                            "border": "none",
                                            "padding": "0",
                                            "cursor": "pointer",
                                            "width": "100%",
                                            "text-align": "left",
                                        },
                                    ),
                                    html.Div(
                                        className="chart-container",
                                        children=[
                                            # 浅色主题 SVG（默认显示）
                                            html.ObjectEl(
                                                data=encoded_image_by_positiondate,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-light",  # 添加专属类名
                                                id=f"{prefix}-by-positiondate-light",
                                            ),
                                            # 深色主题 SVG（默认隐藏）
                                            html.ObjectEl(
                                                data=encoded_image_by_positiondate_dark,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-dark",
                                                style={"display": "none"},  # 初始隐藏
                                                id=f"{prefix}-by-positiondate-dark",
                                            ),
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 5,
                                        },
                                        style={"display": "block"},
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        html.H6(
                                            ["Earnings Trend ▼"],
                                            className="subtitle padded",
                                            id=f"{prefix}-earnings-trend-title",
                                        ),
                                        id={
                                            "type": "collapse-btn",
                                            "page": f"{prefix}",
                                            "index": 6,
                                        },
                                        style={
                                            "background": "none",
                                            "border": "none",
                                            "padding": "0",
                                            "cursor": "pointer",
                                            "width": "100%",
                                            "text-align": "left",
                                        },
                                    ),
                                    html.Div(
                                        className="chart-container",
                                        children=[
                                            # 浅色主题 SVG（默认显示）
                                            html.ObjectEl(
                                                data=encoded_image_bypl_date,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-light",  # 添加专属类名
                                                id=f"{prefix}-bypl-date-light",
                                            ),
                                            # 深色主题 SVG（默认隐藏）
                                            html.ObjectEl(
                                                data=encoded_image_bypl_date_dark,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-dark",
                                                style={"display": "none"},  # 初始隐藏
                                                id=f"{prefix}-bypl-date-dark",
                                            ),
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 6,
                                        },
                                        style={"display": "block"},
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Button(
                                        html.H6(
                                            ["Position Weight ▼"],
                                            className="subtitle padded",
                                            id=f"{prefix}-position-weight-title",
                                        ),
                                        id={
                                            "type": "collapse-btn",
                                            "page": f"{prefix}",
                                            "index": 3,
                                        },
                                        style={
                                            "background": "none",
                                            "border": "none",
                                            "padding": "0",
                                            "cursor": "pointer",
                                            "width": "100%",
                                            "text-align": "left",
                                        },
                                    ),
                                    html.Div(
                                        className="chart-container",
                                        children=[
                                            # 浅色主题 SVG（默认显示）
                                            html.ObjectEl(
                                                data=encoded_image_by_postion,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-light",  # 添加专属类名
                                                id=f"{prefix}-by-position-light",
                                            ),
                                            # 深色主题 SVG（默认隐藏）
                                            html.ObjectEl(
                                                data=encoded_image_by_postion_dark,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-dark",
                                                style={"display": "none"},  # 初始隐藏
                                                id=f"{prefix}-by-position-dark",
                                            ),
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 3,
                                        },
                                        style={"display": "block"},
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        html.H6(
                                            ["Earnings Weight ▼"],
                                            className="subtitle padded",
                                            id=f"{prefix}-earnings-weight-title",
                                        ),
                                        id={
                                            "type": "collapse-btn",
                                            "page": f"{prefix}",
                                            "index": 4,
                                        },
                                        style={
                                            "background": "none",
                                            "border": "none",
                                            "padding": "0",
                                            "cursor": "pointer",
                                            "width": "100%",
                                            "text-align": "left",
                                        },
                                    ),
                                    html.Div(
                                        className="chart-container",
                                        children=[
                                            # 浅色主题 SVG（默认显示）
                                            html.ObjectEl(
                                                data=encoded_image_by_pl,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-light",  # 添加专属类名
                                                id=f"{prefix}-by-pl-light",
                                            ),
                                            # 深色主题 SVG（默认隐藏）
                                            html.ObjectEl(
                                                data=encoded_image_by_pl_dark,
                                                type="image/svg+xml",
                                                className="responsive-svg svg-dark",
                                                style={"display": "none"},  # 初始隐藏
                                                id=f"{prefix}-by-pl-dark",
                                            ),
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 4,
                                        },
                                        style={"display": "block"},
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
                                                    df,
                                                    cols_format_category,
                                                    f"{prefix}",
                                                ),
                                                className="cn_table",
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
                                                    df_detail,
                                                    cols_format_detail,
                                                    f"{prefix}",
                                                ),
                                                className="cn_table",
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
                                                    df_etf, cols_format_etf, f"{prefix}"
                                                ),
                                                className="cn_table",
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
                                                    f"{prefix}",
                                                ),
                                                className="cn_table",
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
