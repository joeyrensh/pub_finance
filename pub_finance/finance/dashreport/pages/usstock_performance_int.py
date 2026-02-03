import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import ast
from finance.dashreport.utils import Header, make_dash_format_table
import pandas as pd
import pathlib
import os
from finance.dashreport.chart_builder import ChartBuilder
from dash import callback, Output, Input, State
import pickle


def create_layout(app):
    PATH = pathlib.Path(__file__).parent
    DATA_PATH = PATH.joinpath("../../data").resolve()
    DATA_PATH_ANUAL_RETURN = PATH.joinpath("../../cache").resolve()
    prefix = "us"
    cb = ChartBuilder()

    # Load tables and other data for page
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

    trade_date = str(df_overall.at[0, "end_date"]).replace("-", "")
    annual_return_file = DATA_PATH_ANUAL_RETURN.joinpath(
        f"pnl_{prefix}_{trade_date}.pkl"
    )
    with open(annual_return_file, "rb") as f:
        pnl, cash, total_value = pickle.load(f)
    app.chart_callback.register_chart(
        chart_type="annual_return",
        page_prefix=prefix,
        chart_builder=cb,
        df_data=pnl,
        index=0,
    )

    df_heatmap = pd.read_csv(DATA_PATH.joinpath(f"{prefix}_pd_calendar_heatmap.csv"))
    app.chart_callback.register_chart(
        chart_type="heatmap",
        page_prefix=prefix,
        chart_builder=cb,
        df_data=df_heatmap,
        index=1,
    )
    df_strategy = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_pd_strategy_tracking_lst180days.csv")
    )
    app.chart_callback.register_chart(
        chart_type="strategy",
        page_prefix=prefix,
        chart_builder=cb,
        df_data=df_strategy,
        index=2,
    )

    df_trade_info = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_pd_trade_info_lst180days.csv")
    )
    app.chart_callback.register_chart(
        chart_type="trade",
        page_prefix=prefix,
        chart_builder=cb,
        df_data=df_trade_info,
        index=5,
    )
    df_pnl_trend = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_pd_top5_industry_profit_trend.csv")
    )
    app.chart_callback.register_chart(
        chart_type="pnl_trend",
        page_prefix=prefix,
        chart_builder=cb,
        df_data=df_pnl_trend,
        index=6,
    )

    df_industry_position = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_pd_top20_industry.csv")
    )
    app.chart_callback.register_chart(
        chart_type="industry_position",
        page_prefix=prefix,
        chart_builder=cb,
        df_data=df_industry_position,
        index=3,
    )

    df_industry_profit = pd.read_csv(
        DATA_PATH.joinpath(f"{prefix}_pd_top20_profit_industry.csv")
    )
    app.chart_callback.register_chart(
        chart_type="industry_profit",
        page_prefix=prefix,
        chart_builder=cb,
        df_data=df_industry_profit,
        index=4,
    )

    cols_category = [
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
    df = (
        pd.read_csv(
            DATA_PATH.joinpath(f"{prefix}_category.csv"),
            usecols=[i for i in range(1, 16)],
        )
        if DATA_PATH.joinpath(f"{prefix}_category.csv").exists()
        else pd.DataFrame(columns=cols_category)
    )
    df["IDX"] = df.index
    df = df[cols_category]

    cols_format_category = {
        "OPEN": ("int",),
        "L5 OPEN": ("int",),
        "L5 CLOSE": ("int",),
        "LRATIO": ("ratio", "format"),
        "PROFIT": ("int", "format"),
        "PNL RATIO": ("ratio", "format"),
        "AVG TRANS": ("float",),
        "AVG DAYS": ("float",),
        "WIN RATE": ("ratio", "format"),
        "ERP": ("float",),
    }

    cols_detail = [
        "IDX",
        "SYMBOL",
        "IND",
        "NAME",
        "TOTAL VALUE",
        "ERP",
        "SHARPE RATIO",
        "SORTINO RATIO",
        "MAX DD",
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
    df_detail = (
        pd.read_csv(
            DATA_PATH.joinpath(f"{prefix}_stockdetail.csv"),
            usecols=[i for i in range(1, 20)],
        )
        if DATA_PATH.joinpath(f"{prefix}_stockdetail.csv").exists()
        else pd.DataFrame(columns=cols_detail)
    )
    df_detail["IDX"] = df_detail.index
    df_detail = df_detail[cols_detail]

    cols_format_detail = {
        "BASE": ("float",),
        "ADJBASE": ("float",),
        "PNL": ("int", "format"),
        "AVG TRANS": ("int",),
        "AVG DAYS": ("float",),
        "PNL RATIO": ("ratio", "format"),
        "WIN RATE": ("ratio", "format"),
        "TOTAL PNL RATIO": ("ratio", "format"),
        "OPEN DATE": ("date", "format"),
        "TOTAL VALUE": ("float",),
        "ERP": ("float",),
        "SHARPE RATIO": ("float",),
        "SORTINO RATIO": ("float",),
        "MAX DD": ("ratio", "format"),
    }
    # 减仓明细
    cols_short = [
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
    df_detail_short = (
        pd.read_csv(
            DATA_PATH.joinpath(f"{prefix}_stockdetail_short.csv"),
            usecols=[i for i in range(1, 15)],
        )
        if DATA_PATH.joinpath(f"{prefix}_stockdetail_short.csv").exists()
        else pd.DataFrame(columns=cols_short)
    )
    df_detail_short["IDX"] = df_detail_short.index
    df_detail_short = df_detail_short[cols_short]
    cols_format_detail_short = {
        "TOTAL VALUE": ("float",),
        "ERP": ("float",),
        "BASE": ("float",),
        "ADJBASE": ("float",),
        "PNL": ("int", "format"),
        "PNL RATIO": ("ratio", "format"),
        "HIS DAYS": ("int",),
    }

    # Build layout with dcc.Graph for interactive charts
    return html.Div(
        [
            Header(app),
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
                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.Div(
                                                        "SWDI 指数",
                                                        className="kpi-label",
                                                    ),
                                                    html.Div(
                                                        f"{int(round(((df_overall.at[0, 'final_value'] - df_overall.at[0, 'cash']) - (df_overall.at[0, 'stock_cnt'] * 10000 - df_overall.at[0, 'cash'])) / df_overall.at[0, 'stock_cnt'], 0))}",
                                                        className="kpi-value",
                                                    ),
                                                ],
                                                className="kpi-card",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div(
                                                        "总资产", className="kpi-label"
                                                    ),
                                                    html.Div(
                                                        f"{df_overall.at[0, 'final_value'] / 10000:,.2f} 万",
                                                        className="kpi-value",
                                                    ),
                                                ],
                                                className="kpi-card",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div(
                                                        "Cash", className="kpi-label"
                                                    ),
                                                    html.Div(
                                                        f"{df_overall.at[0, 'cash'] / 10000:,.2f} 万",
                                                        className="kpi-value",
                                                    ),
                                                ],
                                                className="kpi-card",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div(
                                                        "股票数量",
                                                        className="kpi-label",
                                                    ),
                                                    html.Div(
                                                        f"{df_overall.at[0, 'stock_cnt']}",
                                                        className="kpi-value",
                                                    ),
                                                ],
                                                className="kpi-card",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div(
                                                        "数据日期",
                                                        className="kpi-label",
                                                    ),
                                                    html.Div(
                                                        f"{df_overall.at[0, 'end_date']}",
                                                        className="kpi-value kpi-date",
                                                    ),
                                                ],
                                                className="kpi-card",
                                            ),
                                        ],
                                        className="kpi-container",
                                    )
                                ],
                                className="product",
                            ),
                        ],
                    ),
                    # Annual Return (left as static image)
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Button(
                                        html.H6(
                                            ["Annual Return ‹"],
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
                                    html.Div(
                                        className="chart-container",
                                        children=[
                                            dcc.Graph(
                                                id=app.chart_callback.get_chart_id(
                                                    "annual_return", prefix, 0
                                                ),
                                                figure=cb.annual_return(
                                                    pnl=pnl,
                                                    theme="light",
                                                    client_width=1440,
                                                ),
                                                config={
                                                    "displayModeBar": False,
                                                    "doubleClick": True,
                                                },
                                                style={
                                                    "margin": 0,
                                                    "padding": 0,
                                                    "width": "100%",
                                                    "height": "100%",
                                                },
                                            )
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 0,
                                        },
                                        style={
                                            "display": "block",
                                            "aspectRatio": 1.6,
                                            "width": "100%",
                                        },
                                    ),
                                ],
                                className="twelve columns",
                            )
                        ],
                    ),
                    # Row: Industry trend + Strategy
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Button(
                                        html.H6(
                                            ["Industries Tracking ‹"],
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
                                            dcc.Graph(
                                                id=app.chart_callback.get_chart_id(
                                                    "heatmap", prefix, 1
                                                ),
                                                figure=cb.calendar_heatmap(
                                                    df=df_heatmap,
                                                    theme="light",
                                                    client_width=1440,
                                                ),
                                                config={
                                                    "displayModeBar": False,
                                                    "doubleClick": True,
                                                },
                                                style={
                                                    "margin": 0,
                                                    "padding": 0,
                                                    "width": "100%",
                                                    "height": "100%",
                                                },
                                            )
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 1,
                                        },
                                        style={
                                            "display": "block",
                                            "aspectRatio": 1.6,
                                            "width": "100%",
                                        },
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        html.H6(
                                            ["Strategy Tracking ‹"],
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
                                            dcc.Graph(
                                                id=app.chart_callback.get_chart_id(
                                                    "strategy", prefix, 2
                                                ),
                                                figure=cb.strategy_chart(
                                                    df=df_strategy,
                                                    theme="light",
                                                    client_width=1440,
                                                ),
                                                config={
                                                    "displayModeBar": False,
                                                    "doubleClick": True,
                                                },
                                                style={
                                                    "margin": 0,
                                                    "padding": 0,
                                                    "width": "100%",
                                                    "height": "100%",
                                                },
                                            )
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 2,
                                        },
                                        style={
                                            "display": "block",
                                            "aspectRatio": 1.6,
                                            "width": "100%",
                                        },
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                    ),
                    # Row 2: Position trend + Earnings trend
                    html.Div(
                        [
                            html.Div(
                                className="six columns",
                                children=[
                                    html.Button(
                                        html.H6(
                                            ["Position Trend ‹"],
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
                                            dcc.Graph(
                                                id=app.chart_callback.get_chart_id(
                                                    "trade", prefix, 5
                                                ),
                                                figure=cb.trade_info_chart(
                                                    df=df_trade_info,
                                                    theme="light",
                                                    client_width=1440,
                                                ),
                                                config={
                                                    "displayModeBar": False,
                                                    "doubleClick": True,
                                                },
                                                style={
                                                    "margin": 0,
                                                    "padding": 0,
                                                    "width": "100%",
                                                    "height": "100%",
                                                },
                                            )
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 5,
                                        },
                                        style={
                                            "display": "block",
                                            "aspectRatio": 1.6,
                                            "width": "100%",
                                        },
                                    ),
                                ],
                            ),
                            html.Div(
                                className="six columns",
                                children=[
                                    html.Button(
                                        html.H6(
                                            ["Earnings Trend ‹"],
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
                                            dcc.Graph(
                                                id=app.chart_callback.get_chart_id(
                                                    "pnl_trend", prefix, 6
                                                ),
                                                figure=cb.industry_pnl_trend(
                                                    df=df_pnl_trend,
                                                    theme="light",
                                                    client_width=1440,
                                                ),
                                                config={
                                                    "displayModeBar": False,
                                                    "doubleClick": True,
                                                },
                                                style={
                                                    "margin": 0,
                                                    "padding": 0,
                                                    "width": "100%",
                                                    "height": "100%",
                                                },
                                            )
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 6,
                                        },
                                        style={
                                            "display": "block",
                                            "aspectRatio": 1.6,
                                            "width": "100%",
                                        },
                                    ),
                                ],
                            ),
                        ],
                        className="row",
                    ),
                    # Row: Position weight + Earnings weight
                    html.Div(
                        [
                            html.Div(
                                className="six columns",
                                children=[
                                    html.Button(
                                        html.H6(
                                            ["Position Weight ‹"],
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
                                            dcc.Graph(
                                                id=app.chart_callback.get_chart_id(
                                                    "industry_position", prefix, 3
                                                ),
                                                figure=cb.industry_position_treemap(
                                                    df=df_industry_position,
                                                    theme="light",
                                                    client_width=1440,
                                                ),
                                                config={
                                                    "displayModeBar": False,
                                                    "doubleClick": True,
                                                },
                                                style={
                                                    "margin": 0,
                                                    "padding": 0,
                                                    "width": "100%",
                                                    "height": "100%",
                                                },
                                            )
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 3,
                                        },
                                        style={
                                            "display": "block",
                                            "aspectRatio": 1.6,
                                            "width": "100%",
                                        },
                                    ),
                                ],
                            ),
                            html.Div(
                                className="six columns",
                                children=[
                                    html.Button(
                                        html.H6(
                                            ["Earnings Weight ‹"],
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
                                            dcc.Graph(
                                                id=app.chart_callback.get_chart_id(
                                                    "industry_profit", prefix, 4
                                                ),
                                                figure=cb.industry_profit_treemap(
                                                    df=df_industry_profit,
                                                    theme="light",
                                                    client_width=1440,
                                                ),
                                                config={
                                                    "displayModeBar": False,
                                                    "doubleClick": True,
                                                },
                                                style={
                                                    "margin": 0,
                                                    "padding": 0,
                                                    "width": "100%",
                                                    "height": "100%",
                                                },
                                            )
                                        ],
                                        id={
                                            "type": "collapsible",
                                            "page": f"{prefix}",
                                            "index": 4,
                                        },
                                        style={
                                            "display": "block",
                                            "aspectRatio": 1.6,
                                            "width": "100%",
                                        },
                                    ),
                                ],
                            ),
                        ],
                        className="row",
                    ),
                    # Industries List
                    html.Div(
                        className="row",
                        children=[
                            html.Div(
                                className="twelve columns",
                                children=[
                                    html.H6(
                                        "Industries List", className="subtitle padded"
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                children=make_dash_format_table(
                                                    df,
                                                    cols_format_category,
                                                    f"{prefix}",
                                                    f"{df_overall.at[0, 'end_date']}",
                                                )
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
                            )
                        ],
                    ),
                    # Position Holding
                    html.Div(
                        className="row",
                        children=[
                            html.Div(
                                className="twelve columns",
                                children=[
                                    html.H6(
                                        "Position Holding", className="subtitle padded"
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                children=make_dash_format_table(
                                                    df_detail,
                                                    cols_format_detail,
                                                    f"{prefix}",
                                                    f"{df_overall.at[0, 'end_date']}",
                                                )
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
                            )
                        ],
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
                                                    f"{df_overall.at[0, 'end_date']}",
                                                )
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
