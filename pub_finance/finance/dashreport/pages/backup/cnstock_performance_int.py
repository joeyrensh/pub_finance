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
from dash import callback, Output, Input, State
from finance.dashreport.data_loader import ReportDataLoader
from finance.dashreport.chart_builder import ChartBuilder


def create_layout(app):
    prefix = "cn"
    PATH = pathlib.Path(__file__).parent
    DATA_PATH = PATH.joinpath("../../data").resolve()
    data = ReportDataLoader.load(
        prefix=prefix,
        datasets=[
            "overall",
        ],
    )

    # Load tables and other data for page
    # Overall 信息
    cb = ChartBuilder()
    df_overall = data["overall"]
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
    # annual return 数据
    app.chart_callback.register_chart(
        chart_type="annual_return",
        page_prefix=prefix,
        chart_builder=cb,
        datasets=("annual_return",),
        index=0,
    )
    # heatmap 数据
    app.chart_callback.register_chart(
        chart_type="heatmap",
        page_prefix=prefix,
        chart_builder=cb,
        datasets=("heatmap",),
        index=1,
    )
    # strategy 数据
    app.chart_callback.register_chart(
        chart_type="strategy",
        page_prefix=prefix,
        chart_builder=cb,
        datasets=("strategy",),
        index=2,
    )
    # trade info 数据
    app.chart_callback.register_chart(
        chart_type="trade",
        page_prefix=prefix,
        chart_builder=cb,
        datasets=("trade",),
        index=5,
    )
    # pnl trend 数据
    app.chart_callback.register_chart(
        chart_type="pnl_trend",
        page_prefix=prefix,
        chart_builder=cb,
        datasets=("pnl_trend",),
        index=6,
    )
    # industry position 数据
    app.chart_callback.register_chart(
        chart_type="industry_position",
        page_prefix=prefix,
        chart_builder=cb,
        datasets=("industry_position",),
        index=3,
    )
    # industry profit 数据
    app.chart_callback.register_chart(
        chart_type="industry_profit",
        page_prefix=prefix,
        chart_builder=cb,
        datasets=("industry_profit",),
        index=4,
    )
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
                    # Annual Return
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
                                            dcc.Loading(
                                                id=f"loading-annual-return-{prefix}",
                                                type="circle",
                                                delay_hide=1000,
                                                style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "position": "relative",
                                                    "display": "flex",  # 添加flex布局
                                                    "justifyContent": "center",  # 水平居中
                                                    "alignItems": "center",  # 垂直居中
                                                },
                                                parent_style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "display": "flex",  # 父容器也需要flex
                                                    "justifyContent": "center",
                                                    "alignItems": "center",
                                                },
                                                color="#119DFF",
                                                fullscreen=False,
                                                children=[
                                                    dcc.Graph(
                                                        id=app.chart_callback.get_chart_id(
                                                            "annual_return",
                                                            prefix,
                                                            0,
                                                        ),
                                                        figure=None,
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
                                            dcc.Loading(
                                                id=f"loading-heatmap-{prefix}",
                                                type="circle",
                                                delay_hide=1000,
                                                style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "position": "relative",
                                                    "display": "flex",  # 添加flex布局
                                                    "justifyContent": "center",  # 水平居中
                                                    "alignItems": "center",  # 垂直居中
                                                },
                                                parent_style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "display": "flex",  # 父容器也需要flex
                                                    "justifyContent": "center",
                                                    "alignItems": "center",
                                                },
                                                color="#119DFF",
                                                fullscreen=False,
                                                children=[
                                                    dcc.Graph(
                                                        id=app.chart_callback.get_chart_id(
                                                            "heatmap", prefix, 1
                                                        ),
                                                        figure=None,
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
                                            dcc.Loading(
                                                id=f"loading-strategy-{prefix}",
                                                type="circle",
                                                delay_hide=1000,
                                                style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "position": "relative",
                                                    "display": "flex",  # 添加flex布局
                                                    "justifyContent": "center",  # 水平居中
                                                    "alignItems": "center",  # 垂直居中
                                                },
                                                parent_style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "display": "flex",  # 父容器也需要flex
                                                    "justifyContent": "center",
                                                    "alignItems": "center",
                                                },
                                                color="#119DFF",
                                                fullscreen=False,
                                                children=[
                                                    dcc.Graph(
                                                        id=app.chart_callback.get_chart_id(
                                                            "strategy", prefix, 2
                                                        ),
                                                        figure=None,
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
                                            dcc.Loading(
                                                id=f"loading-trade-{prefix}",
                                                type="circle",
                                                delay_hide=1000,
                                                style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "position": "relative",
                                                    "display": "flex",  # 添加flex布局
                                                    "justifyContent": "center",  # 水平居中
                                                    "alignItems": "center",  # 垂直居中
                                                },
                                                parent_style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "display": "flex",  # 父容器也需要flex
                                                    "justifyContent": "center",
                                                    "alignItems": "center",
                                                },
                                                color="#119DFF",
                                                fullscreen=False,
                                                children=[
                                                    dcc.Graph(
                                                        id=app.chart_callback.get_chart_id(
                                                            "trade", prefix, 5
                                                        ),
                                                        figure=None,
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
                                            dcc.Loading(
                                                id=f"loading-pnl-trend-{prefix}",
                                                type="circle",
                                                delay_hide=1000,
                                                style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "position": "relative",
                                                    "display": "flex",  # 添加flex布局
                                                    "justifyContent": "center",  # 水平居中
                                                    "alignItems": "center",  # 垂直居中
                                                },
                                                parent_style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "display": "flex",  # 父容器也需要flex
                                                    "justifyContent": "center",
                                                    "alignItems": "center",
                                                },
                                                color="#119DFF",
                                                fullscreen=False,
                                                children=[
                                                    dcc.Graph(
                                                        id=app.chart_callback.get_chart_id(
                                                            "pnl_trend", prefix, 6
                                                        ),
                                                        figure=None,
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
                                            dcc.Loading(
                                                id=f"loading-industry-position-{prefix}",
                                                type="circle",
                                                delay_hide=1000,
                                                style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "position": "relative",
                                                    "display": "flex",  # 添加flex布局
                                                    "justifyContent": "center",  # 水平居中
                                                    "alignItems": "center",  # 垂直居中
                                                },
                                                parent_style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "display": "flex",  # 父容器也需要flex
                                                    "justifyContent": "center",
                                                    "alignItems": "center",
                                                },
                                                color="#119DFF",
                                                fullscreen=False,
                                                children=[
                                                    dcc.Graph(
                                                        id=app.chart_callback.get_chart_id(
                                                            "industry_position",
                                                            prefix,
                                                            3,
                                                        ),
                                                        figure=None,
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
                                            dcc.Loading(
                                                id=f"loading-industry-profit-{prefix}",
                                                type="circle",
                                                delay_hide=1000,
                                                style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "position": "relative",
                                                    "display": "flex",  # 添加flex布局
                                                    "justifyContent": "center",  # 水平居中
                                                    "alignItems": "center",  # 垂直居中
                                                },
                                                parent_style={
                                                    "width": "100%",
                                                    "height": "100%",
                                                    "display": "flex",  # 父容器也需要flex
                                                    "justifyContent": "center",
                                                    "alignItems": "center",
                                                },
                                                color="#119DFF",
                                                fullscreen=False,
                                                children=[
                                                    dcc.Graph(
                                                        id=app.chart_callback.get_chart_id(
                                                            "industry_profit", prefix, 4
                                                        ),
                                                        figure=None,
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
                                                id={
                                                    "type": "dynamic-table",
                                                    "page": prefix,
                                                    "table": "category",
                                                },
                                                className="cn_table",
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
                                                id={
                                                    "type": "dynamic-table",
                                                    "page": prefix,
                                                    "table": "detail",
                                                },
                                                className="cn_table",
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
                                        "ETF Position Holding",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Div(
                                                id={
                                                    "type": "dynamic-table",
                                                    "page": prefix,
                                                    "table": "cn_etf",
                                                },
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
                                                id={
                                                    "type": "dynamic-table",
                                                    "page": prefix,
                                                    "table": "detail_short",
                                                },
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
