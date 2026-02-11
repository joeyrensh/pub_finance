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
from finance.dashreport.data_loader import ReportDataLoader


class PageLayout:
    def __init__(self, app, prefix="cn", show_charts=None, show_tables=None):
        self.app = app
        self.prefix = prefix
        self.show_charts = show_charts or []
        self.show_tables = show_tables or []

        # 表格 id -> 显示标题映射
        self.table_display_map = {
            "category": "Industries List",
            "detail": "Position Holding",
            "cn_etf": "ETF Position Holding",
            "detail_short": "Position Reduction",
        }

        # 数据加载
        self.data = ReportDataLoader.load(
            prefix=prefix,
            datasets=[
                "overall",
                "annual_return",
                "heatmap",
                "strategy",
                "trade",
                "pnl_trend",
                "industry_position",
                "industry_profit",
                "category",
                "detail",
                "cn_etf",
                "detail_short",
            ],
        )

        self.cb = ChartBuilder()

        # 注册 charts
        self.register_charts()

    def register_charts(self):
        chart_map = [
            ("annual_return", 0),
            ("heatmap", 1),
            ("strategy", 2),
            ("industry_position", 3),
            ("industry_profit", 4),
            ("trade", 5),
            ("pnl_trend", 6),
        ]
        for chart_type, index in chart_map:
            if chart_type in self.show_charts:
                self.app.chart_callback.register_chart(
                    chart_type=chart_type,
                    page_prefix=self.prefix,
                    chart_builder=self.cb,
                    datasets=(chart_type,),
                    index=index,
                )

    def build_kpi_section(self):
        df_overall = self.data["overall"]
        # 检查是否为NaN、None、0或空字符串
        stock_cnt_value = df_overall.at[0, "stock_cnt"]
        if pd.isna(stock_cnt_value) or stock_cnt_value in [None, 0, ""]:
            df_overall.at[0, "stock_cnt"] = 1

        kpis = [
            (
                "SWDI 指数",
                int(
                    round(
                        (
                            (df_overall.at[0, "final_value"] - df_overall.at[0, "cash"])
                            - (
                                df_overall.at[0, "stock_cnt"] * 10000
                                - df_overall.at[0, "cash"]
                            )
                        )
                        / df_overall.at[0, "stock_cnt"],
                        0,
                    )
                ),
            ),
            ("总资产", f"{df_overall.at[0,'final_value']/10000:,.2f} 万"),
            ("Cash", f"{df_overall.at[0,'cash']/10000:,.2f} 万"),
            ("股票数量", f"{df_overall.at[0,'stock_cnt']}"),
            ("数据日期", df_overall.at[0, "end_date"]),
        ]

        kpi_cards = []
        for label, value in kpis:
            extra_class = " kpi-date" if label == "数据日期" else ""
            kpi_cards.append(
                html.Div(
                    [
                        html.Div(label, className="kpi-label"),
                        html.Div(value, className=f"kpi-value{extra_class}"),
                    ],
                    className="kpi-card",
                )
            )

        return html.Div(
            [
                html.H6(
                    ["Market Trends and Index Summary"], className="subtitle padded"
                ),
                html.Div(
                    [html.Div(kpi_cards, className="kpi-container")],
                    className="product",
                ),
            ]
        )

    def build_chart_card(self, chart_type, index, title):
        if chart_type not in self.show_charts:
            return html.Div()

        return html.Div(
            [
                html.Button(
                    html.H6(
                        [title + " ‹"],
                        className="subtitle padded",
                        id=f"{self.prefix}-{chart_type}-title",
                    ),
                    id={"type": "collapse-btn", "page": self.prefix, "index": index},
                    style={
                        "background": "none",
                        "border": "none",
                        "padding": 0,
                        "cursor": "pointer",
                        "width": "100%",
                        "text-align": "left",
                    },
                ),
                html.Div(
                    className="chart-container",
                    children=[
                        dcc.Loading(
                            id=f"loading-{chart_type}-{self.prefix}",
                            type="circle",
                            delay_hide=1000,
                            style={
                                "width": "100%",
                                "height": "100%",
                                "position": "relative",
                                "display": "flex",
                                "justifyContent": "center",
                                "alignItems": "center",
                            },
                            parent_style={
                                "width": "100%",
                                "height": "100%",
                                "display": "flex",
                                "justifyContent": "center",
                                "alignItems": "center",
                            },
                            color="#119DFF",
                            fullscreen=False,
                            children=[
                                dcc.Graph(
                                    id=self.app.chart_callback.get_chart_id(
                                        chart_type, self.prefix, index
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
                    id={"type": "collapsible", "page": self.prefix, "index": index},
                    style={"display": "block", "aspectRatio": 1.6, "width": "100%"},
                ),
            ],
            className=(
                "six columns"
                if chart_type
                in [
                    "heatmap",
                    "strategy",
                    "trade",
                    "pnl_trend",
                    "industry_position",
                    "industry_profit",
                ]
                else "twelve columns"
            ),
        )

    def build_table_card(self, table_id, is_last=False):
        if table_id not in self.show_tables:
            return html.Div()

        display_name = self.table_display_map.get(table_id, table_id)
        table_class = "cn_table" if "cn" in self.prefix else ""

        # 特殊 max_height
        max_height = 300 if table_id in ["cn_etf", "detail_short"] else 400

        style = {"overflow-x": "auto", "max-height": max_height, "overflow-y": "auto"}
        if is_last:
            style["margin-bottom"] = "10px"

        return html.Div(
            className="row",
            children=[
                html.Div(
                    className="twelve columns",
                    children=[
                        html.H6(display_name, className="subtitle padded"),
                        html.Div(
                            [
                                html.Div(
                                    id={
                                        "type": "dynamic-table",
                                        "page": self.prefix,
                                        "table": table_id,
                                    },
                                    className=table_class,
                                )
                            ],
                            className="table",
                            style=style,
                        ),
                    ],
                )
            ],
        )

    def get_layout(self):
        kpi_section = self.build_kpi_section()

        # Annual return chart
        annual_return_card = self.build_chart_card("annual_return", 0, "Annual Return")

        # Heatmap + Strategy row
        heatmap_card = self.build_chart_card("heatmap", 1, "Industries Tracking")
        strategy_card = self.build_chart_card("strategy", 2, "Strategy Tracking")
        row1 = (
            html.Div([heatmap_card, strategy_card], className="row")
            if any([heatmap_card, strategy_card])
            else html.Div()
        )

        # Row2: Position trend + Earnings trend
        trade_card = self.build_chart_card("trade", 5, "Position Trend")
        pnl_card = self.build_chart_card("pnl_trend", 6, "Earnings Trend")
        row2 = (
            html.Div([trade_card, pnl_card], className="row")
            if any([trade_card, pnl_card])
            else html.Div()
        )

        # Row3: Position weight + Earnings weight
        pos_weight_card = self.build_chart_card(
            "industry_position", 3, "Position Weight"
        )
        earn_weight_card = self.build_chart_card(
            "industry_profit", 4, "Earnings Weight"
        )
        row3 = (
            html.Div([pos_weight_card, earn_weight_card], className="row")
            if any([pos_weight_card, earn_weight_card])
            else html.Div()
        )

        # Tables
        table_cards = []
        for i, t in enumerate(self.show_tables):
            table_cards.append(
                self.build_table_card(t, is_last=(i == len(self.show_tables) - 1))
            )

        return html.Div(
            [
                Header(self.app),
                html.Div(
                    [kpi_section, annual_return_card, row1, row2, row3] + table_cards,
                    className="sub_page",
                ),
            ],
            className="page",
        )
