import dash_html_components as html
import dash_core_components as dcc
from finance.dashreport.utils import Header
from finance.dashreport.chart_builder import ChartBuilder
from finance.dashreport.data_loader import ReportDataLoader
from flask import session


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
                "industry_strength",
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
            ("industry_strength", 7),
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
        return html.Div(
            [
                html.H6(["Backtest Data & Metrics"], className="subtitle padded"),
                html.Div(
                    [
                        dcc.Loading(
                            id=f"loading-kpi-{self.prefix}",
                            type="circle",
                            delay_hide=1000,
                            style={"width": "100%", "height": "100%"},
                            color="#119DFF",
                            fullscreen=False,
                            children=html.Div(
                                id=self.app.kpi_callback.get_container_id(self.prefix),
                                className="kpi-container",
                            ),
                        )
                    ],
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
                                    # figure=None,
                                    config={
                                        "displayModeBar": False,
                                        "doubleClick": False,  # 禁用双击缩放
                                        "responsive": True,
                                    },
                                    style={
                                        "margin": 0,
                                        "padding": 0,
                                        "width": "100%",
                                        "height": "100%",
                                    },
                                    mathjax=True,
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
                    "industry_strength",
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
                                dcc.Loading(
                                    type="circle",
                                    delay_hide=1000,
                                    color="#119DFF",
                                    children=html.Div(
                                        id={
                                            "type": "dynamic-table",
                                            "page": self.prefix,
                                            "table": table_id,
                                        },
                                        className=table_class,
                                    ),
                                )
                            ],
                            className="table_custom",
                            style=style,
                        ),
                    ],
                )
            ],
        )

    def get_layout(self):
        kpi_section = self.build_kpi_section()

        # 1. 固定独占一行的图表
        annual_return_card = self.build_chart_card("annual_return", 0, "Annual Return")

        # 2. 所有半宽图表 + 固定 index 映射
        half_chart_config = [
            ("industry_strength", 7, "Industry Strength"),
            ("heatmap", 1, "Industries Tracking"),
            ("strategy", 2, "Strategy Tracking"),
            ("trade", 5, "Position Trend"),
            ("pnl_trend", 6, "Earnings Trend"),
            ("industry_position", 3, "Position Weight"),
            ("industry_profit", 4, "Earnings Weight"),
        ]

        # 3. 只收集【已启用】的图表卡片
        enabled_half_cards = []
        for chart_type, index, title in half_chart_config:
            if chart_type in self.show_charts:
                card = self.build_chart_card(chart_type, index, title)
                enabled_half_cards.append(card)

        # 4. 核心：动态两两分组（不修改任何card，保证折叠正常）
        dynamic_half_rows = []
        for i in range(0, len(enabled_half_cards), 2):
            pair = enabled_half_cards[i : i + 2]
            dynamic_half_rows.append(html.Div(pair, className="row"))

        # 5. 表格
        table_cards = []
        for i, t in enumerate(self.show_tables):
            table_cards.append(
                self.build_table_card(t, is_last=(i == len(self.show_tables) - 1))
            )

        role = session.get("role")
        is_admin = role == "admin"
        display_style = "block" if is_admin else "none"

        # 最终布局
        return html.Div(
            [
                Header(self.app),
                html.Div(
                    [
                        kpi_section,
                        annual_return_card,
                    ]
                    + dynamic_half_rows
                    + table_cards,
                    className="sub_page",
                ),
                # 按钮与摘要容器同级，靠相对定位堆叠
                html.Div(
                    style={
                        "position": "relative",
                        "marginTop": "8px",
                        "display": display_style,
                    },
                    children=[
                        html.Button(
                            "AI分析",
                            id="global_btn_ai_summary",
                            className="ai-analysis-btn",
                            style={
                                "display": "none",
                                "cursor": "pointer",
                            },
                        ),
                        dcc.Store(id="store_selected_cell_info", data=None),
                        dcc.Store(id="ai_is_loading", data=False),
                        dcc.Store(id="ai_trigger", data=0),
                        dcc.Loading(
                            id="loading_ai_summary_wrapper",
                            type="circle",
                            color="#119DFF",
                            children=html.Div(
                                id="ai_summary_box",
                                className="ai-analysis-panel",
                                style={
                                    "minHeight": "80px",
                                    "whiteSpace": "pre-wrap",
                                    "width": "100%",
                                    "lineHeight": "1.6",
                                },
                                children="点击持仓表格 NAME 列单元格，再点击上方【AI分析】生成个股量化摘要",
                            ),
                        ),
                    ],
                ),
            ],
            className="page",
        )
