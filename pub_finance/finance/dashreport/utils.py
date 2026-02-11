import dash_html_components as html
import dash_core_components as dcc
import re
import dash_table
from datetime import datetime, timedelta
from dash.dash_table.Format import Format, Scheme, Trim
import pathlib
import pandas as pd
import numpy as np
import hashlib
import os
from finance.utility.toolkit import ToolKit


def Header(app):
    return html.Div([get_header(app), html.Br([]), get_menu()])


def get_header(app):
    header = html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.A(
                                html.Button(
                                    [
                                        html.I(
                                            className="fa-brands fa-github",
                                            style={"marginRight": "6px"},
                                        ),
                                        "Source Code",
                                    ],
                                    id="learn-more-button",
                                    style={
                                        "display": "flex",
                                        "alignItems": "center",
                                        "padding": "8px 14px",
                                        # "backgroundColor": "#24292e",
                                        # "color": "white",
                                        "border": "none",
                                        "borderRadius": "6px",
                                        "cursor": "pointer",
                                        # "fontSize": "15px",
                                        "fontWeight": "500",
                                    },
                                ),
                                href="https://github.com/joeyrensh/pub_finance/tree/master/pub_finance/finance",
                                target="_blank",
                                style={"textDecoration": "none"},
                            )
                        ],
                        className="row",
                    ),
                    html.Div(
                        [
                            # html.I(className="fa-regular fa-circle bubble-1"),
                            # html.I(className="fa-regular fa-circle bubble-2"),
                            # html.I(className="fa-regular fa-circle bubble-3"),
                            html.Span("Find Your Truth!", className="bubble-text"),
                        ],
                        className="seven columns main-title",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    dcc.Link(
                                        [
                                            html.I(
                                                className="fa-solid fa-up-right-from-square",
                                                style={"marginRight": "6px"},
                                            ),
                                            "Full View",
                                        ],
                                        href="/dash-financial-report/full-view",
                                        id="full-view-link",
                                        style={
                                            "display": "flex",
                                            "alignItems": "center",
                                            "padding": "8px 14px",
                                            # "backgroundColor": "#f3f4f6",
                                            # "color": "#111827",
                                            "border": "1px solid #d1d5db",
                                            "borderRadius": "6px",
                                            "cursor": "pointer",
                                            # "fontSize": "15px",
                                            "fontWeight": "500",
                                            "textDecoration": "none",
                                        },
                                    )
                                ],
                            ),
                        ],
                        # className="twelve columns",
                        className="row",
                        # style={"padding-left": "0"},
                    ),
                ],
                className="row",
            )
        ],
        className="background-header",
    )
    return header


def get_menu():
    menu = html.Div(
        [
            html.Div(
                [
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-chart-line icon"),
                            html.Span("A.stock"),
                        ],
                        href="/dash-financial-report/cn-stock-performance",
                        className="tab first",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-flag-usa icon"),
                            html.Span("U.stock"),
                        ],
                        href="/dash-financial-report/us-stock-performance",
                        className="tab",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-rocket icon"),
                            html.Span("U.fsl"),
                        ],
                        href="/dash-financial-report/us-special-stock-performance",
                        className="tab",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-rocket icon"),
                            html.Span("A.dsl"),
                        ],
                        href="/dash-financial-report/cn-dynamic-stock-performance",
                        className="tab",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-rocket icon"),
                            html.Span("U.dsl"),
                        ],
                        href="/dash-financial-report/us-dynamic-stock-performance",
                        className="tab",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-regular fa-newspaper icon"),
                            html.Span("Slog"),
                        ],
                        href="/dash-financial-report/slogans",
                        className="tab",
                    ),
                ],
                className="navbar",
            )
        ],
        className="row all-tabs",
    )
    return menu


def check_value_type(value):
    # 如果包含<img>标签，则提取<img>标签中的src属性值
    if re.search(r"<img\s+[^>]*>", value):
        match = re.search(r"<img\s+[^>]*src\s*=\s*\"([^\"]*)\"\s*\/?>", value)
        if match:
            return match.group(1), "img"
    # 如果包含<span>标签，则去除<span>标签但保留其内容
    elif re.search(r"<span\b[^>]*>(.*?)</span>", value):
        # return re.sub(r"<span\b[^>]*>(.*?)</span>", r"\1", value), "richtext"
        return value, "richtext"
    # 如果不是上述两种情况，则直接返回原字符串
    else:
        return value, "text"


def make_dash_table(df):
    """Return a dash_table.DataTable for a Pandas dataframe"""
    data = df.to_dict("records")

    columns = []
    for col in df.columns:
        columns.append({"name": col, "id": col, "presentation": "markdown"})

    # Convert data to support markdown for images and rich text
    for row in data:
        for key in row:
            value = str(row[key])
            new_value, value_type = check_value_type(value)
            if value_type == "img":
                # 使用 HTML 格式添加图片
                img_style = "max-width: 100px; max-height: 50px;"
                row[key] = f'<img src="{new_value}" style="{img_style}" />'
            elif value_type == "richtext":
                row[key] = f"{new_value}"
            else:
                row[key] = new_value

    return dash_table.DataTable(
        data=data,
        columns=columns,
        filter_action="native",
        filter_options={
            "placeholder_text": None,
            "case": "insensitive",
        },
        style_filter={
            # "color": "white",
            "ling-height": "1px"
        },
        markdown_options={"html": True},
        fill_width=True,
        editable=True,
        style_header={
            "fontWeight": "bold",
            "white-space": "normal",
            # "backgroundColor": "white",
        },
        style_cell={
            "textAlign": "left",
            "minHeight": "5px",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
            "font-size": "1rem",
            "margin": "0px",
            "padding": "0px",
            # "border": "1px",
        },
    )


# def data_bars(df, column):
#     col_n = column + "_o"
#     n_bins = 100
#     bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
#     ranges = [
#         ((df[col_n].max() - df[col_n].min()) * i) + df[col_n].min() for i in bounds
#     ]
#     styles = []
#     for i in range(1, len(bounds)):
#         min_bound = ranges[i - 1]
#         max_bound = ranges[i]
#         max_bound_percentage = bounds[i] * 100
#         styles.append(
#             {
#                 "if": {
#                     "filter_query": (
#                         "{{{column}}} >= {min_bound}"
#                         + (
#                             " && {{{column}}} < {max_bound}"
#                             if (i < len(bounds) - 1)
#                             else ""
#                         )
#                         + " && {{{column}}} > 0"  # 只对非负值应用样式
#                     ).format(column=col_n, min_bound=min_bound, max_bound=max_bound),
#                     "column_id": column,
#                 },
#                 "background": (
#                     """
#                     linear-gradient(90deg,
#                     var(--data-bar-color) 0%,
#                     var(--data-bar-color) {max_bound_percentage}%,
#                     transparent {max_bound_percentage}%,
#                     transparent 100%)
#                     """.format(
#                         max_bound_percentage=max_bound_percentage
#                     )
#                 ),
#                 # "padding": "4px 0",
#                 # "paddingBottom": 2,
#                 # "paddingTop": 2,
#             }
#         )
#         # 添加对负值的样式
#         styles.append(
#             {
#                 "if": {
#                     "filter_query": ("{{{column}}} <= 0").format(  # 只对负值应用样式
#                         column=col_n
#                     ),
#                     "column_id": column,
#                 },
#                 "background": "none",  # 负值背景色设置为透明
#                 "paddingBottom": 2,
#                 "paddingTop": 2,
#             }
#         )

#     return styles


def data_bars(df, column):
    col_n = column + "_o"

    # 获取值的范围
    min_val = df[col_n].min()
    max_val = df[col_n].max()

    styles = []
    style_cache = {}  # 缓存样式，避免重复计算

    for value in df[col_n]:
        if pd.isna(value) or value in style_cache:
            continue

        if value > 0:
            # 正值百分比 - 相对于最大值
            percentage = (value / max_val) * 100 if max_val > 0 else 0
            style = {
                "if": {
                    "filter_query": "{{{column}}} = {value}".format(
                        column=col_n, value=value
                    ),
                    "column_id": column,
                },
                "background": """
                    linear-gradient(90deg,
                    var(--positive-databar-color) 0%,
                    var(--positive-databar-color) {percentage}%,
                    transparent {percentage}%,
                    transparent 100%)
                """.format(
                    percentage=percentage
                ),
                "borderRadius": "4px",
                "backgroundSize": "100% 70%",
                "backgroundRepeat": "no-repeat",
                "backgroundPosition": "center",
                "paddingBottom": "4px",
                "paddingTop": "4px",
            }
        elif value < 0:
            # 负值百分比 - 相对于最小值的绝对值
            percentage = (abs(value) / abs(min_val)) * 100 if min_val < 0 else 0
            style = {
                "if": {
                    "filter_query": "{{{column}}} = {value}".format(
                        column=col_n, value=value
                    ),
                    "column_id": column,
                },
                "background": """
                    linear-gradient(90deg,
                    transparent 0%,
                    transparent {transparent_percentage}%,
                    var(--negative-databar-color) {transparent_percentage}%,
                    var(--negative-databar-color) 100%)
                """.format(
                    transparent_percentage=100 - percentage
                ),
                "borderRadius": "4px",
                "backgroundSize": "100% 70%",
                "backgroundRepeat": "no-repeat",
                "backgroundPosition": "center",
                "paddingBottom": "4px",
                "paddingTop": "4px",
            }
        else:  # 零值
            style = {
                "if": {
                    "filter_query": "{{{column}}} = 0".format(column=col_n),
                    "column_id": column,
                },
                "background": "none",
                "borderRadius": "4px",
                "backgroundSize": "100% 70%",
                "backgroundRepeat": "no-repeat",
                "backgroundPosition": "center",
                "paddingBottom": "4px",
                "paddingTop": "4px",
            }

        styles.append(style)
        style_cache[value] = True

    return styles


def discrete_background_color_bins(df, column, n_bins=10, positive_is_red=False, mid=0):
    """
    使用CSS变量和透明度的版本，更好地适应暗黑模式
    修正：正负值区间分开计算，正值用mid到正值最大值，负值用负值最小值到mid
    mid: 渐变中心值，默认0，可设置为1或其它
    """
    col_o = column + "_o"
    if col_o not in df.columns:
        return []

    vals = pd.to_numeric(df[col_o], errors="coerce").dropna()
    if vals.empty:
        return []

    vmin = float(vals.min())
    vmax = float(vals.max())

    styles = []

    # 零值或 mid 值透明
    styles.append(
        {
            "if": {
                "filter_query": "{{{}}} = {}".format(col_o, mid),
                "column_id": column,
            },
            "background": "transparent",
        }
    )

    # 根据参数决定正/负的基础颜色（使用 CSS 变量，保留可覆盖性）
    if positive_is_red:
        pos_base = "var(--positive-value-bg-color, red)"
        neg_base = "var(--negative-value-bg-color, green)"
    else:
        pos_base = "var(--negative-value-bg-color, green)"
        neg_base = "var(--positive-value-bg-color, red)"

    # 使用CSS变量定义颜色，让CSS处理暗黑模式适配
    def get_color_style(value_range, is_positive=True):
        low, high = value_range
        mid_val = (low + high) / 2.0

        if is_positive:
            intensity = min(
                max(abs(mid_val - mid) / max(abs(vmax - mid), 1e-10), 0.1), 1.0
            )
            base_color = pos_base
        else:
            intensity = min(
                max(abs(mid_val - mid) / max(abs(vmin - mid), 1e-10), 0.1), 1.0
            )
            base_color = neg_base

        return {
            "if": {
                "filter_query": "{{{col}}} >= {low} && {{{col}}} <= {high} && {{{col}}} != {mid}".format(
                    col=col_o, low=repr(low), high=repr(high), mid=repr(mid)
                ),
                "column_id": column,
            },
            "background": f"color-mix(in srgb, {base_color} {intensity*100}%, transparent)",
        }

    # 负值区间：从vmin到mid
    if vmin < mid:
        neg_edges = [vmin + (mid - vmin) * (i / n_bins) for i in range(n_bins + 1)]
        for i in range(n_bins):
            styles.append(get_color_style((neg_edges[i], neg_edges[i + 1]), False))

    # 正值区间：从mid到vmax
    if vmax > mid:
        pos_edges = [mid + (vmax - mid) * (i / n_bins) for i in range(n_bins + 1)]
        for i in range(n_bins):
            styles.append(get_color_style((pos_edges[i], pos_edges[i + 1]), True))

    return styles


def make_dash_format_table(df, cols_format, market, trade_date):
    """Return a dash_table.DataTable for a Pandas dataframe"""
    # 创建一个新的 DataFrame 来存储原始列的副本
    original_df = df.copy()
    required_cols = [
        "IND",
        "ERP",
        "OPEN DATE",
        "PNL RATIO",
        "AVG TRANS",
        "WIN RATE",
        "SHARPE RATIO",
        "SORTINO RATIO",
        "MAX DD",
    ]
    has_all_required_cols = all(col in df.columns for col in required_cols)
    if market in ("us", "us_special", "us_dynamic"):
        trade_date_l5 = get_us_specific_trade_date(4)
    elif market in ("cn", "cn_dynamic"):
        trade_date_l5 = get_cn_specific_trade_date(4)

    date_threshold_l5 = datetime.strptime(trade_date_l5, "%Y%m%d").strftime("%Y-%m-%d")
    # 如果有IND列，先生成辅助列
    if "ERP" in df.columns:
        df["ERP"] = pd.to_numeric(df["ERP"], errors="coerce")
        df["ERP"] = df["ERP"].fillna(-99999)

    if has_all_required_cols:
        column_map_default = {
            "symbol": "SYMBOL",
            "name": "NAME",
            "industry": "IND",
            "erp": "ERP",
            "open_date": "OPEN DATE",
            "pnl_ratio": "PNL RATIO",
            "win_rate": "WIN RATE",
            "avg_trans": "AVG TRANS",
            "sortino": "SORTINO RATIO",
            "max_dd": "MAX DD",
        }
        selected_symbols = ToolKit.score_and_select_symbols(
            df,
            column_map_default,
            market,
            trade_date,
        )

        if market in ("us", "cn", "us_special"):
            mask = df["SYMBOL"].isin(selected_symbols)
            df.loc[mask, "NAME"] = "88+" + df.loc[mask, "NAME"].astype(str)

    def create_link(symbol, market):
        if market in ("cn", "cn_dynamic") and symbol.startswith(("SH", "SZ")):
            url = f"https://quote.eastmoney.com/{symbol}.html"
        elif market == "cn" and symbol.startswith("ETF"):
            url = f"https://quote.eastmoney.com/{symbol[3:]}.html"
        else:
            url = f"https://quote.eastmoney.com/us/{symbol}.html"
        # 使用 inline style 让链接继承单元格颜色，并去掉下划线
        return f'<a href="{url}" target="_blank" style="color:inherit;text-decoration:none;">{symbol}</a>'

    if "SYMBOL" in df.columns:
        df["SYMBOL"] = df["SYMBOL"].apply(lambda symbol: create_link(symbol, market))

    highlight_cols = [
        "IDX",
        "SYMBOL",
        "NAME",
    ]
    if (
        has_all_required_cols
        and selected_symbols
        and market in ("us", "cn", "us_special")
    ):
        # 使用原始值列 SYMBOL_o 做过滤（SYMBOL 列已被替换为 HTML/link）
        # filter_query 中字符串请用双引号，以符合 Dash 语法
        filter_query = " || ".join(
            ['({{{}}} = "{}")'.format("SYMBOL_o", sym) for sym in selected_symbols]
        )

        highlight_conditional = [
            {
                "if": {
                    "filter_query": filter_query,
                    "column_id": col,  # 高亮的列
                },
                "color": "var(--row-bg-color)",
                "fontWeight": "bold",
            }
            for col in highlight_cols
        ]
    else:
        highlight_conditional = []

    columns = [
        {
            "name": col,
            "id": col,
            "type": (
                "numeric"
                if col in cols_format
                and cols_format[col][0] in ("ratio", "float", "int")
                else "text"
            ),
            "format": (
                Format(
                    precision=2,
                    scheme=Scheme.percentage,
                )
                if col in cols_format and cols_format[col][0] == "ratio"
                else (
                    Format(
                        precision=2,
                        scheme=Scheme.fixed,
                    )
                    if col in cols_format and cols_format[col][0] == "float"
                    else (
                        Format(
                            precision=0,
                            scheme=Scheme.fixed,
                        )
                        if col in cols_format and cols_format[col][0] == "int"
                        else None
                    )
                )
            ),
            "presentation": (
                None
                if col in cols_format
                and cols_format[col][0] in ("ratio", "float", "int")
                else "markdown"
            ),
        }
        for col in df.columns
        if col
        not in [
            "IND_ARROW_NUM",
            "IND_BRACKET_NUM",
            "industry_arrow_score",
            "industry_bracket_score",
            "DAYS_FROM_OPEN",
            "pnl_score",
            "erp_clean",
            "erp_score",
            "win_rate_score",
            "avg_trans_score",
            "sortino_score",
            "maxdd_score",
            "stability_score",
            "total_score",
        ]
    ]

    # 遍历 DataFrame 的所有列
    for col in df.columns:
        # 为每一列创建一个新的列，新的列名为原列名加上后缀 "_o"
        df[col + "_o"] = original_df[col]
    data = df.to_dict("records")

    def format_value(value, value_type):
        if value_type == "img":
            img_style = "max-width: 150px; max-height: 40px;"
            return f'<img src="{value}" style="{img_style}" />'
        elif value_type == "richtext":
            return f"{value}"
        elif value_type == "float":
            return f"{value:.2f}"
        else:
            return value

    for row in data:
        for key in list(row.keys()):
            if key.endswith("_o"):
                continue  # 跳过副本列

            if key in cols_format and cols_format[key][0] in ("ratio", "float", "int"):
                try:
                    # 若原始是字符串数字，尝试转换为 float；保留 None/空值
                    val = row[key]
                    if val is None or (isinstance(val, str) and val.strip() == ""):
                        row[key] = None
                    else:
                        row[key] = float(val)
                except Exception:
                    # 若不能转换，保留原值（不会破坏 markdown/html 列）
                    row[key] = row[key]
                continue

            # 非数值列：检查是否包含 img/span 等并按 markdown/html 处理
            value = str(row[key])
            new_value, value_type = check_value_type(value)

            if value_type == "img":
                img_style = "max-width: 150px; max-height: 40px;"
                row[key] = f'<img src="{new_value}" style="{img_style}" />'
            elif value_type == "richtext":
                row[key] = f"{new_value}"
            else:
                row[key] = new_value

    style_data_conditional = (
        [
            {
                "if": {
                    "column_id": col,
                },
                "background": "none",
                # "color": "var(--text-color)",
            }
            for col in df.columns
            if col in cols_format and len(cols_format[col]) == 1
        ]
        + [
            {
                "if": {
                    "column_id": col,
                },
                "background": "none",
                # "color": "var(--text-color)",
            }
            for col in df.columns
            if col not in cols_format
        ]
        + [
            {
                "if": {
                    "filter_query": "{{{column}}} >= {value} and {{{column}}} != 'nan'".format(
                        column=col + "_o", value=date_threshold_l5
                    ),
                    "column_id": col,
                },
                # "backgroundColor": "RebeccaPurple",
                # "backgroundColor": "coral",
                "backgroundColor": ("""var(--date-bg-color)"""),
                # "color": "var(--text-color)",
            }
            for col in df.columns
            if col in cols_format
            and len(cols_format[col]) > 1
            and cols_format[col][0] == "date"
            and cols_format[col][1] == "format"
        ]
        + [
            {
                "if": {
                    "filter_query": "{{{}}} < 0".format(col + "_o"),
                    "column_id": col,
                },
                # "color": ("""var(--negative-value-bg-color)"""),
                "color": "var(--negative-value-bg-color)",
            }
            for col in df.columns
            if col in cols_format
            and len(cols_format[col]) > 1
            and cols_format[col][0] in ("float", "int")
            and cols_format[col][1] == "format"
        ]
        + [
            {
                "if": {
                    "filter_query": "{{{}}} > 0".format(col + "_o"),
                    "column_id": col,
                },
                # "color": ("""var(--positive-value-bg-color)"""),
                "color": "var(--positive-value-bg-color)",
            }
            for col in df.columns
            if col in cols_format
            and len(cols_format[col]) > 1
            and cols_format[col][0] in ("float", "int")
            and cols_format[col][1] == "format"
        ]
        + highlight_conditional
    )

    for col in df.columns:
        if (
            col in cols_format
            and len(cols_format[col]) > 1
            and cols_format[col][0] == "ratio"
            and cols_format[col][1] == "format"
        ):
            style_data_conditional.extend(data_bars(df, col))

    # 在这里把按值分段着色应用到需要的列
    gradient_target_cols = ["ERP"]

    for col in gradient_target_cols:
        style_data_conditional.extend(
            discrete_background_color_bins(df, col, n_bins=10, positive_is_red=True)
        )

    # 在这里把按值分段着色应用到需要的列
    gradient_target_cols = ["SHARPE RATIO", "SORTINO RATIO"]

    for col in gradient_target_cols:
        style_data_conditional.extend(
            discrete_background_color_bins(
                df, col, n_bins=10, positive_is_red=True, mid=1
            )
        )

    gradient_target_cols = ["MAX DD"]

    for col in gradient_target_cols:
        style_data_conditional.extend(
            discrete_background_color_bins(
                df, col, n_bins=10, positive_is_red=True, mid=0
            )
        )

    gradient_target_cols = ["L5 OPEN"]

    for col in gradient_target_cols:
        style_data_conditional.extend(
            discrete_background_color_bins(df, col, n_bins=10, positive_is_red=True)
        )

    gradient_target_cols = ["L5 CLOSE"]

    for col in gradient_target_cols:
        style_data_conditional.extend(
            discrete_background_color_bins(df, col, n_bins=10, positive_is_red=False)
        )
    import uuid

    table_key = str(uuid.uuid4())

    table_id = {
        "type": "auto-table",
        "table": table_key,
    }

    count_id = {
        "type": "auto-table-count",
        "table": table_key,
    }

    return html.Div(
        children=[
            # 行数 overlay
            html.Div(
                id=count_id,
                style={
                    "position": "sticky",  # 使用 sticky 定位
                    "zIndex": 1000,
                    "pointerEvents": "none",
                    "whiteSpace": "nowrap",
                    "float": "right",
                    "marginBottom": "2px",
                },
            ),
            dash_table.DataTable(
                # id="default-table",
                id=table_id,
                data=data,
                page_size=50,
                columns=columns,
                filter_action="native",
                sort_action="native",
                sort_mode="single",
                filter_options={
                    "placeholder_text": "Search",
                    # "case": "insensitive",
                },
                markdown_options={"html": True, "link_target": "_blank"},
                fill_width=True,
                editable=False,
                style_header={
                    "position": "sticky",
                    "top": "0",
                    "backgroundColor": "transparent",
                    "z-index": 10,
                    "fontWeight": "bold",
                    "white-space": "normal",
                },
                style_cell={
                    "textAlign": "left",
                    "overflow": "hidden",
                    "textOverflow": "ellipsis",
                    "backgroundColor": "transparent",
                    "margin": "0px",
                    "margin-bottom": "-20px",
                    "padding": "0px",
                },
                style_data={
                    "backgroundColor": "transparent",
                },
                style_table={
                    "paddingBottom": "20px",
                    "position": "relative",
                    # "display": "flex",
                    # "flexDirection": "column",
                    "width": "100%",
                    "maxWidth": "100%",
                    "maxHeight": "400px",
                    "overflow": "auto",  # 让整个容器可以滚动
                },
                style_data_conditional=style_data_conditional,
            ),
        ],
        className="default-table",
        style={
            "position": "relative",
            "display": "inline-block",
            "width": "100%",
        },
    )


def get_us_specific_trade_date(offset) -> str | None:
    """
    utc_us = datetime.fromisoformat('2021-01-18 01:00:00')
    美股休市日，https://www.nyse.com/markets/hours-calendars
    marketclosed.config 是2021和2022两年的美股法定休市配置文件
    """
    BASE_DIR = pathlib.Path(__file__).resolve().parents[1]  # finance
    f = open(BASE_DIR / "usstockinfo" / "marketclosed.config").readlines()
    x = []
    for i in f:
        x.append(i.split(",")[0].strip())
    """ 循环遍历最近一个交易日期 """
    counter = 0
    # 收益率曲线
    DATA_PATH = BASE_DIR / "data"
    df_overall = pd.read_csv(
        DATA_PATH / "us_df_result.csv",
        usecols=[i for i in range(1, 5)],
    )
    utc_us = datetime.strptime(df_overall["end_date"].iloc[0], "%Y-%m-%d")
    for h in range(0, 365):
        # 当前美国时间 UTC-4
        current_date = utc_us - timedelta(days=h)
        # 周末正常休市
        if current_date.isoweekday() in [1, 2, 3, 4, 5]:
            if str(current_date)[0:10] in x:
                continue
            else:
                """返回日期字符串格式20200101"""
                counter += 1
                if counter == offset + 1:  # 找到第 offset 个交易日
                    print("trade date: ", str(current_date)[0:10].replace("-", ""))
                    return str(current_date)[0:10].replace("-", "")
        else:
            continue


def get_cn_specific_trade_date(offset) -> str | None:
    BASE_DIR = pathlib.Path(__file__).resolve().parents[1]  # finance
    f = open(BASE_DIR / "cnstockinfo" / "marketclosed.config").readlines()
    x = []
    for i in f:
        x.append(i.split(",")[0].strip())
    """ 循环遍历最近一个交易日期 """
    counter = 0
    # 收益率曲线
    DATA_PATH = BASE_DIR / "data"
    df_overall = pd.read_csv(
        DATA_PATH / "cn_df_result.csv",
        usecols=[i for i in range(1, 5)],
    )
    utc_cn = datetime.strptime(df_overall["end_date"].iloc[0], "%Y-%m-%d")
    for h in range(0, 365):
        # 当前北京时间 UTC+8
        current_date = utc_cn - timedelta(days=h)
        # 周末正常休市
        if current_date.isoweekday() in [1, 2, 3, 4, 5]:
            if str(current_date)[0:10] in x:
                continue
            else:
                """返回日期字符串格式20200101"""
                counter += 1
                if counter == offset + 1:  # 找到第 offset 个交易日
                    print("trade date: ", str(current_date)[0:10].replace("-", ""))
                    return str(current_date)[0:10].replace("-", "")
        else:
            continue
