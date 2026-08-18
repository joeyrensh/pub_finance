import dash_html_components as html
import dash_core_components as dcc
import re
import dash_table
from datetime import datetime, timedelta
from dash.dash_table.Format import Format, Scheme, Trim
import pandas as pd
import numpy as np
import hashlib
import os
from finance.utility.toolkit import ToolKit
from finance import FINANCE_ROOT


def Header(app):
    return html.Div([get_header(app), html.Br([]), get_menu()])


def get_header(app):
    header = html.Div(
        [
            html.Div(
                [
                    # 左侧/中间：标题区域
                    html.Div(
                        [
                            html.Span(
                                # "Find Your Truth!",
                                id="header-title",
                                className="bubble-text",
                            ),
                        ],
                        className="seven columns main-title",
                    ),
                    # 右侧：仅保留 Source Code 按钮
                    html.Div(
                        [
                            html.A(
                                html.Button(
                                    [
                                        html.I(
                                            # className="fa-solid fa-code",
                                            className="fa-solid fa-magnifying-glass",
                                            style={"marginRight": "6px"},
                                        ),
                                        # "GitHub",
                                    ],
                                    id="learn-more-button",
                                ),
                                href="https://github.com/joeyrensh/pub_finance/tree/master/pub_finance/finance",
                                target="_blank",
                                style={"textDecoration": "none"},
                            )
                        ],
                        className="header-right-action",
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
                            html.Span("A-Share", className="menu-name"),
                        ],
                        href="/dash-financial-report/a-share",
                        className="tab first",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-flag-usa icon"),
                            html.Span("US-Stock", className="menu-name"),
                        ],
                        href="/dash-financial-report/us-stock",
                        className="tab",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-rocket icon"),
                            html.Span("A+", className="menu-name"),
                        ],
                        href="/dash-financial-report/a-picks",
                        className="tab",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-rocket icon"),
                            html.Span("US+", className="menu-name"),
                        ],
                        href="/dash-financial-report/us-picks",
                        className="tab",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-solid fa-rotate-right icon"),
                            html.Span("B-Test", className="menu-name"),
                        ],
                        href="/dash-financial-report/backtest",
                        className="tab",
                    ),
                    dcc.Link(
                        [
                            html.I(className="fa-regular fa-newspaper icon"),
                            html.Span("Setup", className="menu-name"),
                        ],
                        href="/dash-financial-report/settings",
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
        pos_base = "var(--positive-int-bg-color, red)"
        neg_base = "var(--negative-int-bg-color, green)"
    else:
        pos_base = "var(--negative-int-bg-color, green)"
        neg_base = "var(--positive-int-bg-color, red)"

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
            "background": f"color-mix(in srgb, {base_color} {intensity * 100}%, transparent)",
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


def make_dash_format_table(df, cols_format, market, trade_date, table_name):
    """Return a dash_table.DataTable for a Pandas dataframe"""
    # 创建一个新的 DataFrame 来存储原始列的副本
    original_df = df.copy()
    required_cols = [
        "IND",
        "SYMBOL",
        "NAME",
        "ERP",
        "OPEN DATE",
        "DAILY RETURN",
        "PNL RATIO",
        "AVG TRANS",
        "WIN RATE",
        "SORTINO RATIO",
        "MAX DD",
        "STRATEGY CNT",
        "STRATEGY",
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

    tooltip_data = []
    if has_all_required_cols:
        column_map_default = {
            "symbol": "SYMBOL",
            "name": "NAME",
            "industry": "IND",
            "erp": "ERP",
            "open_date": "OPEN DATE",
            "daily_return_array": "DAILY RETURN",
            "pnl_ratio": "PNL RATIO",
            "win_rate": "WIN RATE",
            "avg_trans": "AVG TRANS",
            "sortino": "SORTINO RATIO",
            "max_dd": "MAX DD",
            "strategy_cnt": "STRATEGY CNT",
            "strategy": "STRATEGY",
        }
        selected_symbols, score_dict = ToolKit.score_and_select_symbols(
            df,
            column_map_default,
            market,
            trade_date,
        )

        if market in ("us", "cn", "us_special"):
            mask = df["SYMBOL"].isin(selected_symbols)
            df.loc[mask, "NAME"] = "88+" + df.loc[mask, "NAME"].astype(str)
            # 构建 tooltip_data：为每一行添加 symbol 列上的 score 提示
            score_fields = [
                "total_score",
                "industry_score",
                "erp_score",
                "pnl_score",
                "stability_score",
                "strategy_score",
                "rank",
            ]

            for idx, row in df.iterrows():
                symbol = row["SYMBOL"]  # 注意列名与实际一致
                values = [score_dict[field].get(symbol) for field in score_fields]

                if values[0] is not None:  # 至少总分存在
                    tooltip_text = (
                        f"总分: {values[0]:.4f}\n"
                        f"行业: {values[1]:.4f}\n"
                        f"ERP: {values[2]:.4f}\n"
                        f"收益: {values[3]:.4f}\n"
                        f"稳定性: {values[4]:.4f}\n"
                        f"策略: {values[5]:.4f}\n"
                        f"排名: {values[6]:.0f}"
                    )
                else:
                    tooltip_text = ""

                tooltip_data.append(
                    {"IDX": {"value": tooltip_text, "type": "markdown"}}
                )

    def create_link(symbol, market):
        if market in ("cn", "cn_dynamic") and symbol.startswith(("SH", "SZ")):
            url = f"https://quote.eastmoney.com/{symbol}.html"
        elif market == "cn" and symbol.startswith("ETF"):
            url = f"https://quote.eastmoney.com/{symbol[3:]}.html"
        else:
            url = f"https://quote.eastmoney.com/us/{symbol}.html"
        # 使用 inline style 让链接继承单元格颜色，并去掉下划线
        return f'<a href="{url}" target="_blank" style="color:inherit;text-decoration:underline;">{symbol}</a>'

    if "SYMBOL" in df.columns:
        df["SYMBOL"] = df["SYMBOL"].apply(lambda symbol: create_link(symbol, market))

    # 1. 初始化条件样式列表
    highlight_conditional = []

    # 获取表格当前所有的列名
    existing_cols = list(df.columns) if hasattr(df, "columns") else []

    # 2. 【独立逻辑一】：处理 IDX 列的交互样式（指针手势 + 内置选中高亮）
    if "NAME" in existing_cols:
        highlight_conditional.extend(
            [
                # 基础样式：只要存在 IDX 列，悬停即显示点击手势指针
                {
                    "if": {"column_id": "NAME"},
                    "cursor": "pointer",
                },
            ]
        )

    # 3. 【独立逻辑二】：根据外部条件 selected_symbols 触发的特定列高亮
    highlight_cols = ["SYMBOL", "NAME"]

    if (
        has_all_required_cols
        and selected_symbols
        and market in ("us", "cn", "us_special")
    ):
        # 构造过滤查询语句
        filter_query = " || ".join(
            ['({{{}}} = "{}")'.format("SYMBOL_o", sym) for sym in selected_symbols]
        )

        # 动态过滤：仅对表格中实际存在的列添加外部条件高亮
        target_cols = [col for col in highlight_cols if col in existing_cols]

        highlight_conditional.extend(
            [
                {
                    "if": {
                        "filter_query": filter_query,
                        "column_id": col,
                    },
                    "color": "var(--highlight-symbol-color)",
                    "fontWeight": "bold",
                }
                for col in target_cols
            ]
        )

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
                and cols_format[col][0] in ("ratio", "float", "int", "text")
                else "markdown"
            ),
        }
        for col in df.columns
        if col
        not in [
            "DAILY RETURN",
            "STRATEGY CNT",
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
                    "filter_query": "{{{column}}} = {value} and {{{column}}} != 'nan'".format(
                        column=col + "_o", value="买入"
                    ),
                    "column_id": col,
                },
                "backgroundColor": ("""var(--positive-databar-color)"""),
            }
            for col in df.columns
            if col in cols_format
            and len(cols_format[col]) > 1
            and cols_format[col][0] == "text"
            and cols_format[col][1] == "format"
        ]
        + [
            {
                "if": {
                    "filter_query": "{{{}}} < 0".format(col + "_o"),
                    "column_id": col,
                },
                "color": "var(--negative-int-bg-color)",
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
                "color": "var(--positive-int-bg-color)",
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

    table_id = {
        "type": "auto-table",
        "page": market,
        "table": table_name,
    }

    count_id = {
        "type": "auto-table-count",
        "page": market,
        "table": table_name,
    }
    is_checkbox_enabled = market in ("cn", "us") and table_name in ("detail", "cn_etf")

    # 1. 关键：为每一行数据注入 'id' 键，值为 IDX 的值（前端 UI 不会多显一列）
    for row in data:
        row["id"] = str(row.get("IDX", "SYMBOL_o"))

    return html.Div(
        children=[
            # 行数 overlay
            html.Div(
                id=count_id,
                style={
                    "position": "absolute",
                    "right": 0,
                    "top": "-15px",
                    "zIndex": 1000,
                    "pointerEvents": "none",
                    "whiteSpace": "nowrap",
                },
            ),
            html.Div(
                children=[
                    dash_table.DataTable(
                        id=table_id,
                        data=data,
                        page_size=50,
                        columns=columns,
                        tooltip_data=tooltip_data,
                        tooltip_delay=0,
                        tooltip_duration=None,
                        filter_action="native",
                        sort_action="native",
                        sort_mode="single",
                        filter_options={
                            "placeholder_text": "Search",
                            "case": "insensitive",
                        },
                        markdown_options={"html": True, "link_target": "_blank"},
                        fill_width=True,
                        editable=False,
                        cell_selectable=True,
                        row_selectable="multi" if is_checkbox_enabled else False,
                        selected_row_ids=[],
                        # selected_rows=[],
                        style_header={
                            "position": "sticky",
                            "top": "0",
                            "backgroundColor": "transparent",
                            "zIndex": 10,
                            "fontWeight": "bold",
                            "white-space": "normal",
                        },
                        style_cell={
                            "textAlign": "left",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                            "backgroundColor": "transparent",
                            "margin": "0px",
                            "padding": "0px",
                        },
                        style_data={
                            "backgroundColor": "transparent",
                        },
                        style_table={
                            "paddingBottom": (
                                "2px"
                                if (table_name == "trade" or not data or len(data) < 10)
                                else "20px"
                            ),
                            "position": "relative",
                            "width": "100%",
                            "maxWidth": "100%",
                            "overflow": "auto",
                            "maxHeight": (
                                "300px"
                                if table_name in ("detail_short", "cn_etf")
                                else "400px"
                            ),
                        },
                        style_data_conditional=style_data_conditional,
                    ),
                ],
                style={
                    "overflow": "auto",
                    "maxHeight": (
                        "300px" if table_name in ("detail_short", "cn_etf") else "400px"
                    ),
                    "width": "100%",
                },
            ),
        ],
        className="default-table",
        style={
            "position": "relative",
            "width": "100%",
        },
    )


def get_us_specific_trade_date(offset) -> str | None:
    """
    utc_us = datetime.fromisoformat('2021-01-18 01:00:00')
    美股休市日，https://www.nyse.com/markets/hours-calendars
    marketclosed.config 是2021和2022两年的美股法定休市配置文件
    """
    f = open(FINANCE_ROOT / "usstockinfo" / "marketclosed.config").readlines()
    x = []
    for i in f:
        x.append(i.split(",")[0].strip())
    """ 循环遍历最近一个交易日期 """
    counter = 0
    # 收益率曲线
    DATA_PATH = FINANCE_ROOT / "data"
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
    f = open(FINANCE_ROOT / "cnstockinfo" / "marketclosed.config").readlines()
    x = []
    for i in f:
        x.append(i.split(",")[0].strip())
    """ 循环遍历最近一个交易日期 """
    counter = 0
    # 收益率曲线
    DATA_PATH = FINANCE_ROOT / "data"
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
