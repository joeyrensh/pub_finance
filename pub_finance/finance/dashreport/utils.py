import dash_html_components as html
import dash_core_components as dcc
import re
import dash_table
from datetime import datetime, timedelta
from dash.dash_table.Format import Format, Scheme, Trim
import pathlib
import pandas as pd
import numpy as np


def Header(app):
    return html.Div([get_header(app), html.Br([]), get_menu()])


def get_header(app):
    header = html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            # html.A(
                            #     html.Img(
                            #         src=app.get_asset_url(
                            #             "buyahouse-background-transparent.png"
                            #         ),
                            #         # style={
                            #         #     "width": "40%",
                            #         #     "height": "50%",
                            #         #     "margin-left": "2px",
                            #         #     "margin-top": "5px",
                            #         # },
                            #         className="company-logo",
                            #         # className="pulse",
                            #     ),
                            #     href="#",
                            # ),
                            html.A(
                                html.Button(
                                    "Enterprise Demo",
                                    id="learn-more-button",
                                    style={"margin-left": "-8px"},
                                ),
                                href="https://github.com/joeyrensh/pub_finance/tree/master/pub_finance/finance",
                            ),
                            html.A(
                                html.Button(
                                    "Source Code",
                                    id="learn-more-button",
                                ),
                                href="https://github.com/joeyrensh/pub_finance/tree/master/pub_finance/finance",
                            ),
                        ],
                        className="row",
                    ),
                    html.Div(
                        [
                            html.Div(
                                # [html.H5("Data-Driven Empowering Investments")],
                                # [html.H5("你是一个小呀小呀小苹果！！！")],
                                [html.H5("HaiFeng Capital Investment")],
                                className="seven columns main-title",
                            ),
                            html.Div(
                                [
                                    dcc.Link(
                                        "Full View",
                                        href="/dash-financial-report/full-view",
                                        className="full-view-link",
                                    )
                                ],
                                # className="five columns",
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
            dcc.Link(
                "A.stock",
                href="/dash-financial-report/cn-stock-performance",
                className="tab first",
            ),
            dcc.Link(
                "U.S.stock",
                href="/dash-financial-report/us-stock-performance",
                className="tab",
            ),
            dcc.Link(
                "U.S.stock.special",
                href="/dash-financial-report/us-special-stock-performance",
                className="tab",
            ),
            # dcc.Link(
            #     "上海房产数据分析",
            #     href="/dash-financial-report/overview",
            #     className="tab",
            # ),
            dcc.Link(
                "News & Reviews",
                href="/dash-financial-report/news-and-reviews",
                className="tab",
            ),
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


def extract_arrow_num(s):
    # 去除HTML标签
    clean_text = re.sub("<[^<]+?>", "", s)

    # 提取向上箭头后的数字（新格式：↑7/4）
    arrow_num = re.search(r"↑(\d+)", clean_text)
    arrow_num = int(arrow_num.group(1)) if arrow_num else None

    # 提取斜杠后的当前排名数字（新格式：↑7/4）
    bracket_num = re.search(r"/(\d+)", clean_text)
    bracket_num = int(bracket_num.group(1)) if bracket_num else None

    return arrow_num, bracket_num


def make_dash_format_table(df, cols_format, market):
    """Return a dash_table.DataTable for a Pandas dataframe"""
    required_cols = [
        "IND",
        "ERP",
        "OPEN DATE",
        "PNL RATIO",
        "AVG TRANS",
        "WIN RATE",
        "SHARPE RATIO",
    ]
    has_all_required_cols = all(col in df.columns for col in required_cols)
    if market in ("us", "us_special"):
        trade_date_l40 = get_us_latest_trade_date(39)
        trade_date_l20 = get_us_latest_trade_date(19)
        trade_date_l5 = get_us_latest_trade_date(4)
    elif market == "cn":
        trade_date_l40 = get_cn_latest_trade_date(39)
        trade_date_l20 = get_cn_latest_trade_date(19)
        trade_date_l5 = get_cn_latest_trade_date(4)

    date_threshold_l40 = datetime.strptime(trade_date_l40, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )
    date_threshold_l20 = datetime.strptime(trade_date_l20, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )
    date_threshold_l5 = datetime.strptime(trade_date_l5, "%Y%m%d").strftime("%Y-%m-%d")
    # 如果有IND列，先生成辅助列
    if "ERP" in df.columns:
        df["ERP"] = pd.to_numeric(df["ERP"], errors="coerce")
        df["ERP"] = df["ERP"].fillna(-99999)

    def get_real_quantile(series, q):
        s = series[(series.notna()) & (series != -99999)].sort_values()
        if len(s) == 0:
            return np.nan
        idx = int(np.ceil(q * len(s))) - 1
        idx = min(max(idx, 0), len(s) - 1)
        return s.iloc[idx]

    if has_all_required_cols:
        head_quantile = 0.8
        mid_quantile = 0.5
        tail_quantile = 0.2
        df[["IND_ARROW_NUM", "IND_BRACKET_NUM"]] = df["IND"].apply(
            lambda x: pd.Series(extract_arrow_num(x))
        )
        ind_arrow_num_threshold = get_real_quantile(df["IND_ARROW_NUM"], head_quantile)
        win_rate_threshold = get_real_quantile(df["WIN RATE"], head_quantile)
        avg_trans_threshold = get_real_quantile(df["AVG TRANS"], tail_quantile)

        # 分组后取真实分位值
        df["pnl_ratio_threshold_tail"] = df.groupby("IND")["PNL RATIO"].transform(
            lambda x: get_real_quantile(x, tail_quantile)
        )
        df["pnl_ratio_threshold_head"] = df.groupby("IND")["PNL RATIO"].transform(
            lambda x: get_real_quantile(x, head_quantile)
        )

        # 确保 ERP 列为数值类型，无法转换的变为 NaN
        df["erp_threshold"] = df.groupby("IND")["ERP"].transform(
            lambda x: get_real_quantile(x, mid_quantile)
        )

        # df["CLUSTER"] = ""

        # 标记满足条件的行
        # 近5个交易日涨幅最快的行业中的优秀个股
        condition1 = (
            (df["IND_ARROW_NUM"] >= ind_arrow_num_threshold)
            & (df["OPEN DATE"] >= date_threshold_l20)
            & (df["PNL RATIO"] >= df["pnl_ratio_threshold_head"])
            & (df["PNL RATIO"] > 0)
            & (df["AVG TRANS"] <= avg_trans_threshold)
            & (df["WIN RATE"] >= win_rate_threshold)
            & (df["SHARPE RATIO"] > 1)
        )

        # 近5个交易日TOP 5行业中的优秀个股
        condition2 = (
            (df["IND_BRACKET_NUM"] <= 20)
            & (df["OPEN DATE"] >= date_threshold_l20)
            & (df["PNL RATIO"] >= df["pnl_ratio_threshold_head"])
            & (df["PNL RATIO"] > 0)
            & (df["AVG TRANS"] <= avg_trans_threshold)
            & (df["WIN RATE"] >= win_rate_threshold)
            & (df["SHARPE RATIO"] > 1)
        )

        # PE合理的优秀个股
        condition3 = (
            (df["OPEN DATE"] >= date_threshold_l20)
            & (df["PNL RATIO"] > 0)
            & (df["ERP"] > df["erp_threshold"])
            & (df["ERP"] != -99999)
            & (df["AVG TRANS"] <= avg_trans_threshold)
            & (df["WIN RATE"] >= win_rate_threshold)
            & (df["SHARPE RATIO"] > 1)
        )

        # 然后进行赋值操作
        df.loc[condition1 | condition2 | condition3, "NAME"] = (
            "3A+" + df.loc[condition1 | condition2 | condition3, "NAME"]
        )

    def create_link(symbol, market):
        if market == "cn" and symbol.startswith(("SH", "SZ")):
            return f"[{symbol}](https://quote.eastmoney.com/{symbol}.html)"
        elif market == "cn" and symbol.startswith("ETF"):
            return f"[{symbol}](https://quote.eastmoney.com/{symbol[3:]}.html)"
        else:
            return f"[{symbol}](https://quote.eastmoney.com/us/{symbol}.html)"

    if "SYMBOL" in df.columns:
        df["SYMBOL"] = df["SYMBOL"].apply(lambda symbol: create_link(symbol, market))

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
            "erp_threshold",
            "pnl_ratio_threshold_tail",
            "pnl_ratio_threshold_head",
        ]
    ]

    # 创建一个新的 DataFrame 来存储原始列的副本
    original_df = df.copy()
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

    # date_threshold = str(datetime.now() - timedelta(days=5))[0:10]
    style_data_conditional = (
        [
            {
                "if": {
                    "column_id": col,
                },
                "background": "none",
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
            }
            for col in df.columns
            if col not in cols_format
        ]
        + (
            [
                {
                    "if": {
                        "filter_query": (
                            "({IND_ARROW_NUM} >= "
                            + str(ind_arrow_num_threshold)
                            + " && "
                            "{OPEN DATE_o} >= " + str(date_threshold_l20) + " && "
                            "{PNL RATIO_o} >= {pnl_ratio_threshold_head_o} && "
                            + "{PNL RATIO_o} > 0 && "
                            "{AVG TRANS_o} <= " + str(avg_trans_threshold) + " && "
                            "{WIN RATE_o} >= " + str(win_rate_threshold) + " && "
                            "{SHARPE RATIO} > 1 " + ") || ("
                            "{IND_BRACKET_NUM} <= 20 && "
                            "{OPEN DATE_o} >= " + str(date_threshold_l20) + " && "
                            "{PNL RATIO_o} >= {pnl_ratio_threshold_head_o} && "
                            + "{PNL RATIO_o} > 0 &&"
                            "{AVG TRANS_o} <= " + str(avg_trans_threshold) + " && "
                            "{WIN RATE_o} >= " + str(win_rate_threshold) + " && "
                            "{SHARPE RATIO} > 1 " + ") || ("
                            "{OPEN DATE_o} >= "
                            + str(date_threshold_l20)
                            + " && "
                            + "{PNL RATIO_o} > 0 && "
                            "{ERP_o} != -99999 && {ERP_o} > {erp_threshold_o} && "
                            "{AVG TRANS_o} <= " + str(avg_trans_threshold) + " && "
                            "{WIN RATE_o} >= " + str(win_rate_threshold) + " && "
                            "{SHARPE RATIO} > 1 " + ")"
                        )
                    },
                    "background": ("""var(--row-bg-color)"""),
                }
            ]
            if has_all_required_cols and "IND_ARROW_NUM" in df.columns
            else []
        )
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
                "background": ("""var(--date-bg-color)"""),
                # "color": "white",
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
                # "backgroundColor": "#3D9970",
                # "background": ("""var(--negative-value-bg-color)"""),
                "color": ("""var(--negative-value-bg-color)"""),
                # "color": "white",
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
                # "backgroundColor": "#FF4136",
                # "background": ("""var(--positive-value-bg-color)"""),
                "color": ("""var(--positive-value-bg-color)"""),
                # "color": "white",
            }
            for col in df.columns
            if col in cols_format
            and len(cols_format[col]) > 1
            and cols_format[col][0] in ("float", "int")
            and cols_format[col][1] == "format"
        ]
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
    gradient_target_cols = ["SHARPE RATIO"]

    for col in gradient_target_cols:
        style_data_conditional.extend(
            discrete_background_color_bins(
                df, col, n_bins=10, positive_is_red=True, mid=1
            )
        )

    gradient_target_cols = ["L5 OPEN"]

    for col in gradient_target_cols:
        style_data_conditional.extend(
            discrete_background_color_bins(df, col, n_bins=10, positive_is_red=True)
        )

    return dash_table.DataTable(
        id="defult-table",
        data=data,
        columns=columns,
        filter_action="native",
        sort_action="native",
        filter_options={
            "placeholder_text": "Search",
            # "case": "insensitive",
        },
        style_filter={
            # "top": "-3px",  # 根据需要调整这个值
            # "z-index": "10",
            # "color": "black",
            # "overflow": "hidden",
        },
        markdown_options={"html": True, "link_target": "_blank"},
        fill_width=True,
        editable=True,
        style_header={
            "position": "sticky",
            "top": "0",
            "backgroundColor": "transparent",
            "z-index": 1000,
            "fontWeight": "bold",
            "white-space": "normal",
            # "z-index": "10",
        },
        style_cell={
            "textAlign": "left",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
            "backgroundColor": "transparent",
            # "font-size": "1rem",
            "margin": "0px",
            "margin-bottom": "-20px",
            "padding": "0px",
        },
        style_data={
            "backgroundColor": "transparent",
        },
        style_data_conditional=style_data_conditional,
    )


def get_us_latest_trade_date(offset) -> str | None:
    """
    utc_us = datetime.fromisoformat('2021-01-18 01:00:00')
    美股休市日，https://www.nyse.com/markets/hours-calendars
    marketclosed.config 是2021和2022两年的美股法定休市配置文件
    """
    f = open("../usstockinfo/marketclosed.config").readlines()
    x = []
    for i in f:
        x.append(i.split(",")[0].strip())
    """ 循环遍历最近一个交易日期 """
    counter = 0
    PATH = pathlib.Path(__file__).parent
    # 收益率曲线
    DATA_PATH = PATH.joinpath("../data").resolve()
    df_overall = pd.read_csv(
        DATA_PATH.joinpath("us_df_result.csv"),
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


def get_cn_latest_trade_date(offset) -> str | None:
    f = open("../cnstockinfo/marketclosed.config").readlines()
    x = []
    for i in f:
        x.append(i.split(",")[0].strip())
    """ 循环遍历最近一个交易日期 """
    counter = 0
    PATH = pathlib.Path(__file__).parent
    # 收益率曲线
    DATA_PATH = PATH.joinpath("../data").resolve()
    df_overall = pd.read_csv(
        DATA_PATH.joinpath("cn_df_result.csv"),
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
