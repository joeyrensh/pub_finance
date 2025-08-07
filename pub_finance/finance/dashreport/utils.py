import dash_html_components as html
import dash_core_components as dcc
import re
import dash_table
from datetime import datetime, timedelta
from dash.dash_table.Format import Format, Scheme, Trim
import pathlib
import pandas as pd


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
                                    style={"margin-left": "-10px"},
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
                                [html.H5("Data-Driven Empowering Investments")],
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
                "A Stock Market",
                href="/dash-financial-report/cn-stock-performance",
                className="tab first",
            ),
            dcc.Link(
                "US Stock Market",
                href="/dash-financial-report/us-stock-performance",
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


def data_bars(df, column):
    col_n = column + "_o"
    n_bins = 100
    bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
    ranges = [
        ((df[col_n].max() - df[col_n].min()) * i) + df[col_n].min() for i in bounds
    ]
    styles = []
    for i in range(1, len(bounds)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        max_bound_percentage = bounds[i] * 100
        styles.append(
            {
                "if": {
                    "filter_query": (
                        "{{{column}}} >= {min_bound}"
                        + (
                            " && {{{column}}} < {max_bound}"
                            if (i < len(bounds) - 1)
                            else ""
                        )
                        + " && {{{column}}} > 0"  # 只对非负值应用样式
                    ).format(column=col_n, min_bound=min_bound, max_bound=max_bound),
                    "column_id": column,
                },
                "background": (
                    """
                    linear-gradient(90deg,
                    var(--data-bar-color) 0%,
                    var(--data-bar-color) {max_bound_percentage}%,
                    transparent {max_bound_percentage}%,
                    transparent 100%)
                    """.format(max_bound_percentage=max_bound_percentage)
                ),
                # "padding": "4px 0",
                # "paddingBottom": 2,
                # "paddingTop": 2,
            }
        )
        # 添加对负值的样式
        styles.append(
            {
                "if": {
                    "filter_query": (
                        "{{{column}}} <= 0"  # 只对负值应用样式
                    ).format(column=col_n),
                    "column_id": column,
                },
                "background": "none",  # 负值背景色设置为透明
                "paddingBottom": 2,
                "paddingTop": 2,
            }
        )

    return styles


def extract_arrow_num(s):
    # 提取向上箭头后的数字
    arrow_num = re.search(r"↑(\d+)", s)
    arrow_num = int(arrow_num.group(1)) if arrow_num else None

    # 提取括号内的数字（在箭头后）
    bracket_num = re.search(r"↑[^()]*\((\d+)\)", s)
    bracket_num = int(bracket_num.group(1)) if bracket_num else None

    return arrow_num, bracket_num


def make_dash_format_table(df, cols_format, market):
    """Return a dash_table.DataTable for a Pandas dataframe"""
    required_cols = ["IND", "EPR", "OPEN DATE", "PNL RATIO", "AVG TRANS", "WIN RATE"]
    has_all_required_cols = all(col in df.columns for col in required_cols)

    columns = [
        {
            "name": col,
            "id": col,
            "type": "numeric"
            if col in cols_format and cols_format[col][0] in ("ratio", "float")
            else "text",
            "format": Format(
                precision=2,
                scheme=Scheme.percentage,
            )
            if col in cols_format and cols_format[col][0] == "ratio"
            else None,
            "presentation": None
            if col in cols_format and cols_format[col][0] == "ratio"
            else "markdown",
        }
        for col in df.columns
    ]
    # 如果有IND列，先生成辅助列
    if has_all_required_cols:
        df[["IND_ARROW_NUM", "IND_BRACKET_NUM"]] = df["IND"].apply(
            lambda x: pd.Series(extract_arrow_num(x))
        )

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
        # elif value_type == "ratio":
        # return value * 100
        # return f"{value * 100:.2f}%"
        else:
            return value

    # Convert data to support markdown for images and rich text
    for row in data:
        for key in row:
            if key in cols_format:
                new_value, value_type = row[key], cols_format[key][0]
            else:
                value = str(row[key])
                new_value, value_type = check_value_type(value)

            row[key] = format_value(new_value, value_type)
    if market == "us":
        trade_date_l5 = get_us_latest_trade_date(4)
        trade_date_l20 = get_us_latest_trade_date(19)
    elif market == "cn":
        trade_date_l5 = get_cn_latest_trade_date(4)
        trade_date_l20 = get_cn_latest_trade_date(19)

    date_threshold_l5 = datetime.strptime(trade_date_l5, "%Y%m%d").strftime("%Y-%m-%d")
    date_threshold_l20 = datetime.strptime(trade_date_l20, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )
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
                            "({IND_ARROW_NUM} >= 20 || {IND_BRACKET_NUM} <= 20) && "
                            "{EPR_o} > 0 && "
                            "{OPEN DATE_o} >= '" + date_threshold_l20 + "' && "
                            "{PNL RATIO_o} < 0.2 && "
                            "{AVG TRANS_o} <= 3 && "
                            "{WIN RATE_o} > 0.8"
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
                "background": ("""var(--negative-value-bg-color)"""),
                # "color": "white",
            }
            for col in df.columns
            if col in cols_format
            and len(cols_format[col]) > 1
            and cols_format[col][0] == "float"
            and cols_format[col][1] == "format"
        ]
        + [
            {
                "if": {
                    "filter_query": "{{{}}} > 0".format(col + "_o"),
                    "column_id": col,
                },
                # "backgroundColor": "#FF4136",
                "background": ("""var(--positive-value-bg-color)"""),
                # "color": "white",
            }
            for col in df.columns
            if col in cols_format
            and len(cols_format[col]) > 1
            and cols_format[col][0] == "float"
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

    return dash_table.DataTable(
        id="idAssignedToDataTable",
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
        markdown_options={"html": True},
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
        x.append(re.sub(",.*\n", "", i))
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
        x.append(re.sub(",.*\n", "", i))
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
