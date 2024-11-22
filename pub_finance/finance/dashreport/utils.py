import dash_html_components as html
import dash_core_components as dcc
import re
import dash_table
from datetime import datetime, timedelta


def Header(app):
    return html.Div([get_header(app), html.Br([]), get_menu()])


def get_header(app):
    header = html.Div(
        [
            html.Div(
                [
                    html.A(
                        html.Img(
                            src=app.get_asset_url(
                                "buyahouse-background-transparent.png"
                            ),
                            # style={
                            #     "width": "40%",
                            #     "height": "50%",
                            #     "margin-left": "2px",
                            #     "margin-top": "5px",
                            # },
                            className="company-logo",
                            # className="pulse",
                        ),
                        href="#",
                    ),
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
                        className="five columns",
                    ),
                ],
                className="twelve columns",
                style={"padding-left": "0"},
            ),
        ],
        className="row",
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
        style_filter={"color": "white", "ling-height": "1px"},
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
                    dodgerblue 0%,
                    dodgerblue {max_bound_percentage}%,
                    white {max_bound_percentage}%,
                    white 100%)
                """.format(max_bound_percentage=max_bound_percentage)
                ),
                "paddingBottom": 2,
                "paddingTop": 2,
                "color": "black",
            }
        )

    return styles


def make_dash_format_table(df, cols_format):
    """Return a dash_table.DataTable for a Pandas dataframe"""
    columns = [
        {
            "name": col,
            "id": col,
            "type": "numeric" if col in cols_format else "text",
            "presentation": "markdown",
        }
        for col in df.columns
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
        elif value_type == "ratio":
            return f"{value * 100:.2f}%"
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

    # Flatten the list of styles
    date_threshold = str(datetime.now() - timedelta(days=5))[0:10]
    style_data_conditional = (
        [
            {
                "if": {
                    "filter_query": "{{{column}}} >= {value} and {{{column}}} != 'nan'".format(
                        column=col + "_o", value=date_threshold
                    ),
                },
                # "backgroundColor": "RebeccaPurple",
                "backgroundColor": "coral",
                "color": "white",
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
                "backgroundColor": "#3D9970",
                "color": "white",
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
                "backgroundColor": "#FF4136",
                "color": "white",
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
        filter_options={
            "placeholder_text": "......",
            "case": "insensitive",
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
            # "background-color": "white",
            "z-index": 1000,
            "fontWeight": "bold",
            "white-space": "normal",
            # "z-index": "10",
        },
        style_cell={
            "textAlign": "left",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
            # "font-size": "1rem",
            "margin": "0px",
            "margin-bottom": "-20px",
            "padding": "0px",
        },
        style_data_conditional=style_data_conditional,
    )
