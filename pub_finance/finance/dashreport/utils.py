import dash_html_components as html
import dash_core_components as dcc
import re
import dash_table


def Header(app):
    return html.Div([get_header(app), html.Br([]), get_menu()])


def get_header(app):
    header = html.Div(
        [
            html.Div(
                [
                    html.A(
                        html.Img(
                            src=app.get_asset_url("dash-financial-logo.png"),
                            className="logo",
                        ),
                        href="#",
                    ),
                    html.A(
                        html.Button(
                            "Enterprise Demo",
                            id="learn-more-button",
                            style={"margin-left": "-10px"},
                        ),
                        href="#",
                    ),
                    html.A(
                        html.Button("Source Code", id="learn-more-button"),
                        href="#",
                    ),
                ],
                className="row",
            ),
            html.Div(
                [
                    html.Div(
                        [html.H5("房产以及股市大数据分析")],
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
                "上海房产数据分析",
                href="/dash-financial-report/overview",
                className="tab first",
            ),
            dcc.Link(
                "A股市场数据分析",
                href="/dash-financial-report/cn-stock-performance",
                className="tab",
            ),
            dcc.Link(
                "美股市场数据分析",
                href="/dash-financial-report/us-stock-performance",
                className="tab",
            ),
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
        return re.sub(r"<span\b[^>]*>(.*?)</span>", r"\1", value), "richtext"
    # 如果不是上述两种情况，则直接返回原字符串
    else:
        return value, "text"


# def make_dash_table(df):
#     """Return a dash definition of an HTML table for a Pandas dataframe"""
#     table = []
#     html_row = []
#     for row in df.columns:
#         html_row.append(html.Th([row]))
#     table.append(html.Tr(html_row))
#     for _, row in df.iterrows():
#         html_row = []
#         for i in range(len(row)):
#             value = str(row[i])
#             new_value, value_type = check_value_type(value)
#             if value_type == "img":
#                 # 添加样式设置到图片标签
#                 img_style = {
#                     "max-width": "100px",  # 设置图片最大宽度
#                     "max-height": "80px",  # 设置图片最大高度
#                 }
#                 html_row.append(html.Td(html.Img(src=new_value, style=img_style)))
#             else:
#                 html_row.append(html.Td(html.Span(children=new_value)))
#         table.append(html.Tr(html_row))

#     return table


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
