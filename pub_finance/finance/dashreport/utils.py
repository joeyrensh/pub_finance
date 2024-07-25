import dash_html_components as html
import dash_core_components as dcc


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
                "Portfolio & Management",
                href="/dash-financial-report/portfolio-management",
                className="tab",
            ),
            dcc.Link(
                "Fees & Minimums", href="/dash-financial-report/fees", className="tab"
            ),
            dcc.Link(
                "Distributions",
                href="/dash-financial-report/distributions",
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


def make_dash_table(df):
    """Return a dash definition of an HTML table for a Pandas dataframe"""
    table = []
    html_row = []
    for row in df.columns:
        html_row.append(html.Th([row]))
    table.append(html.Tr(html_row))
    for index, row in df.iterrows():
        html_row = []
        for i in range(len(row)):
            html_row.append(html.Td([row[i]]))
        table.append(html.Tr(html_row))
    return table
