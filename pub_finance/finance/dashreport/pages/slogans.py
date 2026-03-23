import dash_html_components as html
from finance.dashreport.utils import Header
from finance.paths import FINANCE_ROOT

IMAGE_PATH = FINANCE_ROOT / "dashreport" / "assets"
encoded_image_bg = IMAGE_PATH / "cartoon.png"
encoded_image_bg1 = IMAGE_PATH / "cartoon1.png"


def create_layout(app):
    return html.Div(
        [
            Header(app),
            # page 6
            html.Div(
                [
                    # Row 1
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6("News", className="subtitle padded"),
                                    html.Br([]),
                                    html.Div(
                                        [
                                            html.Li(
                                                "Life was like a box of chocolates, you never know what you're gonna get."
                                            ),
                                        ],
                                    ),
                                ],
                                className="row",
                            ),
                            html.Div(
                                [
                                    html.H6("Reviews", className="subtitle padded"),
                                    html.Br([]),
                                    html.Div(
                                        [
                                            html.Li(
                                                "You have got to put the past behind you before you can move on."
                                            ),
                                            html.Li("Miracles happen every day."),
                                            html.Li(
                                                "You are no different than anybody else is."
                                            ),
                                        ],
                                        id="reviews-bullet-pts",
                                    ),
                                    html.Img(
                                        # src=f"data:image/png;base64,{encoded_image_bg}",
                                        src=encoded_image_bg,
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                    html.Img(
                                        src=encoded_image_bg1,
                                        style={
                                            "width": "100%",
                                            "overflow-x": "auto",
                                            "overflow-y": "auto",
                                        },
                                    ),
                                ],
                                className="row",
                            ),
                        ],
                        className="row ",
                    )
                ],
                className="sub_page",
            ),
        ],
        className="page",
    )
