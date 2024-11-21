import dash_html_components as html
from utils import Header
import pathlib
import base64

PATH = pathlib.Path(__file__).parent
IMAGE_PATH = PATH.joinpath("../assets").resolve()
with open(IMAGE_PATH.joinpath("background.jpeg"), "rb") as f:
    image_data = f.read()
    encoded_image_bg = base64.b64encode(image_data).decode("utf-8")


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
                                            html.P(
                                                "Life was like a box of chocolates, you never know what you're gonna get."
                                            ),
                                        ],
                                        style={"color": "#7a7a7a"},
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
                                        ],
                                        id="reviews-bullet-pts",
                                    ),
                                    html.Div(
                                        [
                                            html.Br([]),
                                            html.P(
                                                "You are no different than anybody else is."
                                            ),
                                        ],
                                        style={"color": "#7a7a7a"},
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_bg}",
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
