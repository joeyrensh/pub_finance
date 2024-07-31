import dash_html_components as html
import base64
import re
import geopandas as gpd
import json

from utils import Header, make_dash_table, make_dash_format_table

import pandas as pd
import pathlib


def create_layout(app):
    # get relative data folder
    PATH = pathlib.Path(__file__).parent
    HOUSE_PATH = PATH.joinpath("../../houseinfo").resolve()

    # 新房数据
    df_new_house = pd.read_csv(
        HOUSE_PATH.joinpath("newhouse.csv"), usecols=[i for i in range(0, 8)]
    )
    # 重命名列
    df_new_house = df_new_house.rename(
        columns={
            "house_type": "house type",
            "bizcircle_name": "bizcircle name",
            "avg_price": "avg price",
            "area_range": "area range",
            "sale_status": "sale status",
            "open_date": "open date",
        }
    )
    with open(HOUSE_PATH.joinpath("map_newhouse.png"), "rb") as f:
        image_data = f.read()
        encoded_image_newhouse = base64.b64encode(image_data).decode("utf-8")
    # 二手房数据
    with open(HOUSE_PATH.joinpath("map_secondhouse.png"), "rb") as f:
        image_data = f.read()
        encoded_image_secondhouse = base64.b64encode(image_data).decode("utf-8")
    with open(HOUSE_PATH.joinpath("map_secondhouse2.png"), "rb") as f:
        image_data = f.read()
        encoded_image_secondhouse2 = base64.b64encode(image_data).decode("utf-8")
    with open(HOUSE_PATH.joinpath("map_secondhouse3.png"), "rb") as f:
        image_data = f.read()
        encoded_image_secondhouse3 = base64.b64encode(image_data).decode("utf-8")
    geo_path = HOUSE_PATH.joinpath("shanghaidistrict.json")
    file_path_s = HOUSE_PATH.joinpath("secondhandhouse.csv")

    # 二手房数据分析
    geo_data = gpd.read_file(geo_path, engine="pyogrio", use_arrow=True)
    df_second_hand_house = pd.read_csv(
        file_path_s,
        usecols=[
            "data_id",
            "al_text",
            "sell_cnt",
            "district",
            "unit_price",
            "deal_price",
            "deal_date",
            "lanlong",
            "age",
            "total_cnt",
            "structure",
            "house_type",
        ],
        engine="pyarrow",
    )
    df_second_hand_house = df_second_hand_house.dropna(subset=["lanlong"])

    # 处理二手房经纬度格式
    def extract_values(string):
        values = string.strip("[]").split(",")
        value1 = float(values[0])
        value2 = float(values[1])
        return value1, value2

    df_second_hand_house[["longitude", "latitude"]] = (
        df_second_hand_house["lanlong"].apply(extract_values).apply(pd.Series)
    )

    # 定义一个函数来替换中文字符
    def replace_non_numeric_characters(text, replacement=""):
        # 使用正则表达式匹配非数字字符
        numeric_text = re.sub(r"\D", "", str(text))
        # 检查字符串是否为空
        if numeric_text == "":
            return None  # 返回空字符串
        # 返回转换后的整数类型
        return int(numeric_text)

    df_second_hand_house["total_cnt"] = df_second_hand_house["total_cnt"].apply(
        replace_non_numeric_characters
    )
    df_second_hand_house["deal_price"] = df_second_hand_house["deal_price"].apply(
        replace_non_numeric_characters
    )
    gdf_second_hand_house = gpd.GeoDataFrame(
        df_second_hand_house,
        geometry=gpd.points_from_xy(
            df_second_hand_house["longitude"], df_second_hand_house["latitude"]
        ),
        crs="EPSG:4326",
    )
    # 处理板块级别数据
    exclude_values = [
        310104,
        310101,
        310106,
        310109,
        310105,
        310110,
        310107,
    ]  # 要排除的值的列表
    geo_data_s = geo_data[
        ((geo_data.level == "district") & (geo_data.adcode.isin(exclude_values)))
        | (
            (geo_data.level == "town")
            & (
                ~geo_data["parent"]
                .apply(lambda x: json.loads(x)["adcode"])
                .isin(exclude_values)
            )
        )
    ]
    gdf_merged = gpd.sjoin(
        geo_data_s, gdf_second_hand_house, how="inner", predicate="intersects"
    )

    gdf_merged = gdf_merged[gdf_merged["unit_price"] > 0]
    gdf_agg_unit_price = (
        gdf_merged.groupby("adcode").median({"unit_price": "unit_price"}).round(-2)
    )
    gdf_agg_deal_price = (
        gdf_merged.groupby("adcode").median({"deal_price": "deal_price"}).round(-2)
    )
    gdf_agg_sell = (
        gdf_merged.groupby("adcode")[["sell_cnt", "total_cnt"]].sum().round(0)
    )
    # 计算 sell_cnt / total_cnt 比例，但仅在 total_cnt 不为 0 的情况下
    gdf_agg_sell["ratio"] = gdf_agg_sell.apply(
        lambda row: row["sell_cnt"] / row["total_cnt"]
        if row["total_cnt"] != 0
        else None,
        axis=1,
    )
    result = geo_data_s.merge(
        gdf_agg_unit_price, how="left", left_on="adcode", right_on="adcode"
    )
    result = result.merge(
        gdf_agg_deal_price, how="left", left_on="adcode", right_on="adcode"
    )
    result = result.merge(gdf_agg_sell, how="left", left_on="adcode", right_on="adcode")

    result.sort_values(["parent", "name"], inplace=True)

    result = result[result["unit_price_y"] > 0][
        ["name", "unit_price_y", "deal_price_y", "sell_cnt", "total_cnt", "ratio"]
    ]

    cols_format_result = {
        "ratio": ("ratio", "format"),
    }
    # 重命名列
    result = result.rename(
        columns={
            "unit_price_y": "unit price",
            "deal_price_y": "deal price",
            "sell_cnt": "sell cnt",
            "total_cnt": "total cnt",
        }
    )
    # Page layouts
    return html.Div(
        [
            html.Div([Header(app)]),
            # page 1
            html.Div(
                [
                    # Row 3
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H5("上海房价概览"),
                                    html.Br([]),
                                    html.P(
                                        "\
                                    数据基于网上开放数据，针对上海行政区划，对在售新房以及挂牌二手房做数据分布, \
                                    在售新房主要关注联动均价中位数在各板块的数据分布，\
                                    二手房主要关注挂牌价、最近成交价以及挂牌量中位数在各板块的数据分布。",
                                        style={"color": "#ffffff"},
                                        className="row",
                                    ),
                                ],
                                className="product",
                            )
                        ],
                        className="row",
                    ),
                    # Row 4
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        ["新房零售价中位数分布"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_newhouse}",
                                        style={"width": "100%", "overflow-x": "auto"},
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.H6(
                                        "在售新房明细",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Table(
                                                make_dash_table(df_new_house),
                                            )
                                        ],
                                        className="table",
                                        style={"overflow-x": "auto", "height": 400},
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                        className="row",
                        style={"margin-bottom": "35px"},
                    ),
                    # Row 5
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        ["二手房挂牌价中位数分布"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_secondhouse}",
                                        style={"width": "100%", "overflow-x": "auto"},
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.H6(
                                        ["二手房成交价中位数分布"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_secondhouse2}",
                                        style={"width": "100%", "overflow-x": "auto"},
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                        className="row",
                        style={"margin-bottom": "35px"},
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H6(
                                        ["二手房挂牌量中位数分布"],
                                        className="subtitle padded",
                                    ),
                                    html.Img(
                                        src=f"data:image/png;base64,{encoded_image_secondhouse3}",
                                        style={"width": "100%", "overflow-x": "auto"},
                                    ),
                                ],
                                className="six columns",
                            ),
                            html.Div(
                                [
                                    html.H6(
                                        "二手房板块明细",
                                        className="subtitle padded",
                                    ),
                                    html.Div(
                                        [
                                            html.Table(
                                                # make_dash_table(result),
                                                make_dash_format_table(
                                                    result, cols_format_result
                                                )
                                            )
                                        ],
                                        className="table",
                                        style={"overflow-x": "auto", "height": 400},
                                    ),
                                ],
                                className="six columns",
                            ),
                        ],
                        className="row",
                        style={"margin-bottom": "35px"},
                    ),
                ],
                className="sub_page",
            ),
        ],
        className="page",
    )
