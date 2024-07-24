import requests
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import math
from utility.MyEmail import MyEmail
from utility.ToolKit import ToolKit
import os
from lxml import html
import concurrent.futures
import re
import json

geo_path = "./houseinfo/shanghaidistrict.json"
file_path_s = "./houseinfo/secondhandhouse.csv"
geo_data = gpd.read_file(geo_path, engine="pyogrio")
# 二手房数据分析
df_second_hand_house = pd.read_csv(file_path_s, usecols=[i for i in range(0, 12)])
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
gdf_agg_unit_price = (
    gdf_merged.groupby("adcode").median({"unit_price": "unit_price"}).round(-2)
)
gdf_agg_deal_price = (
    gdf_merged.groupby("adcode").median({"deal_price": "deal_price"}).round(-2)
)
gdf_agg_sell = gdf_merged.groupby("adcode")[["sell_cnt", "total_cnt"]].sum().round(0)
gdf_agg_sell["ratio"] = gdf_agg_sell.apply(
    lambda row: row["sell_cnt"] / row["total_cnt"] if row["total_cnt"] != 0 else None,
    axis=1,
)
result = geo_data_s.merge(
    gdf_agg_unit_price, how="left", left_on="adcode", right_on="adcode"
)
result = result.merge(
    gdf_agg_deal_price, how="left", left_on="adcode", right_on="adcode"
)
result = result.merge(gdf_agg_sell, how="left", left_on="adcode", right_on="adcode")
result.drop(
    columns=[
        "adcode",
        "childrenNum",
        "childrenNum_x",
        "parent",
        "subFeatureIndex_x",
        "geometry",
        "childrenNum_y",
        "subFeatureIndex_y",
        "longitude_y",
        "latitude_y",
        "index_right_x",
        "data_id_x",
        "data_id_x",
    ],
    inplace=True,
)
print(result)
