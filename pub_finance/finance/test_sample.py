import requests
from lxml import html
import pandas as pd
from utility.ToolKit import ToolKit
import concurrent.futures
import geopandas as gpd
import matplotlib.pyplot as plt
import math
from utility.MyEmail import MyEmail
import seaborn as sns
from matplotlib.transforms import Bbox
import os
import contextily as cx
import re
import numpy as np
import json




file_path = './houseinfo/secondhandhouse.csv'
# houseinfo_to_csv(file_path)


plt.rcParams['font.family'] = 'WenQuanYi Zen Hei'

geo_path = './houseinfo/shanghaidistrict.json'
testpath = './houseinfo/test.png'
  

geo_data = gpd.read_file(geo_path, engine="pyogrio")

df_second_hand_house = pd.read_csv(file_path, usecols=[i for i in range(0, 10)])
df_second_hand_house = df_second_hand_house.dropna(subset=['lanlong'])
# 处理二手房经纬度格式
def extract_values(string):
    values = string.strip("[]").split(",")
    value1 = float(values[0])
    value2 = float(values[1])
    return value1, value2
df_second_hand_house[['longitude', 'latitude']] = df_second_hand_house['lanlong'].apply(extract_values).apply(pd.Series)
# 定义一个函数来替换中文字符
def replace_chinese_characters(text, replacement=''):
    # 使用正则表达式匹配中文字符
    return int(re.sub(r'[\u4e00-\u9fff]', replacement, text))

df_second_hand_house['total_cnt'] = df_second_hand_house['total_cnt'].apply(replace_chinese_characters)
gdf_second_hand_house = gpd.GeoDataFrame(
    df_second_hand_house, geometry=gpd.points_from_xy(df_second_hand_house['longitude'], df_second_hand_house['latitude']), crs="EPSG:4326"
)
# 房屋经纬度与地理围栏数据关联
data_filter_bydistrict = geo_data[geo_data.level == 'district']
gdf_merged_bydistrict = gpd.sjoin(data_filter_bydistrict, gdf_second_hand_house, how="inner", predicate = "intersects")

# 行政区均价分析
filtered_data = gdf_merged_bydistrict[gdf_merged_bydistrict['unit_price'] > 0]
agg_bydistrict = filtered_data.groupby('adcode').median({'unit_price': 'unit_price'}).round(-2)
result_bydistrict = data_filter_bydistrict.merge(agg_bydistrict, how='left', left_on='adcode', right_on ='adcode')

ax = result_bydistrict.plot(
    column="unit_price",
    cmap='YlOrRd',
    alpha = 0.5,
    legend=False,
    linewidth=0.5,
    edgecolor='k',
    scheme="natural_breaks",
    k=8,
    figsize=(10, 20),
    legend_kwds={"fmt": "{:.0f}"}
);

cx.add_basemap(ax, 
                crs="EPSG:4326",
                source='https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
                zoom = 10,
                interpolation = 'bicubic'
                )

ax.axis('off')

# 添加标题
ax.set_title('Shanghai Second-hand House Distribution', 
            fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})

# 添加annotation
texts = []
for idx, row in result_bydistrict.iterrows():
    centroid = row.geometry.centroid.coords[0]
    if not math.isnan(row['unit_price']):
        text = ax.annotate(
            text=f"{row['name']}\n{row['unit_price']:.0f}",
            xy=centroid,
            ha='center',
            fontsize=8,  # 设置字体大小
            color='black',  # 设置字体颜色为黑色
            weight='black',  # 设置字体粗细
            bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0),  boxstyle='round, pad=0.5'),  # 设置注释框样式
        )
        texts.append(text)
    # 检查注释是否重叠并调整位置
def check_and_adjust_annotations(texts, vertical_spacing=0.0001, horizontal_spacing=0.0001, min_fontsize=6, default_fontsize=8):
    renderer = ax.get_figure().canvas.get_renderer()
    for i, text in enumerate(texts):
        rect1 = text.get_window_extent(renderer=renderer)
        for j in range(i + 1, len(texts)):
            text2 = texts[j]
            rect2 = text2.get_window_extent(renderer=renderer)
            while rect1.overlaps(rect2):
                x, y = text2.get_position()
                y -= vertical_spacing
                x -= horizontal_spacing
                text2.set_position((x, y))
                # 确保 fontsize 和 alpha 不为 None
                current_fontsize = text2.get_fontsize() if text2.get_fontsize() is not None else default_fontsize
                
                # 调整字体大小和透明度
                if current_fontsize > min_fontsize:
                    text2.set_fontsize(max(min_fontsize, current_fontsize - 0.01))
                rect2 = text2.get_window_extent(renderer=renderer) 

check_and_adjust_annotations(texts)

# 提取并排序注释内容
annotations = result_bydistrict[['name', 'unit_price']].sort_values(by='unit_price', ascending=False)

# 创建自定义图例
legend_texts = [f"{row['name']}: {row['unit_price']:.0f}" for _, row in annotations.iterrows()]
legend = ax.figure.add_axes([1.05, 0.1, 0.2, 0.8], frameon=False)  # 在图的侧边创建一个新的axes
legend.axis('on')

for i, text in enumerate(legend_texts):
    legend.text(0, 1 - (i * 0.05), text, fontsize=10, ha='left')
# plt.savefig(testpath, dpi=500, bbox_inches='tight', pad_inches=0)