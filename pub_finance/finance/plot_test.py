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
from matplotlib.font_manager import FontProperties
import geoplot as gplt
import geoplot.crs as gcrs
import mapclassify as mc
import matplotlib.patches as mpatches
from shapely.geometry import box
from matplotlib.colors import Normalize
import matplotlib.cm as cm
from cmap import Colormap


file_path = './houseinfo/secondhandhouse.csv'
# houseinfo_to_csv(file_path)


plt.rcParams['font.family'] = 'WenQuanYi Zen Hei'

geo_path = './houseinfo/shanghaidistrict.json'
testpath = './houseinfo/test.png'
  

geo_data = gpd.read_file(geo_path, engine="pyogrio")
geo_data = geo_data.to_crs("EPSG:4326")


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
gdf_second_hand_house = gdf_second_hand_house[gdf_second_hand_house['unit_price'] >0]

exclude_values = [310104, 310101, 310106, 310109, 310105, 310110, 310107]  # 要排除的值的列表            ]
data_filter_bystreet = geo_data[
    ((geo_data.level == 'district') & (geo_data.adcode.isin(exclude_values))) |
    ((geo_data.level == 'town') & (geo_data['parent'].apply(lambda x: json.loads(x)['adcode']).isin(exclude_values) == False))
]

gdf_merged_bystreet = gpd.sjoin(data_filter_bystreet, gdf_second_hand_house, how="left", predicate = "intersects")
# 板块均价分析
filtered_data = gdf_merged_bystreet[gdf_merged_bystreet['unit_price'] > 0]
agg_bystreet = gdf_merged_bystreet.groupby('adcode').median({'unit_price': 'unit_price'}).round(-2)
result_bystreet = data_filter_bystreet.merge(agg_bystreet, how='left', left_on='adcode', right_on ='adcode')

k = 8
# ax = gplt.polyplot(df=geo_data,
#                    projection=gcrs.AlbersEqualArea(),
#                 #    edgecolor='lightgrey',
#                    facecolor='#00003a',
#                    edgecolor='white',
#                 #    facecolor='lightgreen',
#                    linewidths=0.5,
#                    figsize=(10, 10)
#                    )
# 创建第一个图例
legend_kwargs_1 = {
    'fmt': '{:.0f}'
}

# 创建第二个图例
legend_kwargs_2 = {
    'loc': 'upper right',  # 图例位置设置为右上角
    'title': '挂牌量',  # 图例标题
    'title_fontsize': 8,  # 图例标题字体大小
    'fontsize': 8,  # 图例非标题外字体大小
    'shadow': True  # 添加图例阴影
}
ax = result_bystreet.plot(
    column="unit_price",
    cmap='RdYlGn_r',
    alpha = 0.8,
    legend=True,
    linewidth=0.5,
    edgecolor='k',
    scheme="natural_breaks",
    k=k,
    figsize=(10, 10),
    legend_kwds=legend_kwargs_1,
    missing_kwds={
        "facecolor": "lightgrey",
        "edgecolor": "white",
        "hatch": "-",
        "label": "Missing values",
    },
);

# 添加annotation
texts = []
for idx, row in result_bystreet.iterrows():
    centroid = row.geometry.centroid.coords[0]
    # if not math.isnan(row['unit_price']):
    text = ax.annotate(
        text=f"{row['name']}",
        xy=centroid,
        ha='center',
        fontsize=4,  # 设置字体大小
        color='black',  # 设置字体颜色为黑色
        weight='black',  # 设置字体粗细
        bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0),  boxstyle='round, pad=0.5'),  # 设置注释框样式
    )
    texts.append(text)
    # 检查注释是否重叠并调整位置
def check_and_adjust_annotations(texts, vertical_spacing=0.0001, horizontal_spacing=0.0001, min_fontsize=3, default_fontsize=4):
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
scheme = mc.Quantiles(gdf_second_hand_house['sell_cnt'], k=k)
# 获取分类的边界值
bounds = scheme.bins.tolist()
bounds.append(gdf_second_hand_house['sell_cnt'].max())  # 添加最大值作为边界值
legend_labels = [f"{bounds[i - 1]:.0f} - {bounds[i]:.0f}" for i in range(1, k+1)]
gplt.pointplot(df=gdf_second_hand_house,
               ax=ax, # 叠加图层
               s=1, # 散点大小
               linewidths=0.1, # 散点轮廓宽度
               hue='sell_cnt', # 以price作为色彩映射列
               cmap='winter', # 色彩方案为Reds
               scheme=scheme, # 传入mapclassify对象
               legend=True, # 开启图例
               legend_kwargs=legend_kwargs_2,
               legend_labels=legend_labels)

ax.axis('off')

plt.savefig('./houseinfo/test.png', dpi=500, bbox_inches='tight', pad_inches=0)    