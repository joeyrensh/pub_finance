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
gdf_second_hand_house = gdf_second_hand_house[gdf_second_hand_house['sell_cnt'] >0]
# 房屋经纬度与地理围栏数据关联
data_filter_bydistrict = geo_data[geo_data.level == 'district']
gdf_merged_bydistrict = gpd.sjoin(data_filter_bydistrict, gdf_second_hand_house, how="inner", predicate = "intersects")

# 行政区均价分析
filtered_data = gdf_merged_bydistrict[gdf_merged_bydistrict['unit_price'] > 0]
agg_bydistrict = filtered_data.groupby('adcode').median({'unit_price': 'unit_price'}).round(-2)
result_bydistrict = data_filter_bydistrict.merge(agg_bydistrict, how='left', left_on='adcode', right_on ='adcode')



exclude_values = [310104, 310101, 310106, 310109, 310105, 310110, 310107]  # 要排除的值的列表            ]
data_filter_bystreet = geo_data[
    ((geo_data.level == 'district') & (geo_data.adcode.isin(exclude_values))) |
    ((geo_data.level == 'town') & (geo_data['parent'].apply(lambda x: json.loads(x)['adcode']).isin(exclude_values) == False))
]

gdf_merged_bystreet = gpd.sjoin(data_filter_bystreet, gdf_second_hand_house, how="inner", predicate = "intersects")
# 板块均价分析
filtered_data = gdf_merged_bystreet[gdf_merged_bystreet['unit_price'] > 0]
agg_bystreet = filtered_data.groupby('adcode').median({'unit_price': 'unit_price'}).round(-2)
result_bystreet = data_filter_bystreet.merge(agg_bystreet, how='left', left_on='adcode', right_on ='adcode')

k = 8


ax = gplt.polyplot(df=geo_data,
                   projection=gcrs.AlbersEqualArea(),
                   edgecolor='lightgrey',
                   facecolor='#00003a',
                   linewidths=0.5,
                   figsize=(10, 10)
                   )



scheme = mc.Quantiles(gdf_second_hand_house['sell_cnt'], k=k)
# 获取分类的边界值
bounds = scheme.bins.tolist()
bounds.append(gdf_second_hand_house['sell_cnt'].max())  # 添加最大值作为边界值

legend_labels = [f"{bounds[i - 1]:.2f} - {bounds[i]:.2f}" for i in range(1, k+1)]


gplt.pointplot(df=gdf_second_hand_house,
               ax=ax, # 叠加图层
               s=1, # 散点大小
               linewidths=0.1, # 散点轮廓宽度
               hue='sell_cnt', # 以price作为色彩映射列
               cmap='Reds_r', # 色彩方案为Reds
               scheme=scheme, # 传入mapclassify对象
               legend=True, # 开启图例
               legend_kwargs={
                   'loc': 'upper right', # 图例位置
                   'title': '价格区间', # 图例标题
                   'title_fontsize': 8, # 图例标题字体大小
                   'fontsize': 8, # 图例非标题外字体大小
                   'shadow': True, # 添加图例阴影
               },
               legend_labels=legend_labels)
ax.axis('off')
