import geopandas as gpd
from shapely.geometry import MultiPolygon
import pandas as pd
from shapely.ops import unary_union
from xyzservices import TileProvider
import contextily as cx
import xyzservices.providers as xyz
import json
import math
import matplotlib.pyplot as plt
import re

# 定义自定义 TileProvider
# xyz.CartoDB.Positron.url = 'https://{s}.basemaps.cartocdn.com/{variant}/{z}/{x}/{y}{r}.png'
# xyz.CartoDB.Positron.attribution = '(C) OpenStreetMap contributors (C) CARTO'
# # 读取 Shapefile
# # gdf = gpd.read_file("path/to/your/file.shp")


# # gdf.to_file('test4.json', driver='GeoJSON')
# ax = gdf.plot()
# cx.add_basemap(ax, 
#                crs="EPSG:4326",
#                 source = xyz.CartoDB.Positron,
#                 zoom = 12
#                 )


# geo_path = './OSMB-743692f42cf1d64dddced1486876337993217878.geojson'
# gdf = gpd.read_file(geo_path,  engine="pyogrio") 
# gdf.to_file('test4.json', driver='GeoJSON')
# gdf.plot()



# gdf_waigaoqiao = gpd.read_file('./test.json',  engine="pyogrio") #waigaoqiao
# gdf_gaohang = gpd.read_file('./test1.json',  engine="pyogrio") #gaohang
# gdf_tangzhen = gpd.read_file('./test2.json',  engine="pyogrio") #tangzhen
# gdf_chuansha = gpd.read_file('./test3.json',  engine="pyogrio") #chuansha
# # 提取 MultiPolygon 几何
# multipolygon_waigaoqiao = gdf_waigaoqiao.unary_union
# multipolygon_gaohang = gdf_gaohang.unary_union
# multipolygon_tangzhen = gdf_tangzhen.unary_union
# multipolygon_chuansha = gdf_chuansha.unary_union

# gdf_inter = multipolygon_waigaoqiao.intersection(multipolygon_chuansha)
# # gaoqiao
# merged_multipolygon = multipolygon_waigaoqiao.difference(gdf_inter)
# # merged_multipolygon = unary_union([multipolygon, gdf_inter])

# # 合并两个 MultiPolygon
# # merged_multipolygon = union_all([multipolygon, multipolygon1])
# # merged_multipolygon = multipolygon.difference(multipolygon3)
# # merged_multipolygon2 = merged_multipolygon1.difference(multipolygon3)

# # 创建一个新的 GeoDataFrame
# merged_gdf = gpd.GeoDataFrame(geometry=[merged_multipolygon])

# merged_gdf.plot()


# # 保存为新的 GeoJSON 文件
# merged_gdf.to_file('test4.json', driver='GeoJSON')
plt.rcParams['font.family'] = 'WenQuanYi Zen Hei'
geo_path = './houseinfo/shanghaidistrict.json'
file_path = './houseinfo/secondhandhouse.csv'
geo_data = gpd.read_file(geo_path,  engine="pyogrio")
# gdf_second_hand_house = pd.read_csv('./houseinfo/secondhandhouse.csv')
# print(df.groupby('district')['sell_cnt'].sum())
df_second_hand_house = pd.read_csv(file_path, usecols=[i for i in range(0, 10)])
df_second_hand_house = df_second_hand_house.dropna(subset=['lanlong'])

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
# 处理行政区级别数据
# print(gdf_second_hand_house)
    # 处理行政区级别数据
data_filter_bydistrict = geo_data[geo_data.level == 'district']
gdf_merged_bydistrict = gpd.sjoin(data_filter_bydistrict, gdf_second_hand_house, how="inner", predicate = "intersects")
agg_bydistrict = gdf_merged_bydistrict.groupby('adcode')[['sell_cnt', 'total_cnt']].sum().round(-2)
# 计算 sell_cnt / total_cnt 比例，但仅在 total_cnt 不为 0 的情况下
agg_bydistrict['ratio'] = agg_bydistrict.apply(
    lambda row: row['sell_cnt'] / row['total_cnt'] if row['total_cnt'] != 0 else None, axis=1
)
result_bydistrict = data_filter_bydistrict.merge(agg_bydistrict, how='left', left_on='adcode', right_on ='adcode')

ax = result_bydistrict.plot(
    column="sell_cnt",
    cmap='YlOrRd',
    alpha = 0.5,
    legend=True,
    linewidth=0.5,
    edgecolor='k',
    scheme="natural_breaks",
    k=8,
    figsize=(10, 20),
    legend_kwds={"fmt": "{:.2%}"},
    # missing_kwds={
    #     # "color": "lightgrey",
    #     "facecolor": "none",
    #     "edgecolor": "white",
    #     "hatch": "///",
    #     "label": "Missing values",
    # },
);

cx.add_basemap(ax, 
                crs="EPSG:4326",
                source='https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
                # source='http://webrd01.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=7&x={x}&y={y}&z={z}',
                # source = xyz.CartoDB.PositronNoLabels,
                zoom = 10,
                interpolation = 'bicubic',
                # alpha = 0.5
                )

ax.axis('off')
# 添加标题
ax.set_title('Shanghai New House Distribution', 
            fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})

# 添加annotation
texts = []
for idx, row in result_bydistrict.iterrows():
    centroid = row.geometry.centroid.coords[0]
    if not math.isnan(row['ratio']):
        text = ax.annotate(
            text=f"{row['name']}\n{row['sell_cnt']:.0f}|{row['ratio']:.2%}",
            xy=centroid,
            ha='center',
            fontsize=6,  # 设置字体大小
            color='black',  # 设置字体颜色为黑色
            weight='black',  # 设置字体粗细
            bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0),  boxstyle='round, pad=0.5'),  # 设置注释框样式
        )
        texts.append(text)
# 检查注释是否重叠并调整位置
def check_and_adjust_annotations(texts, vertical_spacing=0.0001, horizontal_spacing=0.0001, min_fontsize=4, default_fontsize=6):
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

plt.savefig('./test1.png', dpi=500, bbox_inches='tight', pad_inches=0)

# 处理板块级别数据
exclude_values = [310104, 310101, 310106, 310109, 310105, 310110, 310107]  # 要排除的值的列表
data_filter_bystreet = geo_data[
    ((geo_data.level == 'district') & (geo_data.adcode.isin(exclude_values))) |
    ((geo_data.level == 'town') & (geo_data['parent'].apply(lambda x: json.loads(x)['adcode']).isin(exclude_values) == False))
]

gdf_merged_bystreet = gpd.sjoin(data_filter_bystreet, gdf_second_hand_house, how="inner", predicate = "intersects")
filtered_data = gdf_merged_bystreet[gdf_merged_bystreet['sell_cnt'] > 0]
# 按 adcode 分组并汇总 sell_cnt 和 total_cnt
# agg_bystreet = filtered_data.groupby('adcode').sum([{'sell_cnt' : 'sell_cnt'}, {'total_cnt' : 'total_cnt'}]).round(-2)
agg_bystreet = filtered_data.groupby('adcode')[['sell_cnt', 'total_cnt']].sum().round(-2)
# 计算 sell_cnt / total_cnt 比例，但仅在 total_cnt 不为 0 的情况下
agg_bystreet['ratio'] = agg_bystreet.apply(
    lambda row: row['sell_cnt'] / row['total_cnt'] if row['total_cnt'] != 0 else None, axis=1
)


result_bystreet = data_filter_bystreet.merge(agg_bystreet, how='left', left_on='adcode', right_on ='adcode')

ax = result_bystreet.plot(
    column="sell_cnt",
    cmap='YlOrRd',
    alpha = 0.5,
    legend=True,
    linewidth=0.5,
    # edgecolor='gainsboro',
    edgecolor='k',
    scheme="natural_breaks",
    k=8,
    figsize=(10, 20),
    legend_kwds={"fmt": "{:.2%}"},
    # missing_kwds={
    #     # "color": (0, 0, 0, 0),
    #     "facecolor": "none",
    #     "edgecolor": "white",
    #     "hatch": "///",
    #     "label": "Missing values",
    # },
);
cx.add_basemap(ax,
            crs="EPSG:4326",
            source='https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
            # source = 'http://webrd01.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=7&x={x}&y={y}&z={z}',
            # source = xyz.CartoDB.PositronNoLabels,
            zoom = 10,
            interpolation = 'bicubic',
            # alpha = 0.5
            )
ax.axis('off')
# 添加标题
ax.set_title('Shanghai Second House Distribution', 
            fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})

# 添加annotation
texts = []
for idx, row in result_bystreet.iterrows():
    centroid = row.geometry.centroid.coords[0]
    if not math.isnan(row['ratio']):
        text = ax.annotate(
            text=f"{row['name']}\n{row['sell_cnt']:.0f}|{row['ratio']:.2%}",
            xy=centroid,
            ha='center',
            fontsize=4,  # 设置字体大小
            color='black',  # 设置字体颜色为黑色
            weight='black',  # 设置字体粗细
            bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0), boxstyle='round, pad=0.5'),  # 设置注释框样式
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

plt.savefig('./test.png', dpi=500, bbox_inches='tight', pad_inches=0)