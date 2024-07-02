
import requests
import json
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import math
from adjustText import adjust_text
from utility.MyEmail import MyEmail


""" 
上海区域地图数据：https://geo.datav.aliyun.com/areas_v3/bound/310000_full.json
"""


class HouseInfoCrawler:
    def get_house_info(self):
        url = (
            "https://sh.fang.lianjia.com/loupan/nhs1pgpage_no/?_t=1"
        )
        houselist = []
        for i in range(1, 200):
            dict = {}
            list = []
            # time.sleep(0.5)
            url_re = (
                url.replace("page_no", str(i))
            )
            res = requests.get(url_re).text
            json_object = json.loads(res)
            print('i:', i)
            if json_object['errno'] != 0:
                continue
            for h, j in enumerate(json_object['data']['list']):
                if not j['title']:
                    continue
                if j['title'] == '无':
                    continue
                if j['title'] in houselist:
                    continue
                if j['district'] not in ('崇明', '金山', '奉贤', '青浦', '松江', '嘉定', '闵行', '杨浦', '虹口', '宝山', '浦东', '普陀', '长宁', '黄浦', '徐汇'):
                    continue
                if j['house_type'] != '住宅':
                    continue
                dict = {
                    "name": j['title'],
                    "district": j['district'],
                    "bizcircle_name": j['bizcircle_name'],
                    "avg_price": j['average_price'],
                    "sale_status": j['sale_status'],
                    "open_date": j['open_date'],
                    "longitude" : j['longitude'],
                    "latitude" : j['latitude'],
                    "index": i
                }
                houselist.append(j['title'])
                list.append(dict)

            df = pd.DataFrame(list)
            df.to_csv(
                './test.csv',
                mode="a",
                index=False,
                header=(i == 1))


# hi = HouseInfoCrawler()
# hi.get_house_info()
plt.rcParams['font.family'] = 'WenQuanYi Zen Hei'

path = './shanghaidistrict.json'

data = gpd.read_file(path)

df_new_house = pd.read_csv('test.csv', usecols=[
                           i for i in range(1, 9)])
gdf_new_house = gpd.GeoDataFrame(
    df_new_house, geometry=gpd.points_from_xy(df_new_house.longitude, df_new_house.latitude), crs="EPSG:4326"
)
# 处理行政区级别数据
data_filter_bydistrict = data[data.level == 'district']

gdf_merged_bydistrict = gpd.sjoin(data_filter_bydistrict, gdf_new_house, how="left", op="intersects")

agg_bydistrict = gdf_merged_bydistrict.groupby('name')['avg_price'].mean().round(-3)


result_bydistrict = data_filter_bydistrict.merge(agg_bydistrict, how='left', left_on='name', right_on ='name')

ax = result_bydistrict.plot(
    column="avg_price",
    cmap='RdYlGn_r',
    legend=True,
    linewidth=0.8,
    # edgecolor='0.8',
    edgecolor='gainsboro',
    scheme="natural_breaks",
    # k = 8,
    figsize=(15, 10),
    missing_kwds={
        "color": "lightgrey",
        "edgecolor": "white",
        "hatch": "///",
        "label": "Missing values",
    },
);
ax.axis('off')
# 添加标题
ax.set_title('Shanghai New House Distribution', 
             fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})


texts = []
for idx, row in result_bydistrict.iterrows():
    centroid = row.geometry.centroid.coords[0]
    if not math.isnan(row['avg_price']):
        text = ax.annotate(
            text=f"{row['name']}\n{row['avg_price']}",
            xy=centroid,
            # xytext=offset,
            ha='center',
            fontsize=10,  # 设置字体大小
            color='black',  # 设置字体颜色为黑色
            weight='bold',  # 设置字体粗细
            bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0), boxstyle='round, pad=0.5'),  # 设置注释框样式
        )
        texts.append(text)

# 调整注释位置以避免重叠
adjust_text(texts, arrowprops=dict(arrowstyle="->", color='r', lw=0.5), only_move='x-');


plt.savefig('./map_bydistrict.png', dpi=300)
# 处理板块级别数据
data_filter_bystreet = data[data.level == 'street']

gdf_merged_bystreet = gpd.sjoin(data_filter_bystreet, gdf_new_house, how="left", op="intersects")

agg_bystreet = gdf_merged_bystreet.groupby('name')['avg_price'].mean().round(-3)


result_bystreet = data_filter_bystreet.merge(agg_bystreet, how='left', left_on='name', right_on ='name')


ax = result_bystreet.plot(
    column="avg_price",
    cmap='RdYlGn_r',
    legend=True,
    linewidth=0.8,
    # edgecolor='0.8',
    edgecolor='gainsboro',
    scheme="natural_breaks",
    # k = 8,
    figsize=(15, 10),
    missing_kwds={
        "color": "lightgrey",
        "edgecolor": "white",
        "hatch": "///",
        "label": "Missing values",
    },
);
ax.axis('off')
# 添加标题
ax.set_title('Shanghai New House Distribution', 
             fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})


texts = []
for idx, row in result_bystreet.iterrows():
    centroid = row.geometry.centroid.coords[0]
    if not math.isnan(row['avg_price']):
        text = ax.annotate(
            text=f"{row['name']}\n{row['avg_price']}",
            xy=centroid,
            # xytext=offset,
            ha='center',
            fontsize=10,  # 设置字体大小
            color='black',  # 设置字体颜色为黑色
            weight='bold',  # 设置字体粗细
            bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0), boxstyle='round, pad=0.5'),  # 设置注释框样式
        )
        texts.append(text)

# 调整注释位置以避免重叠
adjust_text(texts, arrowprops=dict(arrowstyle="->", color='r', lw=0.5), only_move='x-');


plt.savefig('./map_bystreet.png', dpi=300)




