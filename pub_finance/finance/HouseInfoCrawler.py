
import requests
import json
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import math
from adjustText import adjust_text
from utility.MyEmail import MyEmail
import seaborn as sns
from utility.ToolKit import ToolKit
from matplotlib.transforms import Bbox
import os
import contextily as cx


""" 
上海区域地图数据：https://geo.datav.aliyun.com/areas_v3/bound/310000_full.json
链家数据：https://sh.fang.lianjia.com/loupan/minhang/nht1nht2nhs1pg1/?_t=1/
huangpu-xuhui-changning-jingan-putuo-hongkou-yangpu-minhang-baoshan-jiading-pudong-jinshan-songjiang-qingpu-fengxian-chongming

"""


class HouseInfoCrawler:
    def get_house_info(self):

   
        houselist = []
        filename = './houseinfo/newhouse.csv'

        # 如果文件存在，则删除文件
        if os.path.isfile(filename):
            os.remove(filename)
        district_list = ['huangpu','xuhui','changning','jingan','putuo','hongkou','yangpu','minhang','baoshan','jiading','pudong','jinshan','songjiang','qingpu','fengxian','chongming']
        cnt = 0
        t = ToolKit("策略执行中")
        for idx, district in enumerate(district_list):
            t.progress_bar(len(district_list), idx)
            
            for i in range(1, 10):
                dict = {}
                list = []
                url = (
                    "https://sh.fang.lianjia.com/loupan/district/nht1nht2nhs1pgpageno/?_t=1/"
                )                
                url_re = (
                    url.replace("pageno", str(i)).replace("district", district)
                )
                res = requests.get(url_re).json()
                if len(res['data']['list']) == 0:
                    continue
                for h, j in enumerate(res['data']['list']):
                    if not j['longitude']:
                        continue
                    if not j['latitude']:
                        continue
                    if "".join([j['title'], j['house_type']]) in houselist:
                        continue
                    dict = {
                        "title": j['title'],
                        "house_type": j['house_type'],
                        "district": j['district'],
                        "bizcircle_name": j['bizcircle_name'],
                        "avg_price": j['average_price'],
                        "area_range": j['resblock_frame_area_range'],
                        "sale_status": j['sale_status'],
                        "open_date": j['open_date'],
                        "longitude" : j['longitude'],
                        "latitude" : j['latitude'],
                        "tags" : j['tags'],
                        "index": i
                    }
                    houselist.append("".join([j['title'], j['house_type']]))
                    list.append(dict)
                df = pd.DataFrame(list)
                df.to_csv(
                    filename,
                    mode="a",
                    index=False,
                    header=(cnt == 0))
                cnt = cnt + 1


hi = HouseInfoCrawler()
hi.get_house_info()
plt.rcParams['font.family'] = 'WenQuanYi Zen Hei'

path = './houseinfo/shanghaidistrict.json'

data = gpd.read_file(path)

df_new_house = pd.read_csv('./houseinfo/newhouse.csv', usecols=[
                           i for i in range(0, 12)])

gdf_new_house = gpd.GeoDataFrame(
    df_new_house, geometry=gpd.points_from_xy(df_new_house.longitude, df_new_house.latitude), crs="EPSG:4326"
)
# 处理行政区级别数据
data_filter_bydistrict = data[data.level == 'district']

gdf_merged_bydistrict = gpd.sjoin(data_filter_bydistrict, gdf_new_house, how="left", predicate = "intersects")


agg_bydistrict = gdf_merged_bydistrict.groupby('adcode').median({'avg_price': 'avg_price'}).round(-3)


result_bydistrict = data_filter_bydistrict.merge(agg_bydistrict, how='left', left_on='adcode', right_on ='adcode')

ax = result_bydistrict.plot(
    column="avg_price",
    cmap='RdYlGn_r',
    alpha = 0.6,
    legend=True,
    linewidth=0.8,
    edgecolor='gainsboro',
    scheme="natural_breaks",
    k=8,
    figsize=(15, 10),
    legend_kwds={"fmt": "{:.0f}"},
    missing_kwds={
        # "color": "lightgrey",
        "facecolor": "none",
        "edgecolor": "white",
        "hatch": "///",
        "label": "Missing values",
    },
);

cx.add_basemap(ax, crs="EPSG:4326",
                source='https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}')
# cx.add_basemap(ax, crs="EPSG:4326", source=cx.providers.OpenTopoMap)

ax.axis('off')

# 添加标题
ax.set_title('Shanghai New House Distribution', 
             fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})


texts = []
for idx, row in result_bydistrict.iterrows():
    centroid = row.geometry.centroid.coords[0]
    if not math.isnan(row['avg_price']):
        text = ax.annotate(
            text=f"{row['name']}\n{row['avg_price']:.0f}",
            xy=centroid,
            # xytext=offset,
            ha='center',
            fontsize=10,  # 设置字体大小
            color='black',  # 设置字体颜色为黑色
            weight='bold',  # 设置字体粗细
            bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0),  boxstyle='round, pad=0.5'),  # 设置注释框样式
        )
        texts.append(text)
# 检查注释是否重叠并调整位置
def check_and_adjust_annotations(texts, vertical_spacing=0.002):
    renderer = ax.get_figure().canvas.get_renderer()
    for i, text in enumerate(texts):
        rect1 = text.get_window_extent(renderer=renderer)
        for j in range(i + 1, len(texts)):
            text2 = texts[j]
            rect2 = text2.get_window_extent(renderer=renderer)
            while rect1.overlaps(rect2):
                x, y = text2.get_position()
                y -= vertical_spacing
                text2.set_position((x, y))
                rect2 = text2.get_window_extent(renderer=renderer)

check_and_adjust_annotations(texts)        

plt.savefig('./houseinfo/map_bydistrict.png', dpi=500, bbox_inches='tight', pad_inches=0)
# 处理板块级别数据
data_filter_bystreet = data[(data.level == 'town')]
# | (data.name.isin(['崇明', '金山', '黄浦', '静安', '虹口']))]
gdf_merged_bystreet = gpd.sjoin(data_filter_bystreet, gdf_new_house, how="left", predicate = "intersects")

agg_bystreet = gdf_merged_bystreet.groupby('adcode').median({'avg_price': 'avg_price'}).round(-3)

result_bystreet = data_filter_bystreet.merge(agg_bystreet, how='left', left_on='adcode', right_on ='adcode')

ax = result_bystreet.plot(
    column="avg_price",
    cmap='RdYlGn_r',
    alpha = 0.7,
    legend=True,
    linewidth=0.8,
    edgecolor='gainsboro',
    scheme="natural_breaks",
    k=8,
    figsize=(15, 10),
    legend_kwds={"fmt": "{:.0f}"},
    missing_kwds={
        # "color": (0, 0, 0, 0),
        "facecolor": "none",
        "edgecolor": "white",
        "hatch": "///",
        "label": "Missing values",
    },
);
cx.add_basemap(ax, crs="EPSG:4326",
                source='https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}')
ax.axis('off')
# 添加标题
ax.set_title('Shanghai New House Distribution', 
             fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})

texts = []
for idx, row in result_bystreet.iterrows():
    centroid = row.geometry.centroid.coords[0]
    if not math.isnan(row['avg_price']):
        text = ax.annotate(
            text=f"{row['name']}\n{row['avg_price']:.0f}",
            xy=centroid,
            # xytext=offset,
            ha='center',
            fontsize=5,  # 设置字体大小
            color='black',  # 设置字体颜色为黑色
            weight='bold',  # 设置字体粗细
            bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0), boxstyle='round, pad=0.5'),  # 设置注释框样式
        )
        texts.append(text)
# 检查注释是否重叠并调整位置
def check_and_adjust_annotations(texts, vertical_spacing=0.002):
    renderer = ax.get_figure().canvas.get_renderer()
    for i, text in enumerate(texts):
        rect1 = text.get_window_extent(renderer=renderer)
        for j in range(i + 1, len(texts)):
            text2 = texts[j]
            rect2 = text2.get_window_extent(renderer=renderer)
            while rect1.overlaps(rect2):
                x, y = text2.get_position()
                y -= vertical_spacing
                text2.set_position((x, y))
                rect2 = text2.get_window_extent(renderer=renderer)

check_and_adjust_annotations(texts)

plt.savefig('./houseinfo/map_bystreet.png', dpi=500, bbox_inches='tight', pad_inches=0)

cm = sns.color_palette("coolwarm", as_cmap=True)


new_house_info = gdf_merged_bystreet[['title','house_type','district','bizcircle_name','name','avg_price','area_range','sale_status','open_date']].dropna(subset = ['title'])

new_house_info.sort_values(by=["district", "bizcircle_name", "avg_price"],
                    ascending=[True, True, False], inplace=True)
new_house_info.reset_index(drop=True, inplace=True)

html = (
    "<h2>New House List</h2>"  # 添加标题
    "<table>"
    + new_house_info.style
    .background_gradient(subset=["avg_price"], cmap=cm)
    .format(
        {
            "avg_price": "{:.0f}"
        }
    )    
    .set_properties(
        **{
            "text-align": "left",
            "border": "1px solid #ccc",
            "cellspacing": "0",
            "style": "border-collapse: collapse; width: 100%;",  # 设置表格宽度为100%
        }
    )
    .set_table_styles(
        [
            # 表头样式
            dict(
                selector="th",
                props=[
                    ("border", "1px solid #ccc"),
                    ("text-align", "left"),
                    ("padding", "8px"),  # 增加填充以便更易点击和阅读
                    ("font-size", "18px"),  # 在PC端使用较大字体
                ],
            ),
            # 表格数据单元格样式
            dict(
                selector="td",
                props=[
                    ("border", "1px solid #ccc"),
                    ("text-align", "left"),
                    ("padding", "8px"),
                    (
                        "font-size",
                        "18px",
                    ),  # 同样适用较大字体以提高移动端可读性
                ],
            ),
        ]
    )
    .set_table_styles(
        {
            "title": [
                {
                    "selector": "th",
                    "props": [
                        ("min-width", "150px"),
                        ("max-width", "300px"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("min-width", "150px"),
                        ("max-width", "300px"),
                    ],
                },
            ]
        },
        overwrite=False,
    )
    .set_sticky(axis="columns")
    .to_html(doctype_html=True, escape=False)
    + "<table>"
)
css = """
    <style>
        :root {
            color-scheme: light;
            supported-color-schemes: light;
            background-color: white;
            color: black;
            display: table ;
        }                
        /* Your light mode (default) styles: */
        body {
            background-color: white;
            color: black;
            display: table ;
            width: 100%;                          
        }
        table {
            background-color: white;
            color: black;
            width: 100%;
        }
    </style>
"""

html = css + html

html_img = """
        <html>
            <head>
                <style>
                    body {
                        font-family: Arial, sans-serif;
                        background-color: white; /* 设置默认背景颜色为白色 */
                        color: black; /* 设置默认字体颜色为黑色 */
                    }

                    figure {
                        margin: 0;
                        padding: 5px;
                        border-radius: 8px;
                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                    }

                    img {
                        width: 100%;
                        height: auto;
                        border-radius: 2px;
                    }

                    figcaption {
                        padding: 5px;
                        text-align: center;
                        font-style: italic;
                        font-size: 14px;
                    }
                    
                    body {
                        background-color: white;
                        color: black;
                    }

                    figure {
                        border: 1px solid #ddd;
                    }
                    

                </style>
            </head>
            <body>
                <picture>
                    <!-- 默认模式下的图片 -->
                    <img src="cid:image0" alt="The industry distribution of current positions is as follows:" style="width:100%">
                    <figcaption> Shanghai new house distribution by district.</figcaption>
                </picture>
                <picture>
                    <!-- 默认模式下的图片 -->
                    <img src="cid:image1" alt="The industry distribution of current pnl is as follows:" style="width:100%">
                    <figcaption>Shanghai new house distribution by street.</figcaption>
                </picture>
            </body>
        </html>
        """
image_path = [
    "./houseinfo/map_bydistrict.png",
    "./houseinfo/map_bystreet.png"
]
MyEmail().send_email_embedded_image(
    '上海新房信息跟踪',  html_img + html, image_path
)

