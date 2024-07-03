
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

""" 
上海区域地图数据：https://geo.datav.aliyun.com/areas_v3/bound/310000_full.json
"""


class HouseInfoCrawler:
    def get_house_info(self):
        url = (
            "https://sh.fang.lianjia.com/loupan/nhs1pgpage_no/?_t=1"
        )
        houselist = []
        t = ToolKit("策略执行中")
        for i in range(1, 200):
            dict = {}
            list = []
            # time.sleep(0.5)
            url_re = (
                url.replace("page_no", str(i))
            )
            res = requests.get(url_re).text
            json_object = json.loads(res)
            t.progress_bar(200, i)
            if json_object['errno'] != 0:
                continue
            for h, j in enumerate(json_object['data']['list']):
                if not j['title']:
                    continue
                if j['title'] == '无':
                    continue
                if j['title'] in houselist:
                    continue
                if j['district'] not in ('崇明', '金山', '奉贤', '青浦', '松江', '嘉定', '闵行', '杨浦', '虹口', '宝山', '浦东', '普陀', '长宁', '黄浦', '徐汇', '静安'):
                    continue
                if j['house_type'] not in ('住宅','别墅'):
                    continue
                dict = {
                    "title": j['title'],
                    "district": j['district'],
                    "bizcircle_name": j['bizcircle_name'],
                    "avg_price": j['average_price'],
                    "sale_status": j['sale_status'],
                    "open_date": j['open_date'],
                    "longitude" : j['longitude'],
                    "latitude" : j['latitude'],
                    "tags" : j['tags'],
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

df_new_house = pd.read_csv('./test.csv', usecols=[
                           i for i in range(0, 10)])

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
    k=8,
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
adjust_text(texts, arrowprops=dict(arrowstyle="->", color='r', lw=0.5));

# 设置图像大小
# plt.figure(figsize=(12, 9))
plt.savefig('./map_bydistrict.png', dpi=500, bbox_inches='tight', pad_inches=0)
# 处理板块级别数据
data_filter_bystreet = data[(data.level == 'street') | (data.name.isin(['崇明', '金山', '黄浦', '静安', '虹口']))]
print(data_filter_bystreet)
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
    k=8,
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
adjust_text(texts, arrowprops=dict(arrowstyle="->", color='r', lw=0.5));


plt.savefig('./map_bystreet.png', dpi=500, bbox_inches='tight', pad_inches=0)

cm = sns.color_palette("coolwarm", as_cmap=True)
df_new_house.sort_values(by=["district", "bizcircle_name", "avg_price"],
                    ascending=[True, True, False], inplace=True)
df_new_house.reset_index(drop=True, inplace=True)

html = (
    "<h2>New House List</h2>"  # 添加标题
    "<table>"
    + df_new_house.style.hide(axis=1, subset=["longitude", "latitude", "index"])
    .background_gradient(subset=["avg_price"], cmap=cm)
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
                    ("font-size", "35px"),  # 在PC端使用较大字体
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
                        "35px",
                    ),  # 同样适用较大字体以提高移动端可读性
                ],
            ),
        ]
    )
    .set_table_styles({
        "title": [
            {
                "selector": "td",
                "props": [
                    # ("white-space", "nowrap"),
                ],
            },
        ]
    })
    .set_sticky(axis="columns")
    .to_html(doctype_html=True, escape=False)
    + "</table>"
)
# css = """
#     <style>
#         html, body {
#             margin: 0;
#             padding: 0;
#             width: 100%;
#             height: 100%;
#             overflow-x: hidden;  # 禁止水平滚动
#         }
#         body {
#             display: flex;
#             justify-content: center;
#             align-items: center;
#             background-color: white;
#             color: black;
#         }
#         table {
#             width: 100%;  # 确保表格宽度为100%
#             border-collapse: collapse;
#         }
#     </style>
# """

# html = css + html

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
    "./map_bydistrict.png",
    "./map_bystreet.png"
]
MyEmail().send_email_embedded_image(
    '上海新房信息跟踪',  html_img + html, image_path
)

