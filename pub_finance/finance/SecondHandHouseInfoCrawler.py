import requests
from lxml import html
import re
import pandas as pd
from utility.ToolKit import ToolKit
import concurrent.futures
import json
import geopandas as gpd
import matplotlib.pyplot as plt
import math
from utility.MyEmail import MyEmail
import seaborn as sns
from matplotlib.transforms import Bbox
import os
import contextily as cx


def fetch_houselist(url, page):
    # 添加请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Cookie': 'lianjia_uuid=9bb8fccc-9ed4-4f5d-ac1c-decaebb51169; _smt_uid=66682e43.4b7ae654; _jzqc=1; _qzjc=1; _ga=GA1.2.1669893493.1718103624; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1718166241; crosSdkDT2019DeviceId=uik048-qg82dj-s69ornrx4lw5n96-m70vmzepz; _jzqx=1.1718103621.1719886293.3.jzqsr=google%2Ecom%2Ehk|jzqct=/.jzqsr=google%2Ecom%2Ehk|jzqct=/; _ga_34Q2BG9VYB=GS1.2.1720166985.1.1.1720166989.0.0.0; HMACCOUNT=5E4F5FB17861AFE8; select_city=310000; _jzqckmp=1; _gid=GA1.2.1156257387.1720495740; _ga_00MKBBEWEN=GS1.2.1720495741.16.1.1720495800.0.0.0; GUARANTEE_POPUP_SHOW=true; GUARANTEE_BANNER_SHOW=true; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219006f4b96399d-07f09aa9e418ea-1a525637-1930176-19006f4b9642836%22%2C%22%24device_id%22%3A%2219006f4b96399d-07f09aa9e418ea-1a525637-1930176-19006f4b9642836%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; lianjia_ssid=86e9641a-701e-4911-9fee-869459536e79; _jzqa=1.1115327711180168800.1718103621.1720548541.1720578120.19; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1720578251; srcid=eyJ0Ijoie1wiZGF0YVwiOlwiNGY5MWNhZWU5YmFmODk0MzY2Mjk5ZDkxMjdmYTIyODU5MWM4M2U1ZTc3YjczYmRiNTU3MGFjMjVmMjBhYzBjYzA0MDkwZTNhMmM3NDAzYjBkZWRjNTBmZTk5NjMxYWExYzkxZjU1YWYzMDAzZmE0YjdmMDgxMzcwNzhlYTdmMmMzMTViNjNlYzc2NzZlYWY1NzlkNzlhZTEzOGQ4MmEwYjcwZTI2YmNkYWJlMTY2MjI4ZmEzODIyNWU3NTA4ODA4ZTAzNzQ5ZmFhYjQ0OWQ0MzA1NTFmZmZiNDhiMWI5ZGQ3MTBkMzcwMGI5MjBmZWU0YzM3Mzk3ZTU5NTcwZmU0YlwiLFwia2V5X2lkXCI6XCIxXCIsXCJzaWduXCI6XCJmZDVlYWE0N1wifSIsInIiOiJodHRwczovL3NoLmxpYW5qaWEuY29tL3hpYW9xdS9jaG9uZ21pbmcvcGcxLyIsIm9zIjoid2ViIiwidiI6IjAuMSJ9; _gat=1; _gat_past=1; _gat_global=1; _gat_new_global=1; _gat_dianpu_agent=1; _ga_GVYN2J1PCG=GS1.2.1720578122.13.1.1720578255.0.0.0; _ga_LRLL77SF11=GS1.2.1720578122.13.1.1720578255.0.0.0; _qzja=1.1984203958.1718103621059.1720548541230.1720578120092.1720578251169.1720578282145.0.0.0.117.19; _qzjb=1.1720578120092.3.0.0.0; _qzjto=48.2.0; _jzqb=1.3.10.1720578120.1'
    }
    datalist=[]
    list = []
    for i in range(1, page):
        url_re = (url.replace('pgno', str(i)))
        response = requests.get(url_re, headers=headers)
        print(url_re)

        # 检查请求是否成功
        if response.status_code == 200:
            # 使用lxml解析HTML
            tree = html.fromstring(response.content)
            
            # 查找特定的div
            # 假设我们要查找class为'target-div'的div
            divs = tree.xpath('//div[@class="content"]/div[@class="leftContent"]/ul[@class="listContent"]/li[@class="clear xiaoquListItem"]')
            
            if len(divs) > 0:
                for div in divs:
                    dict = {}
                    # 获取data-id属性的值
                    data_id = div.get('data-id')
                    if data_id in datalist:
                        continue                    
                    # 查找<li>标签下的<img>标签，并获取alt属性的值
                    img_tag = div.xpath('.//img[@class="lj-lazy"]')
                    if img_tag:
                        alt_text = img_tag[0].get('alt')
                    else:
                        continue
                    sell_cnt_div = div.xpath('.//div[@class="xiaoquListItemRight"]/div[@class="xiaoquListItemSellCount"]/a/span')
                    if sell_cnt_div:
                        sell_cnt = sell_cnt_div[0].xpath('string(.)')
                    else:
                        sell_cnt = ''
                    
                    dict = {
                        'data_id': data_id,
                        'al_text': alt_text,
                        'sell_cnt': sell_cnt
                    }
                    list.append(dict)
                    datalist.append(data_id)                   
            else:
                print("未找到目标<ul>标签")
        else:
            print("请求失败，状态码:", response.status_code)

    return list

def fetch_house_info(url, item):

    # 添加请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }    

    dict = {}
    url_re = (url.replace("data_id", item['data_id']))
    response = requests.get(url_re, headers=headers)
    # print(url_re)
    tree = html.fromstring(response.content)
    div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquPrice clear"]/div[@class="fl"]/span[@class="xiaoquUnitPrice"]')
    if len(div) > 0:
        unit_price = div[0].xpath('string(.)')
    else:
        unit_price = ''
    div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemOneLine"]/div[@class="xiaoquInfoItem outerItem"][2]/span[@class="xiaoquInfoContent outer"]/span[@mendian]')
    if len(div) > 0:
        lanlong = div[0].get('xiaoqu')
    else:
        lanlong = ''
    div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][2]/div[@class="xiaoquInfoItem"][2]/span[@class="xiaoquInfoContent"]')
    if len(div) > 0:
        age = div[0].xpath('string(.)')
    else:
        age = ''
    div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][1]/div[@class="xiaoquInfoItem"][2]/span[@class="xiaoquInfoContent"]')
    if len(div) > 0:
        total_cnt = div[0].xpath('string(.)')
    else:
        total_cnt = ''
    

    dict = {
        'data_id': item['data_id'],
        'al_text': item['al_text'],
        'sell_cnt': item['sell_cnt'],
        'unit_price': unit_price,
        'lanlong': lanlong,
        'age': age,
        'total_cnt': total_cnt
    }

    return dict

# 主程序入口
if __name__ == "__main__":
    
    # filename = './houseinfo/test.csv'
    # # 如果文件存在，则删除文件
    # if os.path.isfile(filename):
    #     os.remove(filename)
    # # 发起HTTP请求
    # # url = 'https://sh.lianjia.com/xiaoqu/minhang/pg1/'  # 替换为目标URL
    # # district_list = ['huangpu','xuhui','changning','jingan','putuo','hongkou','yangpu','minhang','baoshan','jiading','pudong','jinshan','songjiang','qingpu','fengxian','chongming']
    # district_list = ['chongming']
    # page_no = 20
    
    # t = ToolKit("列表生成")
    # houselist = []
    # complete_list = []
    # for idx, district in enumerate(district_list):
    #     url = ("https://sh.lianjia.com/xiaoqu/district/pgpgno/")        
    #     url_re = (
    #         url.replace("district", district)
    #     )
    #     houselist = fetch_houselist(url_re, page_no)
    #     complete_list.extend(houselist) 
    #     t.progress_bar(len(district_list), idx + 1)
    # print('complete list cnt is: ', len(complete_list))


    # # 设置并发数上限为6
    # max_workers = 1
    # data_batch_size = 10
    # list = []
    # count = 0
    # url_detail = 'https://sh.lianjia.com/xiaoqu/data_id/'
    # t = ToolKit("信息爬取")
    # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     # 提交任务并获取Future对象列表
    #     futures = [executor.submit(fetch_house_info, url_detail, item) for item in complete_list]
        
    #     # 获取已完成的任务的结果
    #     for future in concurrent.futures.as_completed(futures):
    #         try:
    #             result = future.result()
    #             list.append(result)
    #         except Exception as e:
    #             print(f"获取详细信息时出错: {e}")
        
    #         count += 1
    #         if count % data_batch_size == 0:
    #             # 每处理完一批数据后，将数据写入CSV文件
    #             df = pd.DataFrame(list)
    #             df.to_csv(
    #                 filename,
    #                 mode="a",
    #                 index=False,
    #                 header=(count == data_batch_size)
    #             )
    #             list = []  # 清空列表以继续下一批数据的处理

    # # 处理剩余的数据
    # if list:
    #     df = pd.DataFrame(list)
    #     df.to_csv(
    #         filename,
    #         mode="a",
    #         index=False,
    #         header=False
    #     )


    plt.rcParams['font.family'] = 'WenQuanYi Zen Hei'

    path = './houseinfo/shanghaidistrict.json'

    data = gpd.read_file(path)

    df_second_hand_house = pd.read_csv('./houseinfo/secondhandhouseinfo.csv', usecols=[
                            i for i in range(0, 7)])
    def extract_values(string):
        values = string.strip("[]").split(",")
        value1 = float(values[0])
        value2 = float(values[1])
        return value1, value2
    df_second_hand_house[['longitude', 'latitude']] = df_second_hand_house['lanlong'].apply(extract_values).apply(pd.Series)

    gdf_second_hand_house = gpd.GeoDataFrame(
        df_second_hand_house, geometry=gpd.points_from_xy(df_second_hand_house['longitude'], df_second_hand_house['latitude']), crs="EPSG:4326"
    )
    # 处理行政区级别数据
    data_filter_bydistrict = data[data.level == 'district']

    
    gdf_merged_bydistrict = gpd.sjoin(data_filter_bydistrict, gdf_second_hand_house, how="left", predicate = "intersects")
    filtered_data = gdf_merged_bydistrict[gdf_merged_bydistrict['unit_price'] > 0]

    agg_bydistrict = filtered_data.groupby('adcode').median({'unit_price': 'unit_price'}).round(-3)


    result_bydistrict = data_filter_bydistrict.merge(agg_bydistrict, how='left', left_on='adcode', right_on ='adcode')

    ax = result_bydistrict.plot(
        column="unit_price",
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
    ax.set_title('Shanghai Second Hand House Distribution', 
                fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})


    texts = []
    for idx, row in result_bydistrict.iterrows():
        centroid = row.geometry.centroid.coords[0]
        if not math.isnan(row['unit_price']):
            text = ax.annotate(
                text=f"{row['name']}\n{row['unit_price']:.0f}",
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

    plt.savefig('./houseinfo/map_second_bydistrict.png', dpi=500, bbox_inches='tight', pad_inches=0)

    # 处理板块级别数据
    exclude_values = [310104, 310101, 310106, 310109, 310105, 310110, 310107]  # 要排除的值的列表
    data_filter_bystreet = data[((data.level == 'district') & (data.adcode.isin(exclude_values)))
                        | ((data.level == 'town') & (~data.parent.apply(lambda x: x['adcode'] if isinstance(x, dict) and 'adcode' in x else None).isin(exclude_values)))]
    gdf_merged_bystreet = gpd.sjoin(data_filter_bystreet, gdf_second_hand_house, how="left", predicate = "intersects")

    filtered_data = gdf_merged_bystreet[gdf_merged_bystreet['unit_price'] > 0]
    agg_bystreet = filtered_data.groupby('adcode').median({'unit_price': 'unit_price'}).round(-3)

    result_bystreet = data_filter_bystreet.merge(agg_bystreet, how='left', left_on='adcode', right_on ='adcode')

    ax = result_bystreet.plot(
        column="unit_price",
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
    ax.set_title('Shanghai Second House Distribution', 
                fontdict={'fontsize': 20, 'fontweight': 'bold', 'color': 'darkblue'})

    texts = []
    for idx, row in result_bystreet.iterrows():
        centroid = row.geometry.centroid.coords[0]
        if not math.isnan(row['unit_price']):
            text = ax.annotate(
                text=f"{row['name']}\n{row['unit_price']:.0f}",
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

    plt.savefig('./houseinfo/map_second_bystreet.png', dpi=500, bbox_inches='tight', pad_inches=0)

    cm = sns.color_palette("coolwarm", as_cmap=True)

    print(gdf_merged_bystreet)


    second_hand_house_info = filtered_data[['al_text','name','unit_price','age','total_cnt','sell_cnt']].dropna(subset = ['al_text'])

    second_hand_house_info.sort_values(by=["name", "unit_price"],
                        ascending=[True, False], inplace=True)
    second_hand_house_info.reset_index(drop=True, inplace=True)

    html = (
        "<h2>Second Hand House List</h2>"  # 添加标题
        "<table>"
        + second_hand_house_info.style
        .background_gradient(subset=["unit_price"], cmap=cm)
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
                        <figcaption> Shanghai second hand house distribution by district.</figcaption>
                    </picture>
                    <picture>
                        <!-- 默认模式下的图片 -->
                        <img src="cid:image1" alt="The industry distribution of current pnl is as follows:" style="width:100%">
                        <figcaption>Shanghai second hand house distribution by street.</figcaption>
                    </picture>
                </body>
            </html>
            """
    image_path = [
        "./houseinfo/map_second_bydistrict.png",
        "./houseinfo/map_second_bystreet.png"
    ]
    MyEmail().send_email_embedded_image(
        '上海二手房信息跟踪',  html_img + html, image_path
    )
