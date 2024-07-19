
import requests
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import math
from utility.MyEmail import MyEmail
import seaborn as sns
from utility.ToolKit import ToolKit
import os
from lxml import html
import concurrent.futures
import re
import json

""" 
上海区域地图数据：https://geo.datav.aliyun.com/areas_v3/bound/310000_full.json
链家数据：https://sh.fang.lianjia.com/loupan/minhang/nht1nht2nhs1pg1/?_t=1/

"""

def get_house_info_f(file_path):

    houselist = []

    # 如果文件存在，则删除文件
    if os.path.isfile(file_path):
        os.remove(file_path)
    district_list = ['huangpu','xuhui','changning','jingan',
                     'putuo','hongkou','yangpu','minhang',
                     'baoshan','jiading','pudong','jinshan',
                     'songjiang','qingpu','fengxian','chongming']
    cnt = 0
    t = ToolKit("策略执行中")
    for idx, district in enumerate(district_list):
        t.progress_bar(len(district_list), idx + 1)
        
        for i in range(1, 10):
            dict = {}
            list = []
            url = (
                "https://sh.fang.lianjia.com/loupan/district/nht1nht2nhs1co41pgpageno/?_t=1/"
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
                file_path,
                mode="a",
                index=False,
                header=(cnt == 0))
            cnt = cnt + 1

def fetch_houselist_s(url, page, complete_list):
    # 添加请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Cookie': 'lianjia_uuid=9bb8fccc-9ed4-4f5d-ac1c-decaebb51169; _smt_uid=66682e43.4b7ae654; _jzqc=1; _qzjc=1; _ga=GA1.2.1669893493.1718103624; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1718166241; crosSdkDT2019DeviceId=uik048-qg82dj-s69ornrx4lw5n96-m70vmzepz; _jzqx=1.1718103621.1719886293.3.jzqsr=google%2Ecom%2Ehk|jzqct=/.jzqsr=google%2Ecom%2Ehk|jzqct=/; _ga_34Q2BG9VYB=GS1.2.1720166985.1.1.1720166989.0.0.0; HMACCOUNT=5E4F5FB17861AFE8; select_city=310000; _jzqckmp=1; _gid=GA1.2.1156257387.1720495740; _ga_00MKBBEWEN=GS1.2.1720495741.16.1.1720495800.0.0.0; GUARANTEE_POPUP_SHOW=true; GUARANTEE_BANNER_SHOW=true; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219006f4b96399d-07f09aa9e418ea-1a525637-1930176-19006f4b9642836%22%2C%22%24device_id%22%3A%2219006f4b96399d-07f09aa9e418ea-1a525637-1930176-19006f4b9642836%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; lianjia_ssid=86e9641a-701e-4911-9fee-869459536e79; _jzqa=1.1115327711180168800.1718103621.1720548541.1720578120.19; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1720578251; srcid=eyJ0Ijoie1wiZGF0YVwiOlwiNGY5MWNhZWU5YmFmODk0MzY2Mjk5ZDkxMjdmYTIyODU5MWM4M2U1ZTc3YjczYmRiNTU3MGFjMjVmMjBhYzBjYzA0MDkwZTNhMmM3NDAzYjBkZWRjNTBmZTk5NjMxYWExYzkxZjU1YWYzMDAzZmE0YjdmMDgxMzcwNzhlYTdmMmMzMTViNjNlYzc2NzZlYWY1NzlkNzlhZTEzOGQ4MmEwYjcwZTI2YmNkYWJlMTY2MjI4ZmEzODIyNWU3NTA4ODA4ZTAzNzQ5ZmFhYjQ0OWQ0MzA1NTFmZmZiNDhiMWI5ZGQ3MTBkMzcwMGI5MjBmZWU0YzM3Mzk3ZTU5NTcwZmU0YlwiLFwia2V5X2lkXCI6XCIxXCIsXCJzaWduXCI6XCJmZDVlYWE0N1wifSIsInIiOiJodHRwczovL3NoLmxpYW5qaWEuY29tL3hpYW9xdS9jaG9uZ21pbmcvcGcxLyIsIm9zIjoid2ViIiwidiI6IjAuMSJ9; _gat=1; _gat_past=1; _gat_global=1; _gat_new_global=1; _gat_dianpu_agent=1; _ga_GVYN2J1PCG=GS1.2.1720578122.13.1.1720578255.0.0.0; _ga_LRLL77SF11=GS1.2.1720578122.13.1.1720578255.0.0.0; _qzja=1.1984203958.1718103621059.1720548541230.1720578120092.1720578251169.1720578282145.0.0.0.117.19; _qzjb=1.1720578120092.3.0.0.0; _qzjto=48.2.0; _jzqb=1.3.10.1720578120.1'
    }
    datalist=[]
    df_complete = pd.DataFrame(complete_list)
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
                    if len(df_complete) > 0:
                        if data_id in df_complete['data_id'].values:
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
                    district_div = div.xpath('.//div[@class="info"]/div[@class="positionInfo"]/a[@class="district"]')
                    if district_div:
                        district = district_div[0].xpath('string(.)')
                    else:
                        district = ''
                    
                    dict = {
                        'data_id': data_id,
                        'al_text': alt_text,
                        'sell_cnt': sell_cnt,
                        'district': district
                    }
                    list.append(dict)
                    datalist.append(data_id)                   
            else:
                print("未找到目标<ul>标签")
        else:
            print("请求失败，状态码:", response.status_code)

    return list

def fetch_house_info_s(url, item):

    # 添加请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }    

    dict = {}
    url_re = (url.replace("data_id", item['data_id']))
    response = requests.get(url_re, headers=headers)
    # print(url_re)
    tree = html.fromstring(response.content)
    unit_price_div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquPrice clear"]/div[@class="fl"]/span[@class="xiaoquUnitPrice"]')
    if len(unit_price_div) > 0:
        unit_price = unit_price_div[0].xpath('string(.)')
    else:
        unit_price = ''
    deal_price_div = tree.xpath('//div[@class="m-content"]/div[@class="box-l xiaoquMainContent"]/div[@class="frameDeal"]/div[@class="frameDealList"]/ol[@class="frameDealListItem"]/li[1]/div[@class="frameDealUnitPrice"]')
    if len(deal_price_div) > 0:
        deal_price = deal_price_div[0].xpath('string(.)')
    else:
        deal_price = ''
    deal_date_div = tree.xpath('//div[@class="m-content"]/div[@class="box-l xiaoquMainContent"]/div[@class="frameDeal"]/div[@class="frameDealList"]/ol[@class="frameDealListItem"]/li[1]/div[@class="frameDealDate"]')
    if len(deal_date_div) > 0:
        deal_date = deal_date_div[0].xpath('string(.)')
    else:
        deal_date = ''      
    lanlong_div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemOneLine"]/div[@class="xiaoquInfoItem outerItem"][2]/span[@class="xiaoquInfoContent outer"]/span[@mendian]')
    if len(lanlong_div) > 0:
        lanlong = lanlong_div[0].get('xiaoqu')
    else:
        lanlong = ''
    age_div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][2]/div[@class="xiaoquInfoItem"][2]/span[@class="xiaoquInfoContent"]')
    if len(age_div) > 0:
        age = age_div[0].xpath('string(.)')
    else:
        age = ''
    house_type_div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][2]/div[@class="xiaoquInfoItem"][1]/span[@class="xiaoquInfoContent"]')
    if len(house_type_div) > 0:
        house_type = house_type_div[0].xpath('string(.)')
    else:
        house_type = ''        
    total_cnt_div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][1]/div[@class="xiaoquInfoItem"][2]/span[@class="xiaoquInfoContent"]')
    if len(total_cnt_div) > 0:
        total_cnt = total_cnt_div[0].xpath('string(.)')
    else:
        total_cnt = ''
    structure_div = tree.xpath('//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][1]/div[@class="xiaoquInfoItem"][1]/span[@class="xiaoquInfoContent"]')
    if len(structure_div) > 0:
        structure = structure_div[0].xpath('string(.)')
    else:
        structure = ''   
    

    dict = {
        'data_id': item['data_id'],
        'al_text': item['al_text'],
        'sell_cnt': item['sell_cnt'],
        'district': item['district'],
        'unit_price': unit_price,
        'deal_price': deal_price,
        'deal_date': deal_date,
        'lanlong': lanlong,
        'age': age,
        'total_cnt': total_cnt,
        'structure': structure,
        'house_type': house_type
    }

    return dict

def houseinfo_to_csv_s(file_path):
    # 如果文件存在，则删除文件
    if os.path.isfile(file_path):
        os.remove(file_path)
    # 发起HTTP请求
    # url = 'https://sh.lianjia.com/xiaoqu/minhang/pg1/'  # 替换为目标URL
    district_list = ['huangpu','xuhui','changning','jingan','putuo',
                     'hongkou','yangpu','minhang','baoshan','jiading',
                     'pudong','jinshan','songjiang','qingpu','fengxian','chongming']
    # district_list = ['minhang']
    page_no = 200
    
    t = ToolKit("列表生成")
    houselist = []
    complete_list = []
    count = 0
    url = ("https://sh.lianjia.com/xiaoqu/district/pgpgnocro21/")
    for idx, district in enumerate(district_list):                
        url_re = (
            url.replace("district", district)
        )
        houselist = fetch_houselist_s(url_re, page_no, complete_list)
        complete_list.extend(houselist) 
        t.progress_bar(len(district_list), idx + 1)
        print('complete list cnt is: ', len(houselist))

        # 设置并发数上限为6
        max_workers = 2
        data_batch_size = 10
        list = []
        url_detail = 'https://sh.lianjia.com/xiaoqu/data_id/'
        t = ToolKit("信息爬取")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务并获取Future对象列表
            futures = [executor.submit(fetch_house_info_s, url_detail, item) for item in houselist]
            
            # 获取已完成的任务的结果
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    list.append(result)
                except Exception as e:
                    print(f"获取详细信息时出错: {e}")
            
                count += 1
                if count % data_batch_size == 0:
                    # 每处理完一批数据后，将数据写入CSV文件
                    df = pd.DataFrame(list)
                    df.to_csv(
                        file_path,
                        mode="a",
                        index=False,
                        header=(count == data_batch_size)
                    )
                    list = []  # 清空列表以继续下一批数据的处理

        # 处理剩余的数据
        if list:
            df = pd.DataFrame(list)
            df.to_csv(
                file_path,
                mode="a",
                index=False,
                header=False
            )            

# 主程序入口
if __name__ == "__main__":
    plt.rcParams['font.family'] = 'WenQuanYi Zen Hei'
    file_path = './houseinfo/newhouse.csv'
    geo_path = './houseinfo/shanghaidistrict.json'
    png_path = './houseinfo/map_newhouse.png'
    png_path_s = './houseinfo/map_secondhouse.png'
    png_path_s2 = './houseinfo/map_secondhouse2.png'
    # file_path_s = './houseinfo/secondhandhouse.csv'
    file_path_s = './houseinfo/test.csv'
    # 新房
    # get_house_info_f(file_path)
    # 二手
    # houseinfo_to_csv_s(file_path_s)
    

    geo_data = gpd.read_file(geo_path,  engine="pyogrio")
    df_new_house = pd.read_csv(file_path, usecols=[i for i in range(0, 12)])

    gdf_new_house = gpd.GeoDataFrame(
        df_new_house, geometry=gpd.points_from_xy(df_new_house.longitude, df_new_house.latitude), crs="EPSG:4326"
    )
    # 处理板块级别数据
    geo_data_f = geo_data[(geo_data.level == 'town')]
    gdf_merged = gpd.sjoin(geo_data_f, gdf_new_house, how="inner", predicate = "intersects")
    gdf_agg = gdf_merged.groupby('adcode').median({'avg_price': 'avg_price'}).round(-2)
    result = geo_data_f.merge(gdf_agg, how='left', left_on='adcode', right_on ='adcode')
    legend_kwargs_1 = {
        'fmt': '{:.0f}',
        'title': '单价'
    }
    missing_kwds = {
        "facecolor": "lightgrey",
        "edgecolor": "k",
        "label": "Missing values",
    }
    ax = result.plot(
        column="avg_price",
        cmap='RdYlGn_r',
        alpha = 0.8,
        legend=True,
        linewidth=0.5,
        edgecolor='k',
        scheme="natural_breaks",
        k=8,
        figsize=(10, 10),
        legend_kwds=legend_kwargs_1,
        missing_kwds=missing_kwds
    )
    ax.axis('off')
    # 添加annotation
    texts = []
    for idx, row in result.iterrows():
        centroid = row.geometry.centroid.coords[0]
        if not math.isnan(row['avg_price']):
            text = ax.annotate(
                text=f"{row['name']}\n{row['avg_price']:.0f}",
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
    plt.savefig(png_path, dpi=500, bbox_inches='tight', pad_inches=0)

    df_second_hand_house = pd.read_csv(file_path_s, usecols=[i for i in range(0, 12)])
    df_second_hand_house = df_second_hand_house.dropna(subset=['lanlong'])
    # 处理二手房经纬度格式
    def extract_values(string):
        values = string.strip("[]").split(",")
        value1 = float(values[0])
        value2 = float(values[1])
        return value1, value2
    df_second_hand_house[['longitude', 'latitude']] = df_second_hand_house['lanlong'].apply(extract_values).apply(pd.Series)
    # 定义一个函数来替换中文字符
    def replace_non_numeric_characters(text, replacement=''):
        
        # 使用正则表达式匹配非数字字符
        numeric_text = re.sub(r'\D', '', str(text))
        # 检查字符串是否为空
        if numeric_text == '':
            return None  # 返回空字符串
        # 返回转换后的整数类型
        return int(numeric_text)

    df_second_hand_house['total_cnt'] = df_second_hand_house['total_cnt'].apply(replace_non_numeric_characters)
    df_second_hand_house['deal_price'] = df_second_hand_house['deal_price'].apply(replace_non_numeric_characters)
    gdf_second_hand_house = gpd.GeoDataFrame(
        df_second_hand_house, geometry=gpd.points_from_xy(df_second_hand_house['longitude'], df_second_hand_house['latitude']), crs="EPSG:4326"
    )
    # 处理板块级别数据
    exclude_values = [310104, 310101, 310106, 310109, 310105, 310110, 310107]  # 要排除的值的列表
    geo_data_s = geo_data[
        ((geo_data.level == 'district') & (geo_data.adcode.isin(exclude_values))) |
        ((geo_data.level == 'town') & (geo_data['parent'].apply(lambda x: json.loads(x)['adcode']).isin(exclude_values) == False))
    ]
    gdf_merged = gpd.sjoin(geo_data_s, gdf_second_hand_house, how="inner", predicate = "intersects")
    # 板块均价分析
    gdf_merged = gdf_merged[gdf_merged['unit_price'] > 0]
    gdf_agg = gdf_merged.groupby('adcode').median({'unit_price': 'unit_price'}).round(-2)
    result = geo_data_s.merge(gdf_agg, how='left', left_on='adcode', right_on ='adcode')    
    legend_kwargs_2 = {
        'fmt': '{:.0f}',
        'title': '挂牌价'
    }
    ax = result.plot(
            column="unit_price",
            cmap='RdYlGn_r',
            alpha = 0.8,
            legend=True,
            linewidth=0.5,
            edgecolor='k',
            scheme="natural_breaks",
            k=8,
            figsize=(10, 10),
            legend_kwds=legend_kwargs_2,
            missing_kwds=missing_kwds
        )
    ax.axis('off')

    # 添加annotation
    texts = []
    for idx, row in result.iterrows():
        centroid = row.geometry.centroid.coords[0]
        if not math.isnan(row['unit_price']):
            if row.level == 'town':
                text = ax.annotate(
                    text=f"{row['name']}\n{row['unit_price']:.0f}",
                    xy=centroid,
                    ha='center',
                    fontsize=4,  # 设置字体大小
                    color='black',  # 设置字体颜色为黑色
                    weight='black',  # 设置字体粗细
                    bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0), boxstyle='round, pad=0.5'),  # 设置注释框样式
                )
            else:
                text = ax.annotate(
                    text=f"{row['name']}",
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
    plt.savefig(png_path_s, dpi=500, bbox_inches='tight', pad_inches=0)

    # 板块均价分析
    gdf_merged = gpd.sjoin(geo_data_s, gdf_second_hand_house, how="inner", predicate = "intersects")
    gdf_merged = gdf_merged[gdf_merged['deal_price'] > 0]
    gdf_agg = gdf_merged.groupby('adcode').median({'deal_price': 'deal_price'}).round(-2)
    result = geo_data_s.merge(gdf_agg, how='left', left_on='adcode', right_on ='adcode')    
    legend_kwargs_2 = {
        'fmt': '{:.0f}',
        'title': '最近成交价'
    }
    ax = result.plot(
            column="deal_price",
            cmap='RdYlGn_r',
            alpha = 0.8,
            legend=True,
            linewidth=0.5,
            edgecolor='k',
            scheme="natural_breaks",
            k=8,
            figsize=(10, 10),
            legend_kwds=legend_kwargs_2,
            missing_kwds=missing_kwds
        )
    ax.axis('off')

    # 添加annotation
    texts = []
    for idx, row in result.iterrows():
        centroid = row.geometry.centroid.coords[0]
        if not math.isnan(row['deal_price']):
            if row.level == 'town':
                text = ax.annotate(
                    text=f"{row['name']}\n{row['deal_price']:.0f}",
                    xy=centroid,
                    ha='center',
                    fontsize=4,  # 设置字体大小
                    color='black',  # 设置字体颜色为黑色
                    weight='black',  # 设置字体粗细
                    bbox=dict(facecolor=(1, 1, 1, 0), edgecolor=(1, 1, 1, 0), boxstyle='round, pad=0.5'),  # 设置注释框样式
                )
            else:
                text = ax.annotate(
                    text=f"{row['name']}",
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
    plt.savefig(png_path_s2, dpi=500, bbox_inches='tight', pad_inches=0)    

    # 房屋明细
    new_house_cnt = len(df_new_house)
    html_txt = """
                <!DOCTYPE html>
                <html>
                <head>
                    <style>
                        h2 {{
                            font-style: italic;
                        }}
                    </style>
                </head>                    
                <body>
                    <h2>新房在售 {newhouse_cnt} 个小区</h2>
                </body>
                </html>
                """.format(newhouse_cnt=new_house_cnt)

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
                </body>
            </html>
            """
    image_path = [
        png_path
    ]
    # MyEmail().send_email_embedded_image(
    #     '上海新房信息跟踪',  html_txt + html_img, image_path
    # )

