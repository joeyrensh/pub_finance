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
import matplotlib.colors as colors
import sys
import pysal.viz.mapclassify as mc




def fetch_houselist(url, page, complete_list):
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

def houseinfo_to_csv(file_path):
    # 如果文件存在，则删除文件
    if os.path.isfile(file_path):
        os.remove(file_path)
    # 发起HTTP请求
    # url = 'https://sh.lianjia.com/xiaoqu/minhang/pg1/'  # 替换为目标URL
    district_list = ['pudong']
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
        houselist = fetch_houselist(url_re, page_no, complete_list)
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
            futures = [executor.submit(fetch_house_info, url_detail, item) for item in houselist]
            
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
    
    file_path = './houseinfo/test.csv'
    houseinfo_to_csv(file_path)