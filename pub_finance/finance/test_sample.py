import requests
from lxml import html
import re
import pandas as pd
from utility.ToolKit import ToolKit
import os

def fetch_data(url, page):
    # 添加请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
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
    # 房屋信息爬取
    url_detail = 'https://sh.lianjia.com/xiaoqu/data_id/'
    # 添加请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    list1 = []
    print('list cnt is: ', len(list))
    if len(list) > 0:
        for i in list:
            dict1 = {}
            url_detail_re = (url_detail.replace("data_id", str(i['data_id'])))
            response = requests.get(url_detail_re, headers=headers)
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
                continue
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
            

            dict1 = {
                'data_id': i['data_id'],
                'al_text': i['al_text'],
                'sell_cnt': i['sell_cnt'],
                'unit_price': unit_price,
                'lanlong': lanlong,
                'age': age,
                'total_cnt': total_cnt
            }
            list1.append(dict1)
    df = pd.DataFrame(list1)
    if not hasattr(fetch_data, 'count'):
        # 如果没有，则为函数设置'count'属性并初始化为1
        fetch_data.count = 1
    else:
        # 如果有，则递增'count'属性的值
        fetch_data.count += 1        
    df.to_csv(
        './secondhandhouseinfo.csv',
        mode="a",
        index=False,
        header=(fetch_data.count == 1))     



# 发起HTTP请求
# url = 'https://sh.lianjia.com/xiaoqu/minhang/pg1/'  # 替换为目标URL
district_list = ['huangpu','xuhui','changning','jingan','putuo','hongkou','yangpu','minhang','baoshan','jiading','pudong','jinshan','songjiang','qingpu','fengxian','chongming']
filename = './secondhandhouseinfo.csv'

# 如果文件存在，则删除文件
if os.path.isfile(filename):
    os.remove(filename)
t = ToolKit("策略执行中")
for idx, district in enumerate(district_list):
    url = ("https://sh.lianjia.com/xiaoqu/district/pgpgno/")                
    url_re = (
        url.replace("district", district)
    )
    fetch_data(url_re, 200)
    t.progress_bar(len(district_list), idx)