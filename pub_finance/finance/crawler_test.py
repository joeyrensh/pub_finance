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
import httpx
import asyncio
import aiohttp
from fake_useragent import UserAgent
from selenium import webdriver


def fetch_house_info(url, file_path):

    # 添加请求头
    ua = UserAgent()
    headers =  {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
                'Content-Length': '0',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'host': 'www.fangdi.com.cn',
                'Origin': 'http://www.fangdi.com.cn',
                'Referer': 'http://www.fangdi.com.cn/index.html',
                'Upgrade-Insecure-Requests': '1',
                'X-Requested-With': 'XMLHttpRequest',
                'POST': '/service/index/getIndexMessage.action?MmEwMD=4CoFxR.BwKwk9w7O5h2NIAWhebrwn0Hc1HUNZpKZzuHrLuiO9N.e6Ou6hNuH57EWB1kL9CxOmcQVi68k0WuCkeRoXzjeQHoxydyAwZ5bnWaNAXigCAH8VyPxJAHKRKQ..WszMKrtDTJCie8gWGawxGdKdIDgDrcW3UBNN7h1BoczD29Jxqw1kcEJvWSDzAm4PVtRiNwCBuRWTlX.DNzJfwrGF8YaVtUB87WMTvpOr7poF.A0Vh_ZrU0OqdLSLG_hmwKAJoThMFjLLYknh0_yuRGzy8L6atTFhZ6q6iEahGx4d67jXscB7Mk1hGYYk_RcxsC91Im0Bp.vHY5yyTHYScRQuV..CX.OtfUi6jSoSx829wIZAg2R36NsRPscwREO1hNz HTTP/1.1'
                }
    data = {
    }
    session = session = requests.Session()
    response = session.get(url, headers=headers, data=data)


    dict = {}
    # response = requests.post(url, headers=headers)
    print(response.status_code)  # 打印状态码
    print(response.text)  # 打印响应内容
    # tree = html.fromstring(response.content)
    # first_supply_div = tree.xpath('//div[@class="content_box"]/div[@class="layout content"]/div[@class="house_supply"]/div[@class="main_wrapper clearfix"]/div[@class="main_item_left"]/div[@class="main_item_content house_supply_content grey_shadow"]/div[@class="house_supply_district house_supply_num"]/span2/i1')
    # first_supply_div = tree.xpath('//div[@class="content_box"]/div[@class="layout content"]/div[@class="house_supply"]/div[@class="main_wrapper clearfix"]/div[@class="main_item_right"]/div[@class="today_sign grey_shadow"]/div[@class="sign_top1"]/p/span')
    # first_supply = first_supply_div[0].xpath('string(.)')
 
async def fetch_data(url):
    # 添加请求头
    ua = UserAgent()
    headers =  {'User-Agent':ua.random,
                'Content-Type': 'application/json',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Connection': 'keep-alive',
                'Referer': 'http://www.fangdi.com.cn/service/index/getIndexMessage',
                'Origin': 'http://www.fangdi.com.cn',
                'host': 'www.fangdi.com.cn',
                'Upgrade-Insecure-Requests': '1'
                }
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers, timeout=10)
        print(response.status_code)  # 打印状态码
        print(response.headers)  # 打印响应内容
        print(response.text)  # 打印响应内容
async def main(url):
    result = await fetch_data(url)
    # print(result)

# 主程序入口
if __name__ == "__main__":
    
    file_path = './houseinfo/test.csv'
    # url='http://www.fangdi.com.cn/index.html'
    url = 'http://www.fangdi.com.cn/service/index/getIndexMessage.action'
    # fetch_house_info(url,file_path)
    # 创建一个事件循环并运行异步函数
    # asyncio.run(main(url))
    import datetime, os
    import pandas as pd
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium import webdriver

    options = webdriver.FirefoxOptions()
    # options.set_headless()      # 无头模式
    driver = webdriver.Firefox()
    result = driver.get(r'http://www.fangdi.com.cn/index.html') 
    print(result)