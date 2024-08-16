import requests
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import math
from utility.MyEmail import MyEmail
from utility.ToolKit import ToolKit
import os
from lxml import html
import concurrent.futures
import re
import json
import time
import random
from tenacity import retry, wait_random, stop_after_attempt
import logging
import sys
import uuid
import math

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# https://www.useragents.me/#most-common-desktop-useragents-json-csv 获取最新user agent
user_agent_list = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/25.0 Chrome/121.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Unique/100.7.6266.6",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.",
    "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.3"
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.",
]


# 定义一个函数来打印重试次数
def after_retry(retry_state):
    logger.info(f"Retrying... attempt number {retry_state.attempt_number}")


@retry(wait=wait_random(min=3, max=5), stop=stop_after_attempt(3), after=after_retry)
def get_max_page(url, headers, proxy):
    time.sleep(random.randint(3, 5))  # 随机休眠
    s = requests.Session()
    s.headers.update(headers)
    s.proxies = proxy
    response = s.get(url)
    print("response code:", response.status_code)
    # for cookie in response.headers.getlist("Set-Cookie"):
    #     for part in cookie.split("; "):
    #         if part.startswith("lianjia_ssid="):
    #             # 提取session_id的值
    #             session_id = part.split("=")[1]
    #             print(f"Session ID: {session_id}")
    #             break
    #     else:
    #         # 如果当前cookie中没有找到session_id，则继续检查下一个cookie
    #         continue
    #     break  # 如果找到了session_id，则跳出循环
    # else:
    #     # 如果没有在任何cookie中找到session_id
    #     print("Session ID not found in response headers.")
    # 检查请求是否成功
    if response.status_code != 200:
        print(f"Failed to retrieve data: {response.status_code}")
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    tree = html.fromstring(response.content)

    # 查找特定的div
    # 假设我们要查找class为'target-div'的div
    div = tree.xpath(
        '//div[@class="content"]/div[@class="leftContent"]/div[@class="resultDes clear"]/h2[@class="total fl"]/span'
    )
    if div:
        cnt = div[0].text_content().strip()
        page_no = math.ceil(int(cnt) / 30)
        logger.info("当前获取房源量为%s,总页数为%s" % (cnt, page_no))
        print("当前获取房源量为%s,总页数为%s" % (cnt, page_no))
    else:
        logger.info("XPath query returned no results")
        print("XPath query returned no results")
        raise Exception("XPath query returned no results")
    return page_no


def check_proxy_anonymity(url, headers, proxies):
    try:
        response = requests.get(url, headers=headers, proxies=proxies, timeout=5)
        for key, value in response.headers.items():
            if key.lower() in [
                "x-forwarded-for",
                "x-real-ip",
                "via",
                "proxy-connection",
            ]:
                print(f"{key}:{value}")
            else:
                print(response.content)
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


url = "https://sh.lianjia.com/xiaoqu/pudong/pg1cro21/"
headers = {
    "User-Agent": random.choice(user_agent_list),
    "Connection": "keep-alive",
    # "Referer": "www.baidu.com",
    # "Cookie": "lianjia_uuid=%s;" % (uuid.uuid4()),
}
proxies = [
    "http://221.230.7.45:9000",
    "http://183.242.69.118:3218",
    "http://101.126.44.74:8080",
    "http://118.117.189.220:8089",
]
proxy = {"http": random.choice(proxies)}
print(proxy)
max_page = get_max_page(url, headers, proxy)
# check_proxy_anonymity(url, headers, proxies)
