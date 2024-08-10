#!/usr/bin/env python3

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

# https://www.useragents.me/#most-common-desktop-useragents-json-csv 获取最新user agent
user_agent_list = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/25.0 Chrome/121.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Unique/100.7.6266.6",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.",
    "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115."
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
]

headers = {
    "User-Agent": random.choice(user_agent_list),
    "Connection": "keep-alive",
    "Referer": "https://sh.fang.lianjia.com/",
    "Cookie": "lianjia_uuid=%s" % (uuid.uuid4()),
}

url = "https://sh.fang.lianjia.com/loupan/pudong/nht1nht2nhs1pg1/"
res = requests.get(url, headers=headers)
if res.status_code == 200:
    tree = html.fromstring(res.content)
    count_div = tree.xpath(".//div[3]/div[2]/div/span[2]")
    count = count_div[0].xpath("string(.)")
    print(round(int(count) / 10) + 1)
    divs = tree.xpath(
        '//div[@class="resblock-list-container clearfix"]/ul[@class="resblock-list-wrapper"]/li'
    )
    for div in divs:
        name_div = div.xpath(
            './/div[@class="resblock-desc-wrapper"]/div[@class="resblock-name"]/a'
        )
        name = name_div[0].xpath("string(.)")
        sale_status_div = div.xpath(
            './/div[@class="resblock-desc-wrapper"]/div[@class="resblock-name"]/span[@class="sale-status"]'
        )
        sale_status = sale_status_div[0].xpath("string(.)")
        resblock_type_div = div.xpath(
            './/div[@class="resblock-desc-wrapper"]/div[@class="resblock-name"]/span[@class="resblock-type"]'
        )
        resblock_type = resblock_type_div[0].xpath("string(.)")
        district_div = div.xpath(
            './/div[@class="resblock-desc-wrapper"]/div[@class="resblock-location"]/span[1]'
        )
        district = district_div[0].xpath("string(.)")
        street_div = div.xpath(
            './/div[@class="resblock-desc-wrapper"]/div[@class="resblock-location"]/span[2]'
        )
        street = street_div[0].xpath("string(.)")
        area_div = div.xpath(
            './/div[@class="resblock-desc-wrapper"]/div[@class="resblock-area"]/span'
        )
        area = area_div[0].xpath("string(.)")
        unit_price_div = div.xpath(
            './/div[@class="resblock-desc-wrapper"]/div[@class="resblock-price"]/div[@class="main-price"]/span[@class="number"]'
        )
        unit_price = unit_price_div[0].xpath("string(.)")
        href_div = div.xpath(".//a")
        href = href_div[0].get("href")

        test_div = div.xpath(
            './/div[@class="resblock-desc-wrapper"]/div[@class="resblock-tag"]/span'
        )
        test = test_div[0].xpath("string(.)")
        print(test)


# url = "https://sh.fang.lianjia.com/loupan/p_zjjlsxbmdtp/"
# res = requests.get(url, headers=headers)
# if res.status_code == 200:
#     tree = html.fromstring(res.content)
#     date_div = tree.xpath("//div[2]/div[3]/div[2]/div/div[3]/ul/li[2]/div/span[2]")
#     print(date_div[0].xpath("string(.)"))
#     longlat_div = tree.xpath("//div[2]/div[3]/div[1]/span")

#     longitude = longlat_div[0].get("data-coord").split(",")[1]
#     latitude = longlat_div[0].get("data-coord").split(",")[0]
#     print(longitude)
#     print(latitude)
