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


""" 
上海区域地图数据：https://geo.datav.aliyun.com/areas_v3/bound/310000_full.json
链家数据：https://sh.fang.lianjia.com/loupan/minhang/nht1nht2nhs1pg1/?_t=1/

"""

# https://www.useragents.me/#most-common-desktop-useragents-json-csv 获取最新user agent
user_agent_list = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:129.0) Gecko/20100101 Firefox/129.",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.14"
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.10",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.3",
]
# https://proxyscrape.com/free-proxy-list
# proxyscrape.com免费proxy: https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country=cn&protocol=http&proxy_format=protocolipport&format=text&anonymity=Elite,Anonymous&timeout=3000
# 站大爷免费proxy: https://www.zdaye.com/free/?ip=&adr=&checktime=&sleep=3&cunhuo=&dengji=&nadr=&https=1&yys=&post=&px=
proxies = []


def check_proxy_anonymity(url, headers, proxy):
    try:
        s = requests.Session()
        s.proxies = proxy
        s.headers.update(headers)
        response = s.get(url, timeout=5)
        if response.status_code == 200:
            tree = html.fromstring(response.content)
            div = tree.xpath(
                '//div[@class="content"]/div[@class="leftContent"]/div[@class="resultDes clear"]/h2[@class="total fl"]/span'
            )
            if div:
                return True
            else:
                return False
        else:
            return False

    except requests.exceptions.RequestException:
        return False
    except Exception:
        return False


def get_proxies_listv3(proxies_list, url):
    dlist = []

    for item in proxies_list:
        proxy = {"https": item, "http": item}
        if check_proxy_anonymity(url, get_headers(), proxy):
            dlist.append(item)

    return dlist


def update_proxies():
    df = pd.read_csv("./houseinfo/proxies.csv", names=["proxy"], comment="#")
    pre_proxies = df["proxy"].tolist()
    url = "http://sh.ke.com/xiaoqu/xuhui/pg1cro21/"
    global proxies
    proxies = get_proxies_listv3(pre_proxies, url)

    print("proxies已更新!!!")
    print(proxies)


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
# 全局变量
_last_index = None
_max_attempt = 10
_min_delay = 2
_max_delay = 5
_timeout = 5
_max_workers = 2


# 定义映射关系，模拟枚举值到值的映射
mapping = {
    "huangpu": "黄浦",
    "xuhui": "徐汇",
    "changning": "长宁",
    "jingan": "静安",
    "putuo": "普陀",
    "pudong": "浦东",
    "hongkou": "虹口",
    "yangpu": "杨浦",
    "minhang": "闵行",
    "baoshan": "宝山",
    "jiading": "嘉定",
    "jinshan": "静安",
    "songjiang": "松江",
    "qingpu": "青浦",
    "fengxian": "奉贤",
    "chongming": "崇明",
}


def map_value(input_value, default=None):
    """
    根据预定义的映射关系返回对应的值。

    :param input_value: 输入值，期望在映射字典中存在。
    :param default: 如果输入值不在映射中，则返回此默认值（默认为None）。
    :return: 映射后的值或默认值。
    """
    return mapping.get(input_value, default)


# 定义一个函数来打印重试次数
def after_retry(retry_state):
    logger.info(f"Retrying... attempt number {retry_state.attempt_number}")


def get_proxy(proxies):
    global _last_index
    if len(proxies) == 0:
        return None
    elif _last_index is None:
        _last_index = 0
        ip_port = proxies[_last_index]
        proxy = {"https": ip_port, "http": ip_port}
        return proxy
    else:
        next_index = (_last_index + 1) % len(proxies)
        _last_index = next_index
        ip_port = proxies[next_index]
        proxy = {"https": ip_port, "http": ip_port}
        return proxy


def get_headers():
    headers = {
        "User-Agent": random.choice(user_agent_list),
        "Referer": "www.baidu.com",
        "Connection": "keep-alive",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-encoding": "gzip, deflate, br, zstd",
        "Accept-language": "zh-CN,zh;q=0.9",
        "Cookie": ("lianjia_uuid=%s;") % (uuid.uuid4()),
    }
    return headers


@retry(
    wait=wait_random(min=_min_delay, max=_max_delay),
    stop=stop_after_attempt(_max_attempt),
    after=after_retry,
)
def get_max_page_f(url):
    session = requests.Session()
    proxy = get_proxy(proxies)
    session.proxies = proxy
    session.headers.update(get_headers())
    try:
        response = session.get(url, timeout=_timeout)

        time.sleep(random.randint(_min_delay, _max_delay))  # 随机休眠
        # 检查请求是否成功
        if response.status_code == 200:
            tree = html.fromstring(response.content)
            count_div = tree.xpath(".//div[3]/div[2]/div/span[2]")
            count = count_div[0].xpath("string(.)")
            page_cnt = math.ceil(int(count) / 10)
            logger.info("共找到房源:%s，页数:%s" % (count, page_cnt))
            return page_cnt
        else:
            logger.info("请求失败，状态码:", response.status_code)
            raise Exception(f"Failed to retrieve data: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.info("建立连接失败 %s, proxy: %s", e, proxy)
        raise requests.exceptions.RequestException


def get_house_info_f(file_path, file_path_bk):
    # 如果文件存在，则删除文件
    if os.path.isfile(file_path_bk):
        os.remove(file_path_bk)
    district_list = [
        "huangpu",
        "xuhui",
        "changning",
        "jingan",
        "putuo",
        "pudong",
        "hongkou",
        "yangpu",
        "minhang",
        "baoshan",
        "jiading",
        "jinshan",
        "songjiang",
        "qingpu",
        "fengxian",
        "chongming",
    ]
    cnt = 0
    url = "http://sh.fang.ke.com/loupan/district/nht1nht2nhs1co41pgpageno/"
    base_url = "http://sh.fang.ke.com"
    for idx, district in enumerate(district_list):
        update_proxies()
        s = requests.Session()
        s.headers.update(get_headers())
        url_default = url.replace("district", district).replace("pageno", str(1))
        page = get_max_page_f(url_default)
        numbers = list(range(1, page + 1))
        random.shuffle(numbers)
        for i in numbers:
            # 重试计数器
            max_retries = _max_attempt
            retries = 0
            dict = {}
            dlist = []
            url_re = url.replace("district", district).replace("pageno", str(i))
            s = requests.Session()
            s.headers.update(get_headers())
            while retries < max_retries:
                try:
                    time.sleep(random.randint(_min_delay, _max_delay))
                    proxy = get_proxy(proxies)
                    s.proxies = proxy
                    s.headers.update(get_headers())
                    res = s.get(url_re, timeout=_timeout)
                    logger.info("URL: %s, response: %s" % (url_re, res.status_code))
                    if res.status_code == 200:
                        tree = html.fromstring(res.content)
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
                            district_name = district_div[0].xpath("string(.)")
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

                            url_detail = base_url + href
                            s = requests.Session()
                            s.proxies = proxy
                            s.headers.update(get_headers())
                            res = s.get(url_detail, timeout=_timeout)

                            if res.status_code == 200:
                                tree = html.fromstring(res.content)
                                date_div = tree.xpath(
                                    "//div[2]/div[3]/div[2]/div/div[3]/ul/li[2]/div/span[2]"
                                )

                                if date_div:
                                    date = date_div[0].text_content().strip()
                                else:
                                    date = ""
                                longlat_div = tree.xpath("//div[2]/div[3]/div[1]/span")

                                longitude = (
                                    longlat_div[0].get("data-coord").split(",")[1]
                                )
                                latitude = (
                                    longlat_div[0].get("data-coord").split(",")[0]
                                )

                                dict = {
                                    "title": name,
                                    "house_type": resblock_type,
                                    "district": district_name,
                                    "bizcircle_name": street,
                                    "avg_price": unit_price,
                                    "area_range": area,
                                    "sale_status": sale_status,
                                    "open_date": date,
                                    "longitude": longitude,
                                    "latitude": latitude,
                                    "index": i,
                                }
                                dlist.append(dict)
                        df = pd.DataFrame(dlist)
                        df.to_csv(
                            file_path_bk, mode="a", index=False, header=(cnt == 0)
                        )
                        cnt = cnt + 1
                        break
                    else:
                        retries += 1
                        logger.info("请求失败，状态码:", res.status_code)
                        if retries == max_retries:
                            print(f"Max retries reached for page {i}, skipping...")
                except requests.exceptions.RequestException as e:
                    retries += 1
                    logger.info("建立连接失败 %s, proxy: %s", e, proxy)
                    if retries == max_retries:
                        print(f"Max retries reached for page {i}, skipping...")

    # 如果文件存在，则删除文件
    if os.path.isfile(file_path_bk):
        os.replace(file_path_bk, file_path)


@retry(
    wait=wait_random(min=_min_delay, max=_max_delay),
    stop=stop_after_attempt(_max_attempt),
    after=after_retry,
)
def fetch_house_info_s(url, item):
    dict = {}
    url_re = url.replace("data_id", str(item["data_id"]))

    session = requests.Session()
    proxy = get_proxy(proxies)
    session.proxies = proxy
    session.headers.update(get_headers())
    try:
        response = session.get(url_re, timeout=_timeout)
        logger.info("Url: %s, Proxy: %s" % (url_re, proxy))
        time.sleep(random.randint(_min_delay, _max_delay))  # 随机休眠
        if response.status_code == 200:
            tree = html.fromstring(response.content)
            unit_price_div = tree.xpath(
                '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquPrice clear"]/div[@class="fl"]/span[@class="xiaoquUnitPrice"]'
            )
            if len(unit_price_div) > 0:
                unit_price = unit_price_div[0].xpath("string(.)")
            else:
                unit_price = ""
                raise Exception("XPath query returned no results")
            deal_price_div = tree.xpath(
                '//div[@class="m-content"]/div[@class="box-l xiaoquMainContent"]/div[@class="frameDeal"]/div[@class="frameDealList"]/ol[@class="frameDealListItem"]/li[1]/div[@class="frameDealUnitPrice"]'
            )
            if len(deal_price_div) > 0:
                deal_price = deal_price_div[0].xpath("string(.)")
            else:
                deal_price = ""
            deal_date_div = tree.xpath(
                '//div[@class="m-content"]/div[@class="box-l xiaoquMainContent"]/div[@class="frameDeal"]/div[@class="frameDealList"]/ol[@class="frameDealListItem"]/li[1]/div[@class="frameDealDate"]'
            )
            if len(deal_date_div) > 0:
                deal_date = deal_date_div[0].xpath("string(.)")
            else:
                deal_date = ""
            lanlong_div = tree.xpath(
                '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemOneLine"]/div[@class="xiaoquInfoItem outerItem"][2]/span[@class="xiaoquInfoContent outer"]/span[@mendian]'
            )
            if len(lanlong_div) > 0:
                lanlong = lanlong_div[0].get("xiaoqu")
            else:
                lanlong = ""
            age_div = tree.xpath(
                '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][2]/div[@class="xiaoquInfoItem"][2]/span[@class="xiaoquInfoContent"]'
            )
            if len(age_div) > 0:
                age = age_div[0].xpath("string(.)")
            else:
                age = ""
            house_type_div = tree.xpath(
                '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][2]/div[@class="xiaoquInfoItem"][1]/span[@class="xiaoquInfoContent"]'
            )
            if len(house_type_div) > 0:
                house_type = house_type_div[0].xpath("string(.)")
            else:
                house_type = ""
            total_cnt_div = tree.xpath(
                '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][1]/div[@class="xiaoquInfoItem"][2]/span[@class="xiaoquInfoContent"]'
            )
            if len(total_cnt_div) > 0:
                total_cnt = total_cnt_div[0].xpath("string(.)")
            else:
                total_cnt = ""
            structure_div = tree.xpath(
                '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][1]/div[@class="xiaoquInfoItem"][1]/span[@class="xiaoquInfoContent"]'
            )
            if len(structure_div) > 0:
                structure = structure_div[0].xpath("string(.)")
            else:
                structure = ""

            dict = {
                "data_id": item["data_id"],
                "al_text": item["al_text"],
                "sell_cnt": item["sell_cnt"],
                "district": item["district"],
                "unit_price": unit_price,
                "deal_price": deal_price,
                "deal_date": deal_date,
                "lanlong": lanlong,
                "age": age,
                "total_cnt": total_cnt,
                "structure": structure,
                "house_type": house_type,
            }
        else:
            logger.info("请求失败，状态码:", response.status_code)
            raise Exception(f"Failed to retrieve data: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.info("建立连接失败 %s, proxy: %s", e, proxy)
        raise requests.exceptions.RequestException

    return dict


def fetch_houselist_s(url, page, complete_list, district_marker, file_path_s_cp):
    datalist = []
    df_complete = pd.DataFrame(complete_list)
    dlist = []
    numbers = list(range(1, page + 1))
    random.shuffle(numbers)
    for i in numbers:
        # 重试计数器
        max_retries = _max_attempt
        retries = 0
        while retries < max_retries:
            time.sleep(random.randint(_min_delay, _max_delay))
            url_re = url.replace("pgno", str(i))
            session = requests.Session()
            proxy = get_proxy(proxies)
            session.proxies = proxy
            session.headers.update(get_headers())
            try:
                response = session.get(url_re, timeout=_timeout)

                logger.info("Url: %s proxy: %s retry: %s" % (url_re, proxy, retries))

                # 检查请求是否成功
                if response.status_code == 200:
                    # 使用lxml解析HTML
                    tree = html.fromstring(response.content)

                    # 查找特定的div
                    # 假设我们要查找class为'target-div'的div
                    divs = tree.xpath(
                        '//div[@class="content"]/div[@class="leftContent"]/ul[@class="listContent"]/li[@class="clear xiaoquListItem"]'
                    )

                    if divs:
                        for div in divs:
                            dict = {}
                            # 获取data-id属性的值
                            data_id = div.get("data-id")
                            if data_id in datalist:
                                continue
                            if len(df_complete) > 0:
                                if data_id in df_complete["data_id"].values:
                                    continue
                            # 查找<li>标签下的<img>标签，并获取alt属性的值
                            img_tag = div.xpath('.//img[@class="lj-lazy"]')
                            if img_tag:
                                alt_text = img_tag[0].get("alt")
                            else:
                                continue
                            sell_cnt_div = div.xpath(
                                './/div[@class="xiaoquListItemRight"]/div[@class="xiaoquListItemSellCount"]/a/span'
                            )
                            if sell_cnt_div:
                                sell_cnt = sell_cnt_div[0].xpath("string(.)")
                            else:
                                sell_cnt = ""
                            district_div = div.xpath(
                                './/div[@class="info"]/div[@class="positionInfo"]/a[@class="district"]'
                            )
                            if district_div:
                                district = district_div[0].xpath("string(.)")
                            else:
                                district = ""

                            dict = {
                                "data_id": data_id,
                                "al_text": alt_text,
                                "sell_cnt": sell_cnt,
                                "district": district,
                                "marker": district_marker,
                            }
                            dlist.append(dict)
                            datalist.append(data_id)
                        break  # 成功找到divs，退出while循环
                    else:
                        retries += 1
                        logger.info("未找到目标<ul>标签")
                        if retries == max_retries:
                            print(f"Max retries reached for page {i}, breaking...")
                            return
                else:
                    retries += 1
                    logger.info("请求失败，状态码:", response.status_code)
                    if retries == max_retries:
                        print(f"Max retries reached for page {i}, breaking...")
                        return
            except requests.exceptions.RequestException as e:
                retries += 1
                logger.info("建立连接失败 %s, proxy: %s", e, proxy)
                if retries == max_retries:
                    print(f"Max retries reached for page {i}, breaking...")
                    return
    df_cp = pd.DataFrame(dlist)
    df_cp.to_csv(file_path_s_cp, mode="w", header=True)
    return dlist


@retry(
    wait=wait_random(min=_min_delay, max=_max_delay),
    stop=stop_after_attempt(_max_attempt),
    after=after_retry,
)
def get_max_page(url):
    session = requests.Session()
    proxy = get_proxy(proxies)
    session.proxies = proxy
    session.headers.update(get_headers())

    try:
        response = session.get(url, timeout=_timeout)
        time.sleep(random.randint(_min_delay, _max_delay))  # 随机休眠
        # 检查请求是否成功
        if response.status_code == 200:
            tree = html.fromstring(response.content)

            div = tree.xpath(
                '//div[@class="content"]/div[@class="leftContent"]/div[@class="resultDes clear"]/h2[@class="total fl"]/span'
            )
            if div:
                cnt = div[0].text_content().strip()
                page_no = math.ceil(int(cnt) / 30)
                logger.info("当前获取房源量为%s,总页数为%s" % (cnt, page_no))
            else:
                logger.info("XPath query returned no results %s" % (proxy))
                raise Exception("XPath query returned no results")
            return page_no
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            raise Exception(f"Failed to retrieve data: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logger.info("建立连接失败 %s, proxy: %s", e, proxy)
        raise requests.exceptions.RequestException


def houseinfo_to_csv_s(file_path, file_path_bk, file_path_s_cp):
    # 如果文件存在，则删除文件
    # if os.path.isfile(file_path_bk):
    #     os.remove(file_path_bk)
    # 发起HTTP请求
    district_list = [
        "chongming",
        "xuhui",
        "changning",
        "jingan",
        "putuo",
        "huangpu",
        "pudong",
        "hongkou",
        "yangpu",
        "minhang",
        "baoshan",
        "jiading",
        "jinshan",
        "songjiang",
        "qingpu",
        "fengxian",
    ]
    max_page = 100
    t = ToolKit("列表生成")
    houselist = []
    complete_list = []
    count = 0
    idx_cp = -1
    if os.path.isfile(file_path_s_cp):
        df_cp = pd.read_csv(file_path_s_cp)
        marker = df_cp.loc[0, "marker"]
        idx_cp = district_list.index(marker)

    # url = "https://sh.lianjia.com/xiaoqu/district/pgpgnobp0ep100/"
    url = "http://sh.ke.com/xiaoqu/district/pgpgnocro21/"
    for idx, district in enumerate(district_list):
        # 计数器
        counter = 0

        if os.path.isfile(file_path_bk):
            df_info_cp = pd.read_csv(file_path_bk)

            data_id_list = (
                df_info_cp.loc[
                    df_info_cp["district"] == map_value(district),
                    "data_id",
                ]
                .astype(str)
                .tolist()
            )
        else:
            data_id_list = []
        if idx < idx_cp:
            continue
        elif idx == idx_cp:
            houselist = df_cp.to_dict(orient="records")
            filtered_list = [
                item for item in houselist if str(item["data_id"]) not in data_id_list
            ]
            houselist = filtered_list.copy()
            if len(houselist) == 0:
                continue
            update_proxies()

        else:
            update_proxies()
            url_default = url.replace("pgno", str(1)).replace("district", district)
            max_page = get_max_page(url_default)
            url_re = url.replace("district", district)
            houselist = fetch_houselist_s(
                url_re, max_page, complete_list, district, file_path_s_cp
            )
            filtered_list = [
                item for item in houselist if str(item["data_id"]) not in data_id_list
            ]
            houselist = filtered_list.copy()
            if len(houselist) == 0:
                continue
        complete_list.extend(houselist)
        t.progress_bar(len(district_list), idx + 1)
        print("house list cnt is: ", len(houselist))

        # 设置并发数上限为6
        max_workers = _max_workers
        data_batch_size = 10
        list = []
        url_detail = "http://sh.ke.com/xiaoqu/data_id/"
        t1 = ToolKit("信息爬取")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务并获取Future对象列表
            futures = [
                executor.submit(fetch_house_info_s, url_detail, item)
                for item in houselist
            ]

            # 获取已完成的任务的结果
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    list.append(result)
                except Exception as e:
                    print(f"获取详细信息时出错: {e}")

                count += 1
                counter += 1
                t1.progress_bar(len(houselist), counter)
                if counter % 200 == 0:
                    update_proxies()
                if count % data_batch_size == 0:
                    # 每处理完一批数据后，将数据写入CSV文件
                    df = pd.DataFrame(list)
                    df.to_csv(
                        file_path_bk,
                        mode="a",
                        index=False,
                        header=(
                            count == data_batch_size
                            and idx == 0
                            and len(data_id_list) == 0
                        ),
                    )
                    list = []  # 清空列表以继续下一批数据的处理

        # 处理剩余的数据
        if list:
            df = pd.DataFrame(list)
            df.to_csv(
                file_path_bk,
                mode="a",
                index=False,
                header=(
                    count < data_batch_size and idx == 0 and len(data_id_list) == 0
                ),
            )

    # 如果文件存在，则删除文件
    if os.path.isfile(file_path_bk):
        os.replace(file_path_bk, file_path)
    if os.path.isfile(file_path_s_cp):
        os.remove(file_path_s_cp)


def map_plot(df, legend_title, legend_fmt, png_path, k, col_formats):
    col_name = col_formats["count"]
    fmt_string = "{:." f"{legend_fmt}" "}"
    legend_kwargs = {"fmt": fmt_string, "title": legend_title}
    missing_kwds = {
        "facecolor": "lightgrey",
        "edgecolor": "k",
        "label": "Missing values",
    }
    ax = df.plot(
        column=col_name,
        cmap="RdYlGn_r",
        alpha=0.8,
        legend=True,
        linewidth=0.5,
        edgecolor="k",
        scheme="natural_breaks",
        k=k,
        figsize=(10, 10),
        legend_kwds=legend_kwargs,
        missing_kwds=missing_kwds,
    )
    ax.axis("off")
    # 添加annotation

    texts = []
    for idx, row in df.iterrows():
        centroid = row.geometry.centroid.coords[0]
        if "ratio" in col_formats:
            name_key = "name"
            count_key = col_formats["count"]
            ratio_key = col_formats["ratio"]
            text_f = f"{row[name_key]}\n{row[count_key]:.0f},{row[ratio_key]:.2%}"
        else:
            name_key = "name"
            count_key = col_formats["count"]
            text_f = f"{row[name_key]}\n{row[count_key]:.0f}"
        if not math.isnan(row[col_name]):
            text = ax.annotate(
                text=text_f,
                # text=f"{row['name']}\n{row['avg_price']:.0f}",
                xy=centroid,
                ha="center",
                fontsize=3,  # 设置字体大小
                color="black",  # 设置字体颜色为黑色
                weight="black",  # 设置字体粗细
                bbox=dict(
                    facecolor=(1, 1, 1, 0),
                    edgecolor=(1, 1, 1, 0),
                    boxstyle="round, pad=0.5",
                ),  # 设置注释框样式
            )
            texts.append(text)

    # 检查注释是否重叠并调整位置
    def check_and_adjust_annotations(
        texts,
        vertical_spacing=0.0001,
        horizontal_spacing=0.0000,
        min_fontsize=2,
        default_fontsize=3,
    ):
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
                    current_fontsize = (
                        text2.get_fontsize()
                        if text2.get_fontsize() is not None
                        else default_fontsize
                    )
                    # 调整字体大小和透明度
                    if current_fontsize > min_fontsize:
                        text2.set_fontsize(max(min_fontsize, current_fontsize - 0.01))
                    rect2 = text2.get_window_extent(renderer=renderer)

    check_and_adjust_annotations(texts)
    plt.savefig(png_path, dpi=250, bbox_inches="tight", pad_inches=0)


# 主程序入口
if __name__ == "__main__":
    plt.rcParams["font.family"] = "WenQuanYi Zen Hei"

    file_path = "./houseinfo/newhouse.csv"
    file_path_bk = "./houseinfo/newhouse_bk.csv"
    geo_path = "./houseinfo/shanghaidistrict.json"
    png_path = "./houseinfo/map_newhouse.png"
    png_path_s = "./houseinfo/map_secondhouse.png"
    png_path_s2 = "./houseinfo/map_secondhouse2.png"
    png_path_s3 = "./houseinfo/map_secondhouse3.png"
    file_path_s = "./houseinfo/secondhandhouse.csv"
    file_path_s_bk = "./houseinfo/secondhandhouse_bk.csv"
    file_path_s_cp = "./houseinfo/secondhandhouselist_cp.csv"

    # # 新房
    # get_house_info_f(file_path, file_path_bk)
    # # 二手
    houseinfo_to_csv_s(file_path_s, file_path_s_bk, file_path_s_cp)

    # 新房数据分析
    geo_data = gpd.read_file(geo_path, engine="pyogrio")
    df_new_house = pd.read_csv(file_path, usecols=[i for i in range(0, 11)])

    gdf_new_house = gpd.GeoDataFrame(
        df_new_house,
        geometry=gpd.points_from_xy(df_new_house.longitude, df_new_house.latitude),
        crs="EPSG:4326",
    )
    # 新房单价分析
    geo_data_f = geo_data[(geo_data.level == "town")]
    gdf_merged = gpd.sjoin(
        geo_data_f, gdf_new_house, how="inner", predicate="intersects"
    )
    gdf_agg = gdf_merged.groupby("adcode").median({"avg_price": "avg_price"}).round(-2)
    result = geo_data_f.merge(gdf_agg, how="left", left_on="adcode", right_on="adcode")
    col_formats = {"count": "avg_price"}
    map_plot(result, "单价", "0f", png_path, 8, col_formats)

    # 二手房数据分析
    df_second_hand_house = pd.read_csv(file_path_s, usecols=[i for i in range(0, 12)])
    df_second_hand_house = df_second_hand_house.dropna(subset=["lanlong"])

    # 处理二手房经纬度格式
    def extract_values(string):
        values = string.strip("[]").split(",")
        value1 = float(values[0])
        value2 = float(values[1])
        return value1, value2

    df_second_hand_house[["longitude", "latitude"]] = (
        df_second_hand_house["lanlong"].apply(extract_values).apply(pd.Series)
    )

    # 定义一个函数来替换中文字符
    def replace_non_numeric_characters(text, replacement=""):
        # 使用正则表达式匹配非数字字符
        numeric_text = re.sub(r"\D", "", str(text))
        # 检查字符串是否为空
        if numeric_text == "":
            return None  # 返回空字符串
        # 返回转换后的整数类型
        return int(numeric_text)

    df_second_hand_house["total_cnt"] = df_second_hand_house["total_cnt"].apply(
        replace_non_numeric_characters
    )
    df_second_hand_house["deal_price"] = df_second_hand_house["deal_price"].apply(
        replace_non_numeric_characters
    )
    gdf_second_hand_house = gpd.GeoDataFrame(
        df_second_hand_house,
        geometry=gpd.points_from_xy(
            df_second_hand_house["longitude"], df_second_hand_house["latitude"]
        ),
        crs="EPSG:4326",
    )
    # 处理板块级别数据
    exclude_values = [
        310104,
        310101,
        310106,
        310109,
        310105,
        310110,
        310107,
    ]  # 要排除的值的列表
    geo_data_s = geo_data[
        ((geo_data.level == "district") & (geo_data.adcode.isin(exclude_values)))
        | (
            (geo_data.level == "town")
            & (
                ~geo_data["parent"]
                .apply(lambda x: json.loads(x)["adcode"])
                .isin(exclude_values)
            )
        )
    ]
    gdf_merged = gpd.sjoin(
        geo_data_s, gdf_second_hand_house, how="inner", predicate="intersects"
    )
    # 二手房挂牌价分析
    gdf_merged = gdf_merged[gdf_merged["unit_price"] > 0]
    gdf_agg = (
        gdf_merged.groupby("adcode").median({"unit_price": "unit_price"}).round(-2)
    )
    result = geo_data_s.merge(gdf_agg, how="left", left_on="adcode", right_on="adcode")
    col_formats = {"count": "unit_price"}
    map_plot(result, "挂牌价", "0f", png_path_s, 8, col_formats)

    # 二手房成交价分析
    # gdf_merged = gpd.sjoin(
    #     geo_data_s, gdf_second_hand_house, how="inner", predicate="intersects"
    # )
    # gdf_merged = gdf_merged[gdf_merged["deal_price"] > 0]
    gdf_agg = (
        gdf_merged.groupby("adcode").median({"deal_price": "deal_price"}).round(-2)
    )
    result = geo_data_s.merge(gdf_agg, how="left", left_on="adcode", right_on="adcode")
    col_formats = {"count": "deal_price"}
    map_plot(result, "最近成交价", "0f", png_path_s2, 8, col_formats)

    # 二手房挂牌量分析
    # gdf_merged = gpd.sjoin(
    #     geo_data_s, gdf_second_hand_house, how="inner", predicate="intersects"
    # )
    # gdf_merged = gdf_merged[gdf_merged["sell_cnt"] > 0]
    gdf_agg = gdf_merged.groupby("adcode")[["sell_cnt", "total_cnt"]].sum().round(0)
    # 计算 sell_cnt / total_cnt 比例，但仅在 total_cnt 不为 0 的情况下
    gdf_agg["ratio"] = gdf_agg.apply(
        lambda row: row["sell_cnt"] / row["total_cnt"]
        if row["total_cnt"] != 0
        else None,
        axis=1,
    )
    result = geo_data_s.merge(gdf_agg, how="left", left_on="adcode", right_on="adcode")
    col_formats = {"count": "sell_cnt", "ratio": "ratio"}
    map_plot(result, "挂牌量", "0f", png_path_s3, 8, col_formats)

    # 房屋明细
    new_house_cnt = len(df_new_house)
    second_house_cnt = len(df_second_hand_house)
    agg_s = df_second_hand_house[["sell_cnt", "total_cnt"]].sum().round(0)
    sell_cnt = agg_s["sell_cnt"]
    total_cnt = agg_s["total_cnt"]
    lst_deal_price = df_second_hand_house[
        df_second_hand_house["data_id"] == 5011000020013
    ]["deal_price"]
    lst_deal_date = df_second_hand_house[
        df_second_hand_house["data_id"] == 5011000020013
    ]["deal_date"]
    x_sell_cnt = df_second_hand_house[df_second_hand_house["data_id"] == 5011000020013][
        "sell_cnt"
    ]
    lst_deal_price = lst_deal_price.iloc[0]
    lst_deal_date = lst_deal_date.iloc[0]
    x_sell_cnt = x_sell_cnt.iloc[0]
    result = [
        {
            "nhouse_cnt": new_house_cnt,
            "shouse_cnt": second_house_cnt,
            "sell_cnt": sell_cnt,
            "total_cnt": total_cnt,
        }
    ]
    df_result = pd.DataFrame.from_dict(result)
    df_result.to_csv("./houseinfo/df_result.csv", header=True)
    html_txt = """
                <!DOCTYPE html>
                <html>
                <head>
                        <style>
                            h1 {{
                                font-style: italic;
                                font-size: 18px;
                                color: #333; /* 假设的亮色模式字体颜色 */                                 
                            }}
                        </style>
                </head>                    
                <body>
                    <h1>新房在售{newhouse_cnt}个楼盘</h1>
                    <h1>二手房挂牌{sell_cnt}套, 总套数{total_cnt}, 其中X小区挂牌量{x_sell_cnt},最新成交均价{lst_deal_price},成交日期{lst_deal_date} </h1>
                </body>
                </html>
                """.format(
        newhouse_cnt=new_house_cnt,
        sell_cnt=sell_cnt,
        total_cnt=total_cnt,
        x_sell_cnt=x_sell_cnt,
        lst_deal_price=lst_deal_price,
        lst_deal_date=lst_deal_date,
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
    html_txt = html_txt + css
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
                        <img src="cid:image0" alt="Shanghai house info is as follows:" style="width:100%">
                        <figcaption> Shanghai new house distribution by street.</figcaption>
                    </picture>
                    <picture>
                        <!-- 默认模式下的图片 -->
                        <img src="cid:image1" alt="Shanghai house info is as follows:" style="width:100%">
                        <figcaption> Shanghai second-hand house unit price distribution by street.</figcaption>
                    </picture> 
                    <picture>
                        <!-- 默认模式下的图片 -->
                        <img src="cid:image2" alt="Shanghai house info is as follows:" style="width:100%">
                        <figcaption> Shanghai second-hand house deal price distribution by street.</figcaption>
                    </picture>  
                    <picture>
                        <!-- 默认模式下的图片 -->
                        <img src="cid:image3" alt="Shanghai house info is as follows:" style="width:100%">
                        <figcaption> Shanghai second-hand house sell cnt distribution by street.</figcaption>
                    </picture>                                                                
                </body>
            </html>
            """
    image_path = [png_path, png_path_s, png_path_s2, png_path_s3]
    MyEmail().send_email_embedded_image(
        "上海房产信息跟踪", html_txt + html_img, image_path
    )
