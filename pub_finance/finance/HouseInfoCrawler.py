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
import math


""" 
上海区域地图数据：https://geo.datav.aliyun.com/areas_v3/bound/310000_full.json
链家数据：https://sh.fang.lianjia.com/loupan/minhang/nht1nht2nhs1pg1/?_t=1/

"""
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


# 定义一个函数来打印重试次数
def after_retry(retry_state):
    logger.info(f"Retrying... attempt number {retry_state.attempt_number}")


@retry(wait=wait_random(min=3, max=5), stop=stop_after_attempt(5), after=after_retry)
def get_max_page_f(url, headers, proxies):
    s = requests.Session()
    s.headers.update(headers)
    s.proxies = proxies
    response = s.get(url)
    time.sleep(random.randint(3, 5))  # 随机休眠
    # 检查请求是否成功
    if response.status_code != 200:
        print(f"Failed to retrieve data: {response.status_code}")
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    tree = html.fromstring(response.content)
    count_div = tree.xpath(".//div[3]/div[2]/div/span[2]")
    count = count_div[0].xpath("string(.)")
    page_cnt = math.ceil(int(count) / 10)
    logger.info("共找到房源:%s，页数:%s" % (count, page_cnt))
    return page_cnt


def get_house_info_f(file_path, file_path_bk, headers, proxies):
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
    url = "https://sh.fang.lianjia.com/loupan/district/nht1nht2nhs1co41pgpageno/"
    base_url = "https://sh.fang.lianjia.com"
    for idx, district in enumerate(district_list):
        url_default = url.replace("district", district).replace("pageno", str(1))
        page = get_max_page_f(url_default, headers, proxies)
        numbers = list(range(1, page + 1))
        random.shuffle(numbers)
        for i in numbers:
            time.sleep(random.randint(3, 5))
            dict = {}
            dlist = []
            url_re = url.replace("district", district).replace("pageno", str(i))
            s = requests.Session()
            s.headers.update(headers)
            s.proxies = proxies
            res = s.get(url_re)
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
                    res = s.get(url_detail)
                    if res.status_code == 200:
                        tree = html.fromstring(res.content)
                        date_div = tree.xpath(
                            "//div[2]/div[3]/div[2]/div/div[3]/ul/li[2]/div/span[2]"
                        )

                        # date = date_div[0].xpath("string(.)")
                        if date_div:
                            date = date_div[0].text_content().strip()
                        else:
                            date = ""
                        longlat_div = tree.xpath("//div[2]/div[3]/div[1]/span")

                        longitude = longlat_div[0].get("data-coord").split(",")[1]
                        latitude = longlat_div[0].get("data-coord").split(",")[0]

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
                df.to_csv(file_path_bk, mode="a", index=False, header=(cnt == 0))
                cnt = cnt + 1
    # 如果文件存在，则删除文件
    if os.path.isfile(file_path_bk):
        os.replace(file_path_bk, file_path)


@retry(wait=wait_random(min=3, max=5), stop=stop_after_attempt(5), after=after_retry)
def fetch_house_info_s(url, item, headers, proxies):
    # 添加请求头
    # headers = {
    #     "User-Agent": random.choice(user_agent_list),
    #     "Connection": "keep-alive",
    #     "Referer": "sh.lianjia.com",
    #     "Cookie": "lianjia_uuid=%s;lianjia_ssid=%s" % (uuid.uuid4(), uuid.uuid4()),
    # }
    dict = {}
    url_re = url.replace("data_id", item["data_id"])
    s = requests.Session()
    s.headers.update(headers)
    s.proxies = proxies
    response = s.get(url_re)
    time.sleep(random.randint(1, 3))  # 随机休眠
    if response.status_code == 200:
        logger.info("Url: %s" % (url_re))
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
        print("请求失败，状态码:", response.status_code)
        raise Exception(f"Failed to retrieve data: {response.status_code}")

    return dict


def fetch_houselist_s(url, page, complete_list, headers, proxies):
    datalist = []
    df_complete = pd.DataFrame(complete_list)
    dlist = []
    numbers = list(range(1, page + 1))
    random.shuffle(numbers)
    for i in numbers:
        # 重试计数器
        max_retries = 20
        retries = 0
        while retries < max_retries:
            time.sleep(random.randint(3, 5))

            url_re = url.replace("pgno", str(i))
            s = requests.Session()
            s.headers.update(headers)
            s.proxies = proxies
            response = s.get(url_re)
            logger.info("Url: %s retry: %s" % (url_re, retries))

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
                        }
                        dlist.append(dict)
                        datalist.append(data_id)
                    break  # 成功找到divs，退出while循环
                else:
                    retries += 1
                    logger.info("未找到目标<ul>标签")
                    print("未找到目标<ul>标签")
                    if retries == max_retries:
                        print(f"Max retries reached for page {i}, skipping...")
            else:
                retries += 1
                logger.info("请求失败，状态码:", response.status_code)
                print("请求失败，状态码:", response.status_code)
                if retries == max_retries:
                    print(f"Max retries reached for page {i}, skipping...")

    return dlist


@retry(wait=wait_random(min=3, max=5), stop=stop_after_attempt(5), after=after_retry)
def get_max_page(url, headers, proxies):
    s = requests.Session()
    s.headers.update(headers)
    s.proxies = proxies
    response = s.get(url)
    time.sleep(random.randint(3, 5))  # 随机休眠
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


def houseinfo_to_csv_s(file_path, file_path_bk, headers, proxies):
    # 如果文件存在，则删除文件
    if os.path.isfile(file_path_bk):
        os.remove(file_path_bk)
    # 发起HTTP请求
    district_list = [
        "huangpu",
        "pudong",
        "xuhui",
        "changning",
        "jingan",
        "putuo",
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
    max_page = 100
    t = ToolKit("列表生成")
    houselist = []
    complete_list = []
    count = 0
    # url = "https://sh.lianjia.com/xiaoqu/district/pgpgnobp0ep100/"
    url = "https://sh.lianjia.com/xiaoqu/district/pgpgnocro21/"
    for idx, district in enumerate(district_list):
        url_default = url.replace("pgno", str(1)).replace("district", district)
        max_page = get_max_page(url_default, headers, proxies)
        url_re = url.replace("district", district)
        houselist = fetch_houselist_s(url_re, max_page, complete_list, headers, proxies)
        complete_list.extend(houselist)
        t.progress_bar(len(district_list), idx + 1)
        print("complete list cnt is: ", len(houselist))

        # 设置并发数上限为6
        max_workers = 2
        data_batch_size = 10
        list = []
        url_detail = "https://sh.lianjia.com/xiaoqu/data_id/"
        t = ToolKit("信息爬取")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务并获取Future对象列表
            futures = [
                executor.submit(fetch_house_info_s, url_detail, item, headers, proxies)
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
                if count % data_batch_size == 0:
                    # 每处理完一批数据后，将数据写入CSV文件
                    df = pd.DataFrame(list)
                    df.to_csv(
                        file_path_bk,
                        mode="a",
                        index=False,
                        header=(count == data_batch_size),
                    )
                    list = []  # 清空列表以继续下一批数据的处理

        # 处理剩余的数据
        if list:
            df = pd.DataFrame(list)
            df.to_csv(file_path_bk, mode="a", index=False, header=False)

    # 如果文件存在，则删除文件
    if os.path.isfile(file_path_bk):
        os.replace(file_path_bk, file_path)


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
    # https://proxyscrape.com/free-proxy-list
    proxies = {
        "http": "http://133.18.234.13:80",
        # "https": "http://101.126.44.74:8080",
    }
    headers = {
        "User-Agent": random.choice(user_agent_list),
        "Connection": "keep-alive",
        "Referer": "sh.lianjia.com",
        "Cookie": "lianjia_uuid=%s;" % (uuid.uuid4()),
    }
    # # 新房
    get_house_info_f(file_path, file_path_bk, headers, proxies)
    # # 二手
    houseinfo_to_csv_s(file_path_s, file_path_s_bk, headers, proxies)

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
