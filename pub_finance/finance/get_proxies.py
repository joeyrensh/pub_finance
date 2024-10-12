import requests
import math
from lxml import html
import random
import logging
import sys
import uuid
import json
import time
import pandas as pd
from utility.toolkit import ToolKit

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
]

headers = {
    "User-Agent": random.choice(user_agent_list),
    "Referer": "sh.ke.com",
    "Host": "sh.ke.com",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-encoding": "gzip, deflate, br, zstd",
    "Accept-language": "zh-CN,zh;q=0.9",
    "Cookie": ("ke_uuid=%s;") % (uuid.uuid4()),
}


def check_proxy_anonymity(url, headers, proxy):
    try:
        s = requests.Session()
        s.proxies = proxy
        s.headers.update(headers)
        response = s.get(url, timeout=3)
        if response.status_code == 200:
            tree = html.fromstring(response.content)
            div = tree.xpath(
                '//div[@class="xiaoquListPage"]/div[@class="content"]/div[@class="leftContent"]/div[@class="resultDes clear"]/h2[@class="total fl"]/span'
            )
            if div:
                cnt = div[0].text_content().strip()
                page_no = math.ceil(int(cnt) / 30)
                print("当前获取房源量为%s,总页数为%s" % (cnt, page_no))
                print("proxy %s有效" % (proxy))
                return True
            else:
                print("proxy无效")
                return False
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"{proxy}: {e}")
        return False
    except Exception as e:
        print(f"发生了未知错误: {e}")
        return False


# ZDY: https://api.openproxylist.xyz/http.txt
def get_proxies_listv1(url):
    proxy_url = "https://api.openproxylist.xyz/http.txt"
    count = 0
    dlist = []
    target = 1
    while count < target:
        s = requests.session()
        response = s.get(proxy_url, timeout=3)
        if response.status_code == 200:
            proxy_list = [
                "http://" + proxy
                for proxy in response.content.decode("utf-8").strip().split("\n")
            ]
            for item in proxy_list:
                proxy = {"https": item, "http": item}
                # proxy = {"https": item}
                if check_proxy_anonymity(url, headers, proxy):
                    dlist.append(item)
                    count = count + 1
                if count == target:
                    break
            break
        else:
            print("proxy get failed..")
            break
    print(dlist)


# https://getproxy.bzpl.tech/get/
def get_proxies_listv2(url):
    proxy_url = "https://getproxy.bzpl.tech/get/"
    count = 0
    dlist = []
    target = 1
    while count < target:
        s = requests.session()
        time.sleep(random.randint(1, 3))  # 随机休眠
        response = s.get(proxy_url, timeout=3)
        if response.status_code == 200:
            json_str = response.content.decode("utf-8")
            json_data = json.loads(json_str)
            proxy = {
                "https": "http://" + json_data["proxy"],
                "http": "http://" + json_data["proxy"],
            }
            if check_proxy_anonymity(url, headers, proxy):
                dlist.append("http://" + json_data["proxy"])
                count = count + 1
    print(dlist)


# 使用示例
# proxyscrape.com免费proxy: https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country=cn&protocol=http&proxy_format=protocolipport&format=text&anonymity=Elite,Anonymous&timeout=3000
# 站大爷免费proxy: https://www.zdaye.com/free/?ip=&adr=&checktime=&sleep=3&cunhuo=&dengji=&nadr=&https=1&yys=&post=&px=


def get_proxies_listv3(proxies_list, url):
    dlist = []
    t1 = ToolKit("检验中......")
    df = pd.read_csv("./houseinfo/proxies.csv", names=["proxy"], comment="#")
    pre_proxies = df["proxy"].tolist()
    for idx, item in enumerate(proxies_list):
        proxy = {"https": item, "http": item}
        if check_proxy_anonymity(url, headers, proxy):
            if item not in pre_proxies:
                print("item: %s" % (item))
                df_result = pd.DataFrame([item])
                df_result.to_csv(
                    "./houseinfo/proxies.csv", mode="a", header=False, index=False
                )
                pre_proxies.append(item)
                dlist.append(item)
        t1.progress_bar(len(proxies_list), idx)

    print(dlist)


# url = "http://sh.lianjia.com/xiaoqu/xuhui/pgpgnobp0ep100/"
url = "http://sh.ke.com/xiaoqu/xuhui/pg1cro21/"
proxies_list = [
    "http://47.109.83.196:8800",
    "http://47.122.61.139:80",
    "http://39.104.27.89:3128",
    "http://47.122.5.165:4006",
    "http://47.121.133.212:4006",
    "http://39.102.209.128:8080",
    "http://47.105.122.72:9080",
    "http://39.102.208.23:8081",
    "http://39.102.211.64:8443",
    "http://39.104.59.56:8080",
    "http://39.102.208.236:80",
    "http://114.215.127.92:9100",
    "http://118.31.1.154:80",
    "http://39.102.209.163:3128",
    "http://47.119.22.92:8443",
    "http://36.134.91.82:8888",
    "http://39.102.214.199:8081",
    "http://47.122.65.254:8081",
    "http://121.227.31.32:8118",
    "http://39.102.213.3:8008",
    "http://39.104.23.154:8080",
    "http://39.102.213.3:8008",
    "http://47.116.181.146:8800",
    "http://39.175.89.211:30001",
    "http://39.104.57.33:8081",
    "http://47.122.31.59:8081",
    "http://39.104.16.201:8080",
    "http://47.92.219.102:3128",
    "http://47.92.143.92:80",
    "http://47.99.112.148:8060",
    "http://47.108.159.113:8008",
    "http://47.116.126.57:3128",
    "http://47.122.60.73:3128",
    "http://106.227.95.142:3129",
    "http://120.26.52.35:80",
    "http://39.102.213.187:8443",
    "http://47.119.164.33:8080",
    "http://39.102.209.121:8081",
    "http://39.102.208.149:3128",
    "http://47.104.27.165:80",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
