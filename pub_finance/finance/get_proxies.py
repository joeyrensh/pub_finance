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
]

headers = {
    "User-Agent": random.choice(user_agent_list),
    "Referer": "www.baidu.com",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-encoding": "gzip, deflate, br, zstd",
    "Accept-language": "zh-CN,zh;q=0.9",
    "Cookie": ("lianjia_uuid=%s;") % (uuid.uuid4()),
}


def check_proxy_anonymity(url, headers, proxy):
    try:
        s = requests.Session()
        s.proxies = proxy
        s.headers.update(headers)
        response = s.get(url, timeout=5)
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

    for item in proxies_list:
        proxy = {"https": item, "http": item}
        if check_proxy_anonymity(url, headers, proxy):
            dlist.append(item)

    df = pd.read_csv("./houseinfo/proxies.csv", names=["proxy"], comment="#")
    pre_proxies = df["proxy"].tolist()
    filtered_list = [item for item in dlist if item not in pre_proxies]
    if len(filtered_list) > 0:
        df_result = pd.DataFrame(filtered_list)
        df_result.to_csv("./houseinfo/proxies.csv", mode="a", header=False, index=False)
    print(dlist)


# url = "http://sh.lianjia.com/xiaoqu/xuhui/pgpgnobp0ep100/"
url = "http://sh.ke.com/xiaoqu/xuhui/pg1cro21/"
proxies_list = [
    "http://42.193.222.229:80",
    "http://43.243.234.54:8000",
    "http://47.92.6.221:8089",
    "http://47.96.150.116:80",
    "http://43.243.234.244:8000",
    "http://8.208.90.194:8021",
    "http://8.208.85.34:2087",
    "http://223.244.226.26:7777",
    "http://47.97.160.213:80",
    "http://118.193.102.3:7890",
    "http://81.69.45.84:80",
    "http://47.96.43.57:80",
    "http://43.243.234.240:8000",
    "http://39.96.140.81:8118",
    "http://47.108.59.58:7890",
    "http://39.107.89.178:80",
    "http://43.138.208.113:8080",
    "http://121.41.57.227:80",
    "http://101.37.30.233:80",
    "http://101.37.27.26:80",
    "http://121.43.151.181:80",
    "http://121.43.34.98:80",
    "http://47.96.255.148:80",
    "http://121.40.103.179:80",
    "http://101.37.14.0:80",
    "http://101.37.81.87:80",
    "http://101.37.25.130:80",
    "http://121.43.33.241:80",
    "http://121.40.230.92:80",
    "http://47.96.38.129:80",
    "http://121.199.79.126:80",
    "http://121.41.56.210:80",
    "http://47.99.149.163:80",
    "http://121.41.58.172:80",
    "http://116.62.36.254:80",
    "http://121.43.150.92:80",
    "http://121.41.16.144:80",
    "http://47.99.134.192:80",
    "http://47.96.37.136:80",
    "http://47.97.17.51:80",
    "http://121.40.230.124:80",
    "http://47.97.4.45:80",
    "http://47.99.143.210:80",
    "http://101.37.18.127:80",
    "http://47.97.18.89:80",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
