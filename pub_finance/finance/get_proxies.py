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
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-encoding": "gzip, deflate, br, zstd",
    "Accept-language": "zh-CN,zh;q=0.9",
    # "Cookie": ("ke_uuid=%s;") % (uuid.uuid4()),
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
    "http://188.166.30.17:8888",
    "http://37.120.133.137:3128",
    "http://37.120.222.132:3128",
    "http://89.249.65.191:3128",
    "http://144.91.118.176:3128",
    "http://95.216.17.79:3888",
    "http://85.214.94.28:3128",
    "http://185.123.143.251:3128",
    "http://167.172.109.12:39452",
    "http://176.113.73.104:3128",
    "http://51.158.68.133:8811",
    "http://185.123.143.247:3128",
    "http://95.111.226.235:3128",
    "http://176.113.73.99:3128",
    "http://206.189.130.107:8080",
    "http://79.110.52.252:3128",
    "http://13.229.107.106:80",
    "http://118.99.108.4:8080",
    "http://13.229.47.109:80",
    "http://169.57.157.148:80",
    "http://51.158.68.68:8811",
    "http://167.172.109.12:40825",
    "http://119.81.189.194:80",
    "http://119.81.189.194:8123",
    "http://3.24.178.81:80",
    "http://119.81.71.27:80",
    "http://119.81.71.27:8123",
    "http://185.236.203.208:3128",
    "http://193.239.86.249:3128",
    "http://159.8.114.37:80",
    "http://185.123.101.174:3128",
    "http://222.129.38.21:57114",
    "http://185.236.202.205:3128",
    "http://193.56.255.179:3128",
    "http://35.180.188.216:80",
    "http://106.45.221.168:3256",
    "http://113.121.240.114:3256",
    "http://193.34.95.110:8080",
    "http://84.17.51.235:3128",
    "http://180.183.97.16:8080",
    "http://193.239.86.247:3128",
    "http://185.189.112.157:3128",
    "http://121.206.205.75:4216",
    "http://103.114.53.2:8080",
    "http://139.180.140.254:1080",
    "http://84.17.51.241:3128",
    "http://84.17.51.240:3128",
    "http://185.189.112.133:3128",
    "http://81.12.119.171:8080",
    "http://37.120.140.158:3128",
    "http://159.89.113.155:8080",
    "http://104.248.146.99:3128",
    "http://185.236.202.170:3128",
    "http://67.205.190.164:8080",
    "http://46.21.153.16:3128",
    "http://51.158.172.165:8811",
    "http://84.17.35.129:3128",
    "http://85.214.244.174:3128",
    "http://104.248.59.38:80",
    "http://12.156.45.155:3128",
    "http://161.202.226.194:8123",
    "http://167.172.109.12:41491",
    "http://167.172.109.12:39533",
    "http://115.221.242.131:9999",
    "http://125.87.82.86:3256",
    "http://159.8.114.37:8123",
    "http://183.164.254.8:4216",
    "http://169.57.157.146:8123",
    "http://94.100.18.111:3128",
    "http://18.141.177.23:80",
    "http://193.56.255.181:3128",
    "http://116.242.89.230:3128",
    "http://188.166.252.135:8080",
    "http://103.28.121.58:3128",
    "http://103.28.121.58:80",
    "http://119.84.215.127:3256",
    "http://217.172.122.14:8080",
    "http://79.122.230.20:8080",
    "http://167.172.109.12:46249",
    "http://176.113.73.102:3128",
    "http://88.99.10.252:1080",
    "http://167.172.109.12:37355",
    "http://193.239.86.248:3128",
    "http://113.195.224.222:9999",
    "http://112.98.218.73:57658",
    "http://15.207.196.77:3128",
    "http://223.113.89.138:1080",
    "http://36.7.252.165:3256",
    "http://113.100.209.184:3128",
    "http://185.38.111.1:8080",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
