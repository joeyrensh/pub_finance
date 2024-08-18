import requests
import math
from lxml import html
import time
import random
import logging
import sys


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


def check_proxy_anonymity(url, headers, proxies):
    list = []
    s = requests.Session()
    s.headers.update(headers)

    for ip_port in proxies:
        try:
            proxy = {"https": ip_port, "http": ip_port}
            s.proxies = proxy
            time.sleep(random.randint(1, 3))  # 随机休眠
            response = s.get(url, timeout=3)
            print("response code:", response.status_code)
            if response.status_code != 200:
                print(f"Failed to retrieve data: {response.status_code}")
            tree = html.fromstring(response.content)
            div = tree.xpath(
                '//div[@class="content"]/div[@class="leftContent"]/div[@class="resultDes clear"]/h2[@class="total fl"]/span'
            )
            if div:
                cnt = div[0].text_content().strip()
                page_no = math.ceil(int(cnt) / 30)
                print("当前获取房源量为%s,总页数为%s" % (cnt, page_no))
                print("proxy %s有效" % (ip_port))
                list.append(ip_port)
            else:
                print("proxy无效")
        except requests.exceptions.RequestException as e:
            print(f"Error testing proxy {ip_port}: {e}")
    return list


url = "http://sh.lianjia.com/xiaoqu/pudong/pg1cro21/"
headers = {
    "User-Agent": random.choice(user_agent_list),
    "Connection": "keep-alive",
}

# 使用示例
# https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country=cn&protocol=http&proxy_format=protocolipport&format=text&anonymity=Elite,Anonymous&timeout=20000
proxies = [
    "http://61.186.243.6:9002",
    "http://153.101.67.170:9002",
    "http://61.129.2.212:8080",
    "http://118.117.189.223:8089",
    "http://58.246.58.150:9002",
    "http://218.23.15.154:9002",
    "http://220.248.70.237:9002",
    "http://115.223.11.212:80",
    "http://49.7.11.187:80",
    "http://60.12.168.114:9002",
    "http://125.77.25.177:8080",
    "http://123.103.51.22:3128",
    "http://117.68.38.160:39078",
    "http://221.230.7.39:9000",
    "http://175.6.171.225:30000",
    "http://117.68.38.180:22968",
    "http://119.96.188.171:30000",
    "http://125.77.25.178:8080",
    "http://101.126.44.74:8080",
    "http://121.227.31.32:8118",
    "http://58.20.248.139:9002",
    "http://1.94.31.35:8888",
    "http://115.223.11.212:8103",
    "http://59.175.199.130:7777",
    "http://121.224.6.215:8118",
    "http://117.68.38.152:39593",
    "http://115.223.11.212:50000",
    "http://183.236.232.160:8080",
    "http://223.113.80.158:9091",
    "http://36.99.35.138:82",
    "http://116.63.129.202:6000",
    "http://222.88.167.22:9002",
    "http://221.6.139.190:9002",
    "http://125.77.25.177:8090",
    "http://103.36.136.138:8090",
    "http://125.77.25.178:8090",
    "http://118.117.189.101:8089",
    "http://115.223.31.79:32650",
    "http://222.89.237.101:9002",
    "http://111.59.4.88:9002",
    "http://113.121.66.250:1080",
]

dlist = check_proxy_anonymity(url, headers, proxies)
print(dlist)
