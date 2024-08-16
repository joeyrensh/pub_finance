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
            response = s.get(url, timeout=3)
            print("response code:", response.status_code)
            if response.status_code != 200:
                print(f"Failed to retrieve data: {response.status_code}")
            tree = html.fromstring(response.content)
            div = tree.xpath(
                '//div[@class="content"]/div[@class="leftContent"]/div[@class="resultDes clear"]/h2[@class="total fl"]/span'
            )
            if div:
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
    "http://36.99.35.138:82",
    "http://125.77.25.177:8080",
    "http://115.223.31.74:27777",
    "http://183.60.141.41:443",
    "http://60.12.168.114:9002",
    "http://119.96.118.113:30000",
    "http://123.182.58.150:8089",
    "http://223.113.80.158:9091",
    "http://125.77.25.177:8090",
    "http://119.96.188.171:30000",
    "http://36.111.166.193:8889",
    "http://111.225.153.128:8089",
    "http://115.223.11.212:50000",
    "http://58.20.248.139:9002",
    "http://125.77.25.178:8090",
    "http://113.219.247.226:30000",
    "http://117.68.38.166:24877",
    "http://115.223.11.212:8103",
    "http://183.238.165.170:9002",
    "http://117.184.46.29:18080",
    "http://112.51.96.118:9091",
    "http://58.246.58.150:9002",
    "http://111.225.152.238:8089",
    "http://125.77.25.178:8080",
    "http://117.68.38.174:36278",
    "http://49.7.11.187:80",
    "http://220.248.70.237:9002",
    "http://123.182.58.191:8089",
    "http://153.101.67.170:9002",
    "http://221.6.139.190:9002",
    "http://61.186.243.6:9002",
    "http://118.117.189.45:8089",
    "http://218.23.15.154:9002",
    "http://155.126.176.23:11223",
    "http://123.13.218.68:9002",
    "http://222.88.167.22:9002",
    "http://183.234.215.11:8443",
    "http://119.96.113.193:30000",
    "http://116.63.129.202:6000",
    "http://183.238.163.8:9002",
    "http://222.89.237.101:9002",
    "http://111.59.4.88:9002",
    "http://113.121.66.250:1080",
]

dlist = check_proxy_anonymity(url, headers, proxies)
print(dlist)
