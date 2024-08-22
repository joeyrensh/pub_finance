import requests
import math
from lxml import html
import time
import random
import logging
import sys
import uuid

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
            time.sleep(random.randint(1, 1))  # 随机休眠
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
    "cache-control": "max-age=0",
    "cookie": ("lianjia_uuid=%s;") % (uuid.uuid4()),
}

# 使用示例
# proxyscrape.com免费proxy: https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country=cn&protocol=http&proxy_format=protocolipport&format=text&anonymity=Elite,Anonymous&timeout=3000
# 站大爷免费proxy: https://www.zdaye.com/free/?ip=&adr=&checktime=&sleep=3&cunhuo=&dengji=&nadr=&https=1&yys=&post=&px=
proxies = [
    "http://119.96.188.171:30000",
    "http://183.234.85.26:9002",
    "http://118.117.189.83:8089",
    "http://121.234.73.216:9002",
    "http://117.68.38.145:29038",
    "http://221.231.20.3:9002",
    "http://117.68.38.140:22001",
    "http://183.132.67.64:8085",
    "http://52.82.123.144:3128",
    "http://155.126.176.23:11223",
    "http://155.126.176.23:8800",
    "http://117.68.38.153:30474",
    "http://112.3.21.226:8060",
    "http://116.63.129.202:6000",
    "http://123.13.218.68:9002",
    "http://125.77.25.177:8080",
    "http://36.111.151.156:80",
    "http://36.99.35.138:82",
    "http://125.77.25.177:8090",
    "http://118.117.189.39:8089",
    "http://125.77.25.178:8080",
    "http://222.88.167.22:9002",
    "http://221.6.139.190:9002",
    "http://203.110.176.69:8111",
    "http://119.96.195.62:30000",
    "http://117.68.38.137:33333",
    "http://119.96.235.202:30000",
    "http://125.77.25.178:8090",
    "http://123.126.158.50:80",
    "http://14.23.152.222:9090",
    "http://222.89.237.101:9002",
    "http://111.59.4.88:9002",
    "http://113.121.66.250:1080",
]

print(check_proxy_anonymity(url, headers, proxies))
