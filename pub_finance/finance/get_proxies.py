import requests
import math
from lxml import html
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

    for ip_port in proxies:
        try:
            proxy = {"https": ip_port, "http": ip_port}
            s.proxies = proxy
            s.headers.update(headers)
            # time.sleep(random.randint(1, 1))  # 随机休眠
            response = s.get(url, timeout=3)
            print("response code:", response.status_code)
            if response.status_code == 200:
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
            else:
                print(f"Failed to retrieve data: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Error testing proxy {ip_port}: {e}")
    return list


url = "http://sh.lianjia.com/xiaoqu/xuhui/pg1cro21/"
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
    "http://221.6.139.190:9002",
    "http://119.96.235.202:30000",
    "http://61.191.104.206:8060",
    "http://119.96.188.171:30000",
    "http://220.248.70.237:9002",
    "http://117.68.38.145:29038",
    "http://58.246.58.150:9002",
    "http://112.3.21.226:8060",
    "http://58.20.248.139:9002",
    "http://103.36.136.138:8090",
    "http://61.172.255.102:8060",
    "http://119.96.100.63:30000",
    "http://116.63.129.202:6000",
    "http://183.215.23.242:9091",
    "http://115.223.31.48:32650",
    "http://14.23.152.222:9090",
    "http://125.77.25.178:8090",
    "http://125.77.25.177:8090",
    "http://119.96.118.113:30000",
    "http://123.103.51.22:3128",
    "http://117.40.32.133:8080",
    "http://111.59.4.88:9002",
    "http://49.7.11.187:80",
    "http://183.238.165.170:9002",
    "http://36.99.35.138:82",
    "http://119.96.195.62:30000",
    "http://116.169.54.253:8080",
    "http://112.51.96.118:9091",
    "http://119.96.113.193:30000",
    "http://125.77.25.178:8080",
    "http://117.68.38.164:24161",
    "http://223.113.80.158:9091",
    "http://222.243.174.132:81",
    "http://117.68.38.137:22099",
    "http://113.219.247.226:30000",
    "http://1.94.31.35:8888",
    "http://117.68.38.144:30000",
    "http://153.101.67.170:9002",
    "http://182.46.96.179:1080",
    "http://123.169.118.116:1080",
]

print(check_proxy_anonymity(url, headers, proxies))
