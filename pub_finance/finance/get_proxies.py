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


def check_proxy_anonymity(url, headers, proxies):
    list = []

    for ip_port in proxies:
        try:
            proxy = {"https": ip_port, "http": ip_port}
            # proxy = {"http": ip_port}
            s = requests.Session()
            s.proxies = proxy
            s.headers.update(headers)
            # time.sleep(random.randint(1, 1))  # 随机休眠
            response = s.get(url, timeout=5, headers=headers)
            print("response code:", response.status_code)
            # print("cookies:", requests.cookies)
            print("response cookies:", response.cookies)
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
base_url = "http://sh.lianjia.com/xiaoqu/xuhui/"
# url = "http://sh.lianjia.com/xiaoqu/xuhui/pgpgnobp0ep100/"
headers = {
    "User-Agent": random.choice(user_agent_list),
    "Referer": "sh.lianjia.com",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-encoding": "gzip, deflate, br, zstd",
    "Accept-language": "zh-CN,zh;q=0.9",
    "Cookie": ("lianjia_uuid=%s;") % (uuid.uuid4()),
}

# 使用示例
# proxyscrape.com免费proxy: https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country=cn&protocol=http&proxy_format=protocolipport&format=text&anonymity=Elite,Anonymous&timeout=3000
# 站大爷免费proxy: https://www.zdaye.com/free/?ip=&adr=&checktime=&sleep=3&cunhuo=&dengji=&nadr=&https=1&yys=&post=&px=
proxies = [
    "http://111.59.4.88:9002",
    "http://183.215.23.242:9091",
    "http://115.223.31.72:20766",
    "http://221.6.139.190:9002",
    "http://153.101.67.170:9002",
    "http://116.63.129.202:6000",
    "http://60.188.102.225:18080",
    "http://123.126.158.50:80",
    "http://115.223.31.40:32768",
    "http://14.204.150.66:8080",
    "http://14.23.152.222:9090",
    "http://114.229.218.92:32936",
    "http://125.77.25.177:8080",
    "http://115.223.31.48:32650",
    "http://118.117.189.141:8089",
    "http://58.246.58.150:9002",
    "http://117.68.38.163:31250",
    "http://115.223.31.35:35010",
    "http://36.99.35.138:82",
    "http://117.68.38.160:20005",
    "http://61.186.243.6:9002",
    "http://118.31.165.157:443",
    "http://116.169.54.253:8080",
    "http://114.229.218.109:33945",
    "http://14.204.150.67:8080",
    "http://117.40.32.133:8080",
    "http://220.248.70.237:9002",
    "http://180.103.181.10:80",
    "http://60.204.145.212:8888",
    "http://14.204.150.68:8080",
    "http://222.88.167.22:9002",
    "http://58.20.248.139:9002",
    "http://223.113.80.158:9091",
    "http://117.68.38.152:22589",
    "http://182.46.96.179:1080",
    "http://123.169.118.116:1080",
]

print(check_proxy_anonymity(url, headers, proxies))
