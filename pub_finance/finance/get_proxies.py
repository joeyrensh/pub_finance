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
    for idx, item in enumerate(proxies_list):
        proxy = {"https": item, "http": item}
        if check_proxy_anonymity(url, headers, proxy):
            dlist.append(item)
        t1.progress_bar(len(proxies_list), idx)

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
    "http://103.41.88.33:83",
    "http://45.187.165.5:8080",
    "http://67.43.227.227:23061",
    "http://88.255.102.123:8080",
    "http://67.43.228.253:6205",
    "http://67.43.227.227:25409",
    "http://72.10.160.171:24431",
    "http://67.43.228.251:20617",
    "http://67.43.227.226:3339",
    "http://111.89.146.125:3128",
    "http://67.43.236.22:4583",
    "http://111.89.146.129:3128",
    "http://72.10.164.178:18743",
    "http://183.88.241.167:8080",
    "http://212.123.230.250:8181",
    "http://103.167.168.11:5040",
    "http://72.10.160.91:5099",
    "http://61.129.2.212:8080",
    "http://72.10.160.90:10751",
    "http://79.106.229.118:8989",
    "http://161.34.40.36:3128",
    "http://72.10.164.178:5785",
    "http://186.24.9.204:9898",
    "http://43.225.67.222:80",
    "http://67.43.227.227:32607",
    "http://72.10.160.90:29385",
    "http://165.155.228.16:9480",
    "http://179.189.50.160:80",
    "http://128.199.253.195:9090",
    "http://72.10.164.178:16707",
    "http://15.235.153.57:8089",
    "http://72.10.160.173:15505",
    "http://201.222.83.146:999",
    "http://77.242.98.39:8080",
    "http://121.239.40.158:8081",
    "http://72.10.160.90:29039",
    "http://101.251.204.174:8080",
    "http://84.51.15.253:32650",
    "http://181.129.97.34:999",
    "http://202.61.120.182:8080",
    "http://150.107.136.205:39843",
    "http://67.43.236.20:11229",
    "http://212.252.71.9:8080",
    "http://160.20.165.231:8587",
    "http://67.43.228.250:21645",
    "http://67.43.227.227:11809",
    "http://120.28.195.40:8282",
    "http://103.123.168.202:3932",
    "http://190.26.255.30:999",
    "http://92.249.113.194:55443",
    "http://213.212.204.206:1976",
    "http://103.30.43.183:3128",
    "http://103.247.14.37:8080",
    "http://72.10.160.90:12195",
    "http://1.1.220.100:8080",
    "http://103.5.232.148:8080",
    "http://72.10.160.174:17717",
    "http://182.93.85.225:8080",
    "http://161.34.40.35:3128",
    "http://103.24.106.190:8080",
    "http://35.177.161.93:7777",
    "http://181.115.66.236:999",
    "http://188.132.222.44:8080",
    "http://72.10.160.91:12339",
    "http://103.159.195.29:8080",
    "http://157.66.16.48:8080",
    "http://1.1.189.58:8080",
    "http://212.110.188.195:34411",
    "http://186.115.202.103:8080",
    "http://112.78.47.188:8080",
    "http://120.28.213.45:8080",
    "http://201.65.173.180:8080",
    "http://103.70.79.2:8080",
    "http://92.45.196.83:3310",
    "http://5.104.83.232:8090",
    "http://103.164.223.53:8080",
    "http://159.65.166.126:8118",
    "http://67.43.228.253:30091",
    "http://102.39.215.83:9090",
    "http://45.4.201.141:999",
    "http://41.86.252.90:443",
    "http://103.189.250.69:7777",
    "http://103.152.238.180:8085",
    "http://186.96.101.75:999",
    "http://193.192.124.74:8080",
    "http://83.242.254.122:8080",
    "http://180.211.179.126:8080",
    "http://154.73.29.33:8080",
    "http://189.39.207.47:9090",
    "http://209.14.84.51:8888",
    "http://103.209.36.58:81",
    "http://104.37.102.130:8181",
    "http://161.34.34.169:3128",
    "http://134.209.28.98:3128",
    "http://103.87.148.40:1111",
    "http://67.43.236.18:31849",
    "http://103.144.147.18:8080",
    "http://72.10.160.173:16071",
    "http://90.156.194.72:8080",
    "http://201.47.88.21:3128",
    "http://165.16.6.153:1981",
    "http://103.152.116.82:80",
    "http://113.160.214.209:8080",
    "http://177.93.40.56:999",
    "http://75.128.125.149:8080",
    "http://72.10.160.173:13023",
    "http://157.15.51.18:8080",
    "http://103.102.15.41:18181",
    "http://114.9.52.134:8080",
    "http://102.215.198.97:1981",
    "http://103.56.157.223:8181",
    "http://27.147.131.122:8090",
    "http://38.156.235.113:999",
    "http://66.96.235.34:8080",
    "http://180.148.4.194:8080",
    "http://45.71.114.148:999",
    "http://103.48.71.130:83",
    "http://41.242.44.9:8084",
    "http://1.10.227.16:8080",
    "http://103.68.214.22:8080",
    "http://103.15.140.177:44759",
    "http://67.43.228.250:21863",
    "http://72.10.164.178:33083",
    "http://67.43.227.227:2115",
    "http://67.43.227.228:3339",
    "http://161.34.36.157:3128",
    "http://37.32.23.217:3128",
    "http://186.148.181.69:999",
    "http://103.36.10.110:8090",
    "http://157.120.44.212:3128",
    "http://94.182.40.51:3128",
    "http://177.43.72.250:3128",
    "http://103.171.244.40:8088",
    "http://204.157.170.38:8080",
    "http://45.71.186.183:999",
    "http://154.236.168.176:1976",
    "http://103.48.70.161:83",
    "http://144.86.187.39:3129",
    "http://103.169.186.151:3125",
    "http://45.174.57.26:999",
    "http://190.94.245.178:999",
    "http://177.38.72.49:9292",
    "http://154.0.14.116:3128",
    "http://179.57.170.143:999",
    "http://14.143.75.250:80",
    "http://147.78.169.80:8443",
    "http://103.81.64.52:8080",
    "http://45.189.252.233:999",
    "http://72.10.164.178:29819",
    "http://179.48.80.9:8085",
    "http://205.164.84.247:8591",
    "http://131.0.207.176:8080",
    "http://200.10.29.129:9991",
    "http://124.105.79.237:8080",
    "http://144.86.187.42:3129",
    "http://176.236.232.50:9090",
    "http://72.10.160.90:26173",
    "http://72.10.164.178:33113",
    "http://103.168.254.34:1111",
    "http://188.132.222.162:8080",
    "http://181.209.112.74:999",
    "http://72.10.160.170:12839",
    "http://120.28.139.63:8082",
    "http://72.10.164.178:32247",
    "http://67.43.228.250:10045",
    "http://143.208.84.2:8589",
    "http://103.174.238.98:2022",
    "http://67.43.227.227:1083",
    "http://67.43.236.20:9763",
    "http://72.10.160.172:19773",
    "http://72.10.164.178:8097",
    "http://103.178.42.3:8181",
    "http://218.60.0.214:80",
    "http://188.132.222.47:8080",
    "http://103.177.177.249:8080",
    "http://67.43.227.226:4063",
    "http://103.148.130.107:8080",
    "http://103.180.123.93:8080",
    "http://147.45.48.233:1080",
    "http://190.94.213.109:999",
    "http://72.10.160.170:19773",
    "http://188.132.150.41:8080",
    "http://103.124.199.116:8080",
    "http://180.190.200.77:8082",
    "http://67.43.227.228:3703",
    "http://103.103.88.100:8090",
    "http://67.43.227.228:28315",
    "http://45.161.161.216:5566",
    "http://67.43.227.226:14939",
    "http://187.19.200.217:8090",
    "http://94.131.203.7:8080",
    "http://197.232.85.163:8080",
    "http://103.69.20.52:58080",
    "http://67.43.236.19:10015",
    "http://67.43.236.22:23715",
    "http://111.68.25.49:8080",
    "http://180.211.183.2:8080",
    "http://116.58.239.198:80",
    "http://45.224.20.69:999",
    "http://103.133.223.226:3125",
    "http://103.86.116.21:8080",
    "http://36.95.12.154:8080",
    "http://103.124.137.203:3128",
    "http://157.159.10.86:80",
    "http://38.156.72.203:8080",
    "http://103.154.230.124:8080",
    "http://103.244.207.113:8080",
    "http://72.10.164.178:31759",
    "http://38.137.203.12:999",
    "http://197.246.10.149:8080",
    "http://70.90.138.109:8080",
    "http://45.150.26.170:8080",
    "http://196.219.202.74:8080",
    "http://41.254.48.54:19333",
    "http://38.10.69.101:9090",
    "http://190.211.250.139:999",
    "http://111.1.61.51:3128",
    "http://103.179.182.185:8181",
    "http://77.92.245.34:8080",
    "http://185.53.129.141:3128",
    "http://217.52.247.77:1981",
    "http://115.127.137.51:58080",
    "http://39.62.4.164:8080",
    "http://103.70.93.77:8080",
    "http://62.33.136.242:8080",
    "http://35.177.161.93:11063",
    "http://140.227.228.202:10101",
    "http://67.43.236.19:27015",
    "http://103.247.22.148:8080",
    "http://131.196.219.128:8080",
    "http://45.174.95.221:999",
    "http://188.132.221.135:8080",
    "http://103.153.136.10:8080",
    "http://178.212.53.26:41258",
    "http://103.167.31.156:8080",
    "http://119.40.98.29:20",
    "http://176.9.238.155:16379",
    "http://103.141.247.6:8080",
    "http://103.176.97.166:8080",
    "http://103.138.185.1:83",
    "http://187.103.105.22:8999",
    "http://72.10.160.173:1941",
    "http://38.156.236.162:999",
    "http://103.122.1.74:8181",
    "http://72.10.160.93:19895",
    "http://72.10.164.178:8897",
    "http://72.10.160.90:29337",
    "http://45.114.145.210:32650",
    "http://68.162.217.121:8080",
    "http://144.86.187.57:3129",
    "http://144.86.187.49:3129",
    "http://103.88.239.118:84",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
