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
    "http://136.226.194.151:9480",
    "http://158.255.212.55:3256",
    "http://149.154.157.17:11222",
    "http://13.48.148.35:3128",
    "http://51.250.46.17:1080",
    "http://165.155.229.13:9400",
    "http://165.155.228.9:9400",
    "http://165.155.228.10:9400",
    "http://165.155.229.11:9480",
    "http://165.155.228.14:9400",
    "http://165.155.229.9:9480",
    "http://165.155.229.12:9480",
    "http://165.155.228.19:9480",
    "http://165.155.229.8:9400",
    "http://165.155.228.11:9480",
    "http://165.155.228.16:9400",
    "http://178.48.68.61:18080",
    "http://165.155.229.9:9400",
    "http://165.155.228.8:9400",
    "http://165.155.228.11:9400",
    "http://165.155.228.15:9480",
    "http://165.155.228.8:9480",
    "http://165.155.228.15:9400",
    "http://165.155.230.13:9480",
    "http://165.155.228.16:9480",
    "http://165.155.230.8:9400",
    "http://165.155.230.12:9400",
    "http://165.155.230.8:9480",
    "http://165.155.230.13:9400",
    "http://165.155.228.19:9400",
    "http://165.155.230.10:9400",
    "http://165.155.228.12:9400",
    "http://165.155.230.11:9480",
    "http://176.99.2.43:1081",
    "http://165.155.229.10:9400",
    "http://165.155.228.18:9480",
    "http://165.155.230.9:9400",
    "http://165.155.228.12:9480",
    "http://165.155.228.14:9480",
    "http://165.155.230.12:9480",
    "http://165.155.229.8:9480",
    "http://165.155.228.9:9480",
    "http://38.180.17.233:3128",
    "http://178.167.167.194:9002",
    "http://165.155.228.10:9480",
    "http://72.10.160.172:28073",
    "http://104.129.194.46:9401",
    "http://178.63.180.104:3128",
    "http://165.155.229.12:9400",
    "http://72.10.160.91:17883",
    "http://144.86.187.43:3129",
    "http://165.155.228.13:9480",
    "http://188.124.230.43:16066",
    "http://67.43.236.18:21559",
    "http://72.10.160.90:1593",
    "http://147.28.155.20:10082",
    "http://206.189.135.6:3128",
    "http://196.204.83.227:1976",
    "http://86.104.75.109:1080",
    "http://192.9.237.224:3128",
    "http://72.10.160.171:7567",
    "http://165.155.228.18:9400",
    "http://94.247.129.244:3128",
    "http://103.176.97.223:3127",
    "http://162.55.61.160:9000",
    "http://67.43.236.22:9769",
    "http://128.199.113.85:9090",
    "http://152.42.178.60:9090",
    "http://172.234.252.95:50514",
    "http://123.126.158.50:80",
    "http://128.199.121.61:9090",
    "http://165.155.230.9:9480",
    "http://128.199.118.49:9090",
    "http://128.199.253.195:9090",
    "http://157.230.38.173:3128",
    "http://165.155.229.13:9480",
    "http://218.60.0.220:80",
    "http://52.67.137.145:3128",
    "http://38.191.200.73:999",
    "http://160.86.242.23:8080",
    "http://165.155.230.11:9400",
    "http://128.199.187.36:9090",
    "http://38.191.200.77:999",
    "http://162.55.61.160:9001",
    "http://128.199.187.46:9090",
    "http://38.191.200.72:999",
    "http://38.191.200.74:999",
    "http://152.42.170.225:9090",
    "http://152.42.170.187:9090",
    "http://72.10.160.90:2651",
    "http://202.21.117.74:8080",
    "http://54.39.163.156:3128",
    "http://116.103.26.136:3128",
    "http://67.43.227.227:22685",
    "http://72.10.160.90:25813",
    "http://185.253.32.26:8080",
    "http://35.73.28.87:3128",
    "http://88.248.113.25:3310",
    "http://104.248.151.93:9090",
    "http://67.43.228.252:28971",
    "http://165.155.228.13:9400",
    "http://43.134.232.113:7070",
    "http://202.47.188.133:8090",
    "http://91.239.17.22:8080",
    "http://51.159.159.73:80",
    "http://72.10.164.178:4175",
    "http://72.10.160.93:1859",
    "http://189.125.109.66:3128",
    "http://67.43.236.18:19371",
    "http://72.10.160.170:26959",
    "http://115.147.36.37:8181",
    "http://39.152.55.125:1080",
    "http://92.45.196.83:3310",
    "http://72.10.160.171:5797",
    "http://67.43.236.20:3211",
    "http://61.129.2.212:8080",
    "http://162.240.154.26:3128",
    "http://46.29.237.14:9988",
    "http://89.46.249.248:25585",
    "http://72.10.160.174:14415",
    "http://185.116.236.104:3128",
    "http://128.199.254.13:9090",
    "http://88.225.212.143:3310",
    "http://82.118.225.151:8080",
    "http://67.43.227.227:22151",
    "http://67.43.228.251:14397",
    "http://177.139.195.10:3128",
    "http://77.235.31.24:8080",
    "http://72.10.164.178:27315",
    "http://112.19.241.37:19999",
    "http://103.209.36.58:81",
    "http://72.10.160.90:10975",
    "http://148.72.165.7:30118",
    "http://171.224.88.80:10033",
    "http://160.22.16.16:3128",
    "http://189.232.97.26:8080",
    "http://36.93.130.219:66",
    "http://119.95.175.255:8081",
    "http://38.191.200.75:999",
    "http://117.177.63.75:8118",
    "http://67.43.228.253:10547",
    "http://152.70.235.185:9002",
    "http://193.233.84.88:1080",
    "http://67.43.227.227:8517",
    "http://116.80.47.21:3128",
    "http://67.43.228.253:1291",
    "http://116.80.47.24:3128",
    "http://72.10.164.178:8457",
    "http://103.163.226.253:3125",
    "http://161.34.40.35:3128",
    "http://67.43.227.227:20675",
    "http://67.43.236.20:18081",
    "http://116.107.217.134:10044",
    "http://101.200.161.122:28888",
    "http://67.43.236.20:2179",
    "http://122.222.186.86:8080",
    "http://23.122.184.9:8888",
    "http://72.10.160.90:31063",
    "http://78.188.230.238:3310",
    "http://60.217.33.47:9999",
    "http://72.10.164.178:12695",
    "http://67.43.227.226:5201",
    "http://222.243.174.132:81",
    "http://77.77.210.90:21056",
    "http://86.109.3.27:10048",
    "http://67.43.227.227:7909",
    "http://72.10.160.172:14415",
    "http://14.110.253.16:9000",
    "http://171.224.88.80:10075",
    "http://103.162.54.117:8080",
    "http://66.206.15.147:8134",
    "http://72.10.160.90:13175",
    "http://72.10.164.178:7123",
    "http://111.1.61.47:3128",
    "http://185.191.236.162:3128",
    "http://72.10.164.178:12291",
    "http://45.237.75.6:8080",
    "http://67.43.227.227:27849",
    "http://36.93.129.73:8080",
    "http://116.202.113.187:60280",
    "http://72.10.164.178:9807",
    "http://103.101.193.38:1111",
    "http://38.183.144.117:1111",
    "http://111.62.115.86:80",
    "http://67.43.228.253:26119",
    "http://103.169.194.25:8080",
    "http://67.43.227.227:3027",
    "http://31.43.52.216:41890",
    "http://67.43.236.19:31621",
    "http://67.43.236.20:23827",
    "http://193.233.84.86:1080",
    "http://190.152.5.17:39888",
    "http://193.233.84.90:1080",
    "http://157.66.84.30:8080",
    "http://212.110.188.202:34409",
    "http://144.86.187.60:3129",
    "http://223.70.184.125:3128",
    "http://45.225.89.145:999",
    "http://72.10.164.178:20161",
    "http://72.10.164.178:5439",
    "http://103.133.223.20:8080",
    "http://125.99.106.250:3128",
    "http://67.43.236.19:11349",
    "http://209.146.18.230:8082",
    "http://175.100.91.80:8080",
    "http://116.80.47.29:3128",
    "http://27.72.141.201:10087",
    "http://116.80.84.43:3128",
    "http://200.63.107.118:8089",
    "http://36.67.8.169:8080",
    "http://103.176.96.230:8082",
    "http://203.154.162.230:443",
    "http://103.46.8.11:8088",
    "http://101.255.125.162:8081",
    "http://103.159.46.2:83",
    "http://212.110.188.216:34405",
    "http://161.34.40.117:3128",
    "http://43.153.208.148:3128",
    "http://72.10.160.90:24085",
    "http://67.43.228.253:22261",
    "http://185.138.120.109:8080",
    "http://188.132.150.135:8080",
    "http://62.201.251.217:8585",
    "http://181.233.62.9:999",
    "http://154.73.29.161:8080",
    "http://8.218.198.49:8888",
    "http://103.158.127.135:8080",
    "http://190.94.213.109:999",
    "http://62.3.30.120:8080",
    "http://47.88.31.196:8080",
    "http://72.10.160.170:23335",
    "http://171.224.88.80:10085",
    "http://46.173.211.221:12880",
    "http://62.109.15.27:3120",
    "http://103.70.147.233:8080",
    "http://176.119.20.81:8534",
    "http://129.222.202.169:80",
    "http://185.100.47.105:443",
    "http://102.68.128.210:8080",
    "http://41.242.44.9:8084",
    "http://85.235.150.219:3128",
    "http://72.10.164.178:4003",
    "http://103.40.121.31:8087",
    "http://187.190.127.212:8081",
    "http://103.4.76.58:8082",
    "http://182.140.146.149:3128",
    "http://45.233.169.25:999",
    "http://94.23.204.27:3128",
    "http://1.2.185.214:8080",
    "http://103.157.79.218:8080",
    "http://62.176.27.228:3128",
    "http://165.155.230.10:9480",
    "http://103.103.88.30:6969",
    "http://124.83.74.218:8082",
    "http://45.70.236.194:999",
    "http://147.45.73.176:8443",
    "http://190.107.232.138:999",
    "http://116.202.113.187:60316",
    "http://49.48.89.155:8080",
    "http://181.78.22.109:999",
    "http://193.233.84.93:1080",
    "http://45.70.236.192:999",
    "http://172.81.62.223:92",
    "http://49.48.203.97:8080",
    "http://103.106.218.219:8080",
    "http://8.212.18.158:808",
    "http://72.10.160.170:18077",
    "http://193.233.84.91:1080",
    "http://202.180.54.211:8080",
    "http://103.75.96.142:1111",
    "http://67.43.227.227:33201",
    "http://124.105.186.206:8181",
    "http://103.106.240.18:96",
    "http://43.153.207.93:3128",
    "http://67.43.227.227:2429",
    "http://117.176.129.7:3128",
    "http://72.10.160.90:26827",
    "http://67.43.227.226:15387",
    "http://103.139.127.244:8080",
    "http://103.188.175.35:80",
    "http://103.69.20.52:58080",
    "http://72.10.160.92:13175",
    "http://5.83.244.250:8080",
    "http://161.34.35.226:3128",
    "http://103.217.216.183:8080",
    "http://144.86.187.56:3129",
    "http://188.132.222.24:8080",
    "http://121.101.131.142:8181",
    "http://125.209.110.83:39617",
    "http://161.34.40.34:3128",
    "http://111.89.146.125:3128",
    "http://116.114.20.148:3128",
    "http://103.127.220.70:8181",
    "http://103.155.197.68:8181",
    "http://41.216.186.201:8181",
    "http://188.125.169.237:8080",
    "http://86.109.3.27:10075",
    "http://67.43.228.252:29825",
    "http://116.202.113.187:60219",
    "http://103.215.187.67:8080",
    "http://1.0.170.50:8080",
    "http://111.1.61.49:3128",
    "http://187.251.224.25:8081",
    "http://181.143.126.74:999",
    "http://13.126.79.133:80",
    "http://91.200.163.190:8088",
    "http://101.255.165.130:1111",
    "http://91.150.189.122:30389",
    "http://161.34.40.114:3128",
    "http://182.93.85.225:8080",
    "http://45.121.41.11:8080",
    "http://188.132.221.22:8080",
    "http://72.10.160.173:9215",
    "http://27.147.195.170:58080",
    "http://5.160.32.34:8080",
    "http://79.127.56.147:8080",
    "http://103.188.173.153:8080",
    "http://103.125.174.29:7777",
    "http://188.124.230.43:32199",
    "http://103.179.84.117:8080",
    "http://183.88.212.184:8080",
    "http://154.236.168.176:1981",
    "http://78.108.108.9:8080",
    "http://103.151.246.54:7777",
    "http://67.43.236.19:3655",
    "http://103.147.250.198:83",
    "http://67.43.227.226:23141",
    "http://103.189.197.64:7777",
    "http://103.159.194.121:8080",
    "http://103.191.254.2:8085",
    "http://67.43.227.226:31301",
    "http://36.50.11.196:8080",
    "http://67.43.228.253:8431",
    "http://103.167.71.34:8080",
    "http://103.188.136.44:32650",
    "http://36.89.209.146:8080",
    "http://185.141.213.130:8082",
    "http://13.126.79.133:1080",
    "http://160.19.169.208:8080",
    "http://200.111.232.94:8080",
    "http://103.167.31.157:8080",
    "http://202.51.106.229:8080",
    "http://103.59.163.130:32650",
    "http://144.86.187.45:3129",
    "http://103.156.233.157:3456",
    "http://119.40.98.29:20",
    "http://37.111.52.41:8080",
    "http://190.108.12.117:8080",
    "http://103.35.109.205:58080",
    "http://103.42.228.62:8080",
    "http://23.143.160.193:999",
    "http://43.243.140.58:10001",
    "http://36.88.129.142:7777",
    "http://103.143.197.218:8000",
    "http://72.10.160.174:26837",
    "http://201.91.82.155:3128",
    "http://187.102.217.2:999",
    "http://45.123.142.46:8181",
    "http://168.138.211.5:8080",
    "http://103.38.104.224:8989",
    "http://188.132.222.28:8080",
    "http://103.156.16.182:1111",
    "http://116.107.217.134:10029",
    "http://12.163.95.161:8080",
    "http://200.195.235.226:3128",
    "http://45.5.118.138:999",
    "http://45.235.16.121:27234",
    "http://103.176.97.166:8080",
    "http://161.34.34.169:3128",
    "http://152.172.65.178:999",
    "http://144.86.187.40:3129",
    "http://46.10.209.230:8080",
    "http://38.57.232.10:999",
    "http://125.212.200.103:9000",
    "http://103.175.240.67:8080",
    "http://103.70.93.77:8080",
    "http://103.153.62.158:3125",
    "http://103.65.238.51:8080",
    "http://138.204.154.189:11201",
    "http://154.73.28.89:8080",
    "http://118.97.164.19:8080",
    "http://103.159.194.249:8080",
    "http://67.43.228.253:20917",
    "http://103.153.246.54:8181",
    "http://103.88.90.129:8080",
    "http://62.255.223.195:8080",
    "http://103.124.137.31:3128",
    "http://103.81.254.249:8080",
    "http://160.248.92.13:3128",
    "http://45.133.107.234:81",
    "http://38.9.48.213:8080",
    "http://103.87.170.231:32650",
    "http://177.101.225.149:9090",
    "http://155.232.186.50:3128",
    "http://103.173.244.210:58080",
    "http://72.10.160.170:32007",
    "http://103.137.218.166:83",
    "http://45.70.201.253:999",
    "http://105.174.40.54:8080",
    "http://189.151.19.230:999",
    "http://103.103.88.100:8090",
    "http://103.179.84.50:8080",
    "http://131.0.207.140:8080",
    "http://36.91.155.42:8080",
    "http://94.68.245.147:8080",
    "http://181.174.225.202:999",
    "http://103.247.23.201:8080",
    "http://103.122.1.26:8080",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
