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
    "http://163.172.31.44:80",
    "http://136.226.194.151:9480",
    "http://91.26.124.18:3128",
    "http://165.155.228.8:9480",
    "http://165.155.228.18:9480",
    "http://165.155.230.11:9480",
    "http://165.155.229.8:9400",
    "http://165.155.228.13:9400",
    "http://165.155.230.8:9480",
    "http://165.155.229.11:9400",
    "http://165.155.228.14:9480",
    "http://165.155.228.12:9480",
    "http://165.155.230.13:9400",
    "http://165.155.230.10:9480",
    "http://165.155.229.12:9480",
    "http://165.155.229.10:9480",
    "http://165.155.230.10:9400",
    "http://165.155.230.8:9400",
    "http://38.180.17.233:3128",
    "http://165.155.228.16:9480",
    "http://165.155.228.11:9480",
    "http://165.155.228.8:9400",
    "http://165.155.228.17:9400",
    "http://165.155.230.9:9400",
    "http://165.155.228.10:9480",
    "http://165.155.228.16:9400",
    "http://165.155.228.19:9480",
    "http://192.99.178.146:3128",
    "http://165.155.228.9:9400",
    "http://165.155.229.13:9480",
    "http://165.155.229.9:9480",
    "http://165.155.230.11:9400",
    "http://165.155.228.10:9400",
    "http://165.155.229.8:9480",
    "http://104.129.194.46:10089",
    "http://5.161.104.243:10001",
    "http://165.155.230.12:9400",
    "http://165.155.229.13:9400",
    "http://165.155.228.18:9400",
    "http://51.77.211.107:80",
    "http://165.155.230.13:9480",
    "http://163.172.33.137:4383",
    "http://165.155.229.9:9400",
    "http://206.189.135.6:3128",
    "http://15.235.12.19:3128",
    "http://165.155.228.9:9480",
    "http://144.86.187.62:3129",
    "http://139.84.208.110:3129",
    "http://165.155.228.12:9400",
    "http://109.230.92.50:3128",
    "http://89.169.36.16:3128",
    "http://148.113.1.131:3128",
    "http://45.119.133.218:3128",
    "http://165.155.230.9:9480",
    "http://124.243.133.226:80",
    "http://165.155.229.10:9400",
    "http://165.155.229.12:9400",
    "http://165.155.228.13:9480",
    "http://186.159.116.22:3128",
    "http://116.114.20.148:3128",
    "http://103.172.42.187:1111",
    "http://86.104.75.109:1080",
    "http://194.68.171.247:3128",
    "http://165.155.230.12:9480",
    "http://160.86.242.23:8080",
    "http://157.90.6.20:8888",
    "http://218.60.0.220:80",
    "http://152.42.170.225:9090",
    "http://43.134.121.40:3128",
    "http://43.134.32.184:3128",
    "http://177.139.195.10:3128",
    "http://72.10.160.93:7213",
    "http://152.42.186.41:9090",
    "http://45.238.118.156:27234",
    "http://67.43.228.250:18335",
    "http://189.232.97.26:8080",
    "http://128.199.119.67:9090",
    "http://109.72.66.216:8080",
    "http://72.10.164.178:13549",
    "http://72.10.160.90:31367",
    "http://152.70.235.185:9002",
    "http://54.39.163.156:3128",
    "http://128.199.187.36:9090",
    "http://67.43.227.227:7985",
    "http://72.10.164.178:1563",
    "http://47.103.103.132:8443",
    "http://47.115.173.217:8989",
    "http://128.199.187.46:9090",
    "http://165.155.228.14:9400",
    "http://148.72.165.7:30118",
    "http://67.43.227.227:23285",
    "http://116.202.113.187:60734",
    "http://60.217.33.47:9999",
    "http://18.181.154.135:8888",
    "http://116.202.113.187:60140",
    "http://210.61.207.92:80",
    "http://8.153.12.119:3128",
    "http://188.166.197.129:3128",
    "http://147.75.92.251:9443",
    "http://128.199.118.49:9090",
    "http://179.96.28.58:80",
    "http://91.203.179.72:65056",
    "http://152.42.170.187:9090",
    "http://178.48.68.61:18080",
    "http://123.126.158.50:80",
    "http://88.248.113.25:3310",
    "http://88.225.212.143:3310",
    "http://34.97.45.196:8561",
    "http://117.176.129.7:3128",
    "http://72.10.160.170:10177",
    "http://35.73.28.87:3128",
    "http://67.43.228.250:4377",
    "http://103.189.250.67:8080",
    "http://128.199.121.61:9090",
    "http://72.10.160.172:4837",
    "http://72.10.164.178:3207",
    "http://103.159.96.146:3128",
    "http://67.43.236.20:6147",
    "http://67.43.227.227:13373",
    "http://188.255.118.103:8080",
    "http://146.190.80.158:9090",
    "http://72.10.164.178:30019",
    "http://103.209.36.58:81",
    "http://125.25.43.56:8080",
    "http://36.93.32.137:8080",
    "http://61.129.2.212:8080",
    "http://45.189.118.214:999",
    "http://72.10.164.178:4531",
    "http://85.209.195.77:8080",
    "http://189.125.109.66:3128",
    "http://128.199.113.85:9090",
    "http://116.198.36.86:8080",
    "http://92.45.196.83:3310",
    "http://67.43.236.20:13049",
    "http://161.34.40.117:3128",
    "http://67.43.236.20:26573",
    "http://72.10.160.173:15543",
    "http://103.156.141.130:1111",
    "http://116.80.47.29:3128",
    "http://67.43.228.253:32195",
    "http://121.41.67.116:8080",
    "http://116.80.47.24:3128",
    "http://78.108.109.0:8080",
    "http://5.166.47.95:8080",
    "http://72.10.164.178:30979",
    "http://116.202.113.187:60355",
    "http://78.189.212.252:3310",
    "http://72.10.160.90:31055",
    "http://103.187.86.27:8182",
    "http://27.109.125.195:443",
    "http://103.56.157.223:8080",
    "http://72.10.160.91:17343",
    "http://103.246.79.10:1111",
    "http://130.162.148.105:8080",
    "http://154.236.177.105:1976",
    "http://67.43.228.253:27491",
    "http://112.19.241.37:19999",
    "http://103.193.144.71:8080",
    "http://165.155.228.19:9400",
    "http://66.206.15.148:8138",
    "http://187.190.99.229:8080",
    "http://72.10.160.170:22685",
    "http://160.248.7.207:3128",
    "http://103.176.96.140:8082",
    "http://165.155.228.11:9400",
    "http://103.160.182.171:8080",
    "http://67.43.227.226:31901",
    "http://67.43.227.227:26879",
    "http://82.157.143.14:8080",
    "http://114.130.153.58:58080",
    "http://60.204.145.212:8888",
    "http://103.101.99.45:8080",
    "http://103.191.58.14:8080",
    "http://47.251.43.115:33333",
    "http://103.155.196.105:8080",
    "http://103.247.14.103:1111",
    "http://39.98.107.108:29999",
    "http://103.156.75.40:8181",
    "http://103.191.165.9:8080",
    "http://72.10.160.90:30163",
    "http://143.0.243.80:8080",
    "http://67.43.227.230:10989",
    "http://190.61.101.72:8080",
    "http://131.221.43.249:8080",
    "http://67.43.228.253:32273",
    "http://31.133.0.163:33033",
    "http://27.254.104.134:8080",
    "http://203.81.67.22:8080",
    "http://146.196.41.141:8080",
    "http://164.92.167.4:1194",
    "http://186.96.31.46:7070",
    "http://51.195.200.115:31280",
    "http://185.89.156.2:44224",
    "http://108.62.60.32:3128",
    "http://161.34.40.109:3128",
    "http://111.1.61.49:3128",
    "http://89.237.32.161:37647",
    "http://67.43.227.228:27403",
    "http://103.174.175.99:8085",
    "http://118.174.57.109:8081",
    "http://205.164.84.247:8591",
    "http://119.76.142.235:8080",
    "http://49.0.3.130:8090",
    "http://120.79.132.229:8081",
    "http://138.117.231.131:999",
    "http://78.188.230.238:3310",
    "http://176.99.2.43:1081",
    "http://144.86.187.40:3129",
    "http://202.154.18.141:8088",
    "http://144.86.187.58:3129",
    "http://67.43.228.253:4391",
    "http://220.150.76.92:6000",
    "http://103.137.83.120:8080",
    "http://67.43.227.226:6185",
    "http://67.43.227.227:11677",
    "http://103.9.188.228:8080",
    "http://116.80.47.14:3128",
    "http://88.99.126.109:8080",
    "http://92.247.12.136:9510",
    "http://165.16.6.153:1981",
    "http://114.129.2.82:8080",
    "http://180.191.51.166:8082",
    "http://8.222.170.141:8081",
    "http://116.212.140.118:8080",
    "http://45.174.95.242:8080",
    "http://220.184.127.125:3128",
    "http://46.209.16.180:3128",
    "http://112.198.179.57:8082",
    "http://36.89.16.186:8866",
    "http://95.214.123.11:8080",
    "http://128.199.253.195:9090",
    "http://143.255.111.112:3128",
    "http://190.94.212.240:999",
    "http://27.72.141.201:10003",
    "http://144.86.187.36:3129",
    "http://43.153.207.93:3128",
    "http://103.159.196.145:1080",
    "http://45.169.84.21:8080",
    "http://45.70.203.116:999",
    "http://38.159.227.230:999",
    "http://72.10.160.173:11419",
    "http://66.70.235.23:5454",
    "http://85.185.201.163:8080",
    "http://45.179.201.40:999",
    "http://103.162.63.163:8080",
    "http://36.89.209.146:8080",
    "http://67.43.228.254:24867",
    "http://103.22.99.42:8080",
    "http://161.34.40.111:3128",
    "http://72.10.160.91:6449",
    "http://165.16.27.105:1981",
    "http://72.10.160.173:11245",
    "http://179.1.141.10:8080",
    "http://176.213.141.107:8080",
    "http://181.78.82.211:999",
    "http://45.188.156.121:8088",
    "http://95.216.57.120:8292",
    "http://103.247.23.57:8081",
    "http://202.62.67.209:53281",
    "http://103.165.253.194:8080",
    "http://72.10.160.90:30769",
    "http://67.43.227.227:16531",
    "http://103.156.75.41:8181",
    "http://81.94.135.202:1256",
    "http://184.174.33.250:3128",
    "http://103.48.70.209:83",
    "http://103.124.139.212:1080",
    "http://171.97.85.246:8080",
    "http://67.43.227.227:19115",
    "http://116.163.1.85:9999",
    "http://192.145.206.192:8080",
    "http://157.20.218.31:8080",
    "http://72.10.160.90:11475",
    "http://122.222.186.86:8080",
    "http://38.188.178.250:999",
    "http://186.86.143.161:999",
    "http://116.107.217.134:10010",
    "http://160.19.231.226:8080",
    "http://222.127.153.184:8082",
    "http://119.110.71.161:63123",
    "http://193.233.84.86:1080",
    "http://161.34.35.226:3128",
    "http://45.167.93.234:999",
    "http://144.86.187.41:3129",
    "http://179.48.80.9:8085",
    "http://186.148.181.70:999",
    "http://103.163.134.95:8090",
    "http://185.65.247.133:48049",
    "http://185.41.41.12:3128",
    "http://103.247.22.151:7777",
    "http://111.1.61.47:3128",
    "http://103.159.46.2:83",
    "http://189.223.54.117:999",
    "http://103.44.19.220:3127",
    "http://102.222.252.182:9999",
    "http://138.0.231.202:999",
    "http://193.233.84.116:1080",
    "http://103.125.16.51:8080",
    "http://200.24.159.148:999",
    "http://190.119.76.150:8080",
    "http://161.34.40.113:3128",
    "http://103.190.171.241:1080",
    "http://126.209.9.30:8080",
    "http://129.152.20.44:6969",
    "http://182.253.42.148:8082",
    "http://114.7.160.142:8080",
    "http://188.246.163.163:41258",
    "http://54.70.120.91:3128",
    "http://185.73.103.23:3128",
    "http://202.180.20.11:55443",
    "http://67.43.227.228:26907",
    "http://116.80.47.17:3128",
    "http://103.127.97.187:3128",
    "http://45.236.107.160:999",
    "http://217.52.247.76:1981",
    "http://116.202.113.187:60606",
    "http://27.72.141.201:10054",
    "http://51.159.66.158:3128",
    "http://36.70.150.192:8080",
    "http://45.71.184.197:999",
    "http://103.118.46.12:32650",
    "http://103.81.64.52:8080",
    "http://112.78.40.210:8080",
    "http://88.255.106.27:8080",
    "http://103.152.101.107:8080",
    "http://1.0.171.213:8080",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
