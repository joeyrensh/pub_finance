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
    "http://136.226.194.151:9480",
    "http://178.48.68.61:18080",
    "http://86.110.194.237:3128",
    "http://165.155.229.13:9480",
    "http://165.155.228.18:9480",
    "http://165.155.228.18:9400",
    "http://165.155.230.13:9400",
    "http://165.155.230.10:9480",
    "http://165.155.229.9:9400",
    "http://138.201.21.232:46766",
    "http://165.155.228.14:9480",
    "http://165.155.230.9:9400",
    "http://195.154.43.198:39266",
    "http://165.155.228.10:9400",
    "http://165.155.230.11:9400",
    "http://165.155.228.14:9400",
    "http://165.155.228.11:9480",
    "http://165.155.229.12:9480",
    "http://165.155.229.13:9400",
    "http://165.155.230.12:9480",
    "http://165.155.230.9:9480",
    "http://34.205.61.74:3128",
    "http://165.155.229.8:9480",
    "http://165.155.228.9:9400",
    "http://165.155.228.13:9480",
    "http://165.155.228.8:9480",
    "http://165.155.229.8:9400",
    "http://165.155.228.16:9400",
    "http://20.26.97.150:8080",
    "http://165.155.229.9:9480",
    "http://165.155.230.8:9480",
    "http://165.155.228.12:9480",
    "http://165.155.229.12:9400",
    "http://165.155.230.10:9400",
    "http://165.155.228.19:9480",
    "http://165.155.230.13:9480",
    "http://165.155.229.10:9400",
    "http://51.83.116.2:48232",
    "http://144.76.42.215:8118",
    "http://165.155.228.15:9480",
    "http://165.155.228.16:9480",
    "http://165.155.228.15:9400",
    "http://165.155.228.11:9400",
    "http://51.38.230.146:80",
    "http://212.83.137.142:54046",
    "http://165.155.230.12:9400",
    "http://165.155.228.8:9400",
    "http://165.155.228.13:9400",
    "http://165.155.230.11:9480",
    "http://104.129.194.46:11175",
    "http://85.210.121.11:8080",
    "http://152.70.177.163:8118",
    "http://199.48.129.114:3121",
    "http://165.155.230.8:9400",
    "http://165.155.228.12:9400",
    "http://165.155.228.10:9480",
    "http://77.242.98.39:8080",
    "http://124.243.133.226:80",
    "http://45.235.128.239:999",
    "http://52.222.28.135:443",
    "http://210.5.184.90:8080",
    "http://104.129.194.46:10013",
    "http://154.90.49.103:9090",
    "http://72.10.164.178:1655",
    "http://160.86.242.23:8080",
    "http://67.43.236.20:25499",
    "http://35.73.28.87:3128",
    "http://67.43.227.230:17119",
    "http://43.134.33.254:3128",
    "http://154.90.49.55:9090",
    "http://72.10.160.171:19295",
    "http://177.139.195.10:3128",
    "http://189.232.84.219:8080",
    "http://147.161.200.243:9480",
    "http://65.109.219.84:80",
    "http://152.70.235.185:9002",
    "http://37.46.135.225:3128",
    "http://67.43.228.253:10699",
    "http://165.155.228.9:9480",
    "http://72.10.160.173:14055",
    "http://88.248.113.25:3310",
    "http://4.158.61.222:8080",
    "http://165.155.228.19:9400",
    "http://78.188.230.238:3310",
    "http://67.43.227.226:9831",
    "http://72.10.164.178:19529",
    "http://101.200.161.122:28888",
    "http://47.90.205.231:33333",
    "http://183.102.65.250:8080",
    "http://85.132.37.9:1313",
    "http://72.10.160.90:24707",
    "http://212.83.142.114:54167",
    "http://38.183.146.77:8090",
    "http://78.187.15.92:3310",
    "http://72.10.164.178:25823",
    "http://67.43.227.229:13277",
    "http://67.43.227.228:27761",
    "http://38.156.74.49:8080",
    "http://67.43.227.227:1195",
    "http://182.253.7.146:8180",
    "http://67.43.228.254:29811",
    "http://72.10.164.178:10153",
    "http://180.191.51.166:8082",
    "http://67.43.228.250:28639",
    "http://31.133.0.163:33033",
    "http://104.129.194.46:10006",
    "http://72.10.164.178:5087",
    "http://67.43.227.228:7653",
    "http://72.10.160.90:20093",
    "http://67.43.227.227:6987",
    "http://72.10.164.178:13249",
    "http://67.43.236.18:25863",
    "http://67.43.227.227:9619",
    "http://218.60.0.220:80",
    "http://67.43.228.250:3455",
    "http://67.43.227.228:5101",
    "http://67.43.228.253:24705",
    "http://67.43.227.226:2253",
    "http://39.98.86.246:8118",
    "http://67.43.228.250:15091",
    "http://67.43.236.20:20217",
    "http://67.43.228.254:28353",
    "http://67.43.228.253:16075",
    "http://67.43.236.18:6715",
    "http://4.158.175.186:8080",
    "http://67.43.227.227:16687",
    "http://72.10.164.178:29579",
    "http://171.228.160.88:10089",
    "http://67.43.227.227:21595",
    "http://165.155.229.10:9480",
    "http://67.43.227.227:1529",
    "http://154.90.48.21:9090",
    "http://161.34.40.32:3128",
    "http://116.80.82.214:3128",
    "http://203.138.144.132:3128",
    "http://67.43.227.227:14537",
    "http://161.34.40.114:3128",
    "http://103.125.173.26:8080",
    "http://192.99.169.19:8445",
    "http://72.10.164.178:17939",
    "http://67.43.228.254:6703",
    "http://154.90.49.44:9090",
    "http://67.43.236.20:16355",
    "http://72.10.164.178:6591",
    "http://72.10.160.171:23339",
    "http://67.43.236.20:28047",
    "http://72.10.160.90:23569",
    "http://92.45.196.83:3310",
    "http://148.72.140.24:30110",
    "http://72.10.160.172:31751",
    "http://67.43.236.18:1509",
    "http://72.10.160.90:1063",
    "http://72.10.164.178:27559",
    "http://220.150.76.92:6000",
    "http://38.156.72.166:8080",
    "http://38.156.75.16:8080",
    "http://103.106.219.219:8080",
    "http://154.90.48.10:9090",
    "http://103.191.165.104:8080",
    "http://67.43.227.227:8287",
    "http://190.8.47.122:999",
    "http://67.43.228.253:16177",
    "http://103.145.34.162:8081",
    "http://183.242.69.118:3218",
    "http://67.43.227.228:19295",
    "http://67.43.227.226:6523",
    "http://67.43.236.20:9367",
    "http://187.49.86.114:8222",
    "http://67.43.227.227:24595",
    "http://82.157.143.14:8080",
    "http://103.59.163.130:32650",
    "http://72.10.164.178:1963",
    "http://67.43.236.19:1547",
    "http://67.43.228.250:19211",
    "http://67.43.236.21:13073",
    "http://67.43.228.254:1591",
    "http://72.10.160.90:13861",
    "http://67.43.227.228:24317",
    "http://72.10.160.171:5105",
    "http://15.235.12.19:3128",
    "http://67.43.228.254:1179",
    "http://67.43.227.229:20129",
    "http://43.153.207.93:3128",
    "http://8.153.12.119:3128",
    "http://38.180.154.4:3128",
    "http://186.86.143.161:999",
    "http://72.10.164.178:20941",
    "http://72.10.164.178:14213",
    "http://41.85.8.226:8080",
    "http://20.26.249.29:8080",
    "http://102.68.129.54:8080",
    "http://200.24.152.210:999",
    "http://185.4.201.49:55443",
    "http://67.43.236.20:3879",
    "http://124.158.153.218:8180",
    "http://67.43.227.229:13319",
    "http://67.43.236.18:15721",
    "http://103.162.62.92:8080",
    "http://67.43.228.252:29533",
    "http://103.174.237.67:8080",
    "http://67.43.228.253:28775",
    "http://67.43.236.18:3643",
    "http://72.10.160.92:24707",
    "http://202.179.95.174:58080",
    "http://189.125.109.66:3128",
    "http://38.242.215.185:8118",
    "http://67.43.228.253:26537",
    "http://67.43.236.20:10195",
    "http://72.10.164.178:25497",
    "http://103.172.23.94:1080",
    "http://67.43.227.227:23039",
    "http://45.70.203.98:999",
    "http://72.10.164.178:25189",
    "http://202.183.209.77:8080",
    "http://119.76.142.245:8080",
    "http://161.34.40.109:3128",
    "http://67.43.228.253:9943",
    "http://103.191.165.23:8181",
    "http://198.99.81.198:8080",
    "http://67.43.228.254:11071",
    "http://161.34.35.175:3128",
    "http://161.34.36.157:3128",
    "http://177.234.192.231:999",
    "http://67.43.228.251:1315",
    "http://189.240.60.168:9090",
    "http://46.166.165.33:4000",
    "http://36.93.130.218:66",
    "http://200.119.218.94:999",
    "http://192.145.206.192:8080",
    "http://160.248.187.248:3128",
    "http://72.10.164.178:31315",
    "http://202.93.244.194:8080",
    "http://180.191.32.5:8082",
    "http://119.47.90.240:1111",
    "http://72.10.164.178:25429",
    "http://223.25.110.227:8080",
    "http://67.43.227.226:27167",
    "http://67.43.228.253:26233",
    "http://103.156.141.130:1111",
    "http://192.203.0.122:999",
    "http://103.159.96.141:8080",
    "http://67.43.227.227:32317",
    "http://105.113.2.82:8080",
    "http://36.92.162.220:8080",
    "http://203.128.71.90:8080",
    "http://103.30.43.183:3128",
    "http://72.10.164.178:6871",
    "http://103.156.17.35:8181",
    "http://72.10.164.178:18783",
    "http://83.143.24.66:80",
    "http://203.81.87.186:10443",
    "http://72.10.164.178:32413",
    "http://67.43.236.20:22895",
    "http://72.10.164.178:1531",
    "http://186.65.104.52:2020",
    "http://106.227.95.142:3129",
    "http://38.156.72.59:8080",
    "http://114.130.154.130:58080",
    "http://193.233.84.119:1080",
    "http://67.43.228.252:18469",
    "http://203.176.129.85:8080",
    "http://45.4.148.70:8080",
    "http://112.78.131.6:8080",
    "http://84.204.138.54:8080",
    "http://103.156.248.27:8085",
    "http://72.10.160.171:24251",
    "http://148.72.140.24:10535",
    "http://189.240.60.164:9090",
    "http://72.10.164.178:8507",
    "http://72.10.160.90:2165",
    "http://80.240.55.242:3128",
    "http://67.43.227.227:25991",
    "http://67.43.228.250:5027",
    "http://202.173.217.4:8080",
    "http://45.173.12.141:1994",
    "http://200.94.102.11:999",
    "http://67.43.236.20:16191",
    "http://72.10.160.90:23837",
    "http://67.43.236.19:1509",
    "http://119.148.23.242:8000",
    "http://103.171.244.44:8088",
    "http://109.164.38.189:2306",
    "http://45.143.108.10:8080",
    "http://1.20.200.154:8081",
    "http://171.6.145.227:8080",
    "http://72.10.164.178:24315",
    "http://193.233.84.120:1080",
    "http://193.233.84.43:1080",
    "http://8.219.97.248:80",
    "http://38.44.237.156:999",
    "http://67.43.228.253:6879",
    "http://46.161.196.144:8080",
    "http://45.190.52.24:8080",
    "http://110.74.222.174:8082",
    "http://49.0.32.48:8000",
    "http://72.10.160.90:7283",
    "http://179.1.142.129:8080",
    "http://102.214.166.1:1981",
    "http://181.16.240.69:999",
    "http://192.145.205.97:8080",
    "http://58.137.174.101:8080",
    "http://195.133.28.139:3128",
    "http://72.10.160.90:17439",
    "http://101.108.14.90:8080",
    "http://45.180.140.241:8080",
    "http://49.0.2.194:8090",
    "http://103.81.254.249:8080",
    "http://103.65.238.174:8181",
    "http://67.43.228.253:13705",
    "http://103.237.144.232:1311",
    "http://67.43.227.226:3769",
    "http://103.181.168.202:8080",
    "http://116.212.110.18:58080",
    "http://103.217.217.106:8080",
    "http://200.215.229.3:999",
    "http://103.101.193.46:1111",
    "http://103.162.63.65:8080",
    "http://91.235.75.57:8282",
    "http://101.109.143.181:8080",
    "http://103.95.23.134:8080",
    "http://103.102.15.41:18181",
    "http://125.26.108.122:8080",
    "http://72.10.160.173:25421",
    "http://36.67.7.74:8080",
    "http://178.254.42.100:8118",
    "http://103.166.159.163:8080",
    "http://183.240.196.54:18080",
    "http://67.43.227.227:14771",
    "http://112.19.241.37:19999",
    "http://154.73.29.129:8080",
    "http://72.10.160.173:26121",
    "http://72.10.160.90:31353",
    "http://103.22.99.93:7777",
    "http://202.74.244.139:5020",
    "http://210.87.125.58:1080",
    "http://83.242.254.122:8080",
    "http://67.43.228.252:6937",
    "http://103.247.22.79:8080",
    "http://103.46.4.101:8080",
    "http://72.10.160.170:15465",
    "http://67.43.227.227:1829",
    "http://126.209.9.30:8080",
    "http://161.34.37.126:3128",
    "http://67.43.236.20:14927",
    "http://116.80.84.45:3128",
    "http://103.156.217.101:1111",
    "http://200.10.31.218:999",
    "http://181.214.208.191:8080",
    "http://177.75.1.33:8080",
    "http://5.56.124.176:6734",
    "http://162.240.154.26:3128",
    "http://72.10.160.91:21241",
    "http://190.107.232.138:999",
    "http://67.43.227.227:31057",
    "http://128.199.254.13:9090",
    "http://67.43.228.253:32679",
    "http://101.89.153.105:808",
    "http://89.237.32.33:37647",
    "http://114.130.153.122:58080",
    "http://182.50.65.145:8080",
    "http://103.26.108.118:84",
    "http://103.169.254.155:3127",
    "http://148.153.56.51:80",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
