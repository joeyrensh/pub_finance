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
    "http://195.66.93.188:3128",
    "http://91.26.124.18:3128",
    "http://136.226.194.151:9480",
    "http://178.48.68.61:18080",
    "http://165.155.228.15:9400",
    "http://165.155.228.8:9480",
    "http://165.155.230.8:9400",
    "http://165.155.230.13:9400",
    "http://165.155.228.9:9480",
    "http://165.155.228.12:9400",
    "http://165.155.228.9:9400",
    "http://165.155.229.10:9480",
    "http://165.155.230.12:9400",
    "http://165.155.229.9:9400",
    "http://165.155.228.10:9400",
    "http://165.155.229.8:9480",
    "http://165.155.229.13:9480",
    "http://165.155.228.10:9480",
    "http://165.155.228.16:9480",
    "http://165.155.230.12:9480",
    "http://165.155.228.13:9400",
    "http://165.155.230.13:9480",
    "http://165.155.228.14:9400",
    "http://165.155.230.10:9400",
    "http://165.155.229.10:9400",
    "http://165.155.228.8:9400",
    "http://165.155.230.11:9480",
    "http://165.155.229.9:9480",
    "http://165.155.228.19:9480",
    "http://165.155.230.8:9480",
    "http://165.155.228.14:9480",
    "http://165.155.229.8:9400",
    "http://165.155.228.11:9480",
    "http://165.155.228.15:9480",
    "http://165.155.230.9:9400",
    "http://165.155.228.18:9400",
    "http://165.155.228.17:9480",
    "http://165.155.228.18:9480",
    "http://165.155.228.11:9400",
    "http://165.155.229.11:9480",
    "http://13.40.46.249:39593",
    "http://165.155.230.11:9400",
    "http://165.155.229.11:9400",
    "http://85.175.5.50:3128",
    "http://212.112.113.182:3128",
    "http://38.91.106.252:44211",
    "http://165.155.228.19:9400",
    "http://45.119.133.218:3128",
    "http://72.10.160.172:20679",
    "http://198.7.56.71:15538",
    "http://67.43.236.20:12335",
    "http://38.91.107.2:23292",
    "http://218.60.0.220:80",
    "http://152.42.170.187:9090",
    "http://72.10.164.178:16513",
    "http://72.10.160.94:9469",
    "http://72.10.160.170:13891",
    "http://178.234.40.180:3128",
    "http://72.10.164.178:23809",
    "http://72.10.160.173:21135",
    "http://67.43.228.253:22097",
    "http://189.125.109.66:3128",
    "http://72.10.164.178:14403",
    "http://43.134.229.98:3128",
    "http://177.139.195.10:3128",
    "http://67.43.228.252:5569",
    "http://35.73.28.87:3128",
    "http://128.199.121.61:9090",
    "http://146.190.80.158:9090",
    "http://165.155.229.12:9400",
    "http://165.155.229.12:9480",
    "http://209.200.246.237:9595",
    "http://222.109.192.34:8080",
    "http://103.191.165.23:8181",
    "http://165.155.230.9:9480",
    "http://139.178.67.134:10086",
    "http://72.10.160.90:7829",
    "http://66.29.129.52:40133",
    "http://152.70.235.185:9002",
    "http://3.9.71.167:80",
    "http://45.205.2.64:8088",
    "http://192.9.237.224:3128",
    "http://88.248.113.25:3310",
    "http://165.155.228.12:9480",
    "http://72.10.160.90:15417",
    "http://165.155.230.10:9480",
    "http://67.43.236.21:1283",
    "http://128.199.187.36:9090",
    "http://72.10.160.172:28073",
    "http://67.43.228.253:13143",
    "http://46.166.165.33:4000",
    "http://77.242.98.39:8080",
    "http://67.43.228.250:11399",
    "http://103.156.15.64:1080",
    "http://183.242.69.118:3218",
    "http://67.43.236.20:8519",
    "http://67.43.227.228:11347",
    "http://62.74.173.146:8080",
    "http://72.10.164.178:12389",
    "http://72.10.160.90:32831",
    "http://72.10.164.178:17487",
    "http://67.43.227.228:1467",
    "http://72.10.160.174:27817",
    "http://72.10.164.178:29123",
    "http://67.43.228.251:7309",
    "http://67.43.236.20:8429",
    "http://60.217.33.47:9999",
    "http://72.10.164.178:20655",
    "http://67.43.236.20:33161",
    "http://67.43.236.20:1283",
    "http://72.10.160.172:28191",
    "http://67.43.236.20:2155",
    "http://72.10.164.178:32303",
    "http://72.10.160.170:27817",
    "http://72.10.164.178:31767",
    "http://72.10.160.170:14309",
    "http://67.43.228.254:20541",
    "http://165.155.228.16:9400",
    "http://68.162.217.121:8080",
    "http://67.43.227.228:33215",
    "http://67.43.227.227:33111",
    "http://72.10.164.178:10155",
    "http://72.10.160.170:28191",
    "http://67.43.228.250:2147",
    "http://67.43.236.19:16385",
    "http://159.203.104.153:4550",
    "http://72.10.160.93:18769",
    "http://95.163.235.117:3128",
    "http://72.10.164.178:9083",
    "http://103.167.171.216:1111",
    "http://103.106.219.132:1111",
    "http://67.43.228.254:13429",
    "http://67.43.228.253:26347",
    "http://72.10.160.93:7823",
    "http://72.10.160.171:10383",
    "http://67.43.236.19:13853",
    "http://111.89.146.129:3128",
    "http://67.43.227.227:6639",
    "http://67.43.228.252:11399",
    "http://72.10.160.93:2245",
    "http://72.10.160.93:10305",
    "http://72.10.160.90:1169",
    "http://88.99.126.109:8080",
    "http://196.251.195.254:8083",
    "http://103.50.6.66:83",
    "http://161.34.36.157:3128",
    "http://161.34.34.169:3128",
    "http://72.10.164.178:2029",
    "http://41.242.44.9:8082",
    "http://67.43.228.253:12239",
    "http://152.42.186.41:9090",
    "http://67.43.228.252:15815",
    "http://212.115.232.79:31280",
    "http://72.10.160.92:28037",
    "http://18.135.133.116:80",
    "http://34.97.45.196:8561",
    "http://165.155.229.13:9400",
    "http://165.155.228.17:9400",
    "http://67.43.228.250:3449",
    "http://72.10.160.91:21515",
    "http://58.19.22.61:3128",
    "http://72.10.160.90:8949",
    "http://5.161.219.13:4228",
    "http://110.74.222.174:8082",
    "http://67.43.227.226:1273",
    "http://152.42.178.60:9090",
    "http://188.132.150.164:8080",
    "http://72.10.160.173:3037",
    "http://67.43.227.226:1905",
    "http://104.248.151.93:9090",
    "http://128.199.119.67:9090",
    "http://67.43.236.20:24309",
    "http://103.151.17.201:8080",
    "http://67.43.227.226:26553",
    "http://212.83.142.158:59974",
    "http://67.43.227.229:9149",
    "http://67.43.228.250:12563",
    "http://67.43.227.226:32919",
    "http://45.70.203.98:999",
    "http://72.10.160.171:29913",
    "http://103.48.69.173:83",
    "http://72.10.160.90:21311",
    "http://212.110.188.222:34411",
    "http://67.43.227.226:23805",
    "http://72.10.164.178:18697",
    "http://72.10.160.172:15433",
    "http://72.10.160.170:17373",
    "http://61.129.2.212:8080",
    "http://103.143.168.254:84",
    "http://79.106.108.150:8079",
    "http://103.18.232.153:8085",
    "http://128.199.118.49:9090",
    "http://67.43.227.228:28799",
    "http://67.43.228.253:27285",
    "http://1.179.231.130:8080",
    "http://67.43.236.20:13349",
    "http://103.156.70.70:8090",
    "http://67.43.227.230:10171",
    "http://67.43.227.226:14939",
    "http://67.43.236.18:8731",
    "http://72.10.160.174:12491",
    "http://72.10.160.93:22675",
    "http://72.10.164.178:4879",
    "http://67.43.228.254:5633",
    "http://95.216.208.103:8118",
    "http://67.43.236.19:18243",
    "http://103.184.56.122:8080",
    "http://191.37.66.225:8080",
    "http://103.156.140.203:8080",
    "http://72.10.160.173:30871",
    "http://103.158.162.18:8080",
    "http://72.10.160.90:1171",
    "http://103.233.103.238:5656",
    "http://45.123.142.46:8181",
    "http://201.91.82.155:3128",
    "http://94.232.11.178:46449",
    "http://72.10.164.178:6989",
    "http://72.10.164.178:30117",
    "http://67.43.228.254:1601",
    "http://112.19.241.37:19999",
    "http://15.206.25.41:3128",
    "http://72.10.160.90:26527",
    "http://72.10.164.178:31049",
    "http://67.43.228.253:13429",
    "http://180.191.20.206:8080",
    "http://67.43.227.227:15325",
    "http://72.10.164.178:13471",
    "http://116.80.60.128:3128",
    "http://8.153.12.119:3128",
    "http://72.10.160.170:12491",
    "http://72.10.164.178:25791",
    "http://103.106.195.41:32650",
    "http://161.34.35.226:3128",
    "http://72.10.160.91:7337",
    "http://94.43.164.242:8080",
    "http://103.107.85.249:1111",
    "http://101.43.41.171:9091",
    "http://72.10.164.178:4013",
    "http://18.135.133.116:3128",
    "http://128.199.253.195:9090",
    "http://185.200.38.199:8080",
    "http://103.153.137.5:8080",
    "http://38.159.232.139:999",
    "http://103.174.236.96:8080",
    "http://72.10.160.91:4591",
    "http://79.121.102.227:8080",
    "http://119.47.90.221:8080",
    "http://72.10.160.173:29383",
    "http://67.43.227.226:12953",
    "http://173.249.60.246:14344",
    "http://36.93.129.73:8080",
    "http://188.132.150.137:8080",
    "http://67.43.236.20:15603",
    "http://213.149.182.98:8080",
    "http://103.105.76.10:3125",
    "http://67.43.227.226:19457",
    "http://146.19.196.4:4555",
    "http://103.69.20.99:58080",
    "http://103.189.116.107:8080",
    "http://72.10.164.178:21979",
    "http://72.10.164.178:11979",
    "http://101.255.165.130:1111",
    "http://103.125.174.17:7777",
    "http://38.156.73.55:8080",
    "http://185.253.32.26:8080",
    "http://80.240.55.242:3128",
    "http://72.10.160.173:16819",
    "http://91.197.77.118:443",
    "http://38.156.191.228:999",
    "http://72.10.160.91:1327",
    "http://154.73.87.241:8080",
    "http://67.43.228.252:2247",
    "http://185.9.139.145:8080",
    "http://103.36.10.0:3125",
    "http://103.87.169.199:32650",
    "http://72.10.160.93:24735",
    "http://181.143.181.34:8080",
    "http://45.235.16.121:27234",
    "http://103.44.19.217:1111",
    "http://45.174.79.95:999",
    "http://185.200.38.140:8080",
    "http://103.125.174.59:8080",
    "http://209.14.84.51:8888",
    "http://190.61.61.210:999",
    "http://102.68.131.31:8080",
    "http://92.45.196.83:3310",
    "http://103.115.20.28:8090",
    "http://182.176.164.41:8080",
    "http://182.52.131.108:8180",
    "http://103.189.197.75:8181",
    "http://128.199.187.46:9090",
    "http://36.50.11.196:8080",
    "http://36.129.129.215:9000",
    "http://189.164.200.125:8080",
    "http://67.43.227.230:10451",
    "http://67.43.227.227:17071",
    "http://72.10.160.170:12689",
    "http://103.167.114.211:8080",
    "http://72.10.160.170:13539",
    "http://72.10.160.94:12223",
    "http://31.211.82.232:3128",
    "http://140.227.204.70:3128",
    "http://67.43.228.252:4173",
    "http://103.88.113.202:8080",
    "http://67.43.227.226:33215",
    "http://103.168.254.62:8080",
    "http://161.34.40.111:3128",
    "http://201.134.169.214:8204",
    "http://67.43.228.253:24141",
    "http://190.94.212.199:999",
    "http://123.200.7.110:8080",
    "http://41.254.56.34:1981",
    "http://116.80.92.225:3128",
    "http://110.78.85.161:8080",
    "http://72.10.160.173:24865",
    "http://191.102.248.7:8085",
    "http://67.43.228.253:13449",
    "http://121.239.40.158:8081",
    "http://47.108.217.224:443",
    "http://13.40.239.130:80",
    "http://160.248.187.248:3128",
    "http://119.235.209.77:3125",
    "http://187.1.57.222:8080",
    "http://114.252.232.21:9000",
    "http://67.43.236.20:10175",
    "http://103.70.170.163:8080",
    "http://136.232.116.2:48976",
    "http://109.164.38.189:2306",
    "http://190.94.213.48:999",
    "http://157.100.9.236:999",
    "http://195.25.20.155:3128",
    "http://36.93.22.154:8080",
    "http://109.109.166.176:8104",
    "http://202.179.90.217:58080",
    "http://101.51.107.187:8080",
    "http://128.199.254.13:9090",
    "http://64.227.6.0:4003",
    "http://119.18.149.110:5020",
    "http://193.233.84.107:1080",
    "http://103.173.77.165:8080",
    "http://87.242.9.167:53281",
    "http://45.178.52.25:999",
    "http://182.253.10.21:8080",
    "http://176.88.166.218:8080",
    "http://103.186.8.162:8080",
    "http://101.255.105.26:8181",
    "http://94.182.40.51:3128",
    "http://182.160.123.113:5020",
    "http://67.43.228.250:21863",
    "http://66.211.155.34:8080",
    "http://31.204.199.54:81",
    "http://5.58.25.124:8080",
    "http://103.159.47.34:83",
    "http://185.41.41.12:3128",
    "http://4.158.175.186:8080",
    "http://45.224.247.102:80",
    "http://103.172.42.235:1111",
    "http://103.164.223.53:8080",
    "http://85.117.63.37:8080",
    "http://103.84.177.28:8083",
    "http://72.10.160.90:20021",
    "http://154.73.111.133:1981",
    "http://204.199.81.94:999",
    "http://196.250.239.229:8787",
    "http://41.57.5.104:6060",
    "http://103.122.0.114:8181",
    "http://197.246.10.151:8080",
    "http://49.0.2.194:8090",
    "http://95.0.168.62:1976",
    "http://67.43.228.253:21355",
    "http://78.188.230.238:3310",
    "http://161.34.40.116:3128",
    "http://222.127.67.204:8085",
    "http://103.18.77.50:1111",
    "http://160.248.92.13:3128",
    "http://72.10.160.90:8325",
    "http://62.255.223.195:8080",
    "http://161.34.40.34:3128",
    "http://190.53.46.11:38525",
    "http://103.170.96.64:8090",
    "http://103.194.172.130:8080",
    "http://113.160.155.121:19132",
    "http://171.97.12.92:8080",
    "http://177.184.199.36:80",
    "http://103.159.46.41:83",
    "http://115.112.231.148:443",
    "http://155.138.218.57:8118",
    "http://190.2.212.94:999",
    "http://117.6.104.235:5102",
    "http://78.108.108.9:8080",
    "http://103.122.142.174:8080",
    "http://154.73.111.153:1981",
    "http://72.10.160.170:16077",
    "http://162.0.220.215:32497",
    "http://67.43.228.253:26413",
    "http://72.10.164.178:29105",
    "http://103.168.44.210:3127",
    "http://44.226.167.102:80",
    "http://103.147.246.185:3127",
    "http://43.153.207.93:3128",
    "http://176.99.2.43:1081",
    "http://38.7.31.230:999",
    "http://67.43.227.227:30565",
    "http://154.0.14.116:3128",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
