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
    "http://165.155.228.9:9400",
    "http://165.155.230.12:9400",
    "http://165.155.230.8:9400",
    "http://165.155.228.19:9480",
    "http://165.155.230.9:9480",
    "http://165.155.230.11:9400",
    "http://165.155.229.11:9400",
    "http://165.155.228.14:9400",
    "http://165.155.229.13:9480",
    "http://165.155.230.11:9480",
    "http://165.155.230.13:9480",
    "http://165.155.228.8:9480",
    "http://165.155.230.12:9480",
    "http://165.155.228.18:9400",
    "http://165.155.228.11:9400",
    "http://165.155.229.10:9400",
    "http://165.155.228.13:9480",
    "http://165.155.230.9:9400",
    "http://165.155.228.15:9480",
    "http://165.155.228.9:9480",
    "http://165.155.229.9:9480",
    "http://165.155.228.10:9480",
    "http://165.155.228.16:9480",
    "http://165.155.229.10:9480",
    "http://165.155.228.11:9480",
    "http://165.155.228.10:9400",
    "http://165.155.229.8:9400",
    "http://165.155.229.9:9400",
    "http://165.155.229.11:9480",
    "http://165.155.229.8:9480",
    "http://165.155.228.16:9400",
    "http://165.155.229.12:9400",
    "http://104.129.194.46:10089",
    "http://165.155.228.18:9480",
    "http://165.155.228.8:9400",
    "http://165.155.228.13:9400",
    "http://165.155.228.14:9480",
    "http://165.155.229.13:9400",
    "http://165.155.230.10:9400",
    "http://165.155.228.12:9480",
    "http://72.10.164.178:30677",
    "http://165.155.230.13:9400",
    "http://188.166.197.129:3128",
    "http://165.155.229.12:9480",
    "http://165.155.228.12:9400",
    "http://212.112.113.182:3128",
    "http://67.43.228.253:29511",
    "http://67.43.227.227:22995",
    "http://67.43.227.227:28525",
    "http://165.155.228.15:9400",
    "http://72.10.164.178:12529",
    "http://67.43.228.253:1985",
    "http://165.155.230.10:9480",
    "http://152.70.235.185:9002",
    "http://72.10.160.93:19415",
    "http://67.43.228.253:31727",
    "http://67.43.227.227:5775",
    "http://67.43.236.20:23813",
    "http://67.43.228.254:30175",
    "http://67.43.227.228:29359",
    "http://72.10.164.178:25095",
    "http://67.43.227.227:22621",
    "http://72.10.160.171:18239",
    "http://157.230.89.122:18094",
    "http://45.236.105.81:999",
    "http://67.43.227.228:27921",
    "http://67.43.227.227:6733",
    "http://165.155.230.8:9480",
    "http://67.43.227.228:33033",
    "http://189.8.6.3:8080",
    "http://72.10.160.93:1447",
    "http://72.10.160.90:17949",
    "http://45.70.203.122:999",
    "http://67.43.236.20:14749",
    "http://67.43.228.254:13999",
    "http://67.43.228.251:12947",
    "http://72.10.164.178:13661",
    "http://72.10.164.178:29775",
    "http://171.228.161.102:10089",
    "http://72.10.164.178:3771",
    "http://45.6.224.76:999",
    "http://67.43.236.22:8603",
    "http://72.10.160.174:17901",
    "http://67.43.228.253:11815",
    "http://72.10.160.173:24729",
    "http://72.10.160.91:13721",
    "http://67.43.227.227:19383",
    "http://72.10.164.178:7371",
    "http://72.10.160.170:10637",
    "http://67.43.228.253:14157",
    "http://72.10.164.178:30729",
    "http://200.10.30.217:8083",
    "http://116.102.104.66:10001",
    "http://72.10.160.172:19953",
    "http://72.10.160.90:33255",
    "http://35.73.28.87:3128",
    "http://72.10.164.178:3713",
    "http://67.43.227.227:25323",
    "http://67.43.228.253:5101",
    "http://72.10.160.92:23361",
    "http://72.10.160.91:5167",
    "http://72.10.164.178:24875",
    "http://67.43.228.253:1291",
    "http://72.10.164.178:5339",
    "http://102.68.128.211:8080",
    "http://72.10.160.173:30599",
    "http://67.43.228.253:31049",
    "http://72.10.160.93:18003",
    "http://67.43.227.230:27921",
    "http://72.10.164.178:7497",
    "http://67.43.228.250:3485",
    "http://67.43.227.227:12213",
    "http://67.43.228.254:5209",
    "http://72.10.160.172:8213",
    "http://67.43.228.253:19877",
    "http://67.43.228.253:2887",
    "http://67.43.236.19:17617",
    "http://171.224.88.80:10007",
    "http://67.43.236.20:1069",
    "http://103.157.79.83:1111",
    "http://67.43.227.226:2183",
    "http://67.43.228.253:2021",
    "http://72.10.164.178:33127",
    "http://8.219.97.248:80",
    "http://99.8.168.181:32770",
    "http://12.15.68.21:8080",
    "http://72.10.160.173:17145",
    "http://67.43.228.251:30175",
    "http://72.10.164.178:9991",
    "http://103.93.93.146:8082",
    "http://72.10.160.170:13495",
    "http://116.202.217.96:1080",
    "http://103.47.175.161:83",
    "http://67.43.228.251:27863",
    "http://37.111.52.46:8081",
    "http://165.155.228.19:9400",
    "http://45.70.236.194:999",
    "http://177.53.155.32:999",
    "http://103.28.114.157:66",
    "http://45.169.98.122:8080",
    "http://72.10.160.172:1953",
    "http://59.152.104.29:8080",
    "http://202.168.69.227:8080",
    "http://72.10.164.178:2365",
    "http://67.43.228.254:18185",
    "http://198.24.187.93:8001",
    "http://200.10.29.129:999",
    "http://72.10.164.178:30223",
    "http://72.10.164.178:17647",
    "http://72.10.160.90:23449",
    "http://45.117.30.9:58081",
    "http://72.10.160.170:23449",
    "http://58.69.247.70:8083",
    "http://72.10.164.178:19467",
    "http://103.87.212.140:8999",
    "http://36.93.130.219:66",
    "http://72.10.160.174:3887",
    "http://67.43.227.226:23141",
    "http://67.43.227.227:7993",
    "http://67.43.228.251:11441",
    "http://208.67.28.28:58090",
    "http://45.188.156.121:8088",
    "http://111.1.61.52:3128",
    "http://103.26.110.202:84",
    "http://67.43.227.228:16557",
    "http://67.43.227.227:27921",
    "http://103.148.131.108:8080",
    "http://72.10.164.178:15215",
    "http://72.10.164.178:12759",
    "http://41.128.72.150:1981",
    "http://193.169.4.184:8091",
    "http://103.144.146.40:34343",
    "http://72.10.160.90:24447",
    "http://67.43.228.252:16975",
    "http://103.176.96.178:1111",
    "http://67.43.228.250:27863",
    "http://67.43.227.226:14665",
    "http://27.72.141.201:10003",
    "http://103.176.116.109:83",
    "http://103.124.137.31:3128",
    "http://116.107.217.134:10005",
    "http://102.214.165.241:1981",
    "http://72.10.160.174:22691",
    "http://200.114.87.4:8080",
    "http://67.43.228.253:9725",
    "http://190.8.164.64:999",
    "http://103.31.133.236:3128",
    "http://103.147.118.237:8080",
    "http://72.10.164.178:31487",
    "http://179.189.236.201:20183",
    "http://72.10.160.174:12511",
    "http://103.147.128.97:83",
    "http://171.224.88.80:10087",
    "http://72.10.164.178:7465",
    "http://72.10.160.90:4387",
    "http://67.43.236.22:23715",
    "http://72.10.164.178:14803",
    "http://119.40.98.29:20",
    "http://45.123.142.2:8181",
    "http://72.10.164.178:18225",
    "http://72.10.160.171:11775",
    "http://67.43.236.20:15767",
    "http://113.161.210.60:8080",
]
get_proxies_listv3(proxies_list, url)
# get_proxies_listv2(url)
# get_proxies_listv1(url)
