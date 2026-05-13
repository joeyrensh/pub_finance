import requests
from lxml import html
import time
import re


def fetch_zdaye(max_pages=1):
    proxies = []
    print("   获取站大爷（仅匿名代理）...")
    for page in range(1, max_pages + 1):
        try:
            url = f"https://www.zdaye.com/free/{page}/?ip_adr=&checktime=&sleep=1&cunhuo=2&dengji=&protocol=http&yys=&px="
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
            }
            resp = requests.get(
                url,
                headers=headers,
                timeout=5,
            )
            print(f"      第{page}页状态码：{resp.status_code}")
            if resp.status_code == 200:
                ips = re.findall(r'class="proxy_ip">([\d\.]+)</p>', resp.text)
                ports = re.findall(r"Port：(\d+)", resp.text)
                for ip, port in zip(ips, ports):
                    proxies.append(f"{ip}:{port}")
        except Exception as e:
            print(f"      ⚠️ 第{page}页异常：{e}")
            break
    print(f"   站大爷：{len(proxies)} 个（匿名）")
    return proxies


print(fetch_zdaye())
