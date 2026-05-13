import requests
from lxml import html
import time


def fetch_kuaidaili_proxies(max_pages=10):
    """
    从快代理免费代理页面抓取代理列表。
    参数:
        max_pages (int): 要抓取的最大页数，默认抓取10页。
    返回:
        list: 包含 "ip:port" 格式代理地址的列表。
    """
    # 代理列表存储处
    proxies_list = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    for page in range(1, max_pages + 1):
        # 构造页面URL
        url = f"https://www.kuaidaili.com/free/inha/{page}/"
        print(f"正在抓取: {url}")
        try:
            # 发送HTTP GET请求
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = "utf-8"
            response.raise_for_status()  # 检查HTTP请求状态，如果不是200，会抛出异常

            # 使用lxml解析HTML
            tree = html.fromstring(response.content)

            # 使用XPath定位包含代理信息的行
            # '//tbody/tr' 选取代表每一行代理数据的 <tr> 元素
            proxy_rows = tree.xpath("//tbody/tr")

            for row in proxy_rows:
                # 通过XPath提取该行中IP和端口所在的第四列数据
                # 'td[1]' 代表第一个单元格（IP地址），'td[2]' 代表第二个单元格（端口号）
                ip = row.xpath("./td[1]/text()")
                port = row.xpath("./td[2]/text()")
                # 检查提取的数据是否有效
                if ip and port:
                    proxy = f"{ip[0]}:{port[0]}"
                    proxies_list.append(proxy)

            # 模拟人类行为，抓取页面后暂停一秒，避免给服务器造成过大压力
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"第 {page} 页抓取失败: {e}")
            continue
        except Exception as e:
            print(f"第 {page} 页解析失败: {e}")
            continue

    return proxies_list


print(fetch_kuaidaili_proxies())
