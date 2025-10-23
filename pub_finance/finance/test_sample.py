import requests
from lxml import etree
import time
import concurrent.futures


def get_free_proxies():
    """
    从站大爷免费代理IP页面抓取代理IP列表
    """
    url = "https://www.zdaye.com/free/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = "utf-8"
        print(response.text)
        if response.status_code != 200:
            print(f"请求失败，状态码：{response.status_code}")
            return []

        html = etree.HTML(response.text)
        print(html)
        # XPath路径可能需要根据网页实际结构调整
        ip_list = html.xpath("/html/body/div[3]/div/table/tbody/tr[1]/td[1]")
        port_list = html.xpath("/html/body/div[3]/div/table/tbody/tr[1]/td[2]")
        proxy_type_list = html.xpath("/html/body/div[3]/div/table/tbody/tr[1]/td[3]")

        proxies = []
        for ip, port, proxy_type in zip(ip_list, port_list, proxy_type_list):
            # 过滤高匿或匿名代理，可根据需要调整
            if "高匿" in proxy_type or "匿名" in proxy_type:
                # 根据代理类型构造代理地址
                if proxy_type.startswith("HTTP"):
                    proxies.append(f"http://{ip}:{port}")
                # 可根据需要添加HTTPS和SOCKS
        return list(set(proxies))  # 去重

    except Exception as e:
        print(f"抓取代理IP时发生错误：{e}")
        return []


def check_proxy(proxy):
    """
    验证代理IP是否有效
    """
    test_url = "http://httpbin.org/ip"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    proxies = {"http": proxy, "https": proxy}

    try:
        start_time = time.time()
        response = requests.get(test_url, headers=headers, proxies=proxies, timeout=10)
        end_time = time.time()

        if response.status_code == 200:
            # 可选：检查返回的IP是否为代理IP
            response_ip = response.json().get("origin")
            proxy_ip = proxy.split("://")[1].split(":")[0]
            if response_ip == proxy_ip:
                delay = round((end_time - start_time) * 1000, 2)  # 计算延迟
                print(f"有效代理：{proxy} 延迟：{delay}ms")
                return (proxy, delay)
            else:
                print(f"代理 {proxy} 可能不匿名，或未生效。")
                return None
        else:
            return None
    except Exception as e:
        return None


def main():
    print("正在从站大爷免费代理IP页面抓取代理IP...")
    raw_proxies = get_free_proxies()
    print(f"共抓取到 {len(raw_proxies)} 个代理IP")

    if not raw_proxies:
        print("未抓到代理IP，程序退出。")
        return

    print("开始验证代理IP有效性...")
    valid_proxies = []

    # 使用线程池并发验证，提高效率
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_proxy = {
            executor.submit(check_proxy, proxy): proxy for proxy in raw_proxies
        }
        for future in concurrent.futures.as_completed(future_to_proxy):
            result = future.result()
            if result:
                valid_proxies.append(result)

    # 按延迟排序
    valid_proxies.sort(key=lambda x: x[1])

    print(f"\n验证完毕！共找到 {len(valid_proxies)} 个有效代理IP")
    print("有效代理IP列表（按延迟排序）：")
    for proxy, delay in valid_proxies:
        print(f"{proxy} 延迟：{delay}ms")

    # 将有效代理保存到文件
    with open("valid_proxies.txt", "w") as f:
        for proxy, delay in valid_proxies:
            f.write(f"{proxy}\n")

    print("\n有效代理已保存至 valid_proxies.txt")


if __name__ == "__main__":
    main()
