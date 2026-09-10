#!/usr/bin/env python3
"""
中国大陆代理 IP 获取与测试工具（异步 curl_cffi 版 v5.0）
- 代理源：站大爷 + OpenProxyList + Geonode + Proxifly + 3366net + 66daili + 89ip + 快代理
- 验证顺序：百度访问 -> IP 地理位置 -> 东方财富 API
- 使用 curl_cffi 异步并发测试，支持伪装 Chrome 指纹
- 代理池维护：达到最大失败次数自动清理

用法：
    python3 fetch_cn_proxies.py --target 20 --max-pages 3 --timeout 3 --workers 10
"""

import asyncio
import requests
import re
import json
import time
import sys
import argparse
from datetime import datetime
import random
import hashlib
from pathlib import Path
import urllib3
from lxml import html
from curl_cffi.requests import AsyncSession

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "cn_proxy_sources.json"
PROXIES_TXT = SCRIPT_DIR / "china_proxies.txt"
PROXIES_JSON = SCRIPT_DIR / "china_proxies.json"
COOKIE_DIR = Path(__file__).parent.parent
PROXIES_USED_TXT = COOKIE_DIR / "utility/proxy.txt"
REQUEST_TIMEOUT = 5  # 抓取代理时的超时
MAX_FAILURES = 100


def parse_cookie_string():
    # 读取 JSON 格式的 cookie 文件
    cookie_file = COOKIE_DIR / "utility/eastmoney_cookie.json"
    if cookie_file.exists():
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)
            print("已加载 JSON cookie 信息")
            return cookie_data
    return {}


cookies = parse_cookie_string()


def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_proxy_pool():
    if PROXIES_JSON.exists():
        with open(PROXIES_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_proxy_pool(pool):
    with open(PROXIES_JSON, "w", encoding="utf-8") as f:
        json.dump(pool, f, indent=2, ensure_ascii=False)
    valid = [p for p, info in pool.items() if info.get("failures", 0) == 0]
    with open(PROXIES_TXT, "w", encoding="utf-8") as f:
        f.write(f"# 中国大陆代理 IP 池 - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"# 总数：{len(valid)}\n#\n")
        for p in valid:
            f.write(p + "\n")
    return len(valid)


def sync_valid_proxies():
    """
    将 PROXIES_TXT 中的有效代理同步到 PROXIES_USED_TXT 文件中。
    """
    source_proxies = set()
    if PROXIES_TXT.exists():
        with open(PROXIES_TXT, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    source_proxies.add(line)
    else:
        print(f"源文件 {PROXIES_TXT} 不存在，无法同步")
        return

    existing_proxies = set()
    original_content = ""
    if PROXIES_USED_TXT.exists():
        with open(PROXIES_USED_TXT, "r", encoding="utf-8") as f:
            original_content = f.read()
            for line in original_content.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    existing_proxies.add(line)

    new_proxies = source_proxies - existing_proxies
    if not new_proxies:
        print("没有需要新增的代理")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_content_parts = [
        f"# ===== 添加时间：{timestamp}，共 {len(new_proxies)} 个新代理 =====\n"
    ]
    for proxy in sorted(new_proxies):
        new_content_parts.append(f"{proxy}\n")
    if original_content:
        new_content_parts.append("\n")
    new_content = "".join(new_content_parts)

    PROXIES_USED_TXT.parent.mkdir(parents=True, exist_ok=True)
    with open(PROXIES_USED_TXT, "w", encoding="utf-8") as f:
        f.write(new_content)
        f.write(original_content)

    print(f"已成功将 {len(new_proxies)} 个新代理同步到 {PROXIES_USED_TXT} 的开头")


# ========== 代理抓取逻辑 ==========


def fetch_zdaye(max_pages=3):
    proxies = []
    print("   获取站大爷（仅匿名代理）...")
    for page in range(1, max_pages + 1):
        try:
            url = f"https://www.zdaye.com/free/{page}/?ip_adr=&checktime=&sleep=1&cunhuo=2&dengji=&protocol=http&yys=&px="
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
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


def fetch_3366net_proxies(max_pages=10):
    base_url = "http://www.ip3366.net/free/"
    proxies = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for page in range(1, max_pages + 1):
        url = (
            base_url
            if page == 1
            else f"http://www.ip3366.net/free/?stype=1&page={page}"
        )
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.encoding = "utf-8"
            if resp.status_code != 200:
                continue
            pattern = r"<td>(\d+\.\d+\.\d+\.\d+)</td>\s*<td>(\d+)</td>"
            matches = re.findall(pattern, resp.text)
            for ip, port in matches:
                proxies.append(f"{ip}:{port}")
            time.sleep(1)
        except Exception:
            continue
    print(f"   3366net：{len(proxies)} 个")
    return proxies


def fetch_66daili_proxies(num=100):
    url = f"http://api.66daili.com/?num={num}&anonymity=%E6%99%AE%E5%8C%BF&response_time=3000&format=text"
    proxies = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.encoding = "utf-8"
        if resp.status_code == 200:
            lines = resp.text.strip().splitlines()
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if "@" in line:
                    line = line.split("@")[0]
                proxies.append(line)
    except Exception as e:
        print(f"抓取代理出错: {e}")

    print(f"   66daili：{len(proxies)} 个")
    return proxies


def fetch_89ip_proxies(num=500):
    url = f"http://api.89ip.cn/tqdl.html?api=1&num={num}&port=&address=&isp="
    proxies = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.encoding = "utf-8"
        if resp.status_code == 200:
            pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}:\d{1,5}\b"
            matches = re.findall(pattern, resp.text)
            proxies.extend(list(set(matches)))
    except Exception as e:
        print(f"抓取代理出错: {e}")
    print(f"   89ip：{len(proxies)} 个")
    return proxies


def fetch_kuaidaili_proxies(max_pages=20):
    proxies_list = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for page in range(1, max_pages + 1):
        url = f"https://www.kuaidaili.com/free/inha/{page}/"
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.encoding = "utf-8"
            if response.status_code != 200:
                continue

            tree = html.fromstring(response.content)
            proxy_rows = tree.xpath("//tbody/tr")

            for row in proxy_rows:
                ip = row.xpath("./td[1]/text()")
                port = row.xpath("./td[2]/text()")
                if ip and port:
                    proxies_list.append(f"{ip[0]}:{port[0]}")
            time.sleep(1)
        except Exception:
            continue
    print(f"   快代理：{len(proxies_list)} 个")
    return proxies_list


def fetch_openproxylist():
    proxies = []
    try:
        url = "https://api.openproxylist.xyz/https.txt"
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if line and ":" in line:
                    proxies.append(line)
    except Exception as e:
        print(f"   ⚠️ OpenProxyList: {e}")
    print(f"   OpenProxyList: {len(proxies)} 个")
    return proxies


def fetch_geonode(limit=500):
    proxies = []
    try:
        url = f"https://proxylist.geonode.com/api/proxy-list?limit={limit}&protocols=http&country=CN"
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            if "data" in data:
                for p in data["data"]:
                    proxies.append(f"{p['ip']}:{p['port']}")
    except Exception as e:
        print(f"   ⚠️ Geonode: {e}")
    print(f"   Geonode: {len(proxies)} 个")
    return proxies


def fetch_proxifly():
    proxies = []
    try:
        url = "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/countries/CN/data.txt"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if line and ":" in line and not line.startswith("#"):
                    parts = line.split(":")
                    if len(parts) >= 2 and parts[-1].isdigit():
                        proxies.append(f"{parts[0]}:{parts[1]}")
    except Exception as e:
        print(f"   ⚠️ Proxifly-GitHub: {e}")
    print(f"   Proxifly-GitHub: {len(proxies)} 个")
    return proxies


# ========== 异步验证逻辑 (基于 curl_cffi) ==========


def generate_ut_param():
    """生成基于中国地区随机IP的32位十六进制格式ut参数"""
    china_networks = [
        (58, random.randint(0, 255)),
        (59, random.randint(0, 255)),
        (60, random.randint(0, 255)),
        (61, random.randint(0, 255)),
        (106, random.randint(0, 255)),
        (110, random.randint(0, 255)),
        (111, random.randint(0, 255)),
        (112, random.randint(0, 255)),
        (113, random.randint(0, 255)),
        (114, random.randint(0, 255)),
        (115, random.randint(0, 255)),
        (116, random.randint(0, 255)),
        (117, random.randint(0, 255)),
        (118, random.randint(0, 255)),
        (119, random.randint(0, 255)),
        (120, random.randint(0, 255)),
        (121, random.randint(0, 255)),
        (122, random.randint(0, 255)),
        (123, random.randint(0, 255)),
        (124, random.randint(0, 255)),
        (125, random.randint(0, 255)),
        (171, random.randint(0, 255)),
        (175, random.randint(0, 255)),
        (180, random.randint(0, 255)),
        (182, random.randint(0, 255)),
        (183, random.randint(0, 255)),
        (202, random.randint(0, 255)),
        (210, random.randint(0, 255)),
        (211, random.randint(0, 255)),
        (218, random.randint(0, 255)),
        (219, random.randint(0, 255)),
        (220, random.randint(0, 255)),
        (221, random.randint(0, 255)),
        (222, random.randint(0, 255)),
        (223, random.randint(0, 255)),
    ]
    network = random.choice(china_networks)
    random_ip = (
        f"{network[0]}.{network[1]}.{random.randint(1, 254)}.{random.randint(1, 254)}"
    )
    timestamp = int(time.time() * 1000)
    random_num = random.randint(1000000000, 9999999999)
    base_str = f"{timestamp}{random_num}{random_ip}"
    return hashlib.md5(base_str.encode()).hexdigest()


def build_params(market, mkt_code, page_num):
    base_params = {
        "pn": f"{page_num}",
        "pz": "100",
        "po": "1",
        "np": "1",
        "ut": generate_ut_param(),
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
    }
    if market == "us":
        base_params["fs"] = f"m:{mkt_code}"
    elif market == "cn":
        if mkt_code == "0":
            base_params["fs"] = "m:0 t:6,m:0 t:80"
        elif mkt_code == "1":
            base_params["fs"] = "m:1 t:2,m:1 t:23"
        elif mkt_code == "etf":
            base_params["fs"] = "b:MK0021,b:MK0022,b:MK0023,b:MK0024,b:MK0827"
            base_params["wbp2u"] = "|0|0|0|web"
    return base_params


async def check_baidu_via_proxy(proxy, timeout):
    """使用 curl_cffi 通过代理访问百度"""
    proxy_url = f"http://{proxy}"
    try:
        async with AsyncSession(impersonate="chrome120", verify=False) as session:
            resp = await session.get(
                "http://www.baidu.com",
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=timeout,
            )
            return resp.status_code == 200
    except Exception:
        return False


async def check_ip_location(ip, timeout):
    """通过 ip-api.com 判断 IP 是否在中国大陆（不使用代理）"""
    for _ in range(2):
        try:
            async with AsyncSession(impersonate="chrome120", verify=False) as session:
                resp = await session.get(
                    f"https://api.ip.sb/geoip/{ip}", timeout=timeout
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("country_code") == "CN":
                        return True
            await asyncio.sleep(0.5)
        except Exception:
            await asyncio.sleep(0.5)
    return False


async def check_eastmoney_via_proxy(proxy, timeout):
    """通过代理访问东方财富 API，验证响应并确保 total >= 1000"""
    params = build_params("cn", "0", 1)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    proxy_url = f"http://{proxy}"
    try:
        async with AsyncSession(impersonate="chrome120", verify=False) as session:
            resp = await session.get(
                "https://push2.eastmoney.com/api/qt/clist/get",
                params=params,
                headers=headers,
                proxies={"http": proxy_url, "https": proxy_url},
                cookies=cookies,
                timeout=timeout,
            )
            if resp.status_code == 200:
                # 过滤 HTML 拦截响应
                if resp.text.strip().startswith("<"):
                    return False
                data = resp.json()
                if data.get("data") and data["data"].get("total", 0) >= 1000:
                    return True
        return False
    except Exception:
        return False


async def test_single_proxy(proxy, timeout):
    """完整测试单个代理：顺序为 百度 -> IP 地理位置 -> 东方财富"""
    start = time.time()
    ip = proxy.split(":")[0]

    # 1. 百度访问测试（通过代理）
    if not await check_baidu_via_proxy(proxy, timeout):
        return proxy, False, time.time() - start

    # 2. IP 地理位置校验
    if not await check_ip_location(ip, timeout):
        return proxy, False, time.time() - start

    # 3. 东方财富 API 测试（通过代理）
    if await check_eastmoney_via_proxy(proxy, timeout):
        return proxy, True, time.time() - start
    else:
        return proxy, False, time.time() - start


async def test_proxies_async(proxies, target, timeout, workers):
    """异步并发测试代理，返回有效的代理列表"""
    valid = []
    tested = 0
    total = len(proxies)
    start_time = time.time()
    semaphore = asyncio.Semaphore(workers)

    async def test_with_semaphore(proxy):
        nonlocal tested, valid
        async with semaphore:
            proxy, passed, elapsed = await test_single_proxy(proxy, timeout)
            tested += 1
            if passed:
                valid.append(proxy)

            if tested % 10 == 0 or tested == total:
                percent = int(100 * tested / total) if total else 0
                bar_length = 40
                filled = int(bar_length * tested / total) if total else 0
                bar = "█" * filled + "░" * (bar_length - filled)
                sys.stdout.write(
                    f"\r   [{bar}] {tested}/{total} ({percent}%) | 通过：{len(valid)} | 耗时：{time.time() - start_time:.1f}s"
                )
                sys.stdout.flush()

            return proxy, passed, elapsed

    tasks = [asyncio.create_task(test_with_semaphore(p)) for p in proxies]

    try:
        for coro in asyncio.as_completed(tasks):
            await coro
            if len(valid) >= target:
                break
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    print()
    return valid


# ========== 主函数 ==========


def main():
    parser = argparse.ArgumentParser(
        description="中国大陆代理 IP 获取与测试（curl_cffi 异步版）"
    )
    parser.add_argument(
        "--target", type=int, default=20, help="目标代理数量 (默认：20)"
    )
    parser.add_argument(
        "--max-pages", type=int, default=3, help="抓取最大页数 (默认：3)"
    )
    parser.add_argument("--timeout", type=int, default=3, help="测试超时 (秒)")
    parser.add_argument("--workers", type=int, default=10, help="并发数 (默认：10)")
    args = parser.parse_args()

    print("=" * 60)
    print("🇨🇳 中国大陆代理获取与测试（curl_cffi 异步版 v5.0）")
    print("=" * 60)

    def merge_proxies(pool, new_proxies):
        existing = set(pool.keys())
        unique_new = set(new_proxies) - existing
        return list(unique_new | existing)

    config = load_config()
    pool = load_proxy_pool()

    print(f"\n[步骤 1] 加载现有代理池：{len(pool)} 个")

    print(f"\n[步骤 2] 获取代理...")
    all_proxies = []
    all_proxies.extend(fetch_zdaye(args.max_pages))
    all_proxies.extend(fetch_3366net_proxies(args.max_pages))
    all_proxies.extend(fetch_66daili_proxies())
    all_proxies.extend(fetch_89ip_proxies())
    all_proxies.extend(fetch_kuaidaili_proxies(args.max_pages))
    all_proxies.extend(fetch_openproxylist())
    all_proxies.extend(fetch_geonode())
    all_proxies.extend(fetch_proxifly())
    all_proxies = merge_proxies(pool, all_proxies)

    all_proxies = list(set(all_proxies))
    print(f"   总计获取：{len(all_proxies)} 个")

    print(f"\n[步骤 3] 测试代理（目标：{args.target} 个，并发数：{args.workers}）...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    valid_proxies = loop.run_until_complete(
        test_proxies_async(all_proxies, args.target, args.timeout, args.workers)
    )
    loop.close()

    print(f"\n   测试完成：通过 {len(valid_proxies)} 个")

    print(f"\n[步骤 4] 更新代理池...")
    for p in valid_proxies:
        if p not in pool:
            pool[p] = {"added": datetime.now().isoformat(), "failures": 0}
        else:
            pool[p]["failures"] = 0

    for p in list(pool.keys()):
        if p not in valid_proxies:
            pool[p]["failures"] = pool[p].get("failures", 0) + 1
            if pool[p]["failures"] >= MAX_FAILURES:
                del pool[p]

    count = save_proxy_pool(pool)
    print(f"\n{'=' * 60}")
    print(f"✅ 完成！可用代理：{count} 个")
    print(f"{'=' * 60}")
    sync_valid_proxies()


if __name__ == "__main__":
    main()
