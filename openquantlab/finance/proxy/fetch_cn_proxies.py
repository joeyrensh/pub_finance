#!/usr/bin/env python3
"""
中国大陆代理 IP 获取与测试工具（异步 curl_cffi 版 v6.1 - 支持 HTTP & SOCKS5）
- 代理源：站大爷 + OpenProxyList + Geonode + Proxifly + 3366net + 66daili + 89ip + 快代理
- 协议支持：HTTP / SOCKS5
- 验证顺序：百度访问 -> IP 地理位置 -> 东方财富 API
- 使用 curl_cffi 异步并发测试，支持伪装 Chrome 指纹
"""

import argparse
import asyncio
from datetime import datetime
import hashlib
import json
from pathlib import Path
import random
import re
import sys
import time
import uuid
from curl_cffi.requests import AsyncSession
from lxml import html
import requests
import urllib3

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
            data = json.load(f)

        migrated_pool = {}
        for key, info in data.items():
            # 如果是历史数据（不带协议前缀）
            if "://" not in key:
                new_key = f"http://{key}"
                info["protocol"] = info.get("protocol", "http")
                migrated_pool[new_key] = info
            else:
                # 如果已经是新格式，确保 protocol 字段存在
                if "protocol" not in info:
                    info["protocol"] = key.split("://")[0]
                migrated_pool[key] = info

        return migrated_pool
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
    """将 PROXIES_TXT 中的有效代理同步到 PROXIES_USED_TXT 文件中。"""
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
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    print("   获取站大爷 (HTTP & SOCKS5)...")

    for proto in ["http", "socks5"]:
        for page in range(1, max_pages + 1):
            try:
                url = f"https://www.zdaye.com/free/{page}/?ip_adr=&checktime=&sleep=1&cunhuo=2&dengji=&protocol={proto}&yys=&px="
                resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    ips = re.findall(r'class="proxy_ip">([\d\.]+)</p>', resp.text)
                    ports = re.findall(r"Port：(\d+)", resp.text)
                    for ip, port in zip(ips, ports):
                        proxies.append(f"{proto}://{ip}:{port}")
            except Exception as e:
                print(f"      ⚠️ [{proto}] 第{page}页异常：{e}")
                break

    print(f"   站大爷：{len(proxies)} 个")
    return proxies


def fetch_3366net_proxies(max_pages=10):
    base_url = "http://www.ip3366.net/free/"
    proxies = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
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
                proxies.append(f"http://{ip}:{port}")
            time.sleep(1)
        except Exception:
            continue
    print(f"   3366net：{len(proxies)} 个")
    return proxies


def fetch_66daili_proxies(num=100):
    url = f"http://api.66daili.com/?num={num}&anonymity=%E6%99%AE%E5%8C%BF&response_time=3000&format=text"
    proxies = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
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
                proxies.append(f"http://{line}")
    except Exception as e:
        print(f"抓取代理出错: {e}")

    print(f"   66daili：{len(proxies)} 个")
    return proxies


def fetch_89ip_proxies(num=500):
    url = f"http://api.89ip.cn/tqdl.html?api=1&num={num}&port=&address=&isp="
    proxies = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.encoding = "utf-8"
        if resp.status_code == 200:
            pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}:\d{1,5}\b"
            matches = re.findall(pattern, resp.text)
            for item in set(matches):
                proxies.append(f"http://{item}")
                proxies.append(f"socks5://{item}")
    except Exception as e:
        print(f"抓取代理出错: {e}")
    print(f"   89ip：{len(proxies)} 个")
    return proxies


def fetch_kuaidaili_proxies(max_pages=20):
    proxies_list = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
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
                    proxies_list.append(f"http://{ip[0]}:{port[0]}")
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
                    proxies.append(f"http://{line}")
                    proxies.append(f"socks5://{line}")
    except Exception as e:
        print(f"   ⚠️ OpenProxyList: {e}")
    print(f"   OpenProxyList: {len(proxies)} 个")
    return proxies


def fetch_geonode(limit=500):
    proxies = []
    for proto in ["http", "socks5"]:
        try:
            url = f"https://proxylist.geonode.com/api/proxy-list?limit={limit}&protocols={proto}&country=CN"
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if "data" in data:
                    for p in data["data"]:
                        proxies.append(f"{proto}://{p['ip']}:{p['port']}")
        except Exception as e:
            print(f"   ⚠️ Geonode [{proto}]: {e}")
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
                # 过滤注释与空行
                if not line or line.startswith("#"):
                    continue

                # 仅保留 http:// 与 socks5:// 开头的代理，忽略 socks4 等其他协议
                if line.startswith("http://") or line.startswith("socks5://"):
                    proxies.append(line)
                elif "://" not in line and ":" in line:
                    # 容错处理：如果部分数据没带协议前缀，默认当作 http 和 socks5 拆分处理
                    proxies.append(f"http://{line}")
                    proxies.append(f"socks5://{line}")
    except Exception as e:
        print(f"   ⚠️ Proxifly-GitHub: {e}")

    print(f"   Proxifly-GitHub: {len(proxies)} 个")
    return proxies


# ========== 异步验证逻辑 (基于 curl_cffi) ==========


def generate_ut_param() -> str:
    """生成唯一的 ut 追踪参数（标准 32 位 MD5 格式）"""
    return hashlib.md5(uuid.uuid4().bytes).hexdigest()


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


async def check_baidu_via_proxy(proxy_url, timeout):
    """通过 HTTP/SOCKS5 代理访问百度"""
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
    """通过 ip-api 判断 IP 是否在中国大陆（直连不走代理）"""
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


async def check_eastmoney_via_proxy(proxy_url, timeout):
    """通过 HTTP/SOCKS5 代理访问东方财富 API，验证响应并确保 total >= 1000"""
    params = build_params("cn", "0", 1)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
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
                if resp.text.strip().startswith("<"):
                    return False
                data = resp.json()
                if data.get("data") and data["data"].get("total", 0) >= 1000:
                    return True
        return False
    except Exception:
        return False


async def test_single_proxy(proxy_url, timeout):
    """完整测试单个代理：百度 -> IP 地理位置 -> 东方财富"""
    start = time.time()
    # 提取裸 IP 进行归属地测试
    raw_host = proxy_url.split("://")[-1]
    ip = raw_host.split(":")[0]

    # 1. 百度访问测试
    if not await check_baidu_via_proxy(proxy_url, timeout):
        return proxy_url, False, time.time() - start

    # 2. IP 地理位置校验
    if not await check_ip_location(ip, timeout):
        return proxy_url, False, time.time() - start

    # 3. 东方财富 API 测试
    if await check_eastmoney_via_proxy(proxy_url, timeout):
        return proxy_url, True, time.time() - start
    else:
        return proxy_url, False, time.time() - start


async def test_proxies_async(proxies, target, timeout, workers):
    """异步并发测试代理，返回有效的代理列表"""
    valid = []
    tested = 0
    total = len(proxies)
    start_time = time.time()
    semaphore = asyncio.Semaphore(workers)

    async def test_with_semaphore(proxy_url):
        nonlocal tested, valid
        async with semaphore:
            proxy_url, passed, elapsed = await test_single_proxy(proxy_url, timeout)
            tested += 1
            if passed:
                valid.append(proxy_url)

            if tested % 10 == 0 or tested == total:
                percent = int(100 * tested / total) if total else 0
                bar_length = 40
                filled = int(bar_length * tested / total) if total else 0
                bar = "█" * filled + "░" * (bar_length - filled)
                sys.stdout.write(
                    f"\r   [{bar}] {tested}/{total} ({percent}%) | 通过：{len(valid)} |"
                    f" 耗时：{time.time() - start_time:.1f}s"
                )
                sys.stdout.flush()

            return proxy_url, passed, elapsed

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
        description="中国大陆代理 IP 获取与测试（支持 HTTP 与 SOCKS5）"
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
    print("🇨🇳 中国大陆代理获取与测试（HTTP/SOCKS5 混合异步版 v6.1）")
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
    print(f"   总计去重后候选代理：{len(all_proxies)} 个")

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
        proto = p.split("://")[0]
        if p not in pool:
            pool[p] = {
                "protocol": proto,
                "added": datetime.now().isoformat(),
                "failures": 0,
            }
        else:
            pool[p]["failures"] = 0
            pool[p]["protocol"] = proto

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
