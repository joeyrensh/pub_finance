#!/usr/bin/env python3
"""
中国大陆代理 IP 获取与测试工具（异步版 v4.0）
- 代理源：站大爷 + OpenProxyList + Geonode + Proxifly
- 验证顺序：IP 地理位置（先）→ 百度访问 → 东方财富 API
- 异步并发测试，支持自定义并发数
- 代理池维护：3 次失效自动清理
- 进度条：实时显示测试进度

用法：
    python3 fetch_cn_proxies.py --target 20 --max-pages 3 --timeout 3 --workers 10
"""

import asyncio
import aiohttp
import requests
import re
import json
import time
import sys
import argparse
from datetime import datetime
from pathlib import Path
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "cn_proxy_sources.json"
PROXIES_TXT = SCRIPT_DIR / "china_proxies.txt"
PROXIES_JSON = SCRIPT_DIR / "china_proxies.json"

REQUEST_TIMEOUT = 5  # 抓取代理时的超时
MAX_FAILURES = 3


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


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


def fetch_zdaye(max_pages=3):
    proxies = []
    print("   获取站大爷（仅匿名代理）...")
    for page in range(1, max_pages + 1):
        try:
            url = f"https://www.zdaye.com/free/{page}/?ip_adr=&checktime=&sleep=1&cunhuo=&dengji=3&protocol=http&yys=&px="
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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


def fetch_openproxylist():
    proxies = []
    try:
        url = "https://api.openproxylist.xyz/http.txt"
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if line and ":" in line:
                    proxies.append(line)
            print(f"   OpenProxyList: {len(proxies)} 个")
    except Exception as e:
        print(f"   ⚠️ OpenProxyList: {e}")
    return proxies


def fetch_geonode(limit=500):
    proxies = []
    try:
        url = (
            f"https://proxylist.geonode.com/api/proxy-list?limit={limit}&protocols=http"
        )
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            if "data" in data:
                for p in data["data"]:
                    proxies.append(f"{p['ip']}:{p['port']}")
                print(f"   Geonode: {len(proxies)} 个")
    except Exception as e:
        print(f"   ⚠️ Geonode: {e}")
    return proxies


def fetch_proxifly():
    proxies = []
    try:
        url = "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt"
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
            print(f"   Proxifly-GitHub: {len(proxies)} 个")
    except Exception as e:
        print(f"   ⚠️ Proxifly-GitHub: {e}")
    return proxies


# ========== 异步验证函数 ==========


async def check_ip_location(ip, timeout):
    """通过 ip-api.com 判断 IP 是否在中国大陆（不使用代理）"""
    for attempt in range(2):  # 最多重试 1 次
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://ip-api.com/json/{ip}",
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if (
                            data.get("status") == "success"
                            and data.get("countryCode") == "CN"
                        ):
                            return True
            await asyncio.sleep(0.5)
        except:
            await asyncio.sleep(0.5)
    return False


async def check_baidu_via_proxy(proxy, timeout):
    """通过代理访问百度，返回是否成功"""
    try:
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.get(
                "http://www.baidu.com",
                proxy=f"http://{proxy}",
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                return resp.status == 200
    except:
        return False


async def check_eastmoney_via_proxy(proxy, timeout):
    """通过代理访问东方财富 API，返回是否成功且 total > 1000"""
    params = {
        "pn": "1",
        "pz": "5",
        "po": "1",
        "np": "1",
        "ut": "",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80",
        "fields": "f12,f13,f14,f2",
    }
    try:
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.get(
                "http://push2.eastmoney.com/api/qt/clist/get",
                params=params,
                proxy=f"http://{proxy}",
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("data") and data["data"].get("total", 0) > 1000:
                        return True
        return False
    except:
        return False


async def test_single_proxy(proxy, timeout):
    """完整测试单个代理：顺序为 IP 地理位置 -> 百度 -> 东方财富"""
    start = time.time()
    ip = proxy.split(":")[0]

    # 1. IP 地理位置（先，不通过代理）
    if not await check_ip_location(ip, timeout):
        return proxy, False, time.time() - start

    # 2. 百度访问测试（通过代理）
    if not await check_baidu_via_proxy(proxy, timeout):
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
            # 进度显示（每 10 个或最后一个）
            if tested % 10 == 0 or tested == total:
                percent = int(100 * tested / total)
                bar_length = 40
                filled = int(bar_length * tested / total)
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
        # 🚨 关键：显式取消未完成任务
        for t in tasks:
            if not t.done():
                t.cancel()

        # 🚨 等待所有任务真正退出，吞掉 CancelledError
        await asyncio.gather(*tasks, return_exceptions=True)

    print()  # 换行
    return valid


def print_progress_bar(current, total, passed, elapsed):
    percent = int(100 * current / total) if total else 0
    bar_length = 40
    filled = int(bar_length * current / total) if total else 0
    bar = "█" * filled + "░" * (bar_length - filled)
    sys.stdout.write(
        f"\r   [{bar}] {current}/{total} ({percent}%) | 通过：{passed} | 耗时：{elapsed:.1f}s"
    )
    sys.stdout.flush()


# ========== 主函数 ==========
def main():
    parser = argparse.ArgumentParser(description="中国大陆代理 IP 获取与测试（异步版）")
    parser.add_argument(
        "--target", type=int, default=20, help="目标代理数量 (默认：20)"
    )
    parser.add_argument(
        "--max-pages", type=int, default=3, help="站大爷最大页数 (默认：3)"
    )
    parser.add_argument("--timeout", type=int, default=3, help="测试超时 (秒)")
    parser.add_argument("--workers", type=int, default=10, help="并发数 (默认：10)")
    args = parser.parse_args()

    print("=" * 60)
    print("🇨🇳 中国大陆代理获取与测试（异步版 v4.0）")
    print("=" * 60)

    config = load_config()
    pool = load_proxy_pool()
    print(f"\n[步骤 1] 加载现有代理池：{len(pool)} 个")

    print(f"\n[步骤 2] 获取代理...")
    all_proxies = []
    all_proxies.extend(fetch_zdaye(args.max_pages))
    all_proxies.extend(fetch_openproxylist())
    all_proxies.extend(fetch_geonode())
    all_proxies.extend(fetch_proxifly())

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


if __name__ == "__main__":
    main()
