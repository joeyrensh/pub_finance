#!/usr/bin/env python3
"""
海外代理 IP 获取与测试工具（异步版 v14）
- 代理源：OpenProxyList + Geonode API（中国大陆直连可用）
- 验证：通过代理获取出口 IP → 查询地理位置（非中国即有效）
- 异步并发测试，支持自定义并发数和超时
- 代理池维护：3 次失效自动清理
- 进度条：实时显示测试进度，达到目标提前终止

用法：
    python3 fetch_overseas_proxies.py --target 10 --timeout 3 --workers 5
"""

import os
import sys
import json
import time
import asyncio
import aiohttp
import argparse
import requests
import threading
from datetime import datetime
from pathlib import Path
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 尝试导入 yfinance（保留原逻辑，但实际未使用）
try:
    import yfinance as yf

    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False
    print("⚠️ 警告：缺少 yfinance，但本版本已不再依赖，可忽略")

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "overseas_proxy_sources.json"
PROXIES_TXT = SCRIPT_DIR / "overseas_proxies.txt"
PROXIES_JSON = SCRIPT_DIR / "overseas_proxies.json"

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
        f.write(f"# 海外代理 IP 池 - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"# 总数：{len(valid)}\n#\n")
        for p in valid:
            f.write(p + "\n")
    return len(valid)


# ===== 代理获取函数（同步，与原逻辑完全相同）=====
def fetch_text_source(url, source_name):
    proxies = []
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, timeout=15, headers=headers)
        if resp.status_code == 200:
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    parts = line.split(":")
                    if len(parts) >= 2 and parts[-1].isdigit():
                        ip_port = f"{parts[0]}:{parts[1]}"
                        proxies.append(ip_port)
            print(f"   {source_name}: {len(proxies)} 个")
    except Exception as e:
        print(f"   ⚠️ {source_name}: {str(e)[:50]}")
    return proxies


def fetch_geonode_api(url, source_name):
    proxies = []
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if "data" in data:
                for p in data["data"]:
                    ip = p.get("ip", "")
                    port = p.get("port", "")
                    if ip and port:
                        proxies.append(f"{ip}:{port}")
                print(f"   {source_name}: {len(proxies)} 个")
    except Exception as e:
        print(f"   ⚠️ {source_name}: {e}")
    return proxies


# ===== 异步验证函数（抽象步骤）=====
async def fetch_exit_ip_via_proxy(proxy, timeout):
    """通过代理访问 ip.sb，获取出口 IP"""
    try:
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.get(
                "https://api.ip.sb/ip",
                proxy=f"http://{proxy}",
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers={"User-Agent": "curl/7.68.0"},
            ) as resp:
                if resp.status == 200:
                    ip = await resp.text()
                    return ip.strip()
    except:
        pass
    return None


async def check_ip_country(ip, timeout):
    """通过 ip-api.com 查询 IP 所属国家（不使用代理）"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://ip-api.com/json/{ip}",
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "success":
                        return data.get("countryCode", "")
    except:
        pass
    return None


async def test_proxy_async(proxy, timeout):
    """完整测试单个代理：获取出口 IP → 检查是否为非中国 IP"""
    start = time.time()

    # 1. 通过代理获取出口 IP
    exit_ip = await fetch_exit_ip_via_proxy(proxy, timeout)
    if not exit_ip:
        return proxy, False, time.time() - start

    # 2. 查询该 IP 的国家（本地请求）
    country = await check_ip_country(exit_ip, timeout)
    if country and country != "CN":
        return proxy, True, time.time() - start
    else:
        return proxy, False, time.time() - start


# ===== 批量异步测试 =====
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
            proxy, passed, elapsed = await test_proxy_async(proxy, timeout)
            tested += 1
            if passed:
                valid.append(proxy)

            # 进度显示（每 5 个或最后一个）
            if tested % 5 == 0 or tested == total:
                percent = int(100 * tested / total)
                bar_length = 40
                filled = int(bar_length * tested / total)
                bar = "█" * filled + "░" * (bar_length - filled)
                sys.stdout.write(
                    f"\r   进度：[{bar}] {tested}/{total} ({percent}%) | 通过：{len(valid)} | 耗时：{time.time() - start_time:.1f}s"
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


# ===== 代理池维护（与原逻辑完全相同）=====
def update_proxy_pool(valid_proxies, existing_pool, max_failures=3):
    new_pool = {}
    new_count = 0
    for proxy, info in existing_pool.items():
        if proxy in valid_proxies:
            new_pool[proxy] = {"failures": 0, "last_seen": datetime.now().isoformat()}
        else:
            failures = info.get("failures", 0) + 1
            if failures < max_failures:
                new_pool[proxy] = {
                    "failures": failures,
                    "last_seen": info.get("last_seen"),
                }
    for proxy in valid_proxies:
        if proxy not in new_pool:
            new_pool[proxy] = {"failures": 0, "last_seen": datetime.now().isoformat()}
            new_count += 1
    return new_pool, new_count


# ===== 主函数 =====
def main():
    parser = argparse.ArgumentParser(description="海外代理 IP 获取与测试（异步版）")
    parser.add_argument("--target", type=int, default=20, help="目标代理数量")
    parser.add_argument("--timeout", type=int, default=3, help="测试超时（秒）")
    parser.add_argument("--workers", type=int, default=10, help="并发数")
    parser.add_argument("--skip-verify", action="store_true", help="跳过验证，仅获取")
    args = parser.parse_args()

    print("=" * 60)
    print("🌏 海外代理 IP 获取（异步版 v14）")
    print(f"验证：出口 IP → 非中国 | 并发：{args.workers} | 超时：{args.timeout} 秒")
    print("=" * 60)

    config = load_config()
    print(f"\n[步骤 1] 加载配置...")
    print(f"   配置文件：{CONFIG_FILE.name}")

    print(f"\n[步骤 2] 加载现有代理池...")
    existing_pool = load_proxy_pool()
    print(f"   现有代理：{len(existing_pool)} 个")

    print(f"\n[步骤 3] 获取代理...")
    all_proxies = []

    sources = config.get("sources", {})
    for key, source in sources.items():
        if not source.get("enabled", True):
            continue
        name = source["name"]
        url = source.get("url")
        if not url:
            continue
        if "geonode" in key.lower():
            all_proxies.extend(fetch_geonode_api(url, name))
        else:
            all_proxies.extend(fetch_text_source(url, name))

    all_proxies = list(set(all_proxies))
    print(f"\n   总计：{len(all_proxies)} 个")

    if args.skip_verify:
        print(f"\n[跳过验证] 直接保存所有代理")
        valid_proxies = all_proxies
    else:
        print(
            f"\n[步骤 4] 异步验证代理（目标：{args.target}个，并发：{args.workers}）..."
        )
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        valid_proxies = loop.run_until_complete(
            test_proxies_async(all_proxies, args.target, args.timeout, args.workers)
        )
        loop.close()
        print(f"\n   总计：{len(valid_proxies)} 个可用代理")

    print(f"\n[步骤 5] 更新代理池...")
    maintenance = config.get("maintenance", {})
    max_failures = maintenance.get("max_failures", 3)
    new_pool, new_count = update_proxy_pool(valid_proxies, existing_pool, max_failures)
    valid_count = save_proxy_pool(new_pool)

    print(f"\n{'=' * 60}")
    if valid_count >= args.target:
        print(f"✅ 完成！可用代理：{valid_count} 个")
    else:
        print(f"⚠️ 可用代理：{valid_count} 个（目标：{args.target}个）")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
