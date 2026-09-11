#!/usr/bin/env python3
"""
海外代理 IP 获取与测试工具（curl_cffi 异步版 v16 - 支持 HTTP & SOCKS5）
- 代理源：OpenProxyList + Geonode API + Proxifly（中国大陆直连可用）
- 验证：通过代理获取出口 IP → 查询地理位置（非中国即有效）
- 支持 HTTP 与 SOCKS5 双协议混用测试
- 自动平滑兼容历史 JSON 代理数据

用法：
    python3 fetch_overseas_proxies.py --target 10 --timeout 3 --workers 5
"""

import argparse
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import time
from curl_cffi.requests import AsyncSession
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "overseas_proxy_sources.json"
PROXIES_TXT = SCRIPT_DIR / "overseas_proxies.txt"
PROXIES_JSON = SCRIPT_DIR / "overseas_proxies.json"

MAX_FAILURES = 100


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_proxy_pool():
    """平滑升级并加载历史代理池数据。

    如果历史 Key 是无协议前缀的 "IP:Port"，自动转换为 "http://IP:Port"，保证历史记忆不丢失。
    """
    if PROXIES_JSON.exists():
        with open(PROXIES_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)

        migrated_pool = {}
        for key, info in data.items():
            # 历史数据迁移：补全协议前缀
            if "://" not in key:
                new_key = f"http://{key}"
                info["protocol"] = info.get("protocol", "http")
                migrated_pool[new_key] = info
            else:
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
        f.write(f"# 海外代理 IP 池 - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"# 总数：{len(valid)}\n#\n")
        for p in valid:
            f.write(p + "\n")
    return len(valid)


# ===== 代理获取函数（升级版：支持 HTTP / SOCKS5）=====


def fetch_text_source(url, source_name):
    """抓取文本格式代理源，自动提取 http/socks5 前缀；若无前缀则拆分为两者同时测试"""
    proxies = []
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, timeout=15, headers=headers)
        if resp.status_code == 200:
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                if line.startswith("http://") or line.startswith("socks5://"):
                    proxies.append(line)
                elif line.startswith("socks4://"):
                    continue  # 忽略 socks4
                elif ":" in line:
                    parts = line.split(":")
                    if len(parts) >= 2 and parts[-1].isdigit():
                        ip_port = f"{parts[0]}:{parts[1]}"
                        proxies.append(f"http://{ip_port}")
                        proxies.append(f"socks5://{ip_port}")
            print(f"   {source_name}: {len(proxies)} 个")
    except Exception as e:
        print(f"   ⚠️ {source_name}: {str(e)[:50]}")
    return proxies


def fetch_geonode_api(url, source_name):
    """抓取 Geonode API，支持生成带对应协议的 URL"""
    proxies = []
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if "data" in data:
                for p in data["data"]:
                    ip = p.get("ip", "")
                    port = p.get("port", "")
                    protocols = p.get("protocols", ["http"])
                    if ip and port:
                        for proto in protocols:
                            if proto in ["http", "socks5"]:
                                proxies.append(f"{proto}://{ip}:{port}")
                print(f"   {source_name}: {len(proxies)} 个")
    except Exception as e:
        print(f"   ⚠️ {source_name}: {e}")
    return proxies


# ===== 异步验证函数（基于 curl_cffi）=====


async def fetch_exit_ip_via_proxy(proxy_url, timeout):
    """通过代理 (HTTP/SOCKS5) 访问 ip.sb，若返回的出口 IP 与代理 IP 相同则返回该 IP，否则返回 None"""
    # 提取裸 IP，排除协议和端口
    raw_host = proxy_url.split("://")[-1]
    proxy_ip = raw_host.split(":")[0]

    try:
        async with AsyncSession(impersonate="chrome120", verify=False) as session:
            resp = await session.get(
                "https://api.ip.sb/ip",
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=timeout,
                headers={"User-Agent": "curl/7.68.0"},
            )
            if resp.status_code == 200:
                exit_ip = resp.text.strip()
                # 只有出口 IP 与代理 IP 完全一致时才视为成功
                if exit_ip == proxy_ip:
                    return exit_ip
    except Exception:
        return None
    return None


async def check_ip_country(ip, timeout):
    """通过 ip-api.com 查询 IP 所属国家（直连不走代理）"""
    try:
        async with AsyncSession(impersonate="chrome120", verify=False) as session:
            resp = await session.get(f"https://api.ip.sb/geoip/{ip}", timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("country_code", "")
    except Exception:
        pass
    return None


async def test_proxy_async(proxy_url, timeout):
    """完整测试单个代理：获取出口 IP → 检查是否为非中国 IP"""
    start = time.time()

    # 1. 通过代理获取出口 IP
    exit_ip = await fetch_exit_ip_via_proxy(proxy_url, timeout)
    if not exit_ip:
        return proxy_url, False, time.time() - start

    # 2. 查询该 IP 的国家（本地请求）
    country = await check_ip_country(exit_ip, timeout)
    if country and country != "CN":
        return proxy_url, True, time.time() - start
    else:
        return proxy_url, False, time.time() - start


# ===== 批量异步测试 =====


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
            proxy_url, passed, elapsed = await test_proxy_async(proxy_url, timeout)
            tested += 1
            if passed:
                valid.append(proxy_url)

            # 进度显示（每 5 个或最后一个）
            if tested % 5 == 0 or tested == total:
                percent = int(100 * tested / total) if total else 0
                bar_length = 40
                filled = int(bar_length * tested / total) if total else 0
                bar = "█" * filled + "░" * (bar_length - filled)
                sys.stdout.write(
                    f"\r   进度：[{bar}] {tested}/{total} ({percent}%) | 通过：{len(valid)} |"
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
        # 显式取消未完成任务
        for t in tasks:
            if not t.done():
                t.cancel()

        # 等待所有任务退出，忽略异常
        await asyncio.gather(*tasks, return_exceptions=True)

    print()  # 换行
    return valid


# ===== 代理池维护 =====


def update_proxy_pool(valid_proxies, existing_pool, max_failures=3):
    new_pool = {}
    new_count = 0

    # 处理现有代理池的更新与淘汰
    for proxy, info in existing_pool.items():
        proto = proxy.split("://")[0]
        if proxy in valid_proxies:
            new_pool[proxy] = {
                "protocol": proto,
                "failures": 0,
                "last_seen": datetime.now().isoformat(),
            }
        else:
            failures = info.get("failures", 0) + 1
            if failures < max_failures:
                new_pool[proxy] = {
                    "protocol": proto,
                    "failures": failures,
                    "last_seen": info.get("last_seen"),
                }

    # 处理新加入的成功代理
    for proxy in valid_proxies:
        proto = proxy.split("://")[0]
        if proxy not in new_pool:
            new_pool[proxy] = {
                "protocol": proto,
                "failures": 0,
                "last_seen": datetime.now().isoformat(),
            }
            new_count += 1

    return new_pool, new_count


# ===== 主函数 =====


def main():
    parser = argparse.ArgumentParser(
        description="海外代理 IP 获取与测试（curl_cffi 异步版 v16）"
    )
    parser.add_argument("--target", type=int, default=20, help="目标代理数量")
    parser.add_argument("--timeout", type=int, default=3, help="测试超时（秒）")
    parser.add_argument("--workers", type=int, default=10, help="并发数")
    parser.add_argument("--skip-verify", action="store_true", help="跳过验证，仅获取")
    args = parser.parse_args()

    print("=" * 60)
    print("🌏 海外代理 IP 获取（curl_cffi 异步版 v16 - 支持 HTTP & SOCKS5）")
    print(f"验证：出口 IP → 非中国 | 并发：{args.workers} | 超时：{args.timeout} 秒")
    print("=" * 60)

    def merge_proxies(pool, new_proxies):
        existing = set(pool.keys())
        unique_new = set(new_proxies) - existing
        all_to_test = list(unique_new | existing)
        return all_to_test

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

    all_proxies = merge_proxies(existing_pool, all_proxies)
    all_proxies = list(set(all_proxies))
    print(f"\n   总计去重后候选代理：{len(all_proxies)} 个")

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
