#!/usr/bin/env python3
"""
中国大陆代理 IP 获取与测试工具（独立版 - v3.2 进度条版）
- 代理源：站大爷 + OpenProxyList + Geonode + Proxifly（纯 requests，无需 agent-browser）
- 验证：ipapi.co 地理位置 + 百度访问 + 东方财富 API
- 代理池维护：3 次失效自动清理
- 进度条：实时显示测试进度

用法：
    python3 fetch_cn_proxies.py [--target 20] [--max-pages 3] [--timeout 3]
"""

import requests
import concurrent.futures
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

REQUEST_TIMEOUT = 5
TEST_TIMEOUT = 3
MAX_WORKERS = 2
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
        f.write(f"# 总数：{len(valid)}\n")
        f.write(f"# 验证：东方财富 API\n#\n")
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
            resp = requests.get(url, headers=headers, timeout=30)
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
        resp = requests.get(url, timeout=15)
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
        resp = requests.get(url, timeout=15)
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
        resp = requests.get(url, headers=headers, timeout=15)
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


def is_china_ip(ip, timeout=2):
    """使用 IP-API 验证 IP 是否为中国大陆代理"""
    try:
        # 使用 http://ip-api.com/json/{ip} 接口
        resp = requests.get(f"http://ip-api.com/json/{ip}", timeout=timeout)
        if resp.status_code != 200:
            return False, None

        data = resp.json()
        if data.get("status") != "success":
            return False, None

        country = data.get("countryCode", "")
        # print(f"      IP-API 查询: {ip} -> {country}")
        if country == "CN":
            return True, country
        else:
            return False, country

    except Exception as e:
        print(f"IP-API 查询失败: {e}")
        return False, None


def test_proxy_strict(proxy, timeout=1):
    """
    三级验证：
    1. ipapi.co 地理位置验证（确认中国大陆 IP）
    2. 百度访问测试（确认中国大陆网络可达）
    3. 东方财富 API 验证（确认代理可用 + 数据完整性）
    """
    BAIDU_URL = "http://www.baidu.com"
    EASTMONEY_URL = "http://push2.eastmoney.com/api/qt/clist/get"
    EASTMONEY_PARAMS = {
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

    start = time.time()
    try:
        ip = proxy.split(":")[0]

        # 1. ipapi.co 地理位置验证
        is_china, country = is_china_ip(ip, timeout=timeout)
        if not is_china:
            return proxy, False, time.time() - start

        # 2. 百度访问测试
        try:
            resp_baidu = requests.get(
                BAIDU_URL, proxies={"http": f"http://{proxy}"}, timeout=timeout
            )
            if resp_baidu.status_code != 200:
                return proxy, False, time.time() - start
        except:
            return proxy, False, time.time() - start

        # 3. 东方财富 API 验证
        try:
            resp_em = requests.get(
                EASTMONEY_URL,
                params=EASTMONEY_PARAMS,
                proxies={"http": f"http://{proxy}"},
                timeout=timeout,
            )
            elapsed = time.time() - start
            if resp_em.status_code == 200:
                data = resp_em.json()
                if data.get("data") and data["data"].get("total", 0) > 1000:
                    return proxy, True, elapsed
        except:
            pass

        return proxy, False, time.time() - start

    except Exception as e:
        return proxy, False, time.time() - start


def print_progress(tested, total, passed, start_time):
    """打印进度条"""
    percent = int(100 * tested / total) if total > 0 else 0
    bar_length = 40
    filled_length = int(bar_length * tested / total) if total > 0 else 0
    bar = "█" * filled_length + "░" * (bar_length - filled_length)
    elapsed = time.time() - start_time
    sys.stdout.write(
        f"\r   [{bar}] {tested}/{total} ({percent}%) | 通过：{passed} | 耗时：{elapsed:.1f}s"
    )
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="中国大陆代理 IP 获取与测试")
    parser.add_argument(
        "--target", type=int, default=20, help="目标代理数量 (默认：20)"
    )
    parser.add_argument(
        "--max-pages", type=int, default=3, help="站大爷最大页数 (默认：3)"
    )
    parser.add_argument("--timeout", type=int, default=3, help="测试超时 (秒)")
    args = parser.parse_args()

    print("=" * 60)
    print("🇨🇳 中国大陆代理获取与测试（v3.2 - 进度条版）")
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

    print(f"\n[步骤 3] 测试代理（目标：{args.target} 个）...")
    valid_proxies = []
    tested = 0
    test_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(test_proxy_strict, p, args.timeout): p for p in all_proxies
        }
        for future in concurrent.futures.as_completed(futures):
            proxy, passed, elapsed = future.result()
            tested += 1
            if passed:
                valid_proxies.append(proxy)

            # 每 10 个更新一次进度条
            if tested % 10 == 0 or tested == len(all_proxies):
                print_progress(tested, len(all_proxies), len(valid_proxies), test_start)

            if len(valid_proxies) >= args.target:
                break

    print(
        f"\n\n   测试完成：{tested} 个，通过：{len(valid_proxies)} 个，耗时：{time.time() - test_start:.1f}s"
    )

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
