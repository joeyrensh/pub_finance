#!/usr/bin/env python3
"""
中国大陆代理 IP 获取与测试工具（独立版）
- 代理源：站大爷 + OpenProxyList + Geonode + GitHub
- 验证：HTTP 连通性 + 中国 IP 判断 + 东方财富 API
- 代理池维护：3 次失效自动清理
"""

import requests
import concurrent.futures
import re
import json
import time
import argparse
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "cn_proxy_sources.json"
PROXIES_TXT = SCRIPT_DIR / "china_proxies.txt"
PROXIES_JSON = SCRIPT_DIR / "china_proxies.json"

CHINA_FIRST_OCTETS = {1, 14, 18, 21, 27, 31, 36, 39, 42, 49, 58, 59, 60, 61, 62, 63, 101, 102, 106, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 140, 144, 150, 153, 163, 171, 172, 173, 174, 175, 180, 181, 182, 183, 187, 189, 202, 210, 211, 218, 219, 220, 221, 222, 223}

def is_china_ip(ip):
    try:
        return int(ip.split(".")[0]) in CHINA_FIRST_OCTETS
    except:
        return False

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_proxy_pool():
    if PROXIES_JSON.exists():
        with open(PROXIES_JSON, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_proxy_pool(pool):
    with open(PROXIES_JSON, 'w', encoding='utf-8') as f:
        json.dump(pool, f, indent=2, ensure_ascii=False)
    valid = [p for p, info in pool.items() if info.get('failures', 0) == 0]
    with open(PROXIES_TXT, 'w', encoding='utf-8') as f:
        for p in valid:
            f.write(p + '\n')
    return len(valid)

def fetch_zdaye(max_pages=3):
    proxies = []
    print("   获取站大爷（仅匿名代理）...")
    for page in range(1, max_pages + 1):
        try:
            url = f"https://www.zdaye.com/free/{page}/?ip_adr=&checktime=&sleep=1&cunhuo=&dengji=3&protocol=http&yys=&px="
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Referer": "https://www.zdaye.com/"}
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                ips = re.findall(r'class="proxy_ip">([\d\.]+)</p>', resp.text)
                ports = re.findall(r'Port：(\d+)', resp.text)
                for ip, port in zip(ips, ports):
                    if is_china_ip(ip):
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
        url = f"https://proxylist.geonode.com/api/proxy-list?limit={limit}&protocols=http"
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

def fetch_github_source(source):
    proxies = []
    try:
        resp = requests.get(source['url'], timeout=20)
        if resp.status_code == 200:
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if line and ":" in line:
                    proxies.append(line)
    except Exception as e:
        print(f"      ⚠️ {source['name']}: {e}")
    return proxies

def fetch_github_proxies(sources):
    print("   获取 GitHub 代理...")
    all_proxies = []
    enabled = [s for s in sources if s.get('enabled', True)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(fetch_github_source, enabled)
        for result in results:
            all_proxies.extend(result)
    print(f"   GitHub: {len(all_proxies)} 个")
    return all_proxies

def test_proxy_strict(proxy, timeout=3):
    """
    三级验证：
    1. IP 第一字节判断（快速过滤）
    2. 百度访问测试（确认中国大陆网络可达）
    3. 东方财富 API 验证（确认代理可用 + 数据完整性）
    """
    BAIDU_URL = "http://www.baidu.com"
    EASTMONEY_URL = "http://push2.eastmoney.com/api/qt/clist/get"
    EASTMONEY_PARAMS = {"pn": "1", "pz": "5", "po": "1", "np": "1", "ut": "", "fltt": "2", "invt": "2", "fid": "f12", "fs": "m:0 t:6,m:0 t:80", "fields": "f12,f13,f14,f2"}
    start = time.time()
    try:
        ip = proxy.split(":")[0]
        # 1. IP 第一字节判断
        if not is_china_ip(ip):
            return proxy, False, time.time() - start
        # 2. 百度访问测试
        try:
            resp_baidu = requests.get(BAIDU_URL, proxies={"http": f"http://{proxy}"}, timeout=timeout)
            if resp_baidu.status_code != 200:
                return proxy, False, time.time() - start
        except:
            return proxy, False, time.time() - start
        # 3. 东方财富 API 验证（增加 total > 1000 条件）
        try:
            resp_em = requests.get(EASTMONEY_URL, params=EASTMONEY_PARAMS, proxies={"http": f"http://{proxy}"}, timeout=timeout)
            elapsed = time.time() - start
            if resp_em.status_code == 200:
                res = resp_em.json()
                # 新增条件：res["data"]["total"] > 1000
                if res.get("data") and res["data"].get("total", 0) > 1000:
                    return proxy, True, elapsed
        except:
            pass
        return proxy, False, time.time() - start
    except:
        return proxy, False, time.time() - start

def test_proxies_batch(proxies, max_workers=20):
    config = load_config()
    validation = config.get('validation', {})
    timeout = validation.get('timeout', 3)
    max_workers = validation.get('max_workers', max_workers)
    valid = []
    total = len(proxies)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_proxy = {executor.submit(test_proxy_strict, p, timeout): p for p in proxies}
        for i, future in enumerate(concurrent.futures.as_completed(future_to_proxy)):
            proxy, success, elapsed = future.result()
            if success:
                valid.append(proxy)
            if (i + 1) % 20 == 0 or i + 1 == total:
                print(f"   测试进度：{i+1}/{total} (通过：{len(valid)})")
    return valid

def update_proxy_pool(valid_proxies, existing_pool, max_failures=3):
    new_pool = {}
    new_count = 0
    for proxy, info in existing_pool.items():
        if proxy in valid_proxies:
            new_pool[proxy] = {'failures': 0, 'last_seen': datetime.now().isoformat()}
        else:
            failures = info.get('failures', 0) + 1
            if failures < max_failures:
                new_pool[proxy] = {'failures': failures, 'last_seen': info.get('last_seen')}
    for proxy in valid_proxies:
        if proxy not in new_pool:
            new_pool[proxy] = {'failures': 0, 'last_seen': datetime.now().isoformat()}
            new_count += 1
    return new_pool, new_count

def main():
    parser = argparse.ArgumentParser(description='中国大陆代理 IP 获取与测试')
    parser.add_argument('--target', type=int, default=10, help='目标代理数量')
    args = parser.parse_args()
    print("=" * 60)
    print("🇨🇳 中国大陆代理获取（独立工具版）")
    print("验证：百度连通性 + 中国 IP + 东方财富 API (total>1000)")
    print("=" * 60)
    config = load_config()
    print(f"\n[步骤 1] 加载配置...")
    print(f"   配置文件：{CONFIG_FILE.name}")
    print(f"\n[步骤 2] 加载现有代理池...")
    existing_pool = load_proxy_pool()
    print(f"   现有代理：{len(existing_pool)} 个")
    print(f"\n[步骤 3] 获取代理...")
    all_proxies = []
    sources = config.get('sources', {})
    if sources.get('zdaye', {}).get('enabled', True):
        params = sources['zdaye'].get('params', {})
        all_proxies.extend(fetch_zdaye(params.get('max_pages', 3)))
    if sources.get('openproxylist', {}).get('enabled', True):
        all_proxies.extend(fetch_openproxylist())
    if sources.get('geonode', {}).get('enabled', True):
        params = sources['geonode'].get('params', {})
        all_proxies.extend(fetch_geonode(params.get('limit', 500)))
    github_sources = config.get('github_sources', [])
    if github_sources:
        all_proxies.extend(fetch_github_proxies(github_sources))
    print(f"\n   总计获取：{len(all_proxies)} 个")
    print(f"\n[步骤 4] 测试代理（三级验证）...")
    valid_proxies = test_proxies_batch(all_proxies)
    print(f"\n   测试完成：{len(all_proxies)} 个，通过：{len(valid_proxies)} 个")
    print(f"\n[步骤 5] 更新代理池...")
    maintenance = config.get('maintenance', {})
    max_failures = maintenance.get('max_failures', 3)
    new_pool, new_count = update_proxy_pool(valid_proxies, existing_pool, max_failures)
    valid_count = save_proxy_pool(new_pool)
    print(f"\n   现有：{len(existing_pool)} 个 -> 新增：{new_count} 个")
    print(f"\n{'=' * 60}")
    print(f"✅ 完成！可用代理：{valid_count} 个")
    print(f"{'=' * 60}")
    return valid_count

if __name__ == '__main__':
    main()
