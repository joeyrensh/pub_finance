#!/usr/bin/env python3
"""
海外代理 IP 获取与测试工具（独立版 v12 - 单次验证优化）
- 代理源：OpenProxyList + Geonode API（中国大陆直连可用）
- 验证：yfinance + 环境变量代理（单次验证，固定 AAPL）
- 配置：并发 2 个，超时 2 秒，不重试
- 进度：每 10 个显示，实时更新
"""

import os
import random
import requests
import concurrent.futures
import json
import time
import argparse
import logging
import threading
from datetime import datetime
from pathlib import Path
import urllib3

### free proxy links:
# https://github.com/proxifly/free-proxy-list?tab=readme-ov-file
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import yfinance as yf

    YF_AVAILABLE = True
except ImportError as e:
    YF_AVAILABLE = False
    print(f"⚠️ 警告：缺少依赖，请运行：pip3 install yfinance --break-system-packages")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "overseas_proxy_sources.json"
PROXIES_TXT = SCRIPT_DIR / "overseas_proxies.txt"
PROXIES_JSON = SCRIPT_DIR / "overseas_proxies.json"


# 线程安全的计数器
class ProgressCounter:
    def __init__(self):
        self.lock = threading.Lock()
        self.tested = 0
        self.valid = 0

    def increment(self, is_valid):
        with self.lock:
            self.tested += 1
            if is_valid:
                self.valid += 1
            return self.tested, self.valid


progress = ProgressCounter()


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
        for p in valid:
            f.write(p + "\n")
    return len(valid)


# ===== 代理获取函数 =====


def fetch_text_source(url, source_name):
    proxies = []
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, timeout=15, headers=headers)
        if resp.status_code == 200:
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                # 跳过注释和空行
                if not line or line.startswith("#"):
                    continue
                # 提取 IP:PORT 格式（支持多种格式）
                if ":" in line:
                    parts = line.split(":")
                    if len(parts) >= 2 and parts[-1].isdigit():
                        # 处理可能的额外字段（如：IP:PORT:country:protocol）
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


# ===== 代理测试函数（yfinance + 环境变量代理）=====


def test_proxy_yfinance(proxy, symbols, timeout=2):
    """
    使用 ip.sb API 测试 HTTP 代理
    验证标准：
    1. 能作为 HTTP 代理使用
    2. 成功获取 IP 且为非中国 IP
    """
    try:
        # 明确设置 HTTP 代理格式（确保是 HTTP/HTTPS 协议，不是 SOCKS）
        # proxy 格式应为 "IP:PORT"，我们添加 http:// 前缀
        if not proxy.startswith("http://"):
            proxy_http = f"http://{proxy}"
        else:
            proxy_http = proxy

        proxies = {"http": proxy_http, "https": proxy_http}

        # 测试 1: 使用 HTTP 代理访问 ip.sb（HTTP 测试）
        url = "https://api.ip.sb/ip"
        headers = {"User-Agent": "curl/7.68.0"}

        resp = requests.get(
            url, headers=headers, proxies=proxies, timeout=timeout, verify=False
        )

        if resp.status_code != 200:
            return False

        ip = resp.text.strip()

        # 测试 2: 检查 IP 地理位置（验证是否为海外 IP）
        geo_url = f"https://ipapi.co/{ip}/json/"
        geo_resp = requests.get(geo_url, timeout=timeout, verify=False)

        if geo_resp.status_code != 200:
            return False

        geo = geo_resp.json()
        country = geo.get("country_code", "")

        # 非中国 IP 即为有效海外 HTTP 代理
        if country and country != "CN":
            logger.debug(f"✅ {ip} ({country}) HTTP")
            return True
        else:
            logger.debug(f"❌ {ip} (CN)")
            return False

    except Exception as e:
        logger.debug(f"❌ {str(e)[:30]}")
        return False


def test_proxies_batch(proxies, target_count, config):
    """批量测试代理（线程安全进度显示）"""
    validation = config.get("validation", {})
    symbols = validation.get("test_symbols", ["AAPL", "GOOGL", "MSFT"])
    timeout = validation.get("timeout", 2)
    max_workers = validation.get("max_workers", 2)

    valid = []
    total = len(proxies)
    stop_flag = False
    local_progress = ProgressCounter()

    def test_and_check(proxy):
        nonlocal stop_flag
        if stop_flag:
            return False, False

        result = test_proxy_yfinance(proxy, symbols, timeout)
        tested, valid_count = local_progress.increment(result)

        if result:
            valid.append(proxy)
            if len(valid) >= target_count:
                stop_flag = True

        # 每 5 个显示一次进度
        if tested % 5 == 0 or tested == total:
            progress_pct = tested / total * 100
            print(
                f"\r   进度：{tested}/{total} ({progress_pct:.1f}%) | 通过：{valid_count} | 失败：{tested-valid_count}",
                end="",
                flush=True,
            )

        return result, stop_flag

    print(f"   开始测试 {total} 个代理...")
    print(f"   并发：{max_workers} | 超时：{timeout}秒 | 目标：{target_count}个")
    print(f"   进度：0/{total} (0.0%) | 通过：0 | 失败：0", end="", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(test_and_check, p) for p in proxies]
        for future in concurrent.futures.as_completed(futures):
            if stop_flag:
                break

    print()  # 换行
    return valid


# ===== 代理池维护 =====


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
    parser = argparse.ArgumentParser(description="海外代理 IP 获取与测试")
    parser.add_argument("--target", type=int, default=5, help="目标代理数量")
    parser.add_argument("--skip-verify", action="store_true", help="跳过验证，仅获取")
    args = parser.parse_args()

    if not YF_AVAILABLE:
        print(
            "❌ 错误：缺少依赖，请运行：pip3 install yfinance --break-system-packages"
        )
        return 0

    print("=" * 60)
    print("🌏 海外代理 IP 获取（独立工具版 v11）")
    print("验证：yfinance + 环境变量 | 并发：2 | 超时：2 秒")
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

    print(f"\n   总计：{len(all_proxies)} 个")

    if args.skip_verify:
        print(f"\n[跳过验证] 直接保存所有代理")
        valid_proxies = all_proxies
    else:
        print(f"\n[步骤 4] yfinance 验证（目标：{args.target}个）...")
        valid_proxies = test_proxies_batch(all_proxies, args.target, config)
        print(f"\n总计：{len(valid_proxies)} 个可用代理")

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

    return valid_count


if __name__ == "__main__":
    main()
