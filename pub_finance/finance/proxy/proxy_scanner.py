#!/usr/bin/env python3
"""
ASN 驱动的代理扫描器（优化版，双进度条）
- 支持 whois / asnmap / ipatel 获取 IP 段
- Masscan 扫描进度条（解析百分比）
- 代理验证进度条（显示已验证/成功/总发现端口）
- 简化验证流程（合并 API）
- 单进程异步验证
- 边扫描边验证，低内存

用法：
    python proxy_scanner.py --asn 7018 --targets 100 --concurrency 50
"""
"""
ASN 驱动的代理扫描器
1. 获取指定宽带服务商的ASN编号（以中国电信 AS45057 为例）
2. 通过下面命令获取指定服务商的ASN对应的IP段列表（CIDR格式）：
whois -h whois.radb.net -- '-i origin AS7018' | grep -Eo '([0-9.]+/(23|24))'>as7018_cidrs.txt
3. 执行脚本扫描这些IP段的常见代理端口（80, 443, 8080等），并验证哪些是有效的HTTP代理，最后输出到CSV文件中。
python -u proxy_scanner.py --cidr-file as9341_cidrs.txt --masscan-rate 1000 --targets 50000
# 更新系统并安装系统工具
sudo apt update
sudo apt install -y masscan whois

# 安装 Python 包
pip install aiohttp ipatel

# 安装 asnmap（需要先安装 Go）
# 如果不需要 asnmap 可以跳过
# 安装 Go（如果尚未安装）
wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
# 安装 asnmap
go install -v github.com/projectdiscovery/asnmap/cmd/asnmap@latest
# 将 asnmap 添加到 PATH
export PATH=$PATH:$HOME/go/bin
echo 'export PATH=$PATH:$HOME/go/bin' >> ~/.bashrc

"""

import asyncio
import aiohttp
import random
import time
import csv
import argparse
import sys
import subprocess
import json
import tempfile
import os
import re
from ipaddress import ip_network, ip_address

try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

sys.stdout.reconfigure(line_buffering=True)

# ========== 配置参数 ==========
TEST_URL = "http://httpbin.org/get"
GEO_API = "http://ip-api.com/json/{ip}"
COMMON_PORTS = [
    80,
    443,
    999,
    8080,
    8081,
    8082,
    8085,
    3128,
    4090,
    8000,
    8118,
    8123,
    8181,
    8443,
    8449,
    8888,
    9091,
    9002,
    9999,
    10000,
    10001,
    18080,
]

# 备选住宅 IP 段（回退用）
RESIDENTIAL_IP_RANGES = [
    "1.224.0.0/11",
    "39.7.0.0/16",
    "59.0.0.0/11",
    "211.36.0.0/14",
    "121.128.0.0/10",
    "211.106.0.0/15",
    "1.96.0.0/12",
    "126.0.0.0/8",
    "153.120.0.0/13",
    "219.96.0.0/12",
    "106.128.0.0/9",
    "118.0.0.0/11",
    "153.160.0.0/12",
    "24.0.0.0/13",
    "68.32.0.0/11",
    "69.136.0.0/13",
    "96.224.0.0/13",
    "70.192.0.0/10",
    "71.176.0.0/12",
    "79.192.0.0/10",
    "80.128.0.0/10",
    "87.160.0.0/11",
    "81.48.0.0/13",
    "86.192.0.0/11",
    "90.0.0.0/10",
]
IDC_IP_RANGES = [
    "108.61.0.0/16",
    "149.28.0.0/16",
    "207.148.0.0/16",
    "159.65.0.0/16",
    "159.89.0.0/16",
    "174.138.0.0/16",
    "45.33.0.0/16",
    "96.126.0.0/16",
    "172.104.0.0/16",
    "51.68.0.0/16",
    "91.134.0.0/16",
    "149.202.0.0/16",
    "49.12.0.0/16",
    "78.46.0.0/16",
    "88.198.0.0/16",
    "50.22.0.0/16",
    "169.44.0.0/16",
    "208.43.0.0/16",
    "129.146.0.0/16",
    "130.61.0.0/16",
    "140.238.0.0/16",
    "47.52.0.0/16",
    "47.56.0.0/16",
    "8.129.0.0/16",
    "5.254.0.0/16",
    "31.14.128.0/17",
    "185.156.0.0/16",
]
IP_RANGES = RESIDENTIAL_IP_RANGES + IDC_IP_RANGES


# ========== ASN 查询（三种数据源）==========
def normalize_ip_ranges(ranges):
    normalized = []
    for item in ranges:
        if isinstance(item, tuple) and len(item) == 2:
            normalized.append(f"{item[0]}-{item[1]}")
        elif isinstance(item, str):
            normalized.append(item)
    return normalized


def get_ip_ranges_from_whois(asn_numbers, timeout=120):
    all_cidrs = []
    for asn in asn_numbers:
        asn_str = str(asn).upper().replace("AS", "")
        cmd = ["whois", "-h", "whois.radb.net", "--", f"-i origin AS{asn_str}"]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout
            )
            if result.returncode == 0:
                cidrs = re.findall(r"([0-9.]+/[0-9]+)", result.stdout)
                all_cidrs.extend(cidrs)
                print(
                    f"[ASN] AS{asn_str} (whois) 获取 {len(cidrs)} 个 IP 段", flush=True
                )
        except Exception as e:
            print(f"[WARN] whois 查询失败: {e}", flush=True)
    return list(set(all_cidrs))


def get_ip_ranges_from_asnmap(asn_numbers):
    all_ranges = []
    for asn in asn_numbers:
        asn_str = str(asn).upper().replace("AS", "")
        cmd = ["asnmap", "-a", f"AS{asn_str}", "-silent"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0 and result.stdout:
                lines = result.stdout.strip().split("\n")
                ranges = [line.strip() for line in lines if line.strip()]
                all_ranges.extend(ranges)
                print(
                    f"[ASN] AS{asn_str} (asnmap) 获取 {len(ranges)} 个 IP 段",
                    flush=True,
                )
            else:
                if "authentication" in result.stderr.lower():
                    print("[ERROR] asnmap 未认证，请运行: asnmap -auth", flush=True)
                else:
                    print(f"[WARN] asnmap 未返回任何 IP 段", flush=True)
        except Exception as e:
            print(f"[ERROR] asnmap 失败: {e}", flush=True)
    return list(set(all_ranges))


def get_ip_ranges_from_ipatel(asn_numbers):
    all_ranges = []
    try:
        import ipatel as ip

        for asn in asn_numbers:
            asn_str = str(asn).upper().replace("AS", "")
            asn_int = int(asn_str)
            result = ip.get_ip_ranges_for_asn(asn_int)
            if isinstance(result, dict):
                raw_ranges = result.get("ip_ranges", [])
            else:
                raw_ranges = result if isinstance(result, list) else []
            ranges = normalize_ip_ranges(raw_ranges)
            if ranges:
                all_ranges.extend(ranges)
                print(
                    f"[ASN] AS{asn_int} (ipatel) 提供 {len(ranges)} 个 IP 段",
                    flush=True,
                )
    except ImportError:
        print("[ERROR] ipatel 未安装，请执行: pip install ipatel", flush=True)
        raise
    return list(set(all_ranges))


def get_ip_ranges_from_asn(asn_numbers, source="whois", whois_timeout=120):
    if source == "whois":
        return get_ip_ranges_from_whois(asn_numbers, timeout=whois_timeout)
    elif source == "asnmap":
        return get_ip_ranges_from_asnmap(asn_numbers)
    elif source == "ipatel":
        return get_ip_ranges_from_ipatel(asn_numbers)
    else:
        raise ValueError(f"不支持的 source: {source}")


def filter_cidr_by_mask(cidr_list, min_mask=23):
    filtered = []
    for cidr in cidr_list:
        try:
            mask = int(cidr.split("/")[1])
            if mask >= min_mask:
                filtered.append(cidr)
            else:
                print(
                    f"[FILTER] 跳过超大段: {cidr} (掩码 {mask} < {min_mask})",
                    flush=True,
                )
        except:
            continue
    print(f"[FILTER] 保留 {len(filtered)}/{len(cidr_list)} 个 IP 段", flush=True)
    return filtered


def load_cidr_file(filename):
    cidrs = []
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                cidrs.append(line)
    print(f"[CIDR] 从文件 {filename} 加载了 {len(cidrs)} 个 CIDR 段", flush=True)
    return cidrs


def generate_fallback_targets(n):
    def random_ip_from_network(network_str):
        net = ip_network(network_str)
        first = int(net.network_address) + 1
        last = int(net.broadcast_address) - 1
        if first > last:
            return None
        return str(ip_address(random.randint(first, last)))

    out = []
    print("[FALLBACK] 随机生成目标...", flush=True)
    while len(out) < n:
        try:
            net_str = random.choice(IP_RANGES)
            ip = random_ip_from_network(net_str)
            if ip is None:
                continue
            port = random.choice(COMMON_PORTS)
            out.append(f"{ip}:{port}")
        except:
            continue
    return out


# ========== Masscan 生产者（JSON 输出到 stdout）==========
async def run_masscan_producer(
    cidr_list,
    ports,
    result_queue,
    rate=5000,
    batch_size=100,
    progress_callback=None,
    masscan_pbar=None,
    verbose=False,
):
    if not cidr_list:
        await result_queue.put(None)
        return
    total_batches = (len(cidr_list) + batch_size - 1) // batch_size
    ports_arg = ",".join(str(p) for p in ports)

    # 设置进度条总数为批次数（如果使用批次进度）
    if masscan_pbar is not None:
        # 将 total 改为批次数，而不是 100
        masscan_pbar.total = total_batches
        masscan_pbar.desc = "Masscan批次"
        masscan_pbar.refresh()

    for i in range(0, len(cidr_list), batch_size):
        batch = cidr_list[i : i + batch_size]
        batch_num = i // batch_size + 1
        targets = ",".join(batch)
        tqdm.write(
            f"[MASSCAN] 批次 {batch_num}/{total_batches}，扫描 {len(batch)} 个段"
        )
        cmd = [
            "sudo",
            "masscan",
            targets,
            "-p",
            ports_arg,
            "--rate",
            str(rate),
            "--retries",
            "2",
            "--wait",
            "5",
            "--open-only",
            "--source-port",
            "40000-41023",
            "-oJ",
            "-",
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        async def read_stdout():
            async for line in proc.stdout:
                line = line.decode().strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    ip = data.get("ip")
                    for p in data.get("ports", []):
                        port = p.get("port")
                        if port:
                            await result_queue.put((ip, port))
                            if progress_callback:
                                progress_callback()
                except json.JSONDecodeError:
                    if verbose:
                        print(f"[WARN] 无效 JSON: {line}", flush=True)

        async def read_stderr():
            async for line in proc.stderr:
                line = line.decode().strip()
                if verbose:
                    print(f"[MASSCAN_STDERR] {line}", flush=True)

        await asyncio.gather(read_stdout(), read_stderr())
        await proc.wait()

        # 每完成一个批次，更新进度条（如果使用批次进度）
        if masscan_pbar is not None:
            masscan_pbar.update(1)

    if masscan_pbar is not None:
        masscan_pbar.close()

    # 退出信号由 main 统一广播
    return


# ========== 代理验证消费者（带进度条）==========
async def verify_single_proxy(session, proxy, timeout, retries=2):
    proxy_url = f"http://{proxy}"
    for attempt in range(retries):
        try:
            async with session.get(
                TEST_URL, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=timeout)
            ) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
            origin = data.get("origin", "")
            # 分割 IP 列表，取最后一个作为代理出口 IP
            ip_list = [ip.strip() for ip in origin.split(",")]
            exit_ip = ip_list[-1]  # 代理 IP

            if len(ip_list) > 1:
                level = "transparent"
            elif proxy.split(":")[0] in origin:
                level = "anonymous"
            else:
                level = "elite"

            async with session.get(
                GEO_API.format(ip=exit_ip), timeout=timeout
            ) as geo_resp:
                if geo_resp.status == 200:
                    geo = await geo_resp.json()
                    country = geo.get("countryCode", "Unknown")
                else:
                    country = "Unknown"
            return (proxy, country, level)
        except Exception as e:
            if attempt == retries - 1:
                return None
            await asyncio.sleep(0.5 * (attempt + 1))
    return None


async def verify_consumer(
    result_queue,
    valid_queue,
    target,
    concurrency,
    timeout,
    use_progress=True,
    verbose=False,
):
    valid = []
    valid_set = set()
    valid_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(concurrency)
    stop_flag = False
    tested = 0
    start_time = time.time()
    total_discovered = 0
    stop_event = asyncio.Event()

    pbar = None
    if use_progress and TQDM_AVAILABLE:
        pbar = tqdm(desc="验证代理", unit="个", total=None, ncols=80)

    def update_progress(unique_valid, tested_count, discovered=None):
        if pbar is None:
            return
        pbar.set_postfix(
            {
                "有效": unique_valid,
                "已测": tested_count,
                "发现端口": discovered if discovered is not None else total_discovered,
            }
        )

    async def worker():
        nonlocal stop_flag, tested, total_discovered
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        ) as session:
            while not stop_flag and not stop_event.is_set():
                try:
                    item = await asyncio.wait_for(result_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if item is None:
                    stop_event.set()
                    break
                ip, port = item
                total_discovered += 1
                proxy = f"{ip}:{port}"
                async with semaphore:
                    result = await verify_single_proxy(session, proxy, timeout)
                    tested += 1
                    if pbar is not None:
                        pbar.update(1)

                    if result:
                        async with valid_lock:
                            if proxy not in valid_set:
                                valid_set.add(proxy)
                                valid.append(result)
                                await valid_queue.put(result)
                        async with valid_lock:
                            current_valid = len(valid_set)
                        update_progress(current_valid, tested, total_discovered)
                        if current_valid >= target:
                            stop_flag = True
                            stop_event.set()
                            break
                    else:
                        async with valid_lock:
                            current_valid = len(valid_set)
                        update_progress(current_valid, tested, total_discovered)

    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await asyncio.gather(*workers)

    if total_discovered == 0:
        if pbar is not None:
            pbar.set_description("验证代理 (无端口)")
        print("\n[INFO] masscan 未发现任何开放端口，无需验证", flush=True)

    if pbar is not None:
        pbar.close()
    await valid_queue.put(None)
    if verbose:
        print("[CONSUMER] 所有 worker 退出，已发送结束信号", flush=True)
    return valid


# ========== 主函数 ==========
async def main_async(args):
    # 获取 CIDR 列表
    cidr_list = []
    if args.cidr_file:
        cidr_list = load_cidr_file(args.cidr_file)
    elif args.asn:
        asn_numbers = [a.strip().upper().replace("AS", "") for a in args.asn.split(",")]
        print(f"[MAIN] 查询 ASN: {asn_numbers}，数据源: {args.asn_source}", flush=True)
        try:
            cidr_list = get_ip_ranges_from_asn(
                asn_numbers, source=args.asn_source, whois_timeout=args.whois_timeout
            )
        except Exception as e:
            print(f"[ERROR] ASN 查询失败: {e}", flush=True)
            if args.fallback:
                print("[MAIN] 回退到预定义 IP 段", flush=True)
                cidr_list = IP_RANGES
            else:
                return
    else:
        if args.fallback:
            cidr_list = IP_RANGES
        else:
            print("[ERROR] 请提供 --asn 或 --cidr-file，或启用 --fallback", flush=True)
            return

    if not cidr_list:
        print("[ERROR] 无可用 IP 段", flush=True)
        return

    cidr_list = filter_cidr_by_mask(cidr_list, min_mask=args.min_mask)
    if not cidr_list:
        print("[ERROR] 过滤后无可用 IP 段", flush=True)
        return

    print(f"[MAIN] 共 {len(cidr_list)} 个 IP 段待扫描", flush=True)

    port_queue = asyncio.Queue(maxsize=5000)
    valid_queue = asyncio.Queue()

    use_progress = not args.no_progress
    masscan_pbar = None

    producer = asyncio.create_task(
        run_masscan_producer(
            cidr_list,
            COMMON_PORTS,
            port_queue,
            rate=args.masscan_rate,
            batch_size=100,
            masscan_pbar=masscan_pbar,
            verbose=args.verbose,
        )
    )
    consumer = asyncio.create_task(
        verify_consumer(
            port_queue,
            valid_queue,
            args.targets,
            args.concurrency,
            args.timeout,
            use_progress=use_progress,
            verbose=args.verbose,
        )
    )

    # 1️⃣ 等待 producer 完成（masscan 已结束）
    await producer

    # 2️⃣ 广播退出信号：每个 worker 一个 None
    for _ in range(args.concurrency):
        await port_queue.put(None)

    # 3️⃣ 等待 consumer 正常退出
    await consumer

    # 收集结果
    results_dict = {}

    while True:
        try:
            item = valid_queue.get_nowait()
            if item is None:
                break
            proxy, country, level = item
            if proxy not in results_dict:
                results_dict[proxy] = (country, level)
        except asyncio.QueueEmpty:
            break

    # 写入 CSV
    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["proxy", "country", "anonymity"])
        for proxy, (country, level) in results_dict.items():
            writer.writerow([proxy, country, level])

    print(f"\n✅ 扫描完成 | 有效代理: {len(results_dict)}", flush=True)
    return results_dict


def main():
    parser = argparse.ArgumentParser(description="ASN 代理扫描器（双进度条修复）")
    parser.add_argument("--asn", type=str, default="", help="ASN 编号，多个用逗号分隔")
    parser.add_argument(
        "--asn-source", type=str, default="whois", choices=["whois", "asnmap", "ipatel"]
    )
    parser.add_argument(
        "--whois-timeout", type=int, default=120, help="whois 查询超时（秒）"
    )
    parser.add_argument("--cidr-file", type=str, help="CIDR 列表文件")
    parser.add_argument("--targets", type=int, default=100, help="目标代理数量")
    parser.add_argument("--concurrency", type=int, default=50, help="验证并发数")
    parser.add_argument("--timeout", type=int, default=5, help="HTTP 验证超时（秒）")
    parser.add_argument("--min-mask", type=int, default=23, help="最小 CIDR 掩码")
    parser.add_argument(
        "--masscan-rate", type=int, default=5000, help="masscan 发包速率（包/秒）"
    )
    parser.add_argument("--out", default="proxies_ok.csv", help="输出 CSV 文件名")
    parser.add_argument("--fallback", action="store_true", help="回退到随机生成模式")
    parser.add_argument("--no-progress", action="store_true", help="禁用进度条")
    parser.add_argument("--verbose", action="store_true", help="显示调试信息")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
