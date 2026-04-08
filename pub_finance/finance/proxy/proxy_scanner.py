import asyncio
import aiohttp
import multiprocessing as mp
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
from ipaddress import IPv4Network, IPv4Address

# 强制标准输出行缓冲
sys.stdout.reconfigure(line_buffering=True)

# ========== 配置参数 ==========
TEST_URL = "http://httpbin.org/get"
IP_CHECK_URL = "https://api.ipify.org"
GEO_URL = "https://ipapi.co/{ip}/country_name/"

# 需要扫描的常见代理端口（masscan 会扫描这些端口）
COMMON_PORTS = [
    80,
    443,
    8080,
    8081,
    8082,
    3128,
    8000,
    8118,
    8123,
    8181,
    8888,
    9999,
    10000,
    18080,
]

# 备选住宅 IP 段（如果未使用 ASN 模式，可回退到随机生成）
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


# ========== 阶段一：通过 ASN 获取 IP 段 ==========
def normalize_ip_ranges(ranges):
    normalized = []
    for item in ranges:
        if isinstance(item, tuple) and len(item) == 2:
            normalized.append(f"{item[0]}-{item[1]}")
        elif isinstance(item, str):
            normalized.append(item)
        else:
            print(f"[WARN] 跳过未知格式: {item}", flush=True)
    return normalized


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
            elif isinstance(result, list):
                raw_ranges = result
            else:
                print(f"[WARN] AS{asn_int} 返回未知格式: {type(result)}", flush=True)
                continue
            ranges = normalize_ip_ranges(raw_ranges)
            if ranges:
                all_ranges.extend(ranges)
                print(
                    f"[ASN] AS{asn_int} (ipatel) 提供 {len(ranges)} 个 IP 段",
                    flush=True,
                )
            else:
                print(f"[WARN] AS{asn_int} (ipatel) 未返回任何 IP 段", flush=True)
    except ImportError:
        print("[ERROR] ipatel 未安装，请执行: pip install ipatel", flush=True)
        raise
    except Exception as e:
        print(f"[ERROR] ipatel 查询失败: {e}", flush=True)
        raise
    return all_ranges


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
                    print("[ERROR] asnmap 未认证，请先运行: asnmap -auth", flush=True)
                else:
                    print(f"[WARN] asnmap 未返回 AS{asn_str} 的任何 IP 段", flush=True)
        except FileNotFoundError:
            print(
                "[ERROR] asnmap 未安装，请从 https://github.com/projectdiscovery/asnmap 安装",
                flush=True,
            )
            raise
        except Exception as e:
            print(f"[ERROR] asnmap 执行失败: {e}", flush=True)
            raise
    return all_ranges


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
                unique_cidrs = list(set(cidrs))
                all_cidrs.extend(unique_cidrs)
                print(
                    f"[ASN] AS{asn_str} (whois) 获取 {len(unique_cidrs)} 个 IP 段",
                    flush=True,
                )
            else:
                print(
                    f"[WARN] whois 查询 AS{asn_str} 失败，返回码: {result.returncode}",
                    flush=True,
                )
        except subprocess.TimeoutExpired:
            print(f"[ERROR] whois 查询 AS{asn_str} 超时 ({timeout}s)", flush=True)
            raise
        except FileNotFoundError:
            print(
                "[ERROR] whois 命令未找到，请安装 whois (apt install whois)", flush=True
            )
            raise
        except Exception as e:
            print(f"[ERROR] whois 执行失败: {e}", flush=True)
            raise
    return list(set(all_cidrs))


def get_ip_ranges_from_asn(asn_numbers, source="ipatel", whois_timeout=120):
    if source == "ipatel":
        ranges = get_ip_ranges_from_ipatel(asn_numbers)
    elif source == "asnmap":
        ranges = get_ip_ranges_from_asnmap(asn_numbers)
    elif source == "whois":
        ranges = get_ip_ranges_from_whois(asn_numbers, timeout=whois_timeout)
    else:
        raise ValueError(f"不支持的 source: {source}")
    unique_ranges = list(set(ranges))
    print(f"[ASN] 共获取 {len(unique_ranges)} 个唯一 IP 段", flush=True)
    return unique_ranges


# ========== 阶段二：masscan 快速端口扫描 ==========
def run_masscan_scan(cidr_list, ports, rate=10000, timeout=300, batch_size=100):
    if not cidr_list:
        print("[ERROR] 没有提供 IP 段，无法进行 masscan 扫描", flush=True)
        return []

    all_open_ports = []
    total_batches = (len(cidr_list) + batch_size - 1) // batch_size
    ports_arg = ",".join(str(p) for p in ports)

    for i in range(0, len(cidr_list), batch_size):
        batch = cidr_list[i : i + batch_size]
        batch_num = i // batch_size + 1
        targets = ",".join(batch)
        print(
            f"[MASSCAN] 批次 {batch_num}/{total_batches}，扫描 {len(batch)} 个 IP 段",
            flush=True,
        )

        # 构建命令：不使用任何 -o 参数，直接从 stdout 读取
        cmd = [
            "sudo",
            "masscan",
            targets,
            "-p",
            ports_arg,
            "--rate",
            str(rate),
            "--open-only",  # 只输出开放端口
            "--source-port",
            "40000-41023",
            "--retries",
            "2",
            "--wait",
            "10",
        ]

        try:
            # 启动进程，捕获 stdout 和 stderr
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # 实时读取 stdout 中的每一行
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                # 示例输出: "Discovered open port 3128/tcp on 1.231.81.166"
                if "Discovered open port" in line:
                    parts = line.split()
                    if len(parts) >= 6:
                        port_proto = parts[3]  # "3128/tcp"
                        ip = parts[5]  # "1.231.81.166"
                        port = int(port_proto.split("/")[0])
                        all_open_ports.append((ip, port))
                        print(f"[MASSCAN] 发现开放端口: {ip}:{port}", flush=True)
                # 其他行（如进度信息）暂时忽略，但可以打印 stderr 中的信息

            # 等待进程结束
            proc.wait(timeout=timeout)
            # 读取 stderr 中的进度信息并打印
            stderr_output = proc.stderr.read()
            for line in stderr_output.splitlines():
                if (
                    "rate:" in line.lower()
                    or "done:" in line.lower()
                    or "found=" in line
                ):
                    print(f"[MASSCAN] {line.strip()}", flush=True)

        except subprocess.TimeoutExpired:
            proc.terminate()
            print(f"[MASSCAN] 批次 {batch_num} 超时 ({timeout}s)，强制终止", flush=True)
            continue
        except Exception as e:
            print(f"[MASSCAN] 批次 {batch_num} 执行失败: {e}", flush=True)
            continue

    all_open_ports = list(set(all_open_ports))
    print(
        f"[MASSCAN] 所有批次扫描完成，共发现 {len(all_open_ports)} 个开放端口",
        flush=True,
    )
    return all_open_ports


# ========== 阶段三：代理验证 ==========
async def test_proxy(session, proxy, timeout):
    try:
        async with session.get(
            TEST_URL,
            proxy=f"http://{proxy}",
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
        origin = data.get("origin", "")
        if "," in origin:
            level = "transparent"
        elif proxy.split(":")[0] in origin:
            level = "anonymous"
        else:
            level = "elite"
        return level
    except Exception:
        return None


async def verify_proxy(session, proxy, timeout):
    level = await test_proxy(session, proxy, timeout)
    if not level:
        return None
    try:
        async with session.get(
            IP_CHECK_URL,
            proxy=f"http://{proxy}",
            timeout=timeout,
        ) as r:
            exit_ip = (await r.text()).strip()
    except:
        return None
    try:
        async with session.get(
            GEO_URL.format(ip=exit_ip),
            timeout=timeout,
        ) as r:
            country = (await r.text()).strip()
    except:
        country = "Unknown"
    return (proxy, country, level)


async def verify_worker(
    task_q, result_q, scanned, lock, concurrency, timeout, worker_id
):
    print(f"[Worker-{worker_id}] 启动，并发数={concurrency}", flush=True)
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency * 2, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:

        async def handle_one():
            while True:
                try:
                    proxy = task_q.get_nowait()
                except:
                    return
                async with sem:
                    result = await verify_proxy(session, proxy, timeout)
                    with lock:
                        scanned.value += 1
                        if scanned.value % 100 == 0:
                            print(f"[DEBUG] 已验证 {scanned.value} 个代理", flush=True)
                    if result:
                        result_q.put(result)

        await asyncio.gather(*(handle_one() for _ in range(concurrency)))


def verify_worker_entry(
    task_q, result_q, scanned, lock, concurrency, timeout, worker_id
):
    asyncio.run(
        verify_worker(task_q, result_q, scanned, lock, concurrency, timeout, worker_id)
    )


# ========== 辅助函数 ==========
def generate_fallback_targets(n):
    def random_ip_from_network(network_str):
        net = IPv4Network(network_str)
        first = int(net.network_address) + 1
        last = int(net.broadcast_address) - 1
        if first > last:
            return None
        rand_int = random.randint(first, last)
        return IPv4Address(rand_int).exploded

    out = []
    print("[FALLBACK] 开始随机生成目标代理...", flush=True)
    while len(out) < n:
        try:
            net_str = random.choice(IP_RANGES)
            ip = random_ip_from_network(net_str)
            if ip is None:
                continue
            port = random.choice(COMMON_PORTS)
            out.append(f"{ip}:{port}")
        except Exception as e:
            print(f"[WARN] 生成目标失败: {e}", flush=True)
            continue
    print(f"[FALLBACK] 生成了 {len(out)} 个随机目标", flush=True)
    return out


def load_cidr_file(filename):
    """从文件读取 CIDR 列表，每行一个 CIDR"""
    cidrs = []
    try:
        with open(filename, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    cidrs.append(line)
        print(f"[CIDR] 从文件 {filename} 加载了 {len(cidrs)} 个 CIDR 段", flush=True)
    except Exception as e:
        print(f"[ERROR] 读取 CIDR 文件失败: {e}", flush=True)
        raise
    return cidrs


# ========== 主函数 ==========
def main():
    parser = argparse.ArgumentParser(description="ASN 驱动的代理扫描器")
    parser.add_argument("--asn", type=str, default="", help="ASN 编号，多个用逗号分隔")
    parser.add_argument(
        "--asn-source",
        type=str,
        default="ipatel",
        choices=["ipatel", "asnmap", "whois"],
        help="ASN 数据源",
    )
    parser.add_argument(
        "--whois-timeout", type=int, default=120, help="whois 查询超时时间（秒）"
    )
    parser.add_argument(
        "--cidr-file",
        type=str,
        help="直接提供 CIDR 列表文件（每行一个 CIDR），跳过 ASN 查询",
    )
    parser.add_argument(
        "--targets", type=int, default=10000, help="需要验证的代理目标数量"
    )
    parser.add_argument("--workers", type=int, default=4, help="并行进程数")
    parser.add_argument(
        "--concurrency", type=int, default=100, help="每个进程内部的异步并发数"
    )
    parser.add_argument(
        "--timeout", type=int, default=3, help="HTTP 验证超时时间（秒）"
    )
    parser.add_argument(
        "--masscan-rate", type=int, default=10000, help="masscan 发包速率（包/秒）"
    )
    parser.add_argument("--out", default="proxies_ok.csv", help="输出 CSV 文件名")
    parser.add_argument("--fallback", action="store_true", help="回退到随机生成模式")
    args = parser.parse_args()

    # 获取 CIDR 列表
    cidr_list = []
    if args.cidr_file:
        cidr_list = load_cidr_file(args.cidr_file)
    elif args.asn:
        asn_numbers = [a.strip().upper().replace("AS", "") for a in args.asn.split(",")]
        print(
            f"[MAIN] 正在查询 ASN: {asn_numbers}，数据源: {args.asn_source}", flush=True
        )
        try:
            cidr_list = get_ip_ranges_from_asn(
                asn_numbers, source=args.asn_source, whois_timeout=args.whois_timeout
            )
        except Exception as e:
            print(f"[ERROR] 获取 ASN IP 段失败: {e}", flush=True)
            if args.fallback:
                print("[MAIN] 回退到预定义 IP 段", flush=True)
                cidr_list = IP_RANGES
            else:
                return
    else:
        if args.fallback:
            print("[MAIN] 未提供 ASN 或 CIDR 文件，回退到预定义 IP 段", flush=True)
            cidr_list = IP_RANGES
        else:
            print("[ERROR] 请提供 --asn 或 --cidr-file，或启用 --fallback", flush=True)
            return

    if not cidr_list:
        print("[ERROR] 没有可用的 IP 段", flush=True)
        return

    print(f"[MAIN] 共获取 {len(cidr_list)} 个 IP 段", flush=True)

    # masscan 扫描
    open_ports = run_masscan_scan(cidr_list, COMMON_PORTS, rate=args.masscan_rate)
    if not open_ports:
        print("[ERROR] masscan 未发现任何开放端口", flush=True)
        if args.fallback:
            print("[MAIN] 回退到随机生成模式", flush=True)
            targets = generate_fallback_targets(args.targets)
        else:
            return
    else:
        all_proxies = [f"{ip}:{port}" for ip, port in open_ports]
        if len(all_proxies) > args.targets:
            targets = random.sample(all_proxies, args.targets)
        else:
            targets = all_proxies
        print(
            f"[MAIN] 从 {len(all_proxies)} 个开放端口中选取 {len(targets)} 个进行验证",
            flush=True,
        )

    if not targets:
        print("[ERROR] 没有生成任何验证目标", flush=True)
        return

    # 代理验证
    task_q = mp.Queue()
    result_q = mp.Queue()
    for t in targets:
        task_q.put(t)

    scanned = mp.Value("i", 0)
    lock = mp.Lock()

    workers = []
    for i in range(args.workers):
        p = mp.Process(
            target=verify_worker_entry,
            args=(task_q, result_q, scanned, lock, args.concurrency, args.timeout, i),
        )
        p.start()
        workers.append(p)
        print(f"[MAIN] 启动验证 Worker {i}", flush=True)

    last = 0
    print("[MAIN] 开始验证代理...", flush=True)
    while last < len(targets):
        time.sleep(1)
        with lock:
            cur = scanned.value
        if cur > last:
            print(
                f"[MAIN] 验证进度: {cur}/{len(targets)} ({cur*100/len(targets):.1f}%)",
                flush=True,
            )
            last = cur
        alive = any(p.is_alive() for p in workers)
        if not alive and cur < len(targets):
            print("[WARN] 所有 worker 已退出但任务未完成", flush=True)
            break

    for p in workers:
        p.join()

    results = []
    while not result_q.empty():
        results.append(result_q.get())

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["proxy", "country", "anonymity"])
        writer.writerows(results)

    print(f"\n✅ 扫描完成 | 有效代理: {len(results)}", flush=True)


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
