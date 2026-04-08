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
    """
    将 ipatel 返回的混合格式统一转换为 masscan 可接受的字符串格式。
    支持：
    - CIDR 字符串: "1.224.0.0/11"
    - 元组 (start_ip, end_ip): 转为 "start_ip-end_ip"
    """
    normalized = []
    for item in ranges:
        if isinstance(item, tuple) and len(item) == 2:
            normalized.append(f"{item[0]}-{item[1]}")
        elif isinstance(item, str):
            normalized.append(item)
        else:
            print(f"[WARN] 跳过未知格式: {item}", flush=True)
    return normalized


def get_ip_ranges_from_asn(asn_numbers, use_ipatel=True):
    """
    通过 ASN 编号获取 IP 段列表，返回字符串列表（CIDR 或 start-end 格式）
    """
    all_ranges = []

    if use_ipatel:
        try:
            import ipatel as ip

            for asn in asn_numbers:
                asn_str = str(asn).upper().replace("AS", "")
                asn_int = int(asn_str)
                result = ip.get_ip_ranges_for_asn(asn_int)

                # 提取 ip_ranges 字段
                if isinstance(result, dict):
                    raw_ranges = result.get("ip_ranges", [])
                elif isinstance(result, list):
                    raw_ranges = result
                else:
                    print(
                        f"[WARN] AS{asn_int} 返回未知格式: {type(result)}", flush=True
                    )
                    continue

                # 规范化格式
                ranges = normalize_ip_ranges(raw_ranges)

                if ranges:
                    all_ranges.extend(ranges)
                    print(f"[ASN] AS{asn_int} 提供 {len(ranges)} 个 IP 段", flush=True)
                else:
                    print(f"[WARN] AS{asn_int} 未返回任何 IP 段", flush=True)

        except ImportError:
            print("[WARN] ipatel 未安装，请执行: pip install ipatel", flush=True)
            use_ipatel = False
        except Exception as e:
            print(f"[ERROR] ipatel 查询失败: {e}", flush=True)
            use_ipatel = False

    # 备选：asnmap（输出 CIDR，无需额外转换）
    if not use_ipatel:
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
                        f"[ASN] AS{asn_str} 通过 asnmap 获取 {len(ranges)} 个 IP 段",
                        flush=True,
                    )
            except FileNotFoundError:
                print(
                    "[ERROR] asnmap 未安装，请从 https://github.com/projectdiscovery/asnmap 安装",
                    flush=True,
                )
                break
            except Exception as e:
                print(f"[ERROR] asnmap 执行失败: {e}", flush=True)

    # 去重
    unique_ranges = list(set(all_ranges))
    print(f"[ASN] 共获取 {len(unique_ranges)} 个唯一 IP 段", flush=True)
    return unique_ranges


# ========== 阶段二：masscan 快速端口扫描 ==========


def run_masscan_scan(cidr_list, ports, rate=10000, timeout=300):
    """
    使用 masscan 对 CIDR 列表进行快速端口扫描。
    返回开放端口列表: [(ip, port), ...]
    """
    if not cidr_list:
        print("[ERROR] 没有提供 IP 段，无法进行 masscan 扫描", flush=True)
        return []

    # 将 CIDR 列表合并为逗号分隔的字符串
    targets = ",".join(cidr_list)
    # 端口参数：支持单端口、范围、列表
    ports_arg = ",".join(str(p) for p in ports)

    # 创建临时文件存储 masscan 输出
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    # 构建 masscan 命令
    # -oJ: JSON 格式输出，便于解析
    # --rate: 发包速率（包/秒），根据网络环境调整
    cmd = [
        "sudo",
        "masscan",
        targets,
        "-p",
        ports_arg,
        "--rate",
        "100",  # 降低扫描速率，如 100
        "--source-port",
        "40000-41023",  # 指定源端口范围
        "--retries",
        "2",  # 增加重试次数
        "-e",
        "eth0",  # 指定物理网卡，确保是本机网络出口
        "--wait",
        "20",  # 增加扫描结束后的等待时间，防止漏包
        "-oJ",
        output_file,
    ]

    print(
        f"[MASSCAN] 开始扫描 {len(cidr_list)} 个 IP 段，{len(ports)} 个端口", flush=True
    )
    print(
        f"[MASSCAN] 命令: masscan {targets[:100]}... -p {ports_arg} --rate {rate}",
        flush=True,
    )

    try:
        # 执行 masscan
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # 实时读取 stderr 输出进度信息
        for line in proc.stderr:
            if "rate:" in line.lower() or "done:" in line.lower():
                print(f"[MASSCAN] {line.strip()}", flush=True)

        proc.wait(timeout=timeout)

    except subprocess.TimeoutExpired:
        proc.terminate()
        print(f"[MASSCAN] 扫描超时 ({timeout}s)，强制终止", flush=True)
    except Exception as e:
        print(f"[MASSCAN] 执行失败: {e}", flush=True)
        return []

    # 解析 JSON 输出
    open_ports = []
    try:
        with open(output_file, "r") as f:
            # masscan 输出的是每行一个 JSON 对象，不是标准 JSON 数组
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    ip = data.get("ip")
                    ports_list = data.get("ports", [])
                    for p in ports_list:
                        port = p.get("port")
                        if port:
                            open_ports.append((ip, port))
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"[MASSCAN] 解析结果失败: {e}", flush=True)

    # 清理临时文件
    try:
        os.unlink(output_file)
    except:
        pass

    # 去重并统计
    open_ports = list(set(open_ports))
    print(f"[MASSCAN] 扫描完成，发现 {len(open_ports)} 个开放端口", flush=True)
    return open_ports


# ========== 阶段三：代理验证（复用原有逻辑）==========


async def test_proxy(session, proxy, timeout):
    """测试代理的匿名级别"""
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
    """
    完整验证一个代理：
    1. 测试匿名级别
    2. 获取出口 IP
    3. 获取国家
    """
    level = await test_proxy(session, proxy, timeout)
    if not level:
        return None

    host, port = proxy.split(":")
    # 获取出口 IP
    try:
        async with session.get(
            IP_CHECK_URL,
            proxy=f"http://{proxy}",
            timeout=timeout,
        ) as r:
            exit_ip = (await r.text()).strip()
    except:
        return None

    # 获取国家
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
    """验证工作协程"""
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
    """回退方案：随机生成 IP:端口（复用原有逻辑）"""

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


# ========== 主函数 ==========


def main():
    parser = argparse.ArgumentParser(description="ASN 驱动的代理扫描器")
    parser.add_argument(
        "--asn",
        type=str,
        default="",
        help="ASN 编号，多个用逗号分隔，例如: 9318,4766 (AS 前缀可选)",
    )
    parser.add_argument(
        "--targets",
        type=int,
        default=10000,
        help="需要验证的代理目标数量（从开放端口中随机选取）",
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
    parser.add_argument(
        "--fallback",
        action="store_true",
        help="如果 ASN 未提供或获取失败，回退到随机生成模式",
    )
    args = parser.parse_args()

    # ========== 阶段一：获取 IP 段 ==========
    cidr_list = []

    if args.asn:
        asn_numbers = [a.strip().upper().replace("AS", "") for a in args.asn.split(",")]
        print(f"[MAIN] 正在查询 ASN: {asn_numbers}", flush=True)
        cidr_list = get_ip_ranges_from_asn(asn_numbers, use_ipatel=True)

    # 如果未提供 ASN 或查询结果为空，且允许回退，则使用预定义 IP 段
    if not cidr_list:
        if args.fallback:
            print("[MAIN] 未获取到 ASN 数据，回退到预定义 IP 段", flush=True)
            cidr_list = IP_RANGES
        else:
            print(
                "[ERROR] 未获取到任何 IP 段，请检查 ASN 编号或启用 --fallback",
                flush=True,
            )
            return

    print(f"[MAIN] 共获取 {len(cidr_list)} 个 IP 段", flush=True)

    # ========== 阶段二：masscan 快速扫描 ==========
    open_ports = run_masscan_scan(cidr_list, COMMON_PORTS, rate=args.masscan_rate)

    if not open_ports:
        print("[ERROR] masscan 未发现任何开放端口", flush=True)
        if args.fallback:
            print("[MAIN] 回退到随机生成模式", flush=True)
            targets = generate_fallback_targets(args.targets)
        else:
            return
    else:
        # 将开放端口转换为 "ip:port" 格式
        all_proxies = [f"{ip}:{port}" for ip, port in open_ports]
        # 如果发现的目标超过需求数量，随机抽样
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

    # ========== 阶段三：精细代理验证 ==========
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

    # 监控进度
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
