import asyncio
import aiohttp
import multiprocessing as mp
import random
import time
import csv
import argparse
import sys
from ipaddress import IPv4Network, IPv4Address


# 强制标准输出行缓冲，避免缓冲延迟
sys.stdout.reconfigure(line_buffering=True)

TEST_URL = "http://httpbin.org/get"
IP_CHECK_URL = "https://api.ipify.org"
GEO_URL = "https://ipapi.co/{ip}/country_name/"

COMMON_PORTS = [
    # 标准
    80,
    443,
    # 常见 Web / Proxy
    8080,
    8081,
    8082,
    3128,
    8000,
    8118,
    8123,
    8181,
    8888,
    # 高位常见
    9999,
    10000,
    18080,
]
RESIDENTIAL_IP_RANGES = [
    # 🇰🇷 韩国 ISP
    "1.224.0.0/11",  # SK Broadband
    "39.7.0.0/16",
    "59.0.0.0/11",
    "211.36.0.0/14",  # Korea Telecom
    "121.128.0.0/10",
    "211.106.0.0/15",  # LG DACOM
    "1.96.0.0/12",
    # 🇯🇵 日本 ISP
    "126.0.0.0/8",  # Softbank
    "153.120.0.0/13",
    "219.96.0.0/12",
    "106.128.0.0/9",  # KDDI
    "118.0.0.0/11",
    "153.160.0.0/12",
    # 🇺🇸 美国 ISP
    "24.0.0.0/13",  # Comcast
    "68.32.0.0/11",
    "69.136.0.0/13",
    "96.224.0.0/13",  # Verizon
    "70.192.0.0/10",
    "71.176.0.0/12",
    # 🇩🇪 德国 ISP
    "79.192.0.0/10",  # Deutsche Telekom
    "80.128.0.0/10",
    "87.160.0.0/11",
    # 🇫🇷 法国 ISP
    "81.48.0.0/13",  # Orange
    "86.192.0.0/11",
    "90.0.0.0/10",
]
IDC_IP_RANGES = [
    # Vultr
    "108.61.0.0/16",
    "149.28.0.0/16",
    "207.148.0.0/16",
    # DigitalOcean
    "159.65.0.0/16",
    "159.89.0.0/16",
    "174.138.0.0/16",
    # Linode
    "45.33.0.0/16",
    "96.126.0.0/16",
    "172.104.0.0/16",
    # OVH
    "51.68.0.0/16",
    "91.134.0.0/16",
    "149.202.0.0/16",
    # Hetzner
    "49.12.0.0/16",
    "78.46.0.0/16",
    "88.198.0.0/16",
    # SoftLayer
    "50.22.0.0/16",
    "169.44.0.0/16",
    "208.43.0.0/16",
    # Oracle Cloud
    "129.146.0.0/16",
    "130.61.0.0/16",
    "140.238.0.0/16",
    # Alibaba Cloud (海外)
    "47.52.0.0/16",
    "47.56.0.0/16",
    "8.129.0.0/16",
    # Voxility
    "5.254.0.0/16",
    "31.14.128.0/17",
    "185.156.0.0/16",
]

IP_RANGES = RESIDENTIAL_IP_RANGES + IDC_IP_RANGES


def random_ip_from_network(network_str):
    """从 IPv4 网络中随机生成一个有效的 IP（不包括网络地址和广播地址）"""
    net = IPv4Network(network_str)
    # 网络地址和广播地址之间的整数范围
    first = int(net.network_address) + 1
    last = int(net.broadcast_address) - 1
    if first > last:
        return None
    rand_int = random.randint(first, last)
    return IPv4Address(rand_int).exploded


def generate_targets(n):
    out = []
    print("[MAIN] 开始生成目标代理...", flush=True)
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
    print(f"[MAIN] 生成了 {len(out)} 个目标代理", flush=True)
    return out


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
    except Exception as e:
        # 可选：打印错误（调试用，可注释）
        # print(f"[DEBUG] test_proxy 失败: {proxy} {e}", flush=True)
        return None


async def worker_loop(task_q, result_q, scanned, lock, concurrency, timeout, worker_id):
    print(f"[Worker-{worker_id}] 启动，并发数={concurrency}", flush=True)
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency * 2, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:

        async def handle_one():
            while True:
                try:
                    proxy = task_q.get_nowait()
                except:
                    # print(f"[Worker-{worker_id}] 队列空，协程退出", flush=True)
                    return
                async with sem:
                    level = await test_proxy(session, proxy, timeout)
                    with lock:
                        scanned.value += 1
                        if scanned.value % 1000 == 0:
                            print(f"[DEBUG] 已扫描 {scanned.value} 个代理", flush=True)
                    if not level:
                        continue
                    # 获取出口 IP
                    try:
                        async with session.get(
                            IP_CHECK_URL,
                            proxy=f"http://{proxy}",
                            timeout=timeout,
                        ) as r:
                            exit_ip = (await r.text()).strip()
                    except:
                        continue
                    # 获取国家
                    try:
                        async with session.get(
                            GEO_URL.format(ip=exit_ip),
                            timeout=timeout,
                        ) as r:
                            country = (await r.text()).strip()
                    except:
                        country = "Unknown"
                    result_q.put((proxy, country, level))

        await asyncio.gather(*(handle_one() for _ in range(concurrency)))


def worker_entry(task_q, result_q, scanned, lock, concurrency, timeout, worker_id):
    asyncio.run(
        worker_loop(task_q, result_q, scanned, lock, concurrency, timeout, worker_id)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", type=int, default=100000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=2)
    parser.add_argument("--out", default="proxies_ok.csv")
    args = parser.parse_args()

    targets = generate_targets(args.targets)
    if not targets:
        print("[ERROR] 没有生成任何目标代理，请检查 IP_RANGES 或目标数量", flush=True)
        return

    task_q = mp.Queue()
    result_q = mp.Queue()
    for t in targets:
        task_q.put(t)
    print(f"[MAIN] 已将 {len(targets)} 个目标放入队列", flush=True)

    scanned = mp.Value("i", 0)
    lock = mp.Lock()

    workers = []
    for i in range(args.workers):
        p = mp.Process(
            target=worker_entry,
            args=(task_q, result_q, scanned, lock, args.concurrency, args.timeout, i),
        )
        p.start()
        workers.append(p)
        print(f"[MAIN] 启动 Worker {i}", flush=True)

    # 监控进度
    last = 0
    print("[MAIN] 开始监控扫描进度...", flush=True)
    while last < len(targets):
        time.sleep(1)
        with lock:
            cur = scanned.value
        if cur > last:
            print(
                f"[MAIN] 进度: {cur}/{len(targets)} ({cur*100/len(targets):.1f}%)",
                flush=True,
            )
            last = cur
        # 检查进程是否意外退出
        alive = any(p.is_alive() for p in workers)
        if not alive and cur < len(targets):
            print("[WARN] 所有 worker 已退出但任务未完成，可能发生异常", flush=True)
            break

    for p in workers:
        p.join()
        print(f"[MAIN] Worker 进程已结束", flush=True)

    results = []
    while not result_q.empty():
        results.append(result_q.get())

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["proxy", "country", "anonymity"])
        writer.writerows(results)

    print(f"\n✅ 扫描完成 | 可用代理: {len(results)}", flush=True)


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
