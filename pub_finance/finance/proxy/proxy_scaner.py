import asyncio
import aiohttp
import multiprocessing as mp
import random
import time
import csv
import argparse
from ipaddress import IPv4Network
from tqdm import tqdm

TEST_URL = "http://httpbin.org/get"
IP_CHECK_URL = "https://api.ipify.org"
GEO_URL = "https://ipapi.co/{ip}/country_name/"

COMMON_PORTS = [80, 443, 8080, 3128, 8000, 8888]

IP_RANGES = [
    "3.0.0.0/8",
    "13.0.0.0/8",
    "34.0.0.0/8",
    "35.0.0.0/8",
    "52.0.0.0/8",
    "88.0.0.0/8",
    "91.0.0.0/8",
    "103.0.0.0/8",
]


def generate_targets(n):
    out = []
    while len(out) < n:
        net = IPv4Network(random.choice(IP_RANGES))
        ip = str(random.choice(list(net.hosts())))
        port = random.choice(COMMON_PORTS)
        out.append(f"{ip}:{port}")
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
    except Exception:
        return None


async def worker_loop(
    task_q,
    result_q,
    scanned,
    lock,
    concurrency,
    timeout,
):
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency * 2, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:

        async def handle(proxy):
            async with sem:
                level = await test_proxy(session, proxy, timeout)

                # 🔥 原子计数：这是关键
                with lock:
                    scanned.value += 1

                if not level:
                    return

                try:
                    async with session.get(
                        IP_CHECK_URL,
                        proxy=f"http://{proxy}",
                        timeout=timeout,
                    ) as r:
                        exit_ip = (await r.text()).strip()
                except Exception:
                    return

                try:
                    async with session.get(
                        GEO_URL.format(ip=exit_ip),
                        timeout=timeout,
                    ) as r:
                        country = (await r.text()).strip()
                except Exception:
                    country = "Unknown"

                result_q.put((proxy, country, level))

        while True:
            try:
                batch = [task_q.get_nowait() for _ in range(concurrency)]
            except Exception:
                break

            await asyncio.gather(*(handle(p) for p in batch))


def worker_entry(*args):
    asyncio.run(worker_loop(*args))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", type=int, default=100000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=2)
    parser.add_argument("--out", default="proxies_ok.csv")
    args = parser.parse_args()

    targets = generate_targets(args.targets)

    task_q = mp.Queue()
    result_q = mp.Queue()

    for t in targets:
        task_q.put(t)

    scanned = mp.Value("i", 0)
    lock = mp.Lock()

    workers = []
    for _ in range(args.workers):
        p = mp.Process(
            target=worker_entry,
            args=(
                task_q,
                result_q,
                scanned,
                lock,
                args.concurrency,
                args.timeout,
            ),
        )
        p.start()
        workers.append(p)

    results = []
    last = 0

    with tqdm(total=len(targets), desc="Scanning") as bar:
        while last < len(targets):
            time.sleep(0.2)
            with lock:
                cur = scanned.value
            if cur > last:
                bar.update(cur - last)
                last = cur

    for p in workers:
        p.join()

    while not result_q.empty():
        results.append(result_q.get())

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["proxy", "country", "anonymity"])
        writer.writerows(results)

    print(f"\n✅ 扫描完成 | 可用代理: {len(results)}")


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
