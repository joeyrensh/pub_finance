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
    "40.64.0.0/10",
    "52.0.0.0/8",
    "66.102.0.0/20",
    "72.14.192.0/18",
    "88.0.0.0/8",
    "91.0.0.0/8",
    "103.0.0.0/8",
]


def generate_targets(limit):
    res = []
    while len(res) < limit:
        net = IPv4Network(random.choice(IP_RANGES))
        ip = str(random.choice(list(net.hosts())))
        port = random.choice(COMMON_PORTS)
        res.append(f"{ip}:{port}")
    return res


async def test_proxy(session, proxy, timeout):
    start = time.time()
    try:
        async with session.get(
            TEST_URL,
            proxy=f"http://{proxy}",
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()

        latency = time.time() - start
        origin = data.get("origin", "")

        if "," in origin:
            anonymity = "transparent"
        elif proxy.split(":")[0] in origin:
            anonymity = "anonymous"
        else:
            anonymity = "elite"

        return anonymity, latency
    except Exception:
        return None


async def worker_loop(task_q, result_q, concurrency, timeout):
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency * 2, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:

        async def handle(proxy):
            async with sem:
                ok = 0
                latency_sum = 0
                anonymity = None

                for _ in range(2):
                    r = await test_proxy(session, proxy, timeout)
                    if r:
                        anonymity, lat = r
                        ok += 1
                        latency_sum += lat

                # 无论成功与否，都算“扫描完成”
                result_q.put(("DONE", None))

                if ok == 0:
                    return

                try:
                    async with session.get(
                        IP_CHECK_URL,
                        proxy=f"http://{proxy}",
                        timeout=aiohttp.ClientTimeout(total=timeout),
                    ) as r:
                        exit_ip = (await r.text()).strip()
                except Exception:
                    return

                try:
                    async with session.get(
                        GEO_URL.format(ip=exit_ip),
                        timeout=aiohttp.ClientTimeout(total=timeout),
                    ) as r:
                        country = (await r.text()).strip()
                except Exception:
                    country = "Unknown"

                result_q.put(
                    (
                        "OK",
                        (proxy, country, anonymity, round(latency_sum / ok, 3), ok / 2),
                    )
                )

        while True:
            batch = []
            try:
                for _ in range(concurrency):
                    batch.append(task_q.get_nowait())
            except Exception:
                pass

            if not batch:
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

    workers = []
    for _ in range(args.workers):
        p = mp.Process(
            target=worker_entry,
            args=(task_q, result_q, args.concurrency, args.timeout),
        )
        p.start()
        workers.append(p)

    scanned = 0
    results = []

    with tqdm(total=len(targets), desc="Scanning") as bar:
        while scanned < len(targets):
            typ, payload = result_q.get()
            if typ == "DONE":
                scanned += 1
                bar.update(1)
            elif typ == "OK":
                results.append(payload)

    for p in workers:
        p.join()

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["proxy", "country", "anonymity", "latency", "success_rate"])
        writer.writerows(results)

    print(f"\n✅ 扫描完成 | 可用代理: {len(results)}")


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
