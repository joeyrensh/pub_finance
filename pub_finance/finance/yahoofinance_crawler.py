import yfinance as yf
import pandas as pd
import requests
import time
import random
import logging
from requests.exceptions import ProxyError, ConnectionError, Timeout, HTTPError
import csv
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.em_stock_uti import EMWebCrawlerUti
from finance.utility.toolkit import ToolKit

# ========== 添加日志配置 ==========
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
last_success_proxy = None


def get_industry_info(symbol, proxy_list, max_retries=1, preferred_proxy=None):
    """
    获取股票行业信息，支持代理轮询和优先使用指定代理
    返回 (industry, used_proxy) 或 ("N/A", None)
    """
    # 关闭 SSL 校验（环境变量方式）
    os.environ.setdefault("CURL_CA_BUNDLE", "")
    os.environ.setdefault("SSL_CERT_FILE", "")

    global last_success_proxy
    use_proxy = bool(proxy_list)

    # 构建代理优先级列表
    proxy_priority = []
    if preferred_proxy and preferred_proxy in proxy_list:
        proxy_priority.append(preferred_proxy)
    if (
        last_success_proxy
        and last_success_proxy not in proxy_priority
        and last_success_proxy in proxy_list
    ):
        proxy_priority.append(last_success_proxy)
    for p in proxy_list:
        if p not in proxy_priority:
            proxy_priority.append(p)

    for attempt in range(1, max_retries + 1):
        for proxy in proxy_priority:
            try:
                # 通过环境变量设置代理
                if use_proxy:
                    os.environ["HTTP_PROXY"] = proxy
                    os.environ["HTTPS_PROXY"] = proxy
                    logger.info(f"[{symbol}] 尝试第{attempt}次请求 | 代理: {proxy}")
                else:
                    os.environ.pop("HTTP_PROXY", None)
                    os.environ.pop("HTTPS_PROXY", None)
                    logger.info(f"[{symbol}] 尝试第{attempt}次请求 | 直连")

                ticker = yf.Ticker(symbol)
                info = ticker.info
                industry, sector = info.get("industry", None), info.get("sector", None)

                if use_proxy:
                    last_success_proxy = proxy
                logger.info(f"✅ 成功获取 {symbol} 行业信息: {industry} {sector}")
                return industry, sector, proxy if use_proxy else None

            except (ProxyError, ConnectionError, Timeout, HTTPError) as e:
                logger.warning(f"[{symbol}] 代理 {proxy} 网络错误: {str(e)}")
                time.sleep(random.uniform(1, 3))
                continue

            except Exception as e:
                error_msg = str(e)
                if "Rate limited" in error_msg or "Too Many Requests" in error_msg:
                    logger.warning(f"[{symbol}] 触发限流，等待后重试")
                    time.sleep(random.uniform(5, 10))
                else:
                    logger.warning(f"[{symbol}] 代理 {proxy} 处理错误: {error_msg}")
                time.sleep(random.uniform(1, 3))
                continue

        # 所有代理均失败，等待后进入下一次重试
        if use_proxy and attempt < max_retries:
            wait = 2**attempt * random.uniform(1, 2)
            logger.warning(f"[{symbol}] 所有代理失败，{wait:.1f}秒后重试")
            time.sleep(wait)

    return "N/A", "N/A", None


def get_processed_symbols(output_file):
    """读取已处理的股票代码"""
    processed = set()
    if os.path.exists(output_file):
        try:
            with open(output_file, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                next(reader)  # 跳过表头
                for row in reader:
                    if len(row) >= 2:  # 确保有symbol列
                        processed.add(row[1])
        except Exception as e:
            logger.error(f"读取已处理文件失败: {str(e)}")
    return processed


def get_us_stock_symbols(cache_file, output_file):
    """获取未处理的美股代码列表（带CSV缓存）"""
    processed = get_processed_symbols(output_file)
    trade_date = ToolKit("获取最新交易日").get_us_latest_trade_date(1)
    em = EMWebCrawlerUti()

    try:
        # 如果缓存文件存在则直接读取
        if os.path.exists(cache_file):
            stock_df = pd.read_csv(
                cache_file,
                usecols=["symbol", "mkt_code"],
                on_bad_lines="skip",
                engine="python",
                encoding="utf-8",
            )
            stock_list = stock_df["symbol"].tolist()
            logger.info(f"从缓存文件 {cache_file} 加载股票代码")
        else:
            # 无缓存时请求接口
            logger.info("未找到缓存文件，开始请求原始数据...")
            stock_list = em.get_stock_list(
                market="us", trade_date=trade_date, target_file=cache_file
            )

        all_symbols = stock_list
        logger.info(f"总代码数量：{len(all_symbols)}")

        filtered = [
            s
            for s in all_symbols
            if isinstance(s, str) and s.strip() != "" and s not in processed
        ]
        logger.info(f"待处理代码数量：{len(filtered)}")
        return filtered

    except Exception as e:
        logger.error(f"股票代码获取失败: {str(e)}")
        if os.path.exists(cache_file):
            try:
                os.remove(cache_file)
                logger.warning(f"已移除损坏的缓存文件 {cache_file}")
            except Exception as remove_error:
                logger.error(f"无法移除损坏文件: {str(remove_error)}")
        return []


def main(PROXY_LIST, CACHE_FILE, OUTPUT_FILE):
    symbols = get_us_stock_symbols(CACHE_FILE, OUTPUT_FILE)
    if not symbols:
        logger.info("没有需要处理的新股票代码")
        return

    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["idx", "symbol", "industry", "sector"])

        batch_buffer = []
        global_index = sum(1 for _ in open(OUTPUT_FILE, "r", encoding="utf-8-sig")) - 1

        current_proxy = None

        for idx, symbol in enumerate(symbols, 1):
            try:
                start_time = time.time()

                industry, sector, used_proxy = get_industry_info(
                    symbol,
                    PROXY_LIST,
                    max_retries=1,
                    preferred_proxy=current_proxy,
                )
                if used_proxy:
                    current_proxy = used_proxy
                else:
                    current_proxy = None

                global_index += 1
                record = [global_index, symbol, industry, sector]
                batch_buffer.append(record)

                if len(batch_buffer) >= 10:
                    writer.writerows(batch_buffer)
                    f.flush()
                    batch_buffer.clear()

                logger.info(
                    f"已处理 {idx}/{len(symbols)} | 耗时: {time.time() - start_time:.2f}s"
                )
                # time.sleep(random.uniform(0.2, 0.5))

            except KeyboardInterrupt:
                logger.info("用户中断，保存已处理数据...")
                writer.writerows(batch_buffer)
                f.flush()
                return
            except Exception as e:
                logger.error(f"处理 {symbol} 失败: {str(e)}")

        if batch_buffer:
            writer.writerows(batch_buffer)
            f.flush()


def convert_industry(source_file: str, map_file: str, target_file: str) -> None:
    """转换行业(industry)与板块(sector)信息并生成新文件

    规则：
      1. 使用 map_file 映射 industry 与 sector 列。
      2. 无论是否能映射到中文，均保留记录（无映射时保留原始文本），不过滤任何数据。
      3. 输出字段固定为 ["idx", "symbol", "industry", "sector"]。
    """
    try:
        # 1. 读取源文件与映射文件
        df_source = pd.read_csv(source_file)
        df_map = pd.read_csv(map_file)

        # 2. 建立英文到中文的映射字典
        mapping = df_map.set_index("industry_eng")["industry_cn"].to_dict()

        df_result = df_source.copy()

        # 3. 映射 industry：若映射表中存在则替换，不存在（NaN）则保留原文本
        if "industry" in df_result.columns:
            mapped_ind = df_result["industry"].map(mapping)
            df_result["industry"] = mapped_ind.fillna(df_result["industry"])
        else:
            df_result["industry"] = None

        # 4. 映射 sector：若映射表中存在则替换，不存在（NaN）则保留原文本
        if "sector" in df_result.columns:
            mapped_sec = df_result["sector"].map(mapping)
            df_result["sector"] = mapped_sec.fillna(df_result["sector"])
        else:
            df_result["sector"] = None

        # 5. 重新分配 idx（保证连续递增编号）
        df_result.reset_index(drop=True, inplace=True)
        df_result["idx"] = df_result.index

        # 6. 导出指定的 4 列
        df_result[["idx", "symbol", "industry", "sector"]].to_csv(
            target_file, index=False, encoding="utf-8"
        )

        print(f"成功生成文件: {target_file}")
        print(f"处理记录数: {len(df_result)}")

    except FileNotFoundError as e:
        print(f"文件不存在错误: {str(e)}")
    except KeyError as e:
        print(f"缺少必要列: {str(e)}")
    except Exception as e:
        print(f"处理异常: {str(e)}")


if __name__ == "__main__":
    # 代理列表（请自行替换为有效的代理）
    proxy_list = [
        "http://45.186.6.104:3128",
        "http://95.3.69.222:8080",
        "http://181.39.25.196:8118",
        "http://64.112.184.210:3128",
        "http://15.235.21.254:8080",
        "http://87.251.77.29:3128",
        "http://130.110.103.245:3128",
        "http://42.96.18.62:1311",
        "http://140.238.32.108:3128",
        "http://171.100.254.137:8080",
        "http://43.156.236.238:80",
        "http://190.97.236.128:999",
        "http://45.66.249.187:8181",
        "http://43.134.141.85:80",
        "http://43.156.228.168:80",
        "http://199.7.149.90:3128",
        "http://47.91.65.23:3128",
        "http://164.52.214.97:8080",
        "http://199.7.149.96:3128",
        "http://109.94.1.23:4050",
        "http://43.160.242.118:3128",
        "http://151.185.58.33:8080",
        "http://8.215.25.3:2080",
        "http://117.236.124.166:3128",
        "http://45.43.60.220:8080",
        "http://128.140.113.110:8081",
        "http://45.232.0.2:8080",
        "http://8.215.25.3:2081",
        "http://116.202.108.111:3128",
        "http://151.185.59.20:8080",
        "http://151.185.59.19:8080",
        "http://47.57.69.227:3128",
        "http://103.211.103.170:3128",
        "http://87.236.23.201:3128",
        "http://14.251.13.20:8080",
        "http://95.190.193.74:3128",
        "http://119.18.147.179:96",
        "http://203.150.128.157:8080",
        "http://103.155.196.160:8181",
        "http://223.207.101.166:8080",
        "http://186.0.170.20:8080",
        "http://103.80.82.7:8181",
        "http://103.147.134.117:8082",
        "http://103.218.122.183:8080",
        "http://37.58.221.247:3128",
        "http://31.15.169.77:808",
        "http://180.191.229.193:5050",
        "http://195.62.49.101:22855",
        "http://124.105.87.12:8087",
        "http://62.182.199.134:3128",
        "http://165.99.14.18:2765",
        "http://103.175.237.36:8080",
        "http://46.209.15.187:8080",
        "http://187.190.127.212:80",
        "http://91.210.108.19:8080",
        "http://180.191.234.124:8080",
        "http://151.243.153.157:8118",
        "http://190.0.246.210:4040",
        "http://196.204.80.105:1981",
        "http://128.140.113.110:5678",
        "http://165.99.14.18:5432",
        "http://139.167.218.162:3127",
        "http://139.162.89.198:3128",
        "http://165.99.14.18:1111",
        "http://122.117.203.252:3128",
        "http://195.62.49.101:59061",
        "http://85.209.156.148:1080",
        "http://49.147.127.126:8082",
        "http://101.47.74.252:8888",
        "http://165.154.20.187:10808",
        "http://103.46.8.61:8080",
        "http://180.191.138.172:8082",
        "http://165.154.7.156:8888",
        "http://126.209.3.199:5050",
        "http://187.190.127.212:8081",
        "http://164.52.216.51:8080",
        "http://47.77.186.212:3128",
        "http://181.78.243.243:999",
    ]
    CACHE_FILE = FINANCE_ROOT / "usstockinfo" / "symbol_list_cache.csv"
    OUTPUT_FILE = FINANCE_ROOT / "usstockinfo" / "industry_yfinance.csv"

    main(proxy_list, CACHE_FILE, OUTPUT_FILE)
    convert_industry(
        source_file=OUTPUT_FILE,
        map_file=FINANCE_ROOT / "usstockinfo" / "industry_yfinance_mapping.csv",
        target_file=FINANCE_ROOT / "usstockinfo" / "industry_yfinance_cn.csv",
    )
