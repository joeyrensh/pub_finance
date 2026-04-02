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
from finance.paths import FINANCE_ROOT
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
                industry = info.get("industry", None)

                if use_proxy:
                    last_success_proxy = proxy
                logger.info(f"✅ 成功获取 {symbol} 行业信息: {industry}")
                return industry, proxy if use_proxy else None

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

    return "N/A", None


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
            writer.writerow(["idx", "symbol", "industry"])

        batch_buffer = []
        global_index = sum(1 for _ in open(OUTPUT_FILE, "r", encoding="utf-8-sig")) - 1

        current_proxy = None

        for idx, symbol in enumerate(symbols, 1):
            try:
                start_time = time.time()

                industry, used_proxy = get_industry_info(
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
                record = [global_index, symbol, industry]
                batch_buffer.append(record)

                if len(batch_buffer) >= 10:
                    writer.writerows(batch_buffer)
                    f.flush()
                    batch_buffer.clear()

                logger.info(
                    f"已处理 {idx}/{len(symbols)} | 耗时: {time.time() - start_time:.2f}s"
                )
                time.sleep(random.uniform(0.5, 1))

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
    """
    转换行业信息并生成新文件
    """
    try:
        df_a = pd.read_csv(source_file)
        valid_industry = df_a["industry"].notna() & (
            df_a["industry"].str.upper() != "N/A"
        )
        df_filtered = df_a[valid_industry].copy()
        df_b = pd.read_csv(map_file)
        mapping = df_b.set_index("industry_eng")["industry_cn"].to_dict()
        df_filtered.loc[:, "industry"] = df_filtered["industry"].map(mapping)
        df_result = df_filtered.dropna(subset=["industry"])
        df_result[["idx", "symbol", "industry"]].to_csv(target_file, index=False)
        print(f"成功生成文件: {target_file}")
        print(f"原始记录数: {len(df_a)}, 有效记录数: {len(df_result)}")
    except FileNotFoundError as e:
        print(f"文件不存在错误: {str(e)}")
    except KeyError as e:
        print(f"缺少必要列: {str(e)}")
    except Exception as e:
        print(f"处理异常: {str(e)}")


if __name__ == "__main__":
    # 代理列表（请自行替换为有效的代理）
    proxy_list = [
        "http://1.231.81.166:3128",
        "http://103.178.21.104:3125",
        "http://103.191.254.134:8080",
        "http://113.160.132.26:8080",
        "http://83.219.250.8:62920",
        "http://95.213.217.168:52004",
        "http://208.87.243.199:7878",
        "http://34.101.184.164:3128",
        "http://8.213.151.128:3128",
        "http://202.183.236.219:8080",
    ]
    CACHE_FILE = FINANCE_ROOT / "usstockinfo" / "symbol_list_cache.csv"
    OUTPUT_FILE = FINANCE_ROOT / "usstockinfo" / "industry_yfinance.csv"

    main(proxy_list, CACHE_FILE, OUTPUT_FILE)
    # convert_industry(
    #     source_file=OUTPUT_FILE,
    #     map_file=FINANCE_ROOT / "usstockinfo" / "industry_yfinance_mapping.csv",
    #     target_file=FINANCE_ROOT / "usstockinfo" / "industry_yfinance_cn.csv",
    # )
