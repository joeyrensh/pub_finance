import akshare as ak
import yfinance as yf
import pandas as pd
import requests
import time
import random
import logging
from requests.exceptions import ProxyError, ConnectionError, Timeout, HTTPError
import csv
import os

# 配置日志记录
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_industry_info(symbol, proxy_list, max_retries=1):
    """带代理轮换的重试机制"""
    use_proxy = bool(proxy_list)
    if not use_proxy:
        logger.info("代理列表为空，请求将不使用代理。")

    used_proxies = set()  # 记录已用过的代理

    for attempt in range(1, max_retries + 1):
        current_proxy = None
        if use_proxy:
            # 计算当前应使用的代理索引
            proxy_idx = (attempt - 1) % len(proxy_list)
            current_proxy = proxy_list[proxy_idx]
            if current_proxy in used_proxies:
                continue  # 跳过已尝试的代理
        else:
            current_proxy = None  # 不使用代理

        try:
            # 日志记录代理使用情况
            if use_proxy:
                logger.info(
                    f"尝试 [{symbol}] 第{attempt}次请求 | 使用代理: {current_proxy}"
                )
            else:
                logger.info(f"尝试 [{symbol}] 第{attempt}次请求 | 不使用代理")

            # 创建独立会话并设置代理
            with requests.Session() as session:
                if use_proxy:
                    session.proxies = {"http": current_proxy, "https": current_proxy}

                # 动态User-Agent
                session.headers.update(
                    {
                        "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (尝试次数: {attempt})"
                    }
                )

                # 获取股票信息
                ticker = yf.Ticker(symbol, session=session)
                info = ticker.info
                industry = info.get("industry", "N/A")
                logger.info(f"✅ 成功获取 {symbol} 的行业信息")
                return industry

        except (ProxyError, ConnectionError, Timeout, HTTPError) as e:
            if use_proxy:
                logger.warning(
                    f"代理 {current_proxy} 失效，标记并切换 | 错误: {str(e)}"
                )
                used_proxies.add(current_proxy)
            else:
                logger.warning(f"请求失败（无代理）| 错误: {str(e)}")
            time.sleep(random.uniform(1, 2))

        except Exception as e:
            logger.error(f"处理 {symbol} 时发生其他错误: {str(e)}")
            time.sleep(2)

        # 当所有代理均尝试后重置
        if use_proxy and len(used_proxies) == len(proxy_list):
            logger.warning("所有代理均已尝试，重置代理状态")
            used_proxies.clear()

    return "N/A"


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

    try:
        # 如果缓存文件存在则直接读取
        if os.path.exists(cache_file):
            stock_df = pd.read_csv(cache_file)
            logger.info(f"从缓存文件 {cache_file} 加载股票代码")
        else:
            # 无缓存时请求接口
            logger.info("未找到缓存文件，开始请求原始数据...")
            stock_df = ak.stock_us_spot_em()
            stock_df["clean_symbol"] = stock_df["代码"].str.split(".").str[1]

            # 保存缓存
            stock_df.to_csv(cache_file, index=False)
            logger.info(f"已缓存股票代码到 {cache_file}")

        # 获取有效代码列表
        all_symbols = stock_df["clean_symbol"].tolist()
        logger.info(f"总代码数量：{len(all_symbols)}")

        # 过滤已处理代码
        filtered = [s for s in all_symbols if s not in processed]
        logger.info(f"待处理代码数量：{len(filtered)}")
        return filtered

    except Exception as e:
        logger.error(f"股票代码获取失败: {str(e)}")
        # 如果缓存文件生成失败，尝试删除损坏文件
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

    # 创建文件并写入表头（如果文件不存在）
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["idx", "symbol", "industry"])

        batch_buffer = []
        global_index = (
            sum(1 for _ in open(OUTPUT_FILE, "r", encoding="utf-8-sig")) - 1
        )  # 计算当前索引

        for idx, symbol in enumerate(symbols, 1):
            try:
                start_time = time.time()

                # 获取行业信息
                industry = get_industry_info(symbol, PROXY_LIST, max_retries=3)

                # 构建记录
                global_index += 1
                record = [global_index, symbol, industry]
                batch_buffer.append(record)

                # 批量写入
                if len(batch_buffer) >= 10:
                    writer.writerows(batch_buffer)
                    f.flush()
                    batch_buffer.clear()

                logger.info(
                    f"已处理 {idx}/{len(symbols)} | 耗时: {time.time() - start_time:.2f}s"
                )
                time.sleep(random.uniform(1, 2))

            except KeyboardInterrupt:
                logger.info("用户中断，保存已处理数据...")
                writer.writerows(batch_buffer)
                f.flush()
                return
            except Exception as e:
                logger.error(f"处理 {symbol} 失败: {str(e)}")

        # 写入剩余数据
        if batch_buffer:
            writer.writerows(batch_buffer)
            f.flush()


if __name__ == "__main__":
    proxy_list = [
        "http://52.48.78.67:3128",
        "http://52.18.193.139:3128",
        "http://63.34.176.150:3128",
    ]  # 代理列表
    CACHE_FILE = "./usstockinfo/symbol_list_cache.csv"
    OUTPUT_FILE = "./usstockinfo/industry_yfinance.csv"

    main(proxy_list, CACHE_FILE, OUTPUT_FILE)
