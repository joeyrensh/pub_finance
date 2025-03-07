import akshare as ak
import yfinance as yf
import pandas as pd
import requests
import time
import random
import logging
from requests.exceptions import ProxyError, ConnectionError, Timeout, HTTPError
import csv

# 配置日志记录
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_us_stock_symbols():
    """获取美股代码列表（示例取前20只）"""
    try:
        stock_df = ak.stock_us_spot_em()
        stock_df["clean_symbol"] = stock_df["代码"].str.split(".").str[1]
        # 测试用例，仅取前1只
        return stock_df["clean_symbol"].tolist()[:1]
        # 实际使用时应返回全部
        # return stock_df["clean_symbol"].tolist()
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}")
        return []


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

                # 验证数据完整性
                if "industry" not in info:
                    raise ValueError("行业信息字段缺失")
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

    return "获取失败"


def main(PROXY_LIST, OUTPUT_FILE):
    symbols = get_us_stock_symbols()
    if not symbols:
        return

    global_index = 1  # 全局索引
    batch_buffer = []  # 批量写入缓冲区

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["idx", "symbol", "industry"])  # 写入表头

        for idx, symbol in enumerate(symbols, 1):
            try:
                start_time = time.time()

                # 获取行业信息（最大重试次数5）
                industry = get_industry_info(symbol, PROXY_LIST, max_retries=1)

                # 构建记录
                record = [global_index, symbol, industry]
                batch_buffer.append(record)
                global_index += 1

                # 每10条批量写入
                if len(batch_buffer) >= 10:
                    writer.writerows(batch_buffer)
                    f.flush()
                    batch_buffer.clear()

                # 进度日志
                logger.info(
                    f"已处理 {idx}/{len(symbols)} | 耗时: {time.time() - start_time:.2f}s"
                )

                # 随机延迟（1-2秒）
                time.sleep(random.uniform(1, 2))

            except KeyboardInterrupt:
                logger.info("用户中断操作")
                break
            except Exception as e:
                logger.error(f"处理 {symbol} 时发生意外错误: {str(e)}")

        # 写入剩余数据
        if batch_buffer:
            writer.writerows(batch_buffer)
            f.flush()


if __name__ == "__main__":
    proxy_list = ["http://127.0.0.1:2081"]  # 示例空代理列表
    OUTPUT_FILE = "./usstockinfo/industry_yfinance.csv"
    # OUTPUT_FILE = "./industry_yfinance.csv"
    main(proxy_list, OUTPUT_FILE)
