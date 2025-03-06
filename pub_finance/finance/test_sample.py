import akshare as ak
import yfinance as yf
import pandas as pd
import requests
import time
import random
import logging
from requests.exceptions import ProxyError, ConnectionError, Timeout, HTTPError

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
        return stock_df["clean_symbol"].tolist()[:1]
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}")
        return []


def get_industry_info(symbol, proxy_list, max_retries=3):
    """带代理轮换的重试机制"""
    if not proxy_list:
        logger.error("代理列表为空，无法请求")
        return "获取失败"

    # 记录已用过的代理避免重复尝试
    used_proxies = set()

    for attempt in range(1, max_retries + 1):
        # 计算当前应使用的代理索引
        proxy_idx = (attempt - 1) % len(proxy_list)
        current_proxy = proxy_list[proxy_idx]

        # 如果代理已尝试过则跳过
        if current_proxy in used_proxies:
            continue

        try:
            logger.info(
                f"尝试 [{symbol}] 第{attempt}次请求 | 使用代理: {current_proxy}"
            )

            # 创建独立会话
            with requests.Session() as session:
                session.proxies = {"http": current_proxy, "https": current_proxy}
                session.headers.update(
                    {
                        "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (尝试次数: {attempt})"
                    }
                )

                # 创建带代理的Ticker对象
                ticker = yf.Ticker(symbol, session=session)
                info = ticker.info

                # 验证数据有效性
                if "industry" not in info:
                    raise ValueError("行业信息字段缺失")

                industry = info.get("industry", "N/A")
                logger.info(f"✅ 成功获取 {symbol} 行业信息")
                return industry

        except (ProxyError, ConnectionError, Timeout, HTTPError) as e:
            logger.warning(f"代理 {current_proxy} 失效，标记并切换代理")
            used_proxies.add(current_proxy)
            time.sleep(random.uniform(1, 3))

        except Exception as e:
            logger.error(f"其他错误: {str(e)}")
            time.sleep(2)

        # 当所有代理都尝试过时清空记录重新循环
        if len(used_proxies) == len(proxy_list):
            logger.warning("所有代理均已尝试，重置代理状态")
            used_proxies.clear()
            time.sleep(5)  # 增加冷却时间

    return "获取失败"


def main(PROXY_LIST):
    # 获取股票列表
    symbols = get_us_stock_symbols()
    if not symbols:
        return

    # 结果存储
    results = []

    # 遍历处理
    for idx, symbol in enumerate(symbols, 1):
        try:
            start_time = time.time()

            # 获取行业信息（最多重试5次）
            industry = get_industry_info(symbol, PROXY_LIST, max_retries=50)

            # 记录结果
            results.append(
                {
                    "symbol": symbol,
                    "industry": industry,
                    "time_cost": round(time.time() - start_time, 2),
                }
            )

            # 进度输出
            logger.info(
                f"进度: {idx}/{len(symbols)} | 耗时: {results[-1]['time_cost']}s"
            )

            # 随机延迟（3-8秒）
            time.sleep(random.uniform(3, 8))

        except KeyboardInterrupt:
            logger.info("用户中断操作")
            break

        except Exception as e:
            logger.error(f"处理 {symbol} 时发生意外错误: {str(e)}")

    # 结果展示
    result_df = pd.DataFrame(results)
    print("\n最终结果：")
    print(result_df)


if __name__ == "__main__":
    proxy_list = [
        "http://23.82.137.162:80",
    ]
    main(proxy_list)
