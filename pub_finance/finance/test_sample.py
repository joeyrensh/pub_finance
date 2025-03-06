import akshare as ak
import yfinance as yf
import pandas as pd
import requests
import time
import random
import logging
from requests.exceptions import ProxyError, ConnectionError, Timeout, HTTPError
import os

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


def export_batch(batch_data, batch_number, output_dir="output"):
    """
    导出批次数据到CSV文件
    :param batch_data: 当前批次数据列表
    :param batch_number: 批次编号
    :param output_dir: 输出目录
    """
    try:
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        # 转换为DataFrame并选择字段
        df = pd.DataFrame(batch_data)[["索引", "symbol", "industry"]]

        # 生成文件名
        filename = os.path.join(output_dir, f"batch_{batch_number}.csv")

        # 导出文件
        df.to_csv(filename, index=False)
        logger.info(f"成功导出第 {batch_number} 批次数据，包含 {len(df)} 条记录")

    except Exception as e:
        logger.error(f"导出批次 {batch_number} 失败: {str(e)}")


def main(PROXY_LIST):
    BATCH_SIZE = 10  # 每批导出的记录数
    OUTPUT_DIR = "export_results"  # 输出目录

    # 获取股票列表
    symbols = get_us_stock_symbols()
    if not symbols:
        return

    # 初始化存储
    all_results = []  # 存储所有结果
    current_batch = []  # 当前批次数据
    batch_count = 1  # 批次计数器
    total_count = 0  # 总处理计数器

    # 遍历处理
    for idx, symbol in enumerate(symbols, 1):
        try:
            start_time = time.time()

            # 获取行业信息（最多重试5次）
            industry = get_industry_info(symbol, PROXY_LIST, max_retries=5)

            # 构建结果记录
            total_count += 1
            record = {
                "索引": total_count,  # 全局自增序号
                "symbol": symbol,
                "industry": industry,
                "_processed_time": round(time.time() - start_time, 2),
            }

            # 存储数据
            all_results.append(record)
            current_batch.append(record)

            # 达到批次大小触发导出
            if len(current_batch) >= BATCH_SIZE:
                export_batch(current_batch, batch_count, OUTPUT_DIR)
                current_batch = []
                batch_count += 1

            # 进度日志
            logger.info(
                f"已处理 {idx}/{len(symbols)} | 耗时: {record['_processed_time']}s"
            )

            # 随机延迟（3-8秒）
            time.sleep(random.uniform(3, 8))

        except KeyboardInterrupt:
            logger.info("用户中断操作")
            break

        except Exception as e:
            logger.error(f"处理 {symbol} 时发生意外错误: {str(e)}")

    # 导出最后未满批次的数据
    if current_batch:
        export_batch(current_batch, batch_count, OUTPUT_DIR)

    # 最终汇总展示
    final_df = pd.DataFrame(all_results)[["索引", "symbol", "industry"]]
    print("\n最终汇总结果：")
    print(final_df)


if __name__ == "__main__":
    proxy_list = [
        "http://23.82.137.162:80",
    ]
    main(proxy_list)
