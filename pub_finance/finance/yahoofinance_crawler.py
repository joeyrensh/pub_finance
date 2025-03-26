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
last_success_proxy = None  # 持久化存储最后成功的代理


def get_industry_info(symbol, proxy_list, max_retries=3):
    global last_success_proxy

    use_proxy = bool(proxy_list)
    if not use_proxy:
        logger.info(f"[{symbol}] 代理列表为空，直连访问")

    # 代理使用优先级：最后成功 > 未使用代理 > 已失败代理
    proxy_priority_list = []
    if last_success_proxy and last_success_proxy in proxy_list:
        proxy_priority_list.append(last_success_proxy)
        proxy_priority_list.extend([p for p in proxy_list if p != last_success_proxy])
    else:
        proxy_priority_list = proxy_list.copy()

    for attempt in range(1, max_retries + 1):
        for proxy in proxy_priority_list:
            try:
                # 创建新会话
                with requests.Session() as session:
                    if use_proxy:
                        session.proxies = {"http": proxy, "https": proxy}
                        logger.info(f"[{symbol}] 尝试第{attempt}次请求 | 代理: {proxy}")
                    else:
                        logger.info(f"[{symbol}] 尝试第{attempt}次请求 | 直连")

                    # 动态请求头
                    session.headers.update(
                        {
                            "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            f"AppleWebKit/537.36 (尝试次数: {attempt})"
                        }
                    )

                    # 获取行业信息
                    ticker = yf.Ticker(symbol, session=session)
                    info = ticker.info
                    industry = info.get("industry", "N/A")

                    # 记录成功代理
                    if use_proxy:
                        last_success_proxy = proxy
                        logger.debug(f"[{symbol}] 代理 {proxy} 标记为有效")

                    logger.info(f"✅ 成功获取 {symbol} 行业信息")
                    return industry

            except (ProxyError, ConnectionError, Timeout, HTTPError) as e:
                if use_proxy:
                    logger.warning(f"[{symbol}] 代理 {proxy} 失效 | 错误: {str(e)}")
                else:
                    logger.warning(f"[{symbol}] 直连失败 | 错误: {str(e)}")
                continue  # 继续尝试下一个代理

            except Exception as e:
                logger.error(f"[{symbol}] 处理错误: {str(e)}")
                continue  # 关键修复：继续尝试下一个代理而非直接返回

        # 当前优先级列表全部失败时重置
        if use_proxy:
            logger.warning(f"[{symbol}] 所有代理尝试失败，重置代理状态")
            last_success_proxy = None
            proxy_priority_list = proxy_list.copy()

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
                industry = get_industry_info(symbol, PROXY_LIST, max_retries=1)

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
                time.sleep(random.uniform(0.5, 1))

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


def convert_industry(source_file: str, map_file: str, target_file: str) -> None:
    """
    转换行业信息并生成新文件

    参数:
        source_file: 输入文件a.csv路径
        map_file: 映射文件b.csv路径
        target_file: 输出文件c.csv路径
    """
    try:
        # 读取原始数据
        df_a = pd.read_csv(source_file)

        # 过滤无效行业数据
        valid_industry = df_a["industry"].notna() & (
            df_a["industry"].str.upper() != "N/A"
        )
        df_filtered = df_a[valid_industry].copy()

        # 读取映射关系
        df_b = pd.read_csv(map_file)
        mapping = df_b.set_index("industry_eng")["industry_cn"].to_dict()

        # 进行行业转换
        df_filtered.loc[:, "industry"] = df_filtered["industry"].map(mapping)

        # 移除转换失败的记录
        df_result = df_filtered.dropna(subset=["industry"])

        # 保存结果
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
    # free proxy website : http://free-proxy.cz/en/proxylist/country/all/https/ping/level1
    proxy_list = [
        # "http://118.27.111.97:80",
        "http://142.93.211.107:3128",
    ]  # 代理列表
    CACHE_FILE = "./usstockinfo/symbol_list_cache.csv"
    OUTPUT_FILE = "./usstockinfo/industry_yfinance.csv"

    main(proxy_list, CACHE_FILE, OUTPUT_FILE)
    # convert_industry(
    #     source_file=OUTPUT_FILE,
    #     map_file="./usstockinfo/industry_mapping.csv",
    #     target_file="./usstockinfo/industry_yfinance_cn.csv",
    # )
