import csv
import logging
import os
from pathlib import Path
import random
import sys
import time
from typing import Dict, Optional, Tuple

import pandas as pd
from requests.exceptions import ConnectionError, HTTPError, ProxyError, Timeout
import yfinance as yf

# ========== 1. 项目路径规范导入 ==========
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.em_stock_uti_fqt import EMWebCrawlerUti
from finance.utility.get_proxy import ProxyManager
from finance.utility.toolkit import ToolKit

# ========== 2. 添加日志配置 ==========
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 屏蔽 yfinance 冗余输出，提升日志可读性
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# 全局变量：记录当前可用的粘性代理
CURRENT_WORKING_PROXY: Optional[Dict[str, str]] = None


def format_proxy_url(proxy_str: str) -> str:
    """统一代理 URL 格式，补齐协议前缀"""
    if not proxy_str:
        return ""
    if not proxy_str.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return f"http://{proxy_str}"
    return proxy_str


def get_industry_info(
    symbol: str, proxy_manager: ProxyManager, max_retries: int = 3
) -> Tuple[str, str, Optional[Dict[str, str]]]:
    """获取股票行业信息（支持 ProxyManager + 优先复用上一轮成功代理）

    返回 (industry, sector, used_proxy_dict) 或 ("N/A", "N/A", None)
    """
    global CURRENT_WORKING_PROXY

    # 关闭 SSL 校验环境变量
    os.environ.setdefault("CURL_CA_BUNDLE", "")
    os.environ.setdefault("SSL_CERT_FILE", "")

    for attempt in range(1, max_retries + 1):
        # 1. 优先连用上一轮验证成功的可用代理，无需重复触发 ProxyManager 校验
        if CURRENT_WORKING_PROXY:
            proxy_dict = CURRENT_WORKING_PROXY
        else:
            proxy_dict = proxy_manager.get_working_proxy(
                max_retries=2, enable_proxy=True
            )
            if not proxy_dict:
                proxy_dict = proxy_manager.get_next_proxy()

        raw_proxy = (
            (proxy_dict.get("socks5") or proxy_dict.get("https") or proxy_dict.get("http"))
            if proxy_dict
            else None
        )
        proxy_str = format_proxy_url(raw_proxy) if raw_proxy else None

        try:
            # 2. 设置环境变量供 yfinance/requests 内部读取
            if proxy_str:
                os.environ["HTTP_PROXY"] = proxy_str
                os.environ["HTTPS_PROXY"] = proxy_str
                logger.info(
                    f"[{symbol}] 尝试第 {attempt}/{max_retries} 次请求 | 代理: {proxy_str}"
                )
            else:
                os.environ.pop("HTTP_PROXY", None)
                os.environ.pop("HTTPS_PROXY", None)
                logger.info(
                    f"[{symbol}] 尝试第 {attempt}/{max_retries} 次请求 | 直连"
                )

            # 3. 发起 yfinance 抓取
            ticker = yf.Ticker(symbol)
            info = ticker.info

            # 防范空响应与 TypeError
            if not isinstance(info, dict) or not info:
                raise ValueError("yfinance 返回数据为空或被隐式拦截")

            industry = info.get("industry", "N/A")
            sector = info.get("sector", "N/A")

            # 成功后锁定当前代理，供后续股票持续使用
            CURRENT_WORKING_PROXY = proxy_dict
            if proxy_str and hasattr(proxy_manager, "mark_proxy_working"):
                proxy_manager.mark_proxy_working(proxy_str)

            logger.info(f"✅ 成功获取 {symbol} 行业信息: {industry} | {sector}")
            return industry, sector, proxy_dict

        except (ProxyError, ConnectionError, Timeout, HTTPError) as e:
            logger.warning(
                f"[{symbol}] 代理 [{proxy_str}] 网络错误: {str(e)}"
            )
            # 标记代理失效并重置粘性代理
            if proxy_str and hasattr(proxy_manager, "mark_proxy_failed"):
                proxy_manager.mark_proxy_failed(proxy_str)
            CURRENT_WORKING_PROXY = None
            time.sleep(random.uniform(1, 2))

        except Exception as e:
            error_msg = str(e)
            if "Rate limited" in error_msg or "Too Many Requests" in error_msg:
                logger.warning(
                    f"[{symbol}] 触发限流 ({error_msg})，等待后重试"
                )
                time.sleep(random.uniform(5, 8))
            else:
                logger.warning(
                    f"[{symbol}] 代理 [{proxy_str}] 处理错误: {error_msg}"
                )
                if proxy_str and hasattr(proxy_manager, "mark_proxy_failed"):
                    proxy_manager.mark_proxy_failed(proxy_str)
                CURRENT_WORKING_PROXY = None
            time.sleep(random.uniform(1, 2))

        finally:
            # 清理代理环境变量，避免污染上下文
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)

    # 重试满 max_retries 次均失败
    return "N/A", "N/A", None


def get_processed_symbols(output_file: Path) -> set:
    """读取已处理的股票代码，统一清洗空格并转大写"""
    if not output_file.exists():
        return set()

    try:
        df = pd.read_csv(
            output_file,
            usecols=["symbol"],
            on_bad_lines="skip",
            engine="python",
            encoding="utf-8-sig",
        )
        # 精确读取 symbol 列，统一转为大写字符串集合
        processed = set(
            df["symbol"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .tolist()
        )
        logger.info(f"从 {output_file.name} 中成功解析出 {len(processed)} 个已处理代码")
        return processed
    except Exception as e:
        logger.error(f"读取已处理文件失败: {str(e)}")
        return set()


def get_us_stock_symbols(cache_file: Path, output_file: Path) -> list:
    """获取未处理的美股代码列表（兼容 dict 和 str 结构）"""
    processed = get_processed_symbols(output_file)
    trade_date = ToolKit("获取最新交易日").get_us_latest_trade_date(1)
    em = EMWebCrawlerUti()

    try:
        if cache_file.exists():
            stock_df = pd.read_csv(
                cache_file,
                usecols=["symbol"],
                on_bad_lines="skip",
                engine="python",
                encoding="utf-8-sig",
            )
            raw_symbols = stock_df["symbol"].dropna().tolist()
            logger.info(f"从缓存文件 {cache_file.name} 加载股票代码")
        else:
            logger.info("未找到缓存文件，开始从数据源提取原始数据...")
            raw_symbols = em.get_stock_list(
                market="us", trade_date=trade_date, target_file=cache_file
            )

        # 1. 兼容解析 dict 或 str 类型的 symbol 提取
        all_symbols = []
        for item in raw_symbols:
            if isinstance(item, dict):
                symbol_str = str(item.get("symbol", "")).strip().upper()
            elif isinstance(item, str):
                symbol_str = item.strip().upper()
            else:
                continue

            if symbol_str:
                all_symbols.append(symbol_str)

        logger.info(f"解析后的总代码数量：{len(all_symbols)}")

        # 2. 过滤已处理与重复的代码
        filtered = []
        seen = set()
        for s in all_symbols:
            if s not in processed and s not in seen:
                filtered.append(s)
                seen.add(s)

        logger.info(f"待处理代码数量：{len(filtered)}")
        return filtered

    except Exception as e:
        logger.error(f"股票代码获取失败: {str(e)}")
        return []


def main(proxy_manager: ProxyManager, cache_file: Path, output_file: Path):
    symbols = get_us_stock_symbols(cache_file, output_file)
    if not symbols:
        logger.info("没有需要处理的新股票代码")
        return

    file_exists = output_file.exists()
    with open(output_file, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["idx", "symbol", "industry", "sector"])

        batch_buffer = []
        global_index = 0
        if file_exists:
            with open(output_file, "r", encoding="utf-8-sig") as rf:
                global_index = max(0, sum(1 for _ in rf) - 1)

        for idx, symbol in enumerate(symbols, 1):
            try:
                start_time = time.time()

                industry, sector, _ = get_industry_info(
                    symbol, proxy_manager, max_retries=3
                )

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

            except KeyboardInterrupt:
                logger.info("用户中断，保存已处理数据...")
                if batch_buffer:
                    writer.writerows(batch_buffer)
                    f.flush()
                return
            except Exception as e:
                logger.error(f"处理 {symbol} 失败: {str(e)}")

        if batch_buffer:
            writer.writerows(batch_buffer)
            f.flush()


def convert_industry(
    source_file: Path, map_file: Path, target_file: Path
) -> None:
    """转换行业 (industry) 与板块 (sector) 信息并生成新文件"""
    try:
        df_source = pd.read_csv(source_file)
        df_map = pd.read_csv(map_file)

        mapping = df_map.set_index("industry_eng")["industry_cn"].to_dict()
        df_result = df_source.copy()

        if "industry" in df_result.columns:
            mapped_ind = df_result["industry"].map(mapping)
            df_result["industry"] = mapped_ind.fillna(df_result["industry"])

        if "sector" in df_result.columns:
            mapped_sec = df_result["sector"].map(mapping)
            df_result["sector"] = mapped_sec.fillna(df_result["sector"])

        df_result.reset_index(drop=True, inplace=True)
        df_result["idx"] = df_result.index

        df_result[["idx", "symbol", "industry", "sector"]].to_csv(
            target_file, index=False, encoding="utf-8"
        )

        logger.info(f"成功生成文件: {target_file}")
        logger.info(f"处理记录数: {len(df_result)}")

    except Exception as e:
        logger.error(f"处理转换行业异常: {str(e)}")


if __name__ == "__main__":
    # 使用 ProxyManager 初始化 overseas 代理管理
    proxy_manager = ProxyManager.create_overseas_manager()

    CACHE_FILE = FINANCE_ROOT / "usstockinfo" / "symbol_list_cache.csv"
    OUTPUT_FILE = FINANCE_ROOT / "usstockinfo" / "industry_yfinance.csv"
    MAP_FILE = FINANCE_ROOT / "usstockinfo" / "industry_yfinance_mapping.csv"
    TARGET_FILE = FINANCE_ROOT / "usstockinfo" / "industry_yfinance_cn.csv"

    # 执行爬取
    main(proxy_manager, CACHE_FILE, OUTPUT_FILE)

    # 导出中英文映射结果
    convert_industry(
        source_file=OUTPUT_FILE, map_file=MAP_FILE, target_file=TARGET_FILE
    )