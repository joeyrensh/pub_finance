import datetime
from functools import partialmethod
import gc
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

import akshare as ak
import pandas as pd
from tqdm import tqdm
import yfinance as yf
import logging
from curl_cffi import requests as curl_requests
logger = logging.getLogger(__name__)
# 屏蔽 yfinance 冗余日志
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


def _get_us_main_exchange_symbols_from_sec() -> Set[str]:
    """
    基于跨市场挂牌结构 + CIK 多记录频次统计（>= 2条）的精炼 ADR 池
    (完全消除文本正则判定，结合 CIK 频次特征与跨市场特征，高效提取 ADR 目标池)
    """

    headers = {'User-Agent': 'QuantDataServices admin@quantdata.com'}
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    
    target_symbols = set()
    max_retries = 2

    for attempt in range(1, max_retries + 1):
        try:
            # curl_requests 自动读取 os.environ 中的 HTTP_PROXY / HTTPS_PROXY
            res = curl_requests.get(url, headers=headers, timeout=12)
            if res.status_code == 200:
                data = res.json()
                # 适配 JSON 格式: ['cik', 'name', 'ticker', 'exchange']
                df = pd.DataFrame(data['data'], columns=data['fields'])
                
                df['exchange_upper'] = df['exchange'].str.upper().str.strip()
                df['clean_ticker'] = df['ticker'].str.upper().str.strip()

                main_exchanges = {'NYQ', 'NYSE', 'NMS', 'NGS', 'NCM', 'NASDAQ', 'ASE'}
                otc_exchanges = {'OTC', 'OTCBB', 'PINK'}

                df['is_main'] = df['exchange_upper'].isin(main_exchanges)
                df['is_otc'] = df['exchange_upper'].isin(otc_exchanges)

                # ---------------------------------------------------------------------
                # 规则 1 [跨市场结构]: 查找 CIK 同事跨越【主板】与【OTC/场外】市场的标的
                # (精炼提取 BABA, TSM 等绝大多数通过多重挂牌备案的 ADR)
                # ---------------------------------------------------------------------
                main_ciks = set(df[df['is_main']]['cik'])
                otc_ciks = set(df[df['is_otc']]['cik'])
                cross_market_ciks = main_ciks.intersection(otc_ciks)

                # ---------------------------------------------------------------------
                # 规则 2 [CIK 频次特征]: 统计每个 CIK 出现的总记录数，超过 1 条（即 >= 2 条）即命中
                # (精准补全像 BBD/BBDO 这种同一公司在 SEC 存在多条记录/多类股票的 ADR)
                # ---------------------------------------------------------------------
                cik_counts = df['cik'].value_counts()
                multi_record_ciks = set(cik_counts[cik_counts >= 2].index)

                # ---------------------------------------------------------------------
                # [过滤]: 1. 过滤主板规范代码 (1~5位纯字母)
                #         2. 正则排除以 W (Warrant 权证) 或 U (Unit 单位股) 结尾的 5 位衍生品代码 (如 SATLW)
                # ---------------------------------------------------------------------
                valid_main_mask = df['is_main'] & df['clean_ticker'].str.match(r'^[A-Z]{1,5}$')
                is_derivative = df['clean_ticker'].str.match(r'^[A-Z]{4}[WU]$')
                
                # 显式 .copy() 避免后续衍生 SettingWithCopyWarning
                df_main_valid = df[valid_main_mask & (~is_derivative)].copy()

                # 判定条件：规则 1 (跨市场 CIK) OR 规则 2 (CIK 频次 >= 2)
                cond_cross_market = df_main_valid['cik'].isin(cross_market_ciks)
                cond_multi_record = df_main_valid['cik'].isin(multi_record_ciks)

                # 重点修改 1：通过 .copy() 明确分配独立内存空间，彻底消灭 Warning
                df_adr = df_main_valid[cond_cross_market | cond_multi_record].copy()
                
                # 重点修改 2：安全新增代码长度列并去重 (同 CIK 优先保留最简短的普通股主标的)
                df_adr.loc[:, 'ticker_len'] = df_adr['clean_ticker'].str.len()
                df_adr_sorted = df_adr.sort_values(by=['cik', 'ticker_len'])
                df_adr_unique = df_adr_sorted.drop_duplicates(subset=['cik'], keep='first')

                target_symbols = set(df_adr_unique['clean_ticker'].tolist())

                logger.info(
                    f"✅ [SEC 精炼ADR池] 提取成功: {len(target_symbols)} 只 "
                    f"(逻辑: 跨市场 CIK OR CIK频次>=2 | 已精准包含 BBD, BABA, TSM | 自动剔除 SATLW 等权证)"
                )
                break
            else:
                logger.warning(f"⚠️ [SEC 主板] 响应异常 Status: {res.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ [SEC 主板] 第 {attempt}/{max_retries} 次请求失败: {e}，尝试轮换代理...")

    return target_symbols

print(_get_us_main_exchange_symbols_from_sec())