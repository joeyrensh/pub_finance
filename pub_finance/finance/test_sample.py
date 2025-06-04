import akshare as ak
import pandas as pd
import requests
import time
import random
import logging
from requests.exceptions import ProxyError, ConnectionError, Timeout, HTTPError
import csv
import os

# # 东财股票实时行情
# stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
# print(stock_zh_a_spot_em_df)

# 新浪股票实时行情-缺失总市值
# stock_zh_a_spot_df = ak.stock_zh_a_spot()
# print(stock_zh_a_spot_df)

# 东方财富股票历史数据 （包含ETF）
# stock_zh_a_hist_df = ak.stock_zh_a_hist(
#     symbol="159707",
#     period="daily",
#     start_date="20240101",
#     end_date="20250603",
#     adjust="qfq",
# )
# print(stock_zh_a_hist_df)

# 东方财富股票历史数据-ETF
# fund_etf_hist_em_df = ak.fund_etf_hist_em(
#     symbol="159707",
#     period="daily",
#     start_date="20240101",
#     end_date="20250603",
#     adjust="qfq",
# )
# print(fund_etf_hist_em_df)

# 腾讯股票历史数据(包含ETF，缺失总市值，成交额)
# stock_zh_a_hist_tx_df = ak.stock_zh_a_hist_tx(
#     symbol="sz159707", start_date="20240101", end_date="20250603", adjust="qfq"
# )
# print(stock_zh_a_hist_tx_df)


# # 东财股票实时行情
# stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
# pd_stock = stock_zh_a_spot_em_df[
#     [
#         "代码",
#         "名称",
#         "今开",
#         "最新价",
#         "最高",
#         "最低",
#         "成交量",
#         "总市值",
#         "市盈率-动态",
#     ]
# ].copy()
# # 过滤“最新价”大于0的数据
# pd_stock = pd_stock[pd_stock["最新价"] > 0]

# # 重命名列名
# pd_stock = pd_stock.rename(
#     columns={
#         "代码": "symbol",
#         "名称": "name",
#         "今开": "open",
#         "最新价": "close",
#         "最高": "high",
#         "最低": "low",
#         "成交量": "volume",
#         "总市值": "total_value",
#         "市盈率-动态": "pe",
#     }
# )
# # 东方财富ETF实时行情
# fund_etf_spot_em_df = ak.fund_etf_spot_em()
# pd_etf = fund_etf_spot_em_df[
#     [
#         "代码",
#         "名称",
#         "开盘价",
#         "最新价",
#         "最高价",
#         "最低价",
#         "成交量",
#         "总市值",
#     ]
# ].copy()
# pd_etf = pd_etf[pd_etf["最新价"] > 0]
# # 重命名列名
# pd_etf = pd_etf.rename(
#     columns={
#         "代码": "symbol",
#         "名称": "name",
#         "开盘价": "open",
#         "最新价": "close",
#         "最高价": "high",
#         "最低价": "low",
#         "成交量": "volume",
#         "总市值": "total_value",
#     }
# )
# # 合并 pd_stock 和 pd_etf，保证 symbol 不重复，pd_etf 增加 pe 列为 "-"
# pd_etf["pe"] = "-"
# # 对齐列顺序
# pd_etf = pd_etf[
#     ["symbol", "name", "open", "close", "high", "low", "volume", "total_value", "pe"]
# ]
# pd_stock = pd_stock[
#     ["symbol", "name", "open", "close", "high", "low", "volume", "total_value", "pe"]
# ]

# # 合并并去重（以 symbol 为唯一标识，保留 pd_stock 优先）
# pd_all = pd.concat([pd_stock, pd_etf])
# pd_all = pd_all.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)


# 东财股票实时行情 - 上海市场
stock_sh_a_spot_em = ak.stock_sh_a_spot_em()
pd_stock_sh = stock_sh_a_spot_em[
    [
        "代码",
        "名称",
        "今开",
        "最新价",
        "最高",
        "最低",
        "成交量",
        "总市值",
        "市盈率-动态",
    ]
].copy()
# 过滤“最新价”大于0的数据
pd_stock_sh = pd_stock_sh[pd_stock_sh["最新价"] > 0]

# 重命名列名
pd_stock_sh = pd_stock_sh.rename(
    columns={
        "代码": "symbol",
        "名称": "name",
        "今开": "open",
        "最新价": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "总市值": "total_value",
        "市盈率-动态": "pe",
    }
)


# 东财股票实时行情 - 深圳市场
stock_sz_a_spot_em = ak.stock_sz_a_spot_em()
pd_stock_sz = stock_sz_a_spot_em[
    [
        "代码",
        "名称",
        "今开",
        "最新价",
        "最高",
        "最低",
        "成交量",
        "总市值",
        "市盈率-动态",
    ]
].copy()
# 过滤“最新价”大于0的数据
pd_stock_sz = pd_stock_sz[pd_stock_sz["最新价"] > 0]

# 重命名列名
pd_stock_sz = pd_stock_sz.rename(
    columns={
        "代码": "symbol",
        "名称": "name",
        "今开": "open",
        "最新价": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "总市值": "total_value",
        "市盈率-动态": "pe",
    }
)

# 东方财富ETF实时行情
fund_etf_spot_em_df = ak.fund_etf_spot_em()
pd_etf = fund_etf_spot_em_df[
    [
        "代码",
        "名称",
        "开盘价",
        "最新价",
        "最高价",
        "最低价",
        "成交量",
        "总市值",
    ]
].copy()
pd_etf = pd_etf[pd_etf["最新价"] > 0]
# 重命名列名
pd_etf = pd_etf.rename(
    columns={
        "代码": "symbol",
        "名称": "name",
        "开盘价": "open",
        "最新价": "close",
        "最高价": "high",
        "最低价": "low",
        "成交量": "volume",
        "总市值": "total_value",
    }
)
# 合并 pd_stock 和 pd_etf，保证 symbol 不重复，pd_etf 增加 pe 列为 "-"
pd_etf["pe"] = "-"
# 对齐列顺序
pd_etf = pd_etf[
    [
        "symbol",
        "name",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "total_value",
        "pe",
    ]
]
# 合并并去重（以 symbol 为唯一标识，保留 pd_stock 优先）
pd_all = pd.concat([pd_stock_sh, pd_stock_sz, pd_etf])
pd_all = pd_all.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
