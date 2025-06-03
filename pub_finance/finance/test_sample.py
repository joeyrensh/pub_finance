import akshare as ak
import pandas as pd
import requests
import time
import random
import logging
from requests.exceptions import ProxyError, ConnectionError, Timeout, HTTPError
import csv
import os

# 东财股票实时行情
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

# 东方财富ETF实时行情
fund_etf_spot_em_df = ak.fund_etf_spot_em()
print(fund_etf_spot_em_df)
