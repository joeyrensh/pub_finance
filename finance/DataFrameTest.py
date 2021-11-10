#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
from UsFileInfo import UsFileInfo
from ToolKit import ToolKit
import multiprocessing
import gc


# tickers = list()
# df = pd.read_csv('./usstockinfo/usstock_20211109.csv',
#                  usecols=[i for i in range(1, 13)])
# """ 匹配行业信息 """
# df_o = pd.read_csv('./usstockinfo/usindustry_em.csv',
#                    usecols=[i for i in range(1, 3)])
# df_n = pd.merge(df, df_o, how='right', on='symbol')
# df_n.to_csv('./right_test.csv')
# for index, i in df.iterrows():
#     """
#     单价超过2美金
#     日成交量过50W
#     """
#     if float(i['volume']) > 500000 \
#             and float(i['close']) > 2 \
#             and float(i['open']) > 0 \
#             and float(i['high']) > 0 \
#             and float(i['low']) > 0:
#         tickers.append(str(i['symbol']))

df_right = pd.read_csv('./right_test.csv',
                       usecols=[i for i in range(1, 14)])
df_inner = pd.read_csv('./inner_test.csv',
                       usecols=[i for i in range(1, 14)])                       


for index, i in df_right.iterrows():
    if i['symbol'] not in df_inner['symbol'].values:
        print(i['symbol'])