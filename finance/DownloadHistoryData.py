#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from SinaWebCrawler import SinaWebCrawler
from UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from ToolKit import ToolKit
from DingDing import DingDing
from MyEmail import MyEmail
import re
from datetime import datetime, timedelta, timezone
import pandas as pd
import sys
import requests as rc
import requests_cache
import yfinance as yf
import time

# 爬虫缓存
session = requests_cache.CachedSession('yfinance_tt.cache')
session.headers['User-agent'] = 'yfinance_tt/1.0'


rpath = './usstockinfo/SinaTicker_' + '20211008' + '.csv'
tickers = list()
df = pd.read_csv(rpath, usecols=[i for i in range(1, 13)])
for index, i in df.iterrows():
    if float(i['volume']) > 0 and float(i['amplitude'].replace('%', '')) > 0 \
            and float(i['mktcap']) > 0 \
            and float(i['close']) > 0 \
            and float(i['preclose']) > 0:
        tickers.append(str(i['symbol']))
wpath = './HistoryData.csv'
# tickers = ['BABA']
list1 = []
for j in tickers:
    time.sleep(30)
    # 股票当日收盘价与成交量K线
    data = yf.download(tickers=j, period='30d', interval='1d', session=session)
    data['symbol'] = j
    data.to_csv(wpath, mode='a', index=True, header=False)
    print(data['symbol'])
    print(data.shape)
