#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from tabulate import tabulate
import progressbar
from utility.toolkit import ToolKit
from datetime import datetime
import pandas as pd
import sys
from backtraderref.globalstrategyv2 import GlobalStrategy
import backtrader as bt
from utility.tickerinfo import TickerInfo
from uscrawler.eastmoney_incre_crawler import EMWebCrawler
from backtraderref.pandasdata_ext import BTPandasDataExt
from utility.stock_analysis import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc
from backtraderref.usfixedamount import FixedAmount

tk = TickerInfo("20241203", "cn")
df = tk.get_stock_data_for_day()
count = (
    (df["close"] > 10) & (df["total_value"] > 2000000000)
    # & (df["total_value"] < 10000000000)
).sum()
print(count)
# filtered_df = df[(df["total_value"] < 5000000000) | (df["close"] < 2)]
# print(filtered_df)
