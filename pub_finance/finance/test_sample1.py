#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import progressbar
from utility.ToolKit import ToolKit
from datetime import datetime
import pandas as pd
import sys
from backtraderref.BTStrategyV2 import BTStrategyV2
import backtrader as bt
from utility.TickerInfo import TickerInfo
from cncrawler.EMCNWebCrawler import EMCNWebCrawler
from backtraderref.BTPandasDataExt import BTPandasDataExt
from utility.StockProposal import StockProposal
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyfolio as pf
import gc
from backtraderref.FixAmountCN import FixedAmount

col_names = ["idx", "symbol", "date", "action", "price", "share", "strategy"]
df = pd.read_csv(
    # "./usstockinfo/trade_20240823.csv",
    "./usstockinfo/trade_20240826.csv",
    usecols=[i for i in range(0, 7)],
    names=col_names,
)
print(df.groupby(["action", "strategy"]).count())
