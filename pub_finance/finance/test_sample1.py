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

district_list = [
    "huangpu",
    "pudong",
    "xuhui",
    "changning",
    "jingan",
    "putuo",
    "hongkou",
    "yangpu",
    "minhang",
    "baoshan",
    "jiading",
    "jinshan",
    "songjiang",
    "qingpu",
    "fengxian",
    "chongming",
]

str1 = "xuhui"
index = district_list.index(str1)
print(index)
for idx, district in enumerate(district_list):
    if idx == index:
        print(district)
