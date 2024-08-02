#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import progressbar
from utility.ToolKit import ToolKit
from datetime import datetime
import pandas as pd
import sys
from backtraderref.BTStrategyVol import BTStrategyVol
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
import random

print(random.randint(1, 3))
