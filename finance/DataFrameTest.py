#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from UsFileInfo import UsFileInfo
from SinaWebCrawler import SinaWebCrawler
from UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from ToolKit import ToolKit
from MyEmail import MyEmail
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
import seaborn as sns
import dataframe_image as dfi
from DingDing import DingDing
import matplotlib
from backtrader.cerebro import OptReturn
from backtrader.feeds import PandasData
from BTUsStrategy import BTUsStrategy
import backtrader as bt
from UsTickerInfo import UsTickerInfo
from EMWebCrawler import EMWebCrawler


df = pd.read_csv('./position_log.txt', usecols=[i for i in range(1, 9)])
df_n = df.groupby(by='industry').size().reset_index(name='counts')
print(df_n)


