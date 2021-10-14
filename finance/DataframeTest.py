#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from FileInfo import FileInfo
from SinaWebCrawler import SinaWebCrawler
from UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from ToolKit import ToolKit
from DingDing import DingDing
from MyEmail import MyEmail
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
from UsTicker import UsTicker
from matplotlib import pyplot as plt
from IPython.display import HTML
from jupyter import jupyter

# 小市值大波动策略-策略1
date = '20211012'
us_strate = UsStrategy(date)
df2 = us_strate.get_usstrategy2()
df_style = df2.style.apply(subset=['close', 'chg'], color='#d65f5f')
subject = '今日美股行情'
body = df_style.render()
MyEmail(subject, df_style).send_email()
