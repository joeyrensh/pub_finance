#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from utility.FileInfo import FileInfo
from tabulate import tabulate
from utility.ToolKit import ToolKit
from utility.MyEmail import MyEmail
from datetime import datetime, timedelta
import pandas as pd
import seaborn as sns
from backtraderref.BTStrategy import BTStrategy
import backtrader as bt
from utility.TickerInfo import TickerInfo
from backtraderref.BTPandasDataExt import BTPandasDataExt
import numpy as np

# 主程序入口
if __name__ == "__main__":
    file = './cnstockinfo/stock_20220927.csv'
    df = pd.read_csv(file)
    # np1 = np.loadtxt(fname = file, dtype= str, delimiter= ",")
    # df = pd.DataFrame(np1)
    print('内存占用情况'.ljust(20,'='))
    print(df.memory_usage())

    print('内存占用总额情况'.ljust(20,'='))
    print(df.memory_usage().sum())

    print('使用df.info()查看内存占用情况'.ljust(20,'='))
    df.info()