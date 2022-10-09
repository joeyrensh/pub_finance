#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import email
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
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import sys
from utility.StockProposal import StockProposal
import sparklines
import base64
import pygal
import re

def sparkline_dist(data):
    hist = np.histogram(data, bins=10)[0]
    dist_strings = ''.join(sparklines(hist))
    return dist_strings

# 主程序入口
if __name__ == "__main__":
    trade_date = ToolKit("获取最新美股交易日期").get_us_latest_trade_date(1)
    path_list = os.listdir('./usstockinfo')
    file_list = []
    for file in path_list:
        """返回小于等于当前交易日期的文件列表"""
        if (
            re.search("position_", file)
            and str(file).replace("position_", "").replace(".csv", "")
            <= trade_date
        ):
            file_list.append(file)
    file_list.sort(reverse=True)
    df_latest = pd.read_csv('./usstockinfo/' + file_list[0], usecols=[i for i in range(1, 8)])
    date_index = str(file_list[0]).replace("position_", "").replace(".csv", "")
    df_latest_n = df_latest.groupby(by="industry").size().reset_index(name=date_index)
    df_nn = df_latest_n[df_latest_n[date_index] > 10]
    for file in file_list[1: -2]:
        df = pd.read_csv('./usstockinfo/' + file, usecols=[i for i in range(1, 8)])
        date_index = str(file).replace("position_", "").replace(".csv", "")
        df_n = df.groupby(by="industry").size().reset_index(name=date_index)
        df_nn = pd.merge(df_nn, df_n, how='left', on='industry')
        
    print(df_nn)
