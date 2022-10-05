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

def sparkline_dist(data):
    hist = np.histogram(data, bins=10)[0]
    dist_strings = ''.join(sparklines(hist))
    return dist_strings

# 主程序入口
if __name__ == "__main__":
    data = [
    [10,'Direct Sales','01-01-2019'],
    [12,'Direct Sales','02-01-2019'],
    [11,'Direct Sales','03-01-2019'],
    [15,'Direct Sales','04-01-2019'],
    [12,'Direct Sales','05-01-2019'],
    [20,'Online Sales','01-01-2019'],
    [25,'Online Sales','02-01-2019'],
    [22,'Online Sales','03-01-2019'],
    [30,'Online Sales','04-01-2019'],
    [23,'Online Sales','05-01-2019'],
    ]
    df = pd.DataFrame(data, columns=['Revenue','Department','Date'])
    lis = sparkline_dist(df['Revenue'])
    print(lis)
    # pvt = df.pivot(index='Department', columns='Date', values='Revenue')
    # pvt['Trend'] = pvt.apply(sparkline, axis=1)
    # html = pvt.to_html()
    # MyEmail().send_email('this is test', html)
