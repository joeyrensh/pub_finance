#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import email
import json

import requests
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
    dist_strings = "".join(sparklines(hist))
    return dist_strings


# 主程序入口
if __name__ == "__main__":
    url = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=SH600893"
    res = requests.get(url).text.lower()
    json_object = json.loads(res)
    if "jbzl" in json_object and "sshy" in json_object["jbzl"]:
        dict = {"symbol": "600893", "industry": json_object["jbzl"]["sshy"]}
        print(dict)
