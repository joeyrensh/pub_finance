#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from ToolKit import ToolKit
import re
from datetime import datetime, timedelta
import time
import pandas as pd
from UsTickerInfo import UsTickerInfo
import requests
import json
from MyThread import MyThread
from pandas.errors import EmptyDataError

def get_us_ticker_category():
    # NYSE:N，NASDAQ:O，AMEX:A
    url1 = "https://stock.finance.sina.com.cn/usstock/api/"\
        "jsonp.php/IO.XSRV2.CallbackList/US_CategoryService.getList?"\
        "page=1&num=60&sort=&asc=0"

    res = requests.get(url1).text
    res_p = re.sub('\\\\', '', re.sub(
        '/.*/', '', re.sub('}\\);$', '', re.sub('.*"data":', '', res, 1)))) 

    if res_p is None or len(res_p) <= 10:
        return None
    # 将每页数据解析为dataframe返回处理
    json_object = json.loads(res_p)
    for i in json_object:
        if i['category'] != None and len(i['category']) > 0:
            print(i)

get_us_ticker_category()