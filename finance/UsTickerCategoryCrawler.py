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
import os


def get_us_ticker_category(date):
    """ 字段样例："industry":{"ind_name":"金融","ind_code":"BK1076"}
    https://stock.xueqiu.com/v5/stock/finance/cn/business.json?symbol= """

    # 雪球获取数据需要登陆token
    xq_token_str = os.environ.get('xueqiu_token')
    HEADERS = {'Host': 'stock.xueqiu.com',
               'Accept': 'application/json',
               'User-Agent': 'Chrome/95.0.4638.54',
               'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'Connection': 'keep-alive',
               'Cookie': 'xq_a_token=' + xq_token_str + ';'
               }
    list = []
    dict = {}
    tool = ToolKit('处理进度')
    # 遍历股票列表获取对应行业板块信息
    tickers = UsTickerInfo(date).get_usstock_list()
    for i in tickers:
        url = "https://stock.xueqiu.com/v5/stock/finance/cn/business.json?symbol=ticker"
        print('正在处理: ', i)
        # time.sleep(0.1)
        url_re = url.replace('ticker', i)
        res = requests.get(url_re, headers=HEADERS).text
        json_object = json.loads(res)
        if 'data' in json_object\
                and 'industry' in json_object['data']\
                and 'ind_name' in json_object['data']['industry']:
            dict = {
                'symbol': i,
                'industry': json_object['data']['industry']['ind_name']
            }
            list.append(dict)
        else:
            print('no industry ref')
        tool.progress_bar(len(tickers), tickers.index(i))
    df = pd.DataFrame(list)
    df.to_csv('./usstockinfo/usindustry.csv',
              mode='w', index=True, header=True)

# 主程序入口
if __name__ == '__main__':
    get_us_ticker_category('20211104')
