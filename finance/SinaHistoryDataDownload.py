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


""" 
新浪财经历史数据接口
按照股票代码遍历 
"""
url = "https://stock.finance.sina.com.cn/usstock/api/json_v2.php/"\
    "US_MinKService.getDailyK?symbol=ticker"

""" 美股交易日期 utc-4 """
trade_date = re.sub(' .*', '', str(datetime.now() -
                    timedelta(hours=12)).replace('-', ''))


def get_his_tick_info(trade_date, url, ticker):
    """     
    字典结构：
    股票代码：symbol，今日开盘价：open_alias，今日收盘价：close，今日最高价：high
    今日最低价：low，昨日收盘价：preclose，涨跌额：change，涨跌幅：chg
    今日成交量：volume，今日振幅：amplitude，
    市值：mktcap，所属交易所：market-纽交所、纳斯达克、美国交易所
    附加： 事件时间：event_time，成交额：turnover 
    """
    res = requests.get(url).text
    json_object = json.loads(res)
    list1 = []
    for i in range(len(json_object)):
        date = json_object[i]['d'].replace('-', '')
        """ 获取小于等于20210929之前的数据 """
        if date >= '20211101' or date < '20210101':
            continue
        """ 给字典的所有key重新命名 """
        try:
            dic1 = {'symbol': ticker,
                    'open': json_object[i]['o'],
                    'close': json_object[i]['c'],
                    'high': json_object[i]['h'],
                    'low': json_object[i]['l'],
                    'volume': json_object[i]['v'],
                    'turnover': None,
                    'chg':  None,
                    'change': None,
                    'amplitude': None,
                    'preclose': None,
                    'date': date
                    }
            list1.append(dic1)
        except KeyError:
            print('no key definition: ' + ticker)
    """ 以每个股票代码，返回当前股票所有的历史数据 """
    return list1


def set_his_tick_info_to_csv(trade_date):
    """ 获取实时文件路径 """
    file_name_his = './usstockinfo/usstock_20211029.csv'
    """ 获取股票列表 """
    tickers = UsTickerInfo(trade_date).get_usstock_list()
    """     
    新浪财经美股信息获取
    多线程获取，每次步长为3，为3线程
    """
    list_new = []
    tool = ToolKit('历史数据下载')
    for h in range(0, len(tickers), 3):
        """ 休眠1秒，避免IP Block """
        time.sleep(1)
        """ 3线程同时获取5min数据 """
        for t in range(1, 4):
            if (h+t) <= len(tickers):
                """ 解析URL，传递股票及当前unix time """
                url1 = str(url).replace('ticker', tickers[h+t-1])
                """ 启动多线程获取 """
                my_thread = MyThread(
                    get_his_tick_info, (trade_date, url1, tickers[h+t-1]))
                my_thread.setDaemon(True)
                my_thread.start()
                list1 = my_thread.get_result()
                if list1 is None:
                    continue
                list_new.extend(list1)
        tool.progress_bar(len(tickers), h)
    try:
        """ 将list转化为dataframe，并且存储为csv文件，带index和header """
        df = pd.DataFrame(list_new)
        df.to_csv(file_name_his, mode='w', index=True, header=True)
    except EmptyDataError:
        pass

if __name__ == '__main__':
    set_his_tick_info_to_csv('20211101')
