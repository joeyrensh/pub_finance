#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import time
import requests
import re
import json
import pandas as pd
from ToolKit import ToolKit
from datetime import datetime
import os
from pandas.errors import EmptyDataError
from MyThread import MyThread
from UsTicker import UsTicker
from FileInfo import FileInfo


""" 新浪爬虫，获取每日及5分钟股票数据 """


class SinaWebCrawler:

    def __init__(self):
        """ NYSE:N，NASDAQ:O，AMEX:A """
        self.__url1 = "https://stock.finance.sina.com.cn/usstock/api/"\
            "jsonp.php/IO.XSRV2.CallbackList/US_CategoryService.getList?"\
            "page=page_num&num=60&sort=&asc=0"

        """ 分时股票url """
        self.__url2 = "https://hq.sinajs.cn/?rn=unix_time&list=gb_list"

        """ 5min趋势url """
        self.__url3 = "https://stock.finance.sina.com.cn/usstock/api/"\
            "jsonp_v2.php/var%20_ticker_5_unix_time="\
            "/US_MinKService.getMinK?symbol=ticker&type=5"

        """ 历史数据url """
        self.__url4 = "https://stock.finance.sina.com.cn/usstock/api/"\
            "json_v2.php/US_MinKService.getDailyK?symbol=ticker"

    """ 获取日交易最新数据 """

    def get_daily_tick_info(self, url):
        """ 
        字典结构
        股票代码：symbol，今日开盘价：open_alias，今日收盘价：close，今日最高价：high
        今日最低价：low，昨日收盘价：preclose，涨跌额：change，涨跌幅：chg
        今日成交量：volume，今日振幅：amplitude，
        市值：mktcap，所属交易所：market-纽交所、纳斯达克、美国交易所
        请求sina美股行情中心 
        """
        res = requests.get(url).text
        res_p = re.sub('\\\\', '', re.sub(
            '/.*/', '', re.sub('}\\);$', '', re.sub('.*"data":', '', res, 1))))
        if res_p is None or len(res_p) <= 10:
            return None
        """ 将每页数据解析为dataframe返回处理 """
        json_object = json.loads(res_p)
        list1 = []
        for i in range(len(json_object)):
            try:
                """ 给字典的所有key重新命名 """
                dic1 = {'symbol': json_object[i]['symbol'],
                        'open_alias': json_object[i]['open'],
                        'close': json_object[i]['price'],
                        'high': json_object[i]['high'],
                        'low': json_object[i]['low'],
                        'preclose': json_object[i]['preclose'],
                        'change': json_object[i]['diff'],
                        'chg':  json_object[i]['chg'],
                        'volume': json_object[i]['volume'],
                        'amplitude': json_object[i]['amplitude'],
                        'mktcap': json_object[i]['mktcap'],
                        'market': json_object[i]['market']}
                """ 数据结构为，list内嵌套dict数据类型，每一行代表一支股票所有数据 """
                list1.append(dic1)
            except KeyError:
                print('no key definition: ' + str(json_object[i]['symbol']))
        return list1

    def set_daily_tick_info_to_csv(self, trade_date):
        """ 获取日文件路径及名称 """
        file_name_d = FileInfo(trade_date).get_file_name_day
        """ 每日一个文件，根据交易日期创建 """
        if os.path.exists(file_name_d):
            os.remove(file_name_d)
        """ 定义循环获取新浪美股行情中心的总页码 """
        total_pages = 250
        """ 
        新浪财经美股信息获取
        多线程获取 
        """
        list_new = []
        """ 循环遍历250页全美行情中心，步长为3 """
        tool = ToolKit('日数据下载')
        for h in range(1, total_pages, 3):
            """ 休眠1秒，避免IP Block """
            time.sleep(1)
            """ 一次启动3个线程同时工作 """
            for t in range(1, 4):
                if (h+t) <= total_pages:
                    url1 = str(self.__url1).replace('page_num', str(h+t-1))
                    my_thread = MyThread(self.get_daily_tick_info, (url1,))
                    my_thread.setDaemon(True)
                    my_thread.start()
                    list1 = my_thread.get_result()
                    if list1 is None:
                        continue
                    list_new.extend(list1)
            tool.progress_bar(total_pages, h)
        """ 将list转化为dataframe，并且存储为csv文件，带index和header """
        df = pd.DataFrame(list_new)
        df['date'] = trade_date
        df.to_csv(file_name_d, mode='w', index=True, header=True)

    """ 获取实时数据 """

    def get_realtime_tick_info(self, url):
        """ 
        字典结构
        股票代码：symbol，今日开盘价：open_alias，今日收盘价：close，今日最高价：high
        今日最低价：low，昨日收盘价：preclose，涨跌额：change，涨跌幅：chg
        今日成交量：volume，今日振幅：amplitude，
        市值：mktcap，所属交易所：market-纽交所、纳斯达克、美国交易所
        附加： 事件时间：event_time 
        """
        res = requests.get(url).text
        res_p = re.sub('"', '', re.sub(
            '="', ',', re.sub('.*gb_', '', res))).split(';\n')
        list1 = list()
        for i in range(len(res_p)):
            if len(res_p[i].split(',')) < 10:
                continue
            """ 重新定义字典结构，与daily tick info一致 """
            dic1 = {'symbol': res_p[i].split(',')[0],
                    'open_alias': res_p[i].split(',')[6],
                    'close': res_p[i].split(',')[2],
                    'high': res_p[i].split(',')[7],
                    'low': res_p[i].split(',')[8],
                    'change': res_p[i].split(',')[3],
                    'chg': res_p[i].split(',')[5],
                    'volume': res_p[i].split(',')[11],
                    'mktcap': res_p[i].split(',')[13]}
            """ 数据结构为，list内嵌套dict数据类型，每一行代表一支股票所有数据 """
            list1.append(dic1)
        return list1

    def set_realtime_tick_into_to_csv(self, trade_date):
        """ 获取实时文件路径 """
        file_name_rt = FileInfo(trade_date).get_file_name_rt
        """ url里需要传递unix time当前时间戳 """
        current_timestamp = int(time.mktime(datetime.now().timetuple()))
        """ 读取daily ticker文件，获取股票列表 """
        tickers = UsTicker(trade_date).get_usstock_list()
        if os.path.exists(file_name_rt):
            os.remove(file_name_rt)
        """ 循环遍历股票列表 """
        tool = ToolKit('实时数据下载')
        ticker = str()
        for i in tickers:
            """ url参数里需要股票列表为list，且代码为小写 """
            ticker = ','.join([ticker, 'gb_' + str(i).lower()])
        ticker_p = ticker.replace(',', '', 1).split(',')
        list_new = []
        """ 股票代码以100个为一组，避免请求时参数过长越界 """
        for i in range(0, len(ticker_p), 100):
            tickerlist = re.sub('\\s', '',
                                str(ticker_p[i: i + 100]).replace('[', '')
                                .replace(']', '').replace("'", '').strip())
            url = str(self.__url2).replace('unix_time', str(
                current_timestamp)).replace('gb_list', tickerlist)
            list1 = self.get_realtime_tick_info(url)
            list_new.extend(list1)
            tool.progress_bar(len(ticker_p), i)
        """ 将list转化为dataframe，并且存储为csv文件，带index和header """
        df = pd.DataFrame(list_new)
        df.to_csv(file_name_rt, mode='w', index=True, header=True)

    def get_5min_tick_info(self, trade_date, url, ticker):
        """ 
        字典结构
        股票代码：symbol，今日开盘价：open_alias，今日收盘价：close，今日最高价：high
        今日最低价：low，昨日收盘价：preclose，涨跌额：change，涨跌幅：chg
        今日成交量：volume，今日振幅：amplitude，
        市值：mktcap，所属交易所：market-纽交所、纳斯达克、美国交易所
        附加： 事件时间：event_time，成交额：turnover 
        """
        res = requests.get(url).text
        res_p = re.sub('\\);', '', re.sub('.*\n.*=\\(', '', res, 1))
        if res_p is None or len(res_p) <= 10:
            return None
        json_object = json.loads(res_p)
        list1 = []
        for i in range(len(json_object)):
            """ 新浪返回的5min数据包括前几日的数据，只取当天 """
            if str(json_object[i]['d'])[0:10].replace('-', '').strip()\
                    != str(trade_date):
                continue
            """ 给字典的所有key重新命名 """
            try:
                dic1 = {'symbol': ticker,
                        'event_time': json_object[i]['d'],
                        'open_alias': json_object[i]['o'],
                        'close': json_object[i]['c'],
                        'high': json_object[i]['h'],
                        'low': json_object[i]['l'],
                        'volume': json_object[i]['v'],
                        'turnover': json_object[i]['a']}
                list1.append(dic1)
            except KeyError:
                print('no key definition: ' + ticker)
        """ 以每个股票代码，返回当前股票所有的5min数据 """
        return list1

    def set_5min_tick_info_to_csv(self, trade_date):
        """ 获取实时文件路径 """
        file_name_mi = FileInfo(trade_date).get_file_name_mi
        """ 获取股票列表 """
        tickers = UsTicker(trade_date).get_usstock_list_for_5mi()
        """ 获取unix time 时间戳 """
        current_timestamp = int(time.mktime(datetime.now().timetuple()))
        """ 每日一个文件 """
        if os.path.exists(file_name_mi):
            os.remove(file_name_mi)
        """ 新浪财经美股信息获取
        多线程获取，每次步长为3，为3线程 """
        list_new = []
        tool = ToolKit('5分钟数据下载')
        for h in range(0, len(tickers), 3):
            """ 休眠1秒，避免IP Block """
            time.sleep(1)
            """ 3线程同时获取5min数据 """
            for t in range(1, 4):
                if (h+t) <= len(tickers):
                    """ 解析URL，传递股票及当前unix time """
                    url1 = str(self.__url3)\
                        .replace('unix_time', str(current_timestamp))\
                        .replace('ticker', tickers[h+t-1])
                    """ 启动多线程获取 """
                    my_thread = MyThread(
                        self.get_5min_tick_info,
                        (trade_date, url1, tickers[h+t-1]))
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
            df.to_csv(file_name_mi, mode='w', index=True, header=True)
        except EmptyDataError:
            pass
