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


def get_us_stock_list():

    # url里需要传递unixtime当前时间戳
    current_timestamp = int(time.mktime(datetime.now().timetuple()))

    """     
    市场代码：
    105:纳斯达克 NASDAQ
    106:纽交所 NYSE
    107:美国交易所 AMEX    """
    # 重构数据
    dict = {}
    list = []
    for mkt_code in ['105', '106', '107']:
        # url定义
        url = "https://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery"\
            "&pn=1&pz=20000&po=1&np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:mkt_code&fields=f12&_=unix_time"
        # 请求url，获取数据response
        url_re = url.replace('unix_time', str(current_timestamp)).replace(
            'mkt_code', mkt_code)
        res = requests.get(url_re).text
        # 替换成valid json格式
        res_p = re.sub('\\].*', ']', re.sub('.*:\\[', '[', res, 1), 1)
        json_object = json.loads(res_p)
        for i in json_object:
            dict = {
                'symbol': i['f12'],
                'mkt_code': mkt_code
            }
            list.append(dict)
    return list


def get_us_ticker_category(date):
    """ 市场代码：
    105:纳斯达克 NASDAQ O
    106:纽交所 NYSE N
    107:美国交易所 AMEX A

    https://emweb.eastmoney.com/pc_usf10/CompanyInfo/PageAjax?fullCode=WISH.O """

    # 雪球获取数据需要登陆token
    list = []
    dict = {}
    tool = ToolKit('处理进度')
    # 遍历股票列表获取对应行业板块信息
    tick_list = get_us_stock_list()
    for i in tick_list:
        url = "https://emweb.eastmoney.com/pc_usf10/CompanyInfo/PageAjax?fullCode=symbol.mkt_code"
        if i['mkt_code'] == '105':
            mkt_code = 'O'
        elif i['mkt_code'] == '106':
            mkt_code = 'N'
        else:
            mkt_code = 'A'
        url_re = url.replace('symbol', i['symbol']).replace(
            'mkt_code', mkt_code)
        print(url_re)
        # time.sleep(0.1)
        res = requests.get(url_re).text.lower()
        json_object = json.loads(res)
        if 'data' in json_object\
                and 'gszl' in json_object['data']:
            for h in json_object['data']['gszl']:
                dict = {
                    'symbol': i['symbol'],
                    'industry': h['industry']
                }
            list.append(dict)
        tool.progress_bar(len(tick_list), tick_list.index(i))
    df = pd.DataFrame(list)
    df.drop_duplicates(subset=['symbol', 'industry'], keep='last', inplace=True)
    df.to_csv('./usstockinfo/usindustry_em.csv',
              mode='w', index=True, header=True)


# 主程序入口
if __name__ == '__main__':
    get_us_ticker_category('20211105')
