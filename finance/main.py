#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from SinaWebCrawler import SinaWebCrawler
from UsStrategy import *
import datetime
from tabulate import tabulate
import progressbar
import re
from ToolKit import *
import smtplib
import ssl
import requests
from DingDing import DingDing
import time
from MyEmail import *

if __name__ == '__main__':

    if ToolKit('判断是否交易日').is_us_trade_date():
        pass
    else:
        print('当前不是交易日，不能获取数据')
        sys.exit()

    trade_date = re.sub(' .*', '', str(datetime.datetime.now() - datetime.timedelta(hours=12)).replace('-', ''))

    # 定义程序显示的进度条
    widgets = ["doing task: ", progressbar.Percentage(), " ",
               progressbar.Bar(), " ", progressbar.ETA()]
    # 创建进度条并开始运行
    pbar = progressbar.ProgressBar(maxval=100, widgets=widgets).start()

    # 获取日股票数据
    wpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    SinaWebCrawler.set_daily_tick_info_to_csv(trade_date, wpath)

    # 获取5分钟股票数据
    rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    wpath = './usstockinfo/SinaTicker5Min_' + trade_date + '.csv'
    SinaWebCrawler.set_5min_tick_info_to_csv(trade_date, rpath, wpath)

    # 获取实时股票数据
    # rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    # wpath = './usstockinfo/SinaTickerRealTime_' + trade_date + '.csv'
    # SinaWebCrawler.set_realtime_tick_into_to_csv(trade_date, rpath, wpath)

    # 小市值大波动策略
    rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    wpath = './usstrategy/usstrategy1_' + trade_date + '.csv'
    df1 = UsStrategy().get_usstrategy1(rpath, wpath)
    print(tabulate(df1, headers='keys', tablefmt='pretty'))
    if not df1.empty:
        max_chg = df1['amplitude'].max()
        df_max = df1.loc[df1['amplitude'] == df1['amplitude'].max()]
        symbol1 = df_max.iloc[0]['symbol']

    # 发送钉钉群信息
    dd = DingDing()
    image_address = 'http://x0.ifengimg.com/res/2021/4D88D64CA6D2902D26E15EC99921990596077F0A_size69_w1080_h938.jpeg'
    dd.send_markdown(title='Do It!!!',
                     content='## 今日仙股\n'
                             '#### ' + symbol1 + '振幅' + max_chg +'\n\n'
                             '> ![美景](' + image_address + ')\n'
                             '> ## Just Do It!!!\n')

    # 昨日振幅大，且今天开盘涨，且超过10亿市值
    rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    wpath = './usstrategy/usstrategy2_' + trade_date + '.csv'
    df2 = UsStrategy().get_usstrategy2(rpath, wpath)
    print(tabulate(df2, headers='keys', tablefmt='pretty'))

    # 三重滤网
    wpath = './usstrategy/usstrategy3_' + trade_date + '.csv'
    df3 = UsStrategy().get_usstrategy3(wpath, trade_date)
    print(tabulate(df3, headers='keys', tablefmt='pretty'))

    # 分时波动频繁
    rpath = './usstockinfo/SinaTicker5Min_' + trade_date + '.csv'
    wpath = './usstrategy/usstrategy4_' + trade_date + '.csv'
    df4 = UsStrategy().get_usstrategy4(rpath, wpath, trade_date)
    print(tabulate(df4, headers='keys', tablefmt='pretty'))

    # 发送邮件
    if not df1.empty:
        df1['tag'] = '策略1'
    if not df2.empty:
        df2['tag'] = '策略2'
    if not df3.empty:
        df3['tag'] = '策略3'
    if not df4.empty:
        df4['tag'] = '策略4'

    df_new = pd.concat([df1, df2])
    df_new = pd.concat([df_new, df3])
    df_new = pd.concat([df_new, df4])
    if not df_new.empty:
        subject = '今日美股行情'
        body = df_new.to_html()
        MyEmail(subject, body).send_email()

    # 结束进度条
    pbar.finish()


