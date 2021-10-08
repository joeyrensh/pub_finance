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

    trade_date = re.sub(' .*', '', str(datetime.datetime.now() - datetime.timedelta(hours=21)).replace('-', ''))

    # 定义程序显示的进度条
    widgets = ["doing task: ", progressbar.Percentage(), " ",
               progressbar.Bar(), " ", progressbar.ETA()]
    # 创建进度条并开始运行
    pbar = progressbar.ProgressBar(maxval=100, widgets=widgets).start()

    # 获取日股票数据
    wpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    SinaWebCrawler.set_daily_tick_info_to_csv(trade_date, wpath)

    # 获取实时股票数据
    # rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    # wpath = './usstockinfo/SinaTickerRealTime_' + trade_date + '.csv'
    # SinaWebCrawler.set_realtime_tick_into_to_csv(trade_date, rpath, wpath)

    # 获取5分钟股票数据
    rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    wpath = './usstockinfo/SinaTicker5Min_' + trade_date + '.csv'
    SinaWebCrawler.set_5min_tick_info_to_csv(trade_date, rpath, wpath)

    # 小市值大波动策略
    rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    wpath = './usstrategy/usstrategy1_' + trade_date + '.csv'
    df1 = UsStrategy().get_usstrategy1(rpath, wpath)
    print(tabulate(df1, headers='keys', tablefmt='pretty'))
    # 发送钉钉群信息
    dd = DingDing()
    image_address = 'http://x0.ifengimg.com/res/2021/4D88D64CA6D2902D26E15EC99921990596077F0A_size69_w1080_h938.jpeg'
    dd.send_markdown(title='Do It!!!',
                     content='## 今日仙股\n'
                             '#### ' + df1.iat[0, 0] + '暴涨' + df1.iat[0, 9] +'\n\n'
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
    if df1 is not None:
        df1['tag'] = '策略1'
    if df2 is not None:
        df2['tag'] = '策略2'
    if df3 is not None:
        df3['tag'] = '策略3'
    if df4 is not None:
        df4['tag'] = '策略4'

    df_new = pd.concat([df1, df2])
    df_new = pd.concat([df_new, df3])
    df_new = pd.concat([df_new, df4])
    print(df_new)
    subject = '今日美股行情'
    body = df_new.to_html()
    MyEmail(subject, body).send_email()

    # 结束进度条
    pbar.finish()


