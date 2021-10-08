#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from SinaWebCrawler import SinaWebCrawler
from UsStrategy import *
import datetime
from tabulate import tabulate
import progressbar
import re
from ToolKit import *

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
    df = UsStrategy().get_usstrategy1(rpath, wpath)
    print(tabulate(df, headers='keys', tablefmt='pretty'))

    # 昨日振幅大，且今天开盘涨，且超过10亿市值
    rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    wpath = './usstrategy/usstrategy2_' + trade_date + '.csv'
    df = UsStrategy().get_usstrategy2(rpath, wpath)
    print(tabulate(df, headers='keys', tablefmt='pretty'))

    # 三重滤网
    wpath = './usstrategy/usstrategy3_' + trade_date + '.csv'
    df = UsStrategy().get_usstrategy3(wpath, trade_date)
    print(tabulate(df, headers='keys', tablefmt='pretty'))

    # 分时波动频繁
    rpath = './usstockinfo/SinaTicker5Min_' + trade_date + '.csv'
    wpath = './usstrategy/usstrategy4_' + trade_date + '.csv'
    df = UsStrategy().get_usstrategy4(rpath, wpath, trade_date)
    print(tabulate(df, headers='keys', tablefmt='pretty'))

    # 结束进度条
    pbar.finish()

