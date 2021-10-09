#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from SinaWebCrawler import SinaWebCrawler
from UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from ToolKit import ToolKit
from DingDing import DingDing
from MyEmail import MyEmail
import re
from datetime import datetime, timedelta, timezone
import pandas as pd
import sys


# 发送钉钉群信息
def send_msg_to_dd(symbol, chg):
    dd = DingDing()
    image_address = 'http://x0.ifengimg.com/res/2021/4D88D64CA6D2902D26E15EC99921990596077F0A_size69_w1080_h938.jpeg'
    dd.send_markdown(title='Do It!!!',
                     content='## 今日仙股\n'
                             '#### ' + symbol + '振幅' + chg + '\n\n'
                             '> ![美景](' + image_address + ')\n'
                             '> ## Just Do It!!!\n')


# 执行策略
def exec_strategy(trade_date, daily_path, min5_path):
    # 小市值大波动策略-策略1
    wpath = './usstrategy/usstrategy1_' + trade_date + '.csv'
    df1 = UsStrategy().get_usstrategy1(daily_path, wpath)
    print(tabulate(df1, headers='keys', tablefmt='pretty'))
    # 发送TOP1振幅股票到钉钉
    if not df1.empty:
        max_chg = df1['amplitude'].max()
        df_max = df1.loc[df1['amplitude'] == df1['amplitude'].max()]
        symbol1 = df_max.iloc[0]['symbol']
        send_msg_to_dd(symbol1, max_chg)

    # 昨日振幅大，且今天开盘涨，且超过10亿市值
    wpath = './usstrategy/usstrategy2_' + trade_date + '.csv'
    df2 = UsStrategy().get_usstrategy2(daily_path, wpath)
    print(tabulate(df2, headers='keys', tablefmt='pretty'))

    # 三重滤网
    wpath = './usstrategy/usstrategy3_' + trade_date + '.csv'
    df3 = UsStrategy().get_usstrategy3(wpath, trade_date)
    print(tabulate(df3, headers='keys', tablefmt='pretty'))

    # 分时波动频繁
    wpath = './usstrategy/usstrategy4_' + trade_date + '.csv'
    df4 = UsStrategy().get_usstrategy4(min5_path, wpath, trade_date)
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
    return df_new


# 主程序入口
if __name__ == '__main__':
    # 美股交易日期 utc-4
    trade_date = re.sub(' .*', '', str(datetime.now() - timedelta(hours=12)).replace('-', ''))
    daily_path = './usstockinfo/SinaTicker_' + trade_date + '.csv'
    min5_path = './usstockinfo/SinaTicker5Min_' + trade_date + '.csv'
    realtime_path = './usstockinfo/SinaTickerRealTime_' + trade_date + '.csv'

    # 非交易日程序终止运行
    if ToolKit('判断当天是否交易日').is_us_trade_date():
        pass
    else:
        sys.exit()

    # 定义程序显示的进度条
    widgets = ["doing task: ", progressbar.Percentage(), " ",
               progressbar.Bar(), " ", progressbar.ETA()]
    # 创建进度条并开始运行
    pbar = progressbar.ProgressBar(maxval=100, widgets=widgets).start()

    # 获取日股票数据
    SinaWebCrawler.set_daily_tick_info_to_csv(trade_date, daily_path)

    # 获取5分钟股票数据
    SinaWebCrawler.set_5min_tick_info_to_csv(trade_date, daily_path, min5_path)

    # 获取实时股票数据
    # SinaWebCrawler.set_realtime_tick_into_to_csv(trade_date, daily_path, realtime_path)

    # 执行策略function
    df = exec_strategy(trade_date, daily_path, min5_path)
    # 发送哟件
    if not df.empty:
        subject = '今日美股行情'
        body = df.to_html()
        MyEmail(subject, body).send_email()

    # 结束进度条
    pbar.finish()

