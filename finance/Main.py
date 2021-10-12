#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from FileInfo import FileInfo
from SinaWebCrawler import SinaWebCrawler
from UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from ToolKit import ToolKit
from DingDing import DingDing
from MyEmail import MyEmail
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
from UsTicker import UsTicker


# 执行策略
def exec_strategy(date):

    # 小市值大波动策略-策略1
    us_strate = UsStrategy(date)
    df1 = us_strate.get_usstrategy1()
    print(tabulate(df1, headers='keys', tablefmt='pretty'))

    # 昨日振幅大，且今天开盘涨，且超过10亿市值
    df2 = us_strate.get_usstrategy2()
    print(tabulate(df2, headers='keys', tablefmt='pretty'))

    # 三重滤网
    df3 = us_strate.get_usstrategy3()
    print(tabulate(df3, headers='keys', tablefmt='pretty'))

    # 分时波动频繁
    df4 = us_strate.get_usstrategy4()
    print(tabulate(df4, headers='keys', tablefmt='pretty'))

    # 合并数据
    file_name_sta = FileInfo(date).get_file_name_sta
    df_new = pd.concat([df1, df2])
    df_new = pd.concat([df_new, df3])
    df_new = pd.concat([df_new, df4])
    df_new.to_csv(file_name_sta, index=True, header=True)
    return df_new


# 主程序入口
if __name__ == '__main__':
    # 美股交易日期 utc-4
    trade_date = re.sub(' .*', '', str(datetime.now() -
                        timedelta(hours=12)).replace('-', ''))

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
    sinaweb = SinaWebCrawler()
    sinaweb.set_daily_tick_info_to_csv(trade_date)

    # 获取5分钟股票数据
    sinaweb.set_5min_tick_info_to_csv(trade_date)

    # 执行策略function
    df = exec_strategy(trade_date)
    # 发送邮件
    if not df.empty:
        subject = '今日美股行情'
        body = df.to_html()
        MyEmail(subject, body).send_email()

    # 结束进度条
    pbar.finish()
