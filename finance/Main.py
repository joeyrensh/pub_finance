#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from FileInfo import FileInfo
from SinaWebCrawler import SinaWebCrawler
from UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from ToolKit import ToolKit
from MyEmail import MyEmail
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
import seaborn as sns


# 执行策略
def exec_strategy(date):

    # 小市值大波动策略-策略1
    us_strate = UsStrategy(date)
    df1 = us_strate.get_usstrategy1()
    print(tabulate(df1, headers='keys', tablefmt='pretty'))

    # 破线拐头交叉-策略2
    df2 = us_strate.exec_strategy_with_multiprocessing()
    print(tabulate(df2, headers='keys', tablefmt='pretty'))

    # 合并数据
    file_name_sta = FileInfo(date).get_file_name_sta
    df_new = pd.concat([df1, df2], ignore_index=True)
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
        df.reset_index(inplace=True)
        cm = sns.light_palette("green", as_cmap=True)
        html = (
            df.style.hide_index()
                    .format({"close": "{:.2f}",
                             "chg": "{:.2f}%",
                             "amplitude": "{:.2f}%"}) \
            # .bar(subset=["close"], color='#FFA07A') \
            # .bar(subset=["chg"], color='#FFA07A') \
                    .background_gradient(cmap=cm) \
            # .set_properties(**{'background-color': 'black', \
            #                     'color': 'cyan', \
            #                     'border-color': 'white'}) \
            .set_table_styles([{ \
                "selector": "thead", \
                "props": "background-color:green; \
                                 color:black;" \
                                }]) \
            .render()
        )
        subject = '今日美股行情'
        MyEmail(subject, html).send_email()
    # 结束进度条
    pbar.finish()
