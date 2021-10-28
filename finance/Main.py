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
import dataframe_image as dfi
from DingDing import DingDing
import matplotlib
from backtrader.cerebro import OptReturn
from backtrader.feeds import PandasData
from BTUsStrategy import BTUstrategy
import backtrader as bt
from UsTicker import UsTicker


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

# backtrader策略
def exec_btstrategy(date):

    # 创建cerebro对象
    cerebro = bt.Cerebro()
    # 添加bt相关的策略
    cerebro.addstrategy(BTUstrategy)
    # 初始资金100M
    cerebro.broker.setcash(1000000.0)
    # 每手10股
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    # 费率千分之一
    cerebro.broker.setcommission(commission=0.001)
    # 添加股票当日即历史数据
    list = UsTicker(date).get_backtrader_data_feed()
    # 循环初始化数据进入cerebro
    for h in list:
        data = bt.feeds.PandasData(
            dataname=h, name=h['symbol'][0], fromdate=datetime(2021, 1, 1)) #历史数据最早不超过2021-01-01
        cerebro.adddata(data)
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)
    # 起始资金池
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # 运行cerebro
    cerebro.run()
    # 最终资金池
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # 画图相关
    # cerebro.plot(iplot=True, subplot=True)


# 主程序入口
if __name__ == '__main__':
    # 美股交易日期 utc-4
    trade_date = ToolKit('获取最新美股交易日期').get_latest_trade_date(0)

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
                     "amplitude": "{:.2f}%"})
            .background_gradient(cmap=cm)
            .set_table_styles([{
                "selector": "thead",
                "props": "background-color:green;color:black;"
            }])
            .render()
        )
        subject = '今日美股行情'
        MyEmail(subject, html).send_email()

        # 发送钉钉消息
        # df_style = df.style.hide_columns(['tag', 'index'])\
        #     .hide_index() \
        #     .format({"close": "{:.2f}",
        #              "chg": "{:.2f}%",
        #              "amplitude": "{:.2f}%"}) \
        #     .background_gradient(cmap=cm) \
        #     .set_table_styles([{
        #         "selector": "thead",
        #         "props": "background-color:green;color:black;"
        #     }])
        # dfi.export(df, './images/us_strategy.png',
        #            table_conversion='matplotlib')
        # dd = DingDing()
        # image_address = 'http://81.68.229.169:80/images/us_strategy.png'
        # # 发送TOP1振幅股票到钉钉
        # dd.send_markdown(title='Do It!!!',
        #                  content='## 美股市场行情\n'
        #                  '#### Oh Yeath\n\n'
        #                  '> ![美景](' + image_address + ')\n'
        #                  '> ## Just Do It!!!\n')
    # 执行bt相关策略
    exec_btstrategy(trade_date)
    # 结束进度条
    pbar.finish()
