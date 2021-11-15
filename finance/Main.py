#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from utility.FileInfo import FileInfo
from usstrategy.UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from utility.ToolKit import ToolKit
from utility.MyEmail import MyEmail
import re
from datetime import datetime, timedelta
import pandas as pd
import sys
import seaborn as sns
from backtraderref.BTStrategy import BTStrategy
import backtrader as bt
from utility.TickerInfo import TickerInfo
from uscrawler.EMWebCrawler import EMWebCrawler
from uscrawler.EMUsTickerCategoryCrawler import EMUsTickerCategoryCrawler
from backtraderref.BTPandasDataExt import BTPandasDataExt


""" 执行策略 """


def exec_strategy(date):
    """ 小市值大波动策略-策略1 """
    us_strate = UsStrategy(date)
    df1 = us_strate.get_usstrategy1()
    print(tabulate(df1, headers='keys', tablefmt='pretty'))

    file_name_sta = FileInfo(date, 'us').get_file_name_sta
    # df_new = pd.concat([df1, df2], ignore_index=True)
    # df1.to_csv(file_name_sta, index=True, header=True)
    return df1


""" backtrader策略 """


def exec_btstrategy(date):
    """ 创建cerebro对象 """
    cerebro = bt.Cerebro()
    """ 添加bt相关的策略 """
    cerebro.addstrategy(BTStrategy)
    """ 初始资金100M """
    cerebro.broker.setcash(100000.0)
    """ 每手10股 """
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0.001)
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, 'us').get_backtrader_data_feed()
    """ 循环初始化数据进入cerebro """
    for h in list:
        print("正在初始化: ", h['symbol'][0])
        """ 历史数据最早不超过2021-01-01 """
        data = BTPandasDataExt(
            dataname=h, name=h['symbol'][0], fromdate=datetime(2021, 1, 1))
        cerebro.adddata(data)
        # 周数据
        # cerebro.resampledata(data, timeframe=bt.TimeFrame.Weeks, compression=1)
    """ 起始资金池 """
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    """ 运行cerebro """
    cerebro.run()
    """ 最终资金池 """
    print('当前现金持有: ', cerebro.broker.get_cash())
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    """ 画图相关 """
    # cerebro.plot(iplot=True, subplot=True)


# 主程序入口
if __name__ == '__main__':
    """ 美股交易日期 utc-4 """
    trade_date = ToolKit('获取最新美股交易日期').get_us_latest_trade_date(0)

    """ 非交易日程序终止运行 """
    if ToolKit('判断当天是否交易日').is_us_trade_date():
        pass
    else:
        sys.exit()

    """ 定义程序显示的进度条 """
    widgets = ["doing task: ", progressbar.Percentage(), " ",
               progressbar.Bar(), " ", progressbar.ETA()]
    """ 创建进度条并开始运行 """
    pbar = progressbar.ProgressBar(maxval=100, widgets=widgets).start()

    """ 东方财经爬虫 """
    """ 爬取每日最新股票数据 """
    em = EMWebCrawler()
    em.get_us_daily_stock_info(trade_date)
    """ 爬取每日最新股票对应行业数据 """
    emi = EMUsTickerCategoryCrawler()
    emi.get_us_ticker_category(trade_date)

    """ 执行策略 """
    df = exec_strategy(trade_date)
    file = FileInfo(trade_date, 'us')
    file_name_industry = file.get_file_name_industry
    """ 发送邮件 """
    if not df.empty:
        """ 匹配行业信息 """
        df_o = pd.read_csv(file_name_industry,
                           usecols=[i for i in range(1, 3)])
        df_n = pd.merge(df, df_o, how='left', on='symbol')
        df_n.sort_values(by=['chg', 'amplitude'],
                         ascending=False, inplace=True)
        df_n.reset_index(drop=True, inplace=True)
        cm = sns.color_palette("Blues", as_cmap=True)
        html = (
            df_n.style.hide_index()
            .format({"close": "{:.2f}",
                     "chg": "{:.2f}%",
                     "volume": "{:.0f}",
                     "amplitude": "{:.2f}%"})
            .background_gradient(subset=['close', 'chg', 'volume', 'amplitude'], cmap=cm)
            .bar(subset=['chg'], align='mid', color=['#5fba7d', '#d65f5f'], width=100)
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
    """ 执行bt相关策略 """
    exec_btstrategy(trade_date)

    """ 发送邮件 """
    file = FileInfo(trade_date, 'us')
    """ 取最新一天数据，获取股票名称 """
    file_name_day = file.get_file_name_day
    df_d = pd.read_csv(file_name_day,
                       usecols=[i for i in range(1, 3)])
    """ 取仓位数据 """
    file_name_position = file.get_file_name_position
    df_p = pd.read_csv(file_name_position,
                       usecols=[i for i in range(1, 8)])
    if not df_p.empty:
        df_n = pd.merge(df_p, df_d, how='inner', on='symbol')
        cm = sns.color_palette("Blues", as_cmap=True)
        html = (
            df_n.style.hide_index()
            .format({"price": "{:.2f}",
                     "adjbase": "{:.2f}",
                     "p&l": "{:.2f}",
                     "p&l_ratio": "{:.2f}%"})
            .background_gradient(subset=['price', 'adjbase', 'p&l'], cmap=cm)
            .bar(subset=['p&l_ratio'], align='mid', color=['#5fba7d', '#d65f5f'], width=100)
            .render()
        )
        subject = 'BT策略美股模拟盘'
        MyEmail(subject, html).send_email()
        """ 按照行业板块聚合，统计最近成交率最高的行业 """
        df_s = df_n.groupby(by='industry').size().reset_index(name='count')
        df_s.sort_values(by=['count'],
                         ascending=False, inplace=True)
        df_s.reset_index(drop=True, inplace=True)
        """ 发送邮件 """
        if not df_s.empty:
            cm = sns.color_palette("Blues", as_cmap=True)
            html = (
                df_s.style.hide_index()
                .format({"count": "{:.0f}"})
                .background_gradient(subset=['count'], cmap=cm)
                .render()
            )
            subject = 'BT策略美股模拟盘行业统计'
            MyEmail(subject, html).send_email()
    """ 结束进度条 """
    pbar.finish()
