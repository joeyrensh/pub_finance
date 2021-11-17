#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from utility.FileInfo import FileInfo
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

""" backtrader策略 """


def exec_btstrategy(date):
    """ 创建cerebro对象 """
    cerebro = bt.Cerebro()
    """ 添加bt相关的策略 """
    cerebro.addstrategy(BTStrategy)
    """ 初始资金100M """
    cerebro.broker.setcash(1000000.0)
    """ 每手10股 """
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    """ 费率千分之一 """
    cerebro.broker.setcommission(commission=0.001)
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, 'us').get_backtrader_data_feed_test()
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
    trade_date = ToolKit('获取最新美股交易日期').get_us_latest_trade_date(1)
    """ 执行bt相关策略 """
    exec_btstrategy(trade_date)
    file = FileInfo(trade_date, 'us')
    """ 发送邮件 """
    """ 取最新一天数据，获取股票名称 """
    file_name_day = file.get_file_name_day
    df_lst_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
    """ 取仓位数据 """
    file_name_position = file.get_file_name_position
    df_lst_p = pd.read_csv(file_name_position, usecols=[
                           i for i in range(1, 8)])
    if not df_lst_p.empty:
        df_np = pd.merge(df_lst_p, df_lst_d, how='inner', on='symbol')
        max_len = "{:.2f}".format(round(df_np['p&l_ratio'].max(), 2))
        min_len = "{:.2f}".format(round(df_np['p&l_ratio'].min(), 2))
        cm = sns.color_palette("Blues", as_cmap=True)
        html = (
            df_np.style.hide_index()
            .format({"price": "{:.2f}",
                     "adjbase": "{:.2f}",
                     "p&l": "{:.2f}",
                     "p&l_ratio": "{:.2f}"})
            .background_gradient(subset=['price', 'adjbase', 'p&l'], cmap=cm)
            .bar(subset=['p&l_ratio'], align='mid', color=['#5fba7d', '#d65f5f'], vmin=0, vmax=1)
            .set_properties(**{'text-align': 'left'})
            .set_table_styles([dict(selector='th', props=[('text-align', 'left')])])
            .render()
        )
        subject = 'BT策略美股模拟盘'
        MyEmail(subject, html).send_email()
