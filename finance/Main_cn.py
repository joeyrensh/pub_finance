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
from cncrawler.EMCNWebCrawler import EMCNWebCrawler
from cncrawler.EMCNTickerCategoryCrawler import EMCNTickerCategoryCrawler
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
    cerebro.broker.setcommission(commission=0.001, stocklike=True)
    """ 添加股票当日即历史数据 """
    list = TickerInfo(date, 'cn').get_backtrader_data_feed()
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
    """ 美股交易日期 utc+8 """
    trade_date = ToolKit('获取最新A股交易日期').get_cn_latest_trade_date(0)

    """ 非交易日程序终止运行 """
    if ToolKit('判断当天是否交易日').is_cn_trade_date():
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
    em = EMCNWebCrawler()
    em.get_cn_daily_stock_info(trade_date)
    """ 爬取每日最新股票对应行业数据 """
    emi = EMCNTickerCategoryCrawler()
    emi.get_cn_ticker_category(trade_date)

    """ 执行bt相关策略 """
    exec_btstrategy(trade_date)

    """ 发送邮件 """
    file = FileInfo(trade_date, 'cn')

    """ 发送邮件 """
    """ 取最新一天数据，获取股票名称 """
    file_name_day = file.get_file_name_day
    df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
    """ 取仓位数据 """
    file_cur_p = file.get_file_name_position
    df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 8)])
    pre_file_cur_p = file.get_pre_file_name_position
    df_pre_p = pd.read_csv(pre_file_cur_p, usecols=[i for i in range(1, 8)])
    if not df_cur_p.empty:
        df_np = pd.merge(df_cur_p, df_d, how='inner', on='symbol')
        df_np.rename(columns={'symbol': '股票代码',
                              'buy_date': '策略命中时间',
                              'price': '买入价',
                              'adjbase': '当前价',
                              'p&l': '收益金额',
                              'p&l_ratio': '收益率',
                              'industry': '所属行业',
                              'name': '公司名称'}, inplace=True)
        cm = sns.color_palette("Blues", as_cmap=True)
        html = (
            df_np.style.hide_index()
            .hide_columns(['收益金额'])
            .format({"买入价": "{:.2f}",
                     "当前价": "{:.2f}",
                     "收益率": "{:.2f}"})
            .background_gradient(subset=['买入价', '当前价'], cmap=cm)
            .bar(subset=['收益率'], align='mid', color=['#5fba7d', '#d65f5f'], vmin=0, vmax=1)
            .set_properties(**{'text-align': 'left', 'width': 'auto'})
            .set_table_styles([dict(selector='th', props=[('text-align', 'left')], width='auto')])
            .render()
        )
        subject = 'A股行情'
        MyEmail(subject, html).send_email()
        """ 按照行业板块聚合，统计最近成交率最高的行业 """
        df_sum = df_np.groupby(by='所属行业').size().reset_index(name='今日策略')
        df_sum.sort_values(by=['今日策略'],
                           ascending=False, inplace=True)
        df_sum.reset_index(drop=True, inplace=True)
        """ 取上一交易日的行业板块聚合 """
        df_sum_pre = df_pre_p.groupby(
            by='industry').size().reset_index(name='pre_count')
        df_sum_pre.sort_values(by=['pre_count'],
                               ascending=False, inplace=True)
        df_sum_pre.reset_index(drop=True, inplace=True)
        df_sum_pre.rename(columns={'industry': '所属行业',
                                   'pre_count': '昨日策略'}, inplace=True)
        df_sum_np = pd.merge(df_sum, df_sum_pre, how='left', on='所属行业')
        df_sum_np['变化比'] = (df_sum_np['今日策略'] -
                            df_sum_np['昨日策略']) / df_sum_np['昨日策略']
        """ 发送邮件 """
        if not df_sum_np.empty:
            cm = sns.color_palette("Blues", as_cmap=True)
            html = (
                df_sum_np.style.hide_index()
                .format({"今日策略": "{:.0f}",
                        "昨日策略": "{:.0f}",
                         "变化比": "{:.2f}"})
                .background_gradient(subset=['今日策略', '昨日策略'], cmap=cm)
                .bar(subset=['变化比'], align='mid', color=['#5fba7d', '#d65f5f'], vmin=0, vmax=1)
                .set_properties(**{'text-align': 'left', 'width': 'auto'})
                .set_table_styles([dict(selector='th', props=[('text-align', 'left')], width='auto')])
                .render()
            )
            subject = 'A股行业行情'
            MyEmail(subject, html).send_email()
    """ 结束进度条 """
    pbar.finish()
