#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from utility.FileInfo import FileInfo
from usstrategy.UsStrategy import UsStrategy
from tabulate import tabulate
import progressbar
from utility.ToolKit import ToolKit
from utility.MyEmail import MyEmail
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
from utility.StockProposal import StockProposal


""" 执行策略 """


def exec_strategy(date):
    """ 小市值大波动策略-策略1 """
    us_strate = UsStrategy(date)
    df1 = us_strate.get_usstrategy1()
    print(tabulate(df1, headers='keys', tablefmt='pretty'))
    return df1


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
    """ 发送邮件 """
    if not df.empty:
        StockProposal('us', trade_date).send_strategy_df_by_email(df)

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
    StockProposal('us', trade_date).send_btstrategy_by_email()

    """ 结束进度条 """
    pbar.finish()
