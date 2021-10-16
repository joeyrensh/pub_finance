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
from UsTicker import UsTicker
import multiprocessing
import talib as tl


def test1():
    trade_date = '20211014'
    # 获取美股历史数据
    his_data = UsTicker(trade_date).get_history_data()
    tickers = UsTicker(trade_date).get_usstock_list()
    # symbol, close, ma20, ma60, ema20, ema60, dif, dea, macd
    list = []
    tool = ToolKit('策略测试')
    pool = multiprocessing.Pool(processes=4)  # 创建4个进程
    # for ticker in tickers:
    for ticker in ['BABA', 'ATER', 'LKQ']:
        results = [pool.apply_async(strategy_test, (his_data, ticker))]

    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    for dic in results:
        if dic.get():
            list.append(dic.get())
        tool.progress_bar(len(results), results.index(dic))
    print(list)

    # df = pd.DataFrame(list)
    # print(df)


def strategy_test(his_data, ticker):
    # 数据按股票代码分组
    group_obj = his_data.groupby(by='symbol').get_group(ticker)
    # 收盘价
    close = group_obj['close'].values
    # 成交量
    volume = group_obj['volume'].values
    # MA20
    ma20 = tl.MA(close, timeperiod=20)
    # MA60
    ma60 = tl.MA(close, timeperiod=60)
    # EMA20
    ema20 = tl.EMA(close, timeperiod=20)
    # EMA60
    ema60 = tl.EMA(close, timeperiod=60)
    # MACD
    dif, dea, macd = tl.MACD(close, 12, 26, 9)
    # MACD多头
    if macd.size > 3 \
            and macd[-1] != 'nan' and macd[-2] != 'nan' and macd[-3] != 'nan' \
            and macd[-1] > macd[-2] > macd[-3] > 0:
        pass
    else:
        return
    # dif刚刚向上穿越DEA
    if dif.size > 1 and dea.size > 1 \
            and dif[-1] != 'nan' and dea[-1] != 'nan' and dif[-1] > dea[-1]:
        pass
    else:
        return
    # print('macd阶段判断结束')
    # 收盘价占上20均线，20站上60
    if ema20.size > 2 and ema60.size > 2 \
            and ema20[-1] != 'nan' and ema20[-2] != 'nan' \
            and ema60[-1] != 'nan' and ema60[-2] != 'nan' \
            and close[-1] > ema20[-1] and ema20[-1] > ema60[-1]:
        pass
    else:
        return
    if ma20.size > 2 and ma60.size > 2 \
            and ma20[-1] != 'nan' and ma20[-2] != 'nan' \
            and ma60[-1] != 'nan' and ma60[-2] != 'nan' \
            and close[-1] > ma20[-1] and ma20[-1] > ma60[-1]:
        pass
    else:
        return
    # EMA斜率大于0
    if ema20[-1] > ema20[-2]:
        pass
    else:
        return
    # print('ma阶段判断结束')
    # 今日收盘价高于昨日收盘价
    if len(close) > 2 and close[-1] > close[-2] \
            and len(volume) > 2 and volume[-1] > volume.max(axis=0) * 0.5:
        pass
    else:
        return
    # print('close阶段判断结束')
    # 乖离率，短期/中期都低于0.01
    if (close[-1] - ema20[-1]) / ema20[-1] < 0.1 \
            and (ema20[-1] - ema60[-1]) / ema60[-1] < 0.1:
        pass
    else:
        return
    # 字典
    dic1 = {'symbol': ticker, 'close': close[-1]}
    return dic1


# test1()

df = pd.read_csv('./usstrategy/UsStrategy_20211015.csv',
                 usecols=[i for i in range(1, 5)])
df.reindex
# print(df)
cm = sns.light_palette("gray", as_cmap=True)

df.style.hide_index() \
    .format({"close": "{:.2f}", "chg": "{:.2f}%"}) \
    .background_gradient(cmap=cm) \
    .set_table_styles([{
        "selector": "thead",
        "props": "background-color: green \
    color: black"
    }]) \
    #         .render()
# )
# subject = '今日美股行情'
# body = html
# MyEmail(subject, body).send_email()
# msg = MIMEText(body, "html")
# dd = DingDing()
# dd.send_markdown(title='Do It!!!',
#                 content=msg)
