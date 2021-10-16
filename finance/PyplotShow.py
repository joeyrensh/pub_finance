#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as ticker
import talib as tl


# trade_date = '20211012'
# # us_strate = UsStrategy(trade_date)
# # df4 = us_strate.get_usstrategy4()
# # list4 = []
# # for index, row in df4.iterrows():
# #     list4.append(row['symbol'])

# rpath = './usstockinfo/SinaTicker5Min_' + trade_date + '.csv'
# df = pd.read_csv(rpath)
# for index, row in df.iterrows():
#     # if row['symbol'] not in list4:
#     if row['symbol'] not in ['LAC', 'AFIB']:
#         df.drop(index=index, inplace=True)
# df.to_csv('./test.csv', index=False)


df = pd.read_csv('./test.csv')
for index, row in df.iterrows():
    df.loc[index, ['event_time']] = str(row['event_time'])[11:16]


for usl in ['LAC']:
    # 数据分组
    fig, ax = plt.subplots(num=2, figsize=(15, 10))
    group_obj = df.groupby(by='symbol').get_group(usl)
    close = group_obj['close'].values
    event_time = group_obj['event_time'].values
    ma = tl.MA(close, timeperiod=20)
    ax.plot(event_time, close, linestyle='-',
            color='g', marker='o', markersize=10)
    ax.plot(event_time, ma, linestyle='-', color='r')
    ax.xaxis.set_major_locator(ticker.MultipleLocator(base=15))
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.title(usl)
    plt.show()

# print(tt)


# df.groupby('symbol').plot(x='event_time', y='close', ax=ax, legend=False)
