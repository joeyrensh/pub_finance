#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
from ToolKit import ToolKit
from UsTicker import UsTicker
import talib as tl
from DingDing import DingDing
import time


trade_date = '20211014'
# 获取美股历史数据
his_data = UsTicker(trade_date).get_history_data()
tickers = UsTicker(trade_date).get_usstock_list()
# symbol, close, ma20, ma60, ema20, ema60, dif, dea, macd
list = []
tool = ToolKit('策略测试')
for ticker in tickers:
# for ticker in ['BABA']:
    tool.progress_bar(len(tickers), tickers.index(ticker))
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
        continue
    # dif刚刚向上穿越DEA
    if dif.size > 1 and dea.size > 1 \
        and dif[-1] != 'nan' and dea[-1] != 'nan' and dif[-1] > dea[-1] :
        pass
    else:
        continue
    # print('macd阶段判断结束')
    # 收盘价占上20均线，20站上60
    if ema20.size > 2 and ema60.size > 2 \
        and ema20[-1] != 'nan' and ema20[-2] != 'nan' and ema60[-1] != 'nan' and ema60[-2] != 'nan' \
        and close[-1] > ema20[-1] and ema20[-1] > ema60[-1]:
        pass
    else:
        continue
    if ma20.size > 2 and ma60.size > 2 \
        and ma20[-1] != 'nan' and ma20[-2] != 'nan' and ma60[-1] != 'nan' and ma60[-2] != 'nan' \
        and close[-1] > ma20[-1] and ma20[-1] > ma60[-1]:
        pass
    else:
        continue
    # EMA斜率大于0
    if ema20[-1] > ema20[-2]:
        pass
    else:
        continue
    # print('ma阶段判断结束')
    # 今日收盘价高于昨日收盘价
    if len(close) > 2 and close[-1] > close[-2] \
        and len(volume) > 2 and volume[-1] > volume.max(axis=0) * 0.5 :
        pass
    else:
        continue
    # print('close阶段判断结束')
    # 乖离率，短期/中期都低于0.01
    if (close[-1] - ema20[-1]) / ema20[-1] < 0.1 \
        and (ema20[-1] - ema60[-1]) / ema60[-1] < 0.1:
        pass
    else:
        continue
    # 字典
    dic1 = {'symbol': ticker, 'close': close[-1]}
    list.append(dic1)
    print(dic1)
    

df = pd.DataFrame(list)
print(df)

