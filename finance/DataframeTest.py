#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from UsTicker import UsTicker
from ToolKit import ToolKit


trade_date = ToolKit('取最近一天').get_latest_trade_date(1)
df = UsTicker(trade_date).get_usstock_data_for_day()


lst_trade_date = ToolKit('取上一个交易日').get_latest_trade_date(2)
df_lst = UsTicker(lst_trade_date).get_usstock_data_for_day()

# print(df_lst.loc[df_lst['symbol'] == 'BABA']['close'].values[0])
for index, row in df.iterrows():
    if row['symbol'] in df_lst['symbol'].values:
        if row['preclose'] != df_lst.loc[df_lst['symbol'] == row['symbol']]['close'].values[0]:
            print(row['symbol'], row['preclose'],
                  df_lst.loc[df_lst['symbol'] == row['symbol']]['close'].values[0])
