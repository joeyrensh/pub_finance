#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from UsTicker import UsTicker


# 小市值大波动策略-策略1
date = '20211012'
us_tickers = UsTicker(date).get_usstock_data_for_day()
for i, row in us_tickers.iterrows():
    if float(row['mktcap']) > 0:
        print(i)
