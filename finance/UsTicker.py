#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
import os
import re


# 组合每日股票数据为一个dataframe
class UsTicker:

    def __init__(self):
        rpath = './usstockinfo/'
        path_list = os.listdir(rpath)
        file_list = []
        for file in path_list:
            if re.search('SinaTicker_', file):
                file_list.append(file)
        dic = {}
        for j in range(len(file_list)):
            df = pd.read_csv(rpath+file_list[j], usecols=[i for i in range(1, 13)])
            df['date'] = re.findall(r'\d+', file_list[j])[0]
            dic[j] = df
        self.final = pd.concat(list(dic.values()), ignore_index=True)
        self.final.sort_values(by=['symbol', 'date'], ascending=True, inplace=True)

    @staticmethod
    def get_usstock_list(trade_date):
        rpath = './usstockinfo/SinaTicker_' + trade_date + '.csv'
        tickers = list()
        df = pd.read_csv(rpath, usecols=[i for i in range(1, 13)])
        for index, i in df.iterrows():
            if float(i['volume']) > 0 and float(i['amplitude'].replace('%', '')) > 1 \
                    and float(i['mktcap']) > 0 \
                    and float(i['close']) > 0 \
                    and float(i['preclose']) > 0 \
                    and (float(i['volume']) * float(i['close'])) > float(i['mktcap']) * 0.001:
                tickers.append(str(i['symbol']))
        return tickers

    def get_dataframe(self):
        return self.final

