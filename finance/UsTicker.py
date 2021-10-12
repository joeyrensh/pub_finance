#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
import os
import re
from FileInfo import FileInfo


# 组合每日股票数据为一个dataframe
class UsTicker:

    def __init__(self, trade_date):
        # 获取交易日期
        self.trade_date = trade_date
        # 获取文件列表
        file = FileInfo(trade_date)
        # 获取交易日当天日数据文件
        self.file_day = file.get_file_name_day
        # 获取交易日当天5分钟数据文件
        self.file_mi = file.get_file_name_mi
        # 获取截止交易日当天历史日数据文件列表
        self.files = file.get_files_day_list

    # 获取股票代码列表
    def get_usstock_list(self):
        tickers = list()
        df = pd.read_csv(self.file_day, usecols=[i for i in range(1, 13)])
        for index, i in df.iterrows():
            if float(i['volume']) > 0 \
                    and float(i['amplitude'].replace('%', '')) > 0 \
                    and float(i['mktcap']) > 0 \
                    and float(i['close']) > 0 \
                    and float(i['preclose']) > 0 \
                    and float(i['mktcap']) > 0:
                tickers.append(str(i['symbol']))
        return tickers

    # 获取最新一天股票数据
    def get_usstock_data_for_day(self):
        df = pd.read_csv(self.file_day, usecols=[i for i in range(1, 13)])
        return df

    # 获取历史数据股票合集
    def get_history_data(self):
        dic = {}
        for j in range(len(self.files)):
            df = pd.read_csv(self.files[j], usecols=[i for i in range(1, 13)])
            df['date'] = re.findall(r'\d+', self.files[j])[0]
            dic[j] = df
        df = pd.concat(list(dic.values()), ignore_index=True)
        df.sort_values(by=['symbol', 'date'], ascending=True, inplace=True)
        return df

    # 5分钟数据获取的股票列表，需要更大振幅做Filter
    def get_usstock_list_for_5mi(self):
        tickers = list()
        df = pd.read_csv(self.file_day, usecols=[i for i in range(1, 13)])
        for index, i in df.iterrows():
            if float(i['volume']) > 0 \
                    and float(i['amplitude'].replace('%', '')) > 10 \
                    and float(i['mktcap']) > 0 \
                    and float(i['close']) > 0 \
                    and float(i['preclose']) > 0 \
                    and float(i['mktcap']) > 0:
                tickers.append(str(i['symbol']))
        return tickers

    # 获取5分钟的分时数据
    def get_usstock_data_for_5mi(self):
        df = pd.read_csv(self.file_mi, usecols=[i for i in range(1, 9)])
        return df
