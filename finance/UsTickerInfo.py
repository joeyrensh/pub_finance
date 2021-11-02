#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
from UsFileInfo import UsFileInfo
from ToolKit import ToolKit
from datetime import datetime, timedelta


# 组合每日股票数据为一个dataframe
class UsTickerInfo:

    def __init__(self, trade_date):
        # 获取交易日期
        self.trade_date = trade_date
        # 获取文件列表
        file = UsFileInfo(trade_date)
        # 获取交易日当天日数据文件
        self.file_day = file.get_file_name_day
        # 获取截止交易日当天历史日数据文件列表
        self.files = file.get_files_day_list

    # 获取股票代码列表
    def get_usstock_list(self):
        tickers = list()
        df = pd.read_csv(self.file_day, usecols=[i for i in range(1, 13)])
        for index, i in df.iterrows():
            if float(i['volume']) > 0 \
                    and float(i['close']) > 0 \
                    and float(i['open']) > 0 \
                    and float(i['high']) > 0 \
                    and float(i['low']) > 0:
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
            # df['date'] = re.findall(r'\d+', self.files[j])[0]
            dic[j] = df
        df = pd.concat(list(dic.values()), ignore_index=True)
        df.sort_values(by=['symbol', 'date'], ascending=True, inplace=True)
        return df

    # 获取BackTrader对应的DataFeed
    def get_backtrader_data_feed(self):
        tickers = self.get_usstock_list()
        his_data = self.get_history_data()
        t = ToolKit('加载历史数据')
        list = []
        for i in tickers:
            t.progress_bar(len(tickers), tickers.index(i))
            df = his_data.groupby(by='symbol').get_group(i)
            # 过滤历史数据不完整的股票
            if len(df) < 200:
                continue
            # 适配BackTrader数据结构
            df_copy = pd.DataFrame({'open': df['open'].values,
                                   'close': df['close'].values,
                                    'high': df['high'].values,
                                    'low': df['low'].values,
                                    'volume': df['volume'].values,
                                    'symbol': df['symbol'].values
                                    }, index=pd.to_datetime(df['date'], format='%Y%m%d'))
            list.append(df_copy)
        return list
