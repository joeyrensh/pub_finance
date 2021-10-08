#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
from ToolKit import *
from UsTicker import UsTicker
import talib as tl


class UsStrategy:

    # 5分钟数据获取的股票清单
    def get_5min_tick_list(self, trade_date, rpath):
        tickers = list()
        df = pd.read_csv(rpath, usecols=[i for i in range(1, 13)])
        for index, i in df.iterrows():
            if float(i['volume']) > 0 and float(i['amplitude'].replace('%', '')) > 5 \
                    and float(i['mktcap']) > 0 \
                    and float(i['close']) > 0 \
                    and float(i['preclose']) > 0 \
                    and (float(i['volume']) * float(i['close'])) > float(i['mktcap']) * 0.01:
                tickers.append(str(i['symbol']))
        tickers1 = list(set(tickers))
        return tickers1

    # 小市值大波动
    def get_usstrategy1(self, rpath, wpath):
        df = pd.read_csv(rpath, usecols=[i for i in range(1, 13)])
        lenth = len(df)
        tool = ToolKit('策略1')
        for index, row in df.iterrows():
            # 振幅超过10%
            if float(row['amplitude'].replace('%', '')) < 10:
                df.drop(index=index, inplace=True)
            # 市值低于5亿
            elif float(row['mktcap']) > 500000000:
                df.drop(index=index, inplace=True)
            # 日换手金额超过市值10%
            elif float(row['mktcap']) != 0 and (float(row['volume']) * float(row['close'])) * 100 * 10 \
                    / float(row['mktcap']) < 100:
                df.drop(index=index, inplace=True)
            # 成交量>0 且市值>0
            elif float(row['volume']) <= 0 or float(row['mktcap']) <= 0:
                df.drop(index=index, inplace=True)
            tool.progress_bar(lenth, index)

        # 存取T+1股票行情
        df.to_csv(wpath, index=False, header=True)
        return df

    # 昨日振幅大，且今天开盘涨，且超过10亿市值
    def get_usstrategy2(self, rpath, wpath):
        # name,cname,category,symbol,price,diff,chg,preclose,open_price,high,low,amplitude,volume,mktcap,pe,market,category_id
        df = pd.read_csv(rpath, usecols=[i for i in range(1, 13)])
        lenth = len(df)
        tool = ToolKit('策略2')
        for index, row in df.iterrows():
            # 振幅超过10%
            if float(row['amplitude'].replace('%', '')) < 10:
                df.drop(index=index, inplace=True)
            # 今日开盘价高于昨日收盘价
            elif float(row['open_alias']) < float(row['preclose']):
                df.drop(index=index, inplace=True)
            # 市值超过10亿
            elif float(row['mktcap']) < 1000000000:
                df.drop(index=index, inplace=True)
            # 成交量和市值均大于0
            elif float(row['volume']) <= 0 or float(row['mktcap']) <= 0:
                df.drop(index=index, inplace=True)
            tool.progress_bar(lenth, index)
        # 存取T+1股票行情
        df.to_csv(wpath, index=False, header=True)
        return df

    # 三重滤网，当日股价在MA20上方，MACD在0轴上方，今日收盘价高于昨日
    def get_usstrategy3(self, wpath, trade_date):
        # 获取美股多日分组数据
        us = UsTicker().get_dataframe()
        uslist = UsTicker().get_usstock_list(trade_date)
        # symbol, close, ma20, dif, dea, macd
        dic1 = {}
        list1 = []
        tool = ToolKit('策略3')
        for usl in uslist:
            # 数据按股票代码分组
            group_obj = us.groupby(by='symbol').get_group(usl)
            # 取某个股票的收盘价
            close = group_obj['close'].values
            # 取MA20均线当前值
            ma = tl.MA(close, timeperiod=20)
            # MACD当前值
            dif, dea, macd = tl.MACD(close, 12, 26, 9)
            # ma20最新值非空且今日收盘价高于ma20
            if ma[-1] != 'nan' and close[-1] > ma[-1]:
                # dif,dea,macd最新值非空，且dif上穿dea
                if dif[-1] != 'nan' and dea[-1] != 'nan' and dif[-1] >= dea[-1]:
                    # dif,dea,macd最新值非空，且macd今日高于昨日
                    if macd[-1] != 'nan' and macd[-2] != 'nan' and macd[-1] > macd[-2]:
                        # 今日收盘价高于昨日
                        if close[-1] > close[-2]:
                            try:
                                dic1 = {'symbol': usl, 'close': close[-1], 'ma20': ma[-1],
                                        'dif': dif[-1], 'dea': dea[-1], 'macd': macd[-1]}
                                list1.append(dic1)
                            except KeyError:
                                print('no key definition: ', usl)
            tool.progress_bar(len(uslist), uslist.index(usl))

        if len(list1) > 0:
            # 将list转化为dataframe，并且存储为csv文件，带index和header
            df = pd.DataFrame(list1)
            df.to_csv(wpath, mode='w', index=True, header=True)
            return df
        else:
            return None

    # 分时波动频繁
    def get_usstrategy4(self, rpath, wpath, trade_date):
        df = pd.read_csv(rpath, usecols=[i for i in range(1, 9)])
        us_uniquelist = df['symbol'].unique().tolist()
        dic1 = {}
        list1 = []
        tool = ToolKit('策略4')
        for i in us_uniquelist:
            group_obj = df.groupby(by='symbol').get_group(i)
            close = group_obj['close'].values
            open_alias = group_obj['open_alias'].values
            dif, dea, macd = tl.MACD(close, 12, 26, 9)
            macdp = sum([int(xi >= 0) for xi in macd])  # 正数
            macdn = sum([int(xi < 0) for xi in macd])  # 负数
            # macd为正次数高于为负，收盘价高于开盘价10%
            if macdp > macdn and close[-1] > open_alias[0] * 1.1:
                try:
                    dic1 = {'symbol': i, 'close': close[-1], 'open_alias': open_alias[0],
                            'macdp': macdp, 'macdn': macdn}
                    list1.append(dic1)
                except KeyError:
                    print('no key definition: ', i)
            tool.progress_bar(len(us_uniquelist), us_uniquelist.index(i))
        if len(list1) > 0:
            # 将list转化为dataframe，并且存储为csv文件，带index和header
            df = pd.DataFrame(list1)
            df.to_csv(wpath, mode='w', index=True, header=True)
            return df
        else:
            return None


