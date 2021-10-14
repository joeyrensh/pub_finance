#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
from ToolKit import ToolKit
from UsTicker import UsTicker
import talib as tl
from DingDing import DingDing


# 策略类
class UsStrategy:

    def __init__(self, trade_date):
        self.trade_date = trade_date
    # 小市值大波动

    def get_usstrategy1(self):
        # 获取日数据
        df1 = UsTicker(self.trade_date).get_usstock_data_for_day()
        lenth = len(df1)
        tool = ToolKit('策略1')
        for index, row in df1.iterrows():
            # 振幅超过10%
            if float(row['amplitude'].replace('%', '')) < 10:
                df1.drop(index=index, inplace=True)
            # 市值低于5亿
            elif float(row['mktcap']) > 500000000:
                df1.drop(index=index, inplace=True)
            # 日换手金额超过市值10%
            elif float(row['mktcap']) != 0 \
                    and (float(row['volume']) * float(row['close'])) * 100 * 10 / float(row['mktcap']) < 100:
                df1.drop(index=index, inplace=True)
            # 成交量>0 且市值>0
            elif float(row['volume']) <= 0 or float(row['mktcap']) <= 0:
                df1.drop(index=index, inplace=True)
            # 正向波动
            elif float(row['chg']) < 0:
                df1.drop(index=index, inplace=True)
            tool.progress_bar(lenth, index)

        # 返回T+1股票行情
        if not df1.empty:
            df1['tag'] = '策略1'
            # 发送钉钉消息
            dd = DingDing()
            image_address = 'http://imgs.jushuo.com/editor/2016-11-21/5832b9d2ac8e0.gif'
            # 发送TOP1振幅股票到钉钉
            max_chg = df1['amplitude'].max()
            df_max = df1.loc[df1['amplitude'] == df1['amplitude'].max()]
            symbol1 = df_max.iloc[0]['symbol']
            dd.send_markdown(title='Do It!!!',
                            content='## 今日仙股\n'
                            '#### ' + symbol1 + '振幅' + max_chg + '\n\n'
                            '> ![美景](' + image_address + ')\n'
                            '> ## Just Do It!!!\n')
        return df1

    # 昨日振幅大，且今天开盘涨，且超过10亿市值
    def get_usstrategy2(self):
        # 获取日数据
        df2 = UsTicker(self.trade_date).get_usstock_data_for_day()
        lenth = len(df2)
        tool = ToolKit('策略2')
        for index, row in df2.iterrows():
            # 振幅超过10%
            if float(row['amplitude'].replace('%', '')) < 10:
                df2.drop(index=index, inplace=True)
            # 今日开盘价高于昨日收盘价
            elif float(row['open_alias']) < float(row['preclose']):
                df2.drop(index=index, inplace=True)
            # 市值超过10亿
            elif float(row['mktcap']) < 1000000000:
                df2.drop(index=index, inplace=True)
            # 成交量和市值均大于0
            elif float(row['volume']) <= 0 or float(row['mktcap']) <= 0:
                df2.drop(index=index, inplace=True)
            # 正向波动
            elif float(row['chg']) < 0:
                df2.drop(index=index, inplace=True)
            tool.progress_bar(lenth, index)
        # 返回T+1股票行情
        if not df2.empty:
            df2['tag'] = '策略2'
        return df2

    # 三重滤网，当日股价在MA20上方，MACD在0轴上方，今日收盘价高于昨日
    def get_usstrategy3(self):
        # 获取美股多日分组数据
        us = UsTicker(self.trade_date).get_history_data()
        uslist = UsTicker(self.trade_date).get_usstock_list()
        # symbol, close, ma20, dif, dea, macd
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
            df['tag'] = '策略3'
            return df
        else:
            return pd.DataFrame()

    # 分时波动频繁
    def get_usstrategy4(self):
        # 初始化5分钟数据
        try:
            df = UsTicker(self.trade_date).get_usstock_data_for_5mi()
        except Exception as e:
            print('分时数据读取错误：', e)
            return pd.DataFrame()
        tickers = df['symbol'].unique().tolist()
        list1 = []
        tool = ToolKit('策略4')
        for i in tickers:
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
            tool.progress_bar(len(tickers), tickers.index(i))
        if len(list1) > 0:
            # 将list转化为dataframe，并且存储为csv文件，带index和header
            df = pd.DataFrame(list1)
            df['tag'] = '策略4'
            return df
        else:
            return pd.DataFrame()
