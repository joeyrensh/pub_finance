#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
from UsFileInfo import UsFileInfo
from ToolKit import ToolKit
import multiprocessing
import gc


""" 组合每日股票数据为一个dataframe """


class UsTickerInfo:

    def __init__(self, trade_date):
        """ 获取交易日期 """
        self.trade_date = trade_date
        """ 获取文件列表 """
        file = UsFileInfo(trade_date)
        """ 获取交易日当天日数据文件 """
        self.file_day = file.get_file_name_day
        """ 获取截止交易日当天历史日数据文件列表 """
        self.files = file.get_files_day_list

    """ 获取股票代码列表 """

    def get_usstock_list(self):
        tickers = list()
        df = pd.read_csv(self.file_day, usecols=[i for i in range(1, 13)])
        """ 匹配行业信息 """
        df_o = pd.read_csv('./usstockinfo/usindustry_em.csv',
                           usecols=[i for i in range(1, 3)])
        df_n = pd.merge(df, df_o, how='inner', on='symbol')
        for index, i in df_n.iterrows():
            """ 
            单价超过2美金
            日成交量过50W 
            """
            if float(i['volume']) > 500000 \
                    and float(i['close']) > 2 \
                    and float(i['open']) > 0 \
                    and float(i['high']) > 0 \
                    and float(i['low']) > 0:
                tickers.append(str(i['symbol']))
        return tickers

    """ 获取最新一天股票数据 """

    def get_usstock_data_for_day(self):
        df = pd.read_csv(self.file_day, usecols=[i for i in range(1, 13)])
        """ 匹配行业信息 """
        df_o = pd.read_csv('./usstockinfo/usindustry_em.csv',
                           usecols=[i for i in range(1, 3)])
        df_n = pd.merge(df, df_o, how='inner', on='symbol')        
        return df_n

    """ 获取历史数据 """

    def get_history_data(self):
        dic = {}
        for j in range(len(self.files)):
            df = pd.read_csv(self.files[j], usecols=[i for i in range(1, 13)])
            dic[j] = df
        df = pd.concat(list(dic.values()), ignore_index=True)
        df.sort_values(by=['symbol', 'date'], ascending=True, inplace=True)
        return df

    """ 
    获取backtrader所需的datafeed
    将历史数据按照backtrader datafeed格式重构
    """

    def get_backtrader_data_feed(self):
        tickers = self.get_usstock_list()
        his_data = self.get_history_data().groupby(by='symbol')
        t = ToolKit('加载历史数据')
        """ 存放策略结果 """
        list = []
        results = []
        """ 创建多进程 """
        pool = multiprocessing.Pool(processes=8)
        for i in tickers:
            """
            适配BackTrader数据结构
            每个股票数据为一组
            """
            group_obj = his_data.get_group(i)
            result = pool.apply_async(
                self.reconstruct_dataframe, (group_obj, i))
            results.append(result)
        """ 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用 """
        pool.close()
        """ 等待进程池中的所有进程执行完毕 """
        pool.join()

        """ 垃圾回收 """
        del his_data
        gc.collect()
        """ 获取进程内数据 """
        for dic in results:
            if len(dic.get()) > 0:
                list.append(dic.get())
            t.progress_bar(len(results), results.index(dic))
        return list

    """ 重构dataframe封装 """

    def reconstruct_dataframe(self, group_obj, i):
        print("正在处理股票：", i)
        """ 
        过滤历史数据不完整的股票
        小于200天的股票暂时不进入回测列表
        """
        if len(group_obj) < 200:
            return pd.DataFrame()
        """ 适配BackTrader数据结构 """
        df_copy = pd.DataFrame({'open': group_obj['open'].values,
                                'close': group_obj['close'].values,
                                'high': group_obj['high'].values,
                                'low': group_obj['low'].values,
                                'volume': group_obj['volume'].values,
                                'symbol': group_obj['symbol'].values
                                }, index=pd.to_datetime(group_obj['date'], format='%Y-%m-%d')).copy()
        return df_copy

    """ 测试用途 """

    def get_backtrader_data_feed_test(self):
        tickers = self.get_usstock_list()
        his_data = self.get_history_data().groupby(by='symbol')
        t = ToolKit('加载历史数据')
        """ 存放策略结果 """
        list = []
        results = []
        """ 创建多进程 """
        pool = multiprocessing.Pool(processes=8)
        for i in ['UAL']:
            """
            适配BackTrader数据结构
            每个股票数据为一组
            """
            group_obj = his_data.get_group(i)
            result = pool.apply_async(
                self.reconstruct_dataframe, (group_obj, i))
            results.append(result)
        """ 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用 """
        pool.close()
        """ 等待进程池中的所有进程执行完毕 """
        pool.join()

        """ 垃圾回收 """
        del his_data
        gc.collect()
        """ 获取进程内数据 """
        for dic in results:
            if len(dic.get()) > 0:
                list.append(dic.get())
            t.progress_bar(len(results), results.index(dic))
        return list
