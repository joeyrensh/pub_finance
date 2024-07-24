#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import pandas as pd
from utility.ToolKit import ToolKit
import talib as tl
import multiprocessing
from utility.TickerInfo import TickerInfo


""" 策略类 """


class UsStrategy:
    def __init__(self, trade_date):
        self.trade_date = trade_date
        self.market = "us"

    """ 小市值大波动 """

    def get_usstrategy1(self):
        """获取日数据"""
        df1 = TickerInfo(self.trade_date, self.market).get_stock_data_for_day()
        lenth = len(df1)
        tool = ToolKit("策略1")
        """ 以字典和列表形式来存储 """
        list = []
        for index, row in df1.iterrows():
            """计算进度"""
            tool.progress_bar(lenth, index)
            """ 振幅超过40% """
            if (
                float(row["preclose"]) > 0
                and (float(row["high"]) - float(row["low"])) / float(row["preclose"])
                > 0.4
            ):
                pass
            else:
                continue
            """ 涨幅超过20% """
            if float(row["chg"]) > 0.2:
                pass
            else:
                continue
            dic = {
                "symbol": row["symbol"],
                "close": row["close"],
                "chg": float(row["chg"]) / 100,
                "volume": float(row["volume"]),
                "amplitude": (float(row["high"]) - float(row["low"]))
                * 100
                / float(row["preclose"]),
                "tag": "振幅剧烈",
            }
            list.append(dic)

        """ 返回T+1股票行情 """
        if len(list) > 0:
            df = pd.DataFrame(list)
            # # 发送钉钉消息
            # dd = DingDing()
            # image_address = 'http://5b0988e595225.cdn.sohucs.com/images/'\
            #                 '20180208/fee9b75f1bf544388867cf0391b52dfa.gif'
            # # 发送TOP1振幅股票到钉钉
            # max_chg = df['amplitude'].max()
            # df_max = df.loc[df['amplitude'] == df['amplitude'].max()]
            # symbol = df_max.iloc[0]['symbol']
            # dd.send_markdown(title='Do It!!!',
            #                  content='## 今日仙股\n'
            #                  '#### ' + str(symbol) + '振幅' +
            #                  str(max_chg) + '\n\n'
            #                  '> ![美景](' + image_address + ')\n'
            #                  '> ## Just Do It!!!\n')
            return df
        else:
            return pd.DataFrame()

    """ 破线拐头交叉 """

    def get_usstrategy2(self, group_obj, ticker):
        """收盘价"""
        close = group_obj["close"].values
        """ 成交量 """
        volume = group_obj["volume"].values
        """ 涨跌幅 """
        chg = group_obj["chg"].values
        """ 高 """
        high = group_obj["high"].values
        """ 低 """
        low = group_obj["low"].values
        """ MA20 """
        ma20 = tl.MA(close, timeperiod=20)
        """ MA60 """
        ma60 = tl.MA(close, timeperiod=60)
        """ EMA20 """
        ema20 = tl.EMA(close, timeperiod=20)
        """ EMA60 """
        ema60 = tl.EMA(close, timeperiod=60)
        """ MACD """
        dif, dea, macd = tl.MACD(close, 12, 26, 9)
        """ MACD多头 """
        if (
            macd.size > 3
            and macd[-1] != "nan"
            and macd[-2] != "nan"
            and macd[-3] != "nan"
            and macd[-1] > macd[-2] > macd[-3]
        ):
            pass
        else:
            return
        """ dif刚刚向上穿越DEA """
        if (
            dif.size > 2
            and dea.size > 2
            and dif[-1] != "nan"
            and dea[-1] != "nan"
            and dif[-2] != "nan"
            and dea[-2] != "nan"
            and dif[-1] >= dea[-1]
            and dif[2] < dea[-2]
        ):
            pass
        else:
            return
        """ 收盘价站上20均线，20站上60 """
        if (
            ema20.size > 3
            and ema60.size > 2
            and ema20[-1] != "nan"
            and ema20[-2] != "nan"
            and ema60[-1] != "nan"
            and ema60[-2] != "nan"
            and close[-1] >= ema20[-1]
            and ema20[-1] >= ema60[-1]
        ):
            pass
        else:
            return
        if (
            ma20.size > 2
            and ma60.size > 2
            and ma20[-1] != "nan"
            and ma20[-2] != "nan"
            and ma60[-1] != "nan"
            and ma60[-2] != "nan"
            and close[-1] >= ma20[-1]
            and ma20[-1] >= ma60[-1]
        ):
            pass
        else:
            return
        """ EMA斜率大于0 """
        if ema20[-1] > ema20[-2] > ema20[-3]:
            pass
        else:
            return
        """ 今日收盘价高于昨日收盘价, 且成交量超过历史成交最大量的50% """
        if (
            len(close) > 2
            and close[-1] > close[-2]
            and len(volume) > 2
            and volume[-1] > volume.max(axis=0) * 0.5
        ):
            pass
        else:
            return
        """ 乖离率，短期/中期都低于0.01 """
        if (close[-1] - ema20[-1]) / ema20[-1] < 0.1 and (
            ema20[-1] - ema60[-1]
        ) / ema60[-1] < 0.1:
            pass
        else:
            return
        """ 昨日收盘价不能为0用来计算振幅 """
        if close[-2] > 0:
            pass
        else:
            return
        """ 每组策略返回结果为字典 """
        dic = {
            "symbol": ticker,
            "close": close[-1],
            "chg": float(chg[-1]),
            "volume": volume[-1],
            "amplitude": (high[-1] - low[-1]) * 100 / close[-2],
            "tag": "破线拐头交叉",
        }
        return dic

    # # 分时波动频繁
    # def get_usstrategy4(self):
    #     # 初始化5分钟数据
    #     try:
    #         df = UsTicker(self.trade_date).get_usstock_data_for_5mi()
    #     except Exception as e:
    #         print('分时数据读取错误：', e)
    #         return pd.DataFrame()
    #     tickers = df['symbol'].unique().tolist()
    #     list1 = []
    #     tool = ToolKit('策略4')
    #     for i in tickers:
    #         group_obj = df.groupby(by='symbol').get_group(i)
    #         close = group_obj['close'].values
    #         open_alias = group_obj['open_alias'].values
    #         dif, dea, macd = tl.MACD(close, 12, 26, 9)
    #         macdp = sum([int(xi >= 0) for xi in macd])  # 正数
    #         macdn = sum([int(xi < 0) for xi in macd])  # 负数
    #         # macd为正次数高于为负，收盘价高于开盘价10%
    #         if macdp > macdn and close[-1] > open_alias[0] * 1.1:
    #             try:
    #                 dic1 = {'symbol': i, 'close': close[-1],
    #                           'open_alias': open_alias[0],
    #                         'macdp': macdp, 'macdn': macdn}
    #                 list1.append(dic1)
    #             except KeyError:
    #                 print('no key definition: ', i)
    #         tool.progress_bar(len(tickers), tickers.index(i))
    #     if len(list1) > 0:
    #         # 将list转化为dataframe，并且存储为csv文件，带index和header
    #         df = pd.DataFrame(list1)
    #         df['tag'] = '策略4'
    #         return df
    #     else:
    #         return pd.DataFrame()

    def exec_strategy_with_multiprocessing(self):
        """获取历史数据"""
        his_data = TickerInfo(self.trade_date, self.market).get_history_data()
        tickers = TickerInfo(self.trade_date, self.market).get_usstock_list()

        """ 
        关键指标 symbol, close, ma20, ma60, ema20, ema60, dif, dea, macd
        存放策略结果 
        """
        list = []
        results = []
        tool = ToolKit("多进程执行策略")
        pool = multiprocessing.Pool(processes=4)
        """ 循环股票列表，并行执行策略 """
        group_data = his_data.groupby(by="symbol")
        for ticker in tickers:
            """按股票分组"""
            group_obj = group_data.get_group(ticker)
            result = pool.apply_async(self.get_usstrategy2, (group_obj, ticker))
            results.append(result)
            tool.progress_bar(len(tickers), tickers.index(ticker))
        """ 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用 """
        pool.close()
        """ 等待进程池中的所有进程执行完毕 """
        pool.join()

        """ 获取进程内数据 """
        for dic in results:
            if dic.get():
                list.append(dic.get())

        """ 结果数据返回 """
        if len(list) > 0:
            df = pd.DataFrame(list)
            return df
        else:
            return pd.DataFrame()
