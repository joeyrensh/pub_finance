#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from utility.ToolKit import ToolKit
import re
from datetime import datetime, timedelta
import time
import pandas as pd
import requests
import json
from utility.MyThread import MyThread
from pandas.errors import EmptyDataError

""" 
东方财经的美股日K数据获取接口：

样例：
MSFT 纳斯达克
BABA 纽交所
BBJP 美国交易所

市场代码：
105:纳斯达克 NASDAQ
106:纽交所 NYSE
107:美国交易所 AMEX

股票列表URL：
https://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery&pn=1&pz=20000&po=1&np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:105&fields=f12&_=1635820578136
https://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery&pn=1&pz=20000&po=1&np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:106&fields=f12&_=1635820578136
https://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery&pn=1&pz=20000&po=1&np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:107&fields=f12&_=1635820578136 

历史数据URL：
https://92.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery&secid=106.BABA&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20210101&end=20500101&smplmt=755&lmt=1000000&_=1636002112627

历史数据返回字段列表：
date,open,close,high,low,volume,turnover,amplitude,chg,change,换手率

"""


class EMHistoryDataDownload:
    def get_us_stock_list(self):
        """url里需要传递unixtime当前时间戳"""
        current_timestamp = int(time.mktime(datetime.now().timetuple()))

        """     
        市场代码：
        105:纳斯达克 NASDAQ
        106:纽交所 NYSE
        107:美国交易所 AMEX    
        """
        dict = {}
        list = []
        for mkt_code in ["105", "106", "107"]:
            """url定义"""
            url = (
                "https://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery"
                "&pn=1&pz=20000&po=1&np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:mkt_code&fields=f2,f5,f12,f14,f15,f16,f17&_=unix_time"
            )
            """ 请求url，获取数据response """
            url_re = url.replace("unix_time", str(current_timestamp)).replace(
                "mkt_code", mkt_code
            )
            res = requests.get(url_re).text
            """ 替换成valid json格式 """
            res_p = re.sub("\\].*", "]", re.sub(".*:\\[", "[", res, 1), 1)
            json_object = json.loads(res_p)
            for i in json_object:
                if (
                    i["f12"] == "-"
                    or i["f17"] == "-"
                    or i["f2"] == "-"
                    or i["f15"] == "-"
                    or i["f16"] == "-"
                    or i["f5"] == "-"
                ):
                    continue
                dict = {"symbol": i["f12"], "mkt_code": mkt_code}
                list.append(dict)
        return list

    def get_his_tick_info(self, mkt_code, symbol, start_date, end_date):
        """
        历史数据URL：
        https://92.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery&secid=106.BABA&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20210101&end=20500101&smplmt=755&lmt=1000000&_=1636002112627

        历史数据返回字段列表：
        date,open,close,high,low,volume,turnover,amplitude,chg,change,换手率
        """
        """ url里需要传递unixtime当前时间戳 """
        current_timestamp = int(time.mktime(datetime.now().timetuple()))

        """     
        beg: 开始日期 例如 20200101
        end: 结束日期 例如 20200201
        klt: k线间距 默认为 101 即日k
                klt:1 1 分钟
                klt:5 5 分钟
                klt:101 日
                klt:102 周
            fqt: 复权方式
                不复权 : 0
                前复权 : 1
                后复权 : 2
        """

        url = (
            "https://92.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery"
            "&secid=mkt_code.symbol&ut="
            "&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
            "&klt=101&fqt=1&beg=start_date&end=end_date&smplmt=755&lmt=1000000&_=unix_time"
        )

        url_re = (
            url.replace("mkt_code", mkt_code)
            .replace("symbol", symbol)
            .replace("start_date", start_date)
            .replace("end_date", end_date)
            .replace("unix_time", str(current_timestamp))
        )

        res = requests.get(url_re).text
        """ 抽取公司名称 """
        name = re.search('\\"name\\":\\"(.*?)\\",', res).group(1)
        print("开始处理：", name)
        """ 替换成valid json格式 """
        res_p = re.sub("\\].*", "]", re.sub(".*:\\[", "[", res, 1), 1)
        json_object = json.loads(res_p)
        dict = {}
        list = []
        for i in json_object:
            """
            历史数据返回字段列表：
            date,open,close,high,low,volume,turnover,amplitude,chg,change,换手率
            """
            if (
                i.split(",")[1] == "-"
                or i.split(",")[2] == "-"
                or i.split(",")[3] == "-"
                or i.split(",")[4] == "-"
                or i.split(",")[5] == "-"
            ):
                continue
            dict = {
                "symbol": symbol,
                "name": name,
                "open": i.split(",")[1],
                "close": i.split(",")[2],
                "high": i.split(",")[3],
                "low": i.split(",")[4],
                "volume": i.split(",")[5],
                "turnover": i.split(",")[6],
                "chg": i.split(",")[8],
                "change": i.split(",")[9],
                "amplitude": i.split(",")[7],
                "preclose": None,
                "date": i.split(",")[0],
            }
            list.append(dict)
        return list

    def set_his_tick_info_to_csv(self, start_date, end_date, file_path):
        """获取股票列表"""
        tickinfo = self.get_us_stock_list()
        """ 多线程获取，每次步长为3，为3线程 """
        list_new = []
        tool = ToolKit("历史数据下载")
        for h in range(0, len(tickinfo), 2):
            """休眠, 避免IP Block"""
            time.sleep(0.5)
            for t in range(1, 3):
                if (h + t) <= len(tickinfo):
                    my_thread = MyThread(
                        self.get_his_tick_info,
                        (
                            tickinfo[h + t - 1]["mkt_code"],
                            tickinfo[h + t - 1]["symbol"],
                            start_date,
                            end_date,
                        ),
                    )
                    my_thread.setDaemon(True)
                    my_thread.start()
                    list1 = my_thread.get_result()
                    if list1 is None:
                        continue
                    list_new.extend(list1)
            tool.progress_bar(len(tickinfo), h)
        try:
            """将list转化为dataframe，并且存储为csv文件，带index和header"""
            df = pd.DataFrame(list_new)
            df.to_csv(file_path, mode="w", index=True, header=True)
        except EmptyDataError:
            pass


#历史数据起始时间，结束时间
#文件名称定义 
em = EMHistoryDataDownload()
start_date = '20220701'
end_date = '20220926'
file_path = './usstockinfo/stock_20220926.csv'
em.set_his_tick_info_to_csv(start_date, end_date, file_path)
