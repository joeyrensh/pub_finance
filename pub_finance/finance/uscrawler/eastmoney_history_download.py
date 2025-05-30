#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from utility.toolkit import ToolKit
import re
from datetime import datetime
import time
import random
import pandas as pd
import requests
import json
import concurrent.futures

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
    def __init__(self):
        self.proxy = {
            # "http": "http://60.210.40.190:9091",
            # "https": "http://60.210.40.190:9091",
        }
        self.proxy = None
        self.headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "host": "push2.eastmoney.com",
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

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
        """url定义"""
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery"
            "&pn=i&pz=200&po=1&np=1&ut=&fltt=2&invt=2&fid=f12&fs=m:mkt_code&fields=f2,f3,f4,f5,f6,f7,f12,f14,f15,f16,f17,f18,f20,f21&_=unix_time"
        )
        for mkt_code in ["105", "106", "107"]:
            for i in range(1, 500):
                """请求url，获取数据response"""
                url_re = (
                    url.replace("unix_time", str(current_timestamp))
                    .replace("mkt_code", mkt_code)
                    .replace("pn=i", "pn=" + str(i))
                )
                time.sleep(random.uniform(1, 2))
                res = requests.get(
                    url_re, proxies=self.proxy, headers=self.headers
                ).text
                """ 替换成valid json格式 """
                res_p = re.sub("\\].*", "]", re.sub(".*:\\[", "[", res, 1), 1)
                try:
                    json_object = json.loads(res_p)
                except ValueError:
                    break
                for i in json_object:
                    if (
                        i["f12"] == "-"
                        or i["f17"] == "-"
                        or i["f2"] == "-"
                        or i["f15"] == "-"
                        or i["f16"] == "-"
                        or i["f5"] == "-"
                        or i["f20"] == "-"
                        or i["f21"] == "-"
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
            "https://push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery"
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

        res = requests.get(url_re, proxies=self.proxy, headers=self.headers).text
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
        batch_size = 10  # Number of tickinfo items to process in each batch
        batch_count = 0
        tool = ToolKit("历史数据下载")

        with open(file_path, "a") as csvfile:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                for h in range(0, len(tickinfo), batch_size):
                    """休眠, 避免IP Block"""
                    time.sleep(1 + random.uniform(1, 3))
                    batch_count += 1
                    batch_list = []
                    futures = []
                    for t in range(1, batch_size + 1):
                        index = h + t - 1
                        if index < len(tickinfo):
                            future = executor.submit(
                                self.get_his_tick_info,
                                tickinfo[index]["mkt_code"],
                                tickinfo[index]["symbol"],
                                start_date,
                                end_date,
                            )
                            futures.append(future)
                    for future in concurrent.futures.as_completed(futures):
                        list1 = future.result()
                        if list1 is not None:  # Only append if list1 is not empty
                            batch_list.extend(list1)

                    tool.progress_bar(len(tickinfo), h)
                    # Write batch data to CSV file
                    if len(batch_list) > 0:  # Check if batch_list is not empty
                        try:
                            df = pd.DataFrame(batch_list)
                            df.to_csv(
                                csvfile,
                                mode="a",
                                index=True,
                                header=(h == 0),
                            )
                        except IOError:
                            pass


# 历史数据起始时间，结束时间
# 文件名称定义
em = EMHistoryDataDownload()
start_date = "20230101"
end_date = "20240222"
file_path = "./usstockinfo/stock_20240222.csv"
em.set_his_tick_info_to_csv(start_date, end_date, file_path)
