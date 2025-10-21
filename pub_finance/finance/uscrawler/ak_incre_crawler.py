#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from datetime import datetime
import os
from utility.fileinfo import FileInfo
import akshare as ak
from utility.em_stock_uti import EMWebCrawlerUti
import pandas as pd


class AKUSWebCrawler:
    def __init__(self):
        self.proxy = None

    """ AKshare获取美股历史数据"""

    def get_us_daily_stock_info_ak(self, trade_date):
        """新浪财经接口获取美股数据"""
        ak_stock_daily = ak.stock_us_spot()
        pd_ak_stock_daily = pd.DataFrame(ak_stock_daily)
        df_stock_daily = pd_ak_stock_daily[
            [
                "symbol",
                "cname",
                "open",
                "price",
                "high",
                "low",
                "volume",
                "mktcap",
                "pe",
            ]
        ].copy()
        df_stock_daily["open"] = pd.to_numeric(df_stock_daily["open"], errors="coerce")
        df_stock_daily["price"] = pd.to_numeric(
            df_stock_daily["price"], errors="coerce"
        )
        df_stock_daily["high"] = pd.to_numeric(df_stock_daily["high"], errors="coerce")
        df_stock_daily["low"] = pd.to_numeric(df_stock_daily["low"], errors="coerce")
        df_stock_daily["volume"] = pd.to_numeric(
            df_stock_daily["volume"], errors="coerce"
        )
        df_stock_daily["mktcap"] = pd.to_numeric(
            df_stock_daily["mktcap"], errors="coerce"
        )
        df_stock_daily = df_stock_daily[
            (df_stock_daily["open"] > 0)
            & (df_stock_daily["price"] > 0)
            & (df_stock_daily["high"] > 0)
            & (df_stock_daily["low"] > 0)
            & (df_stock_daily["volume"] > 0)
            & (df_stock_daily["mktcap"] > 0)
        ]
        df_stock_daily = df_stock_daily.rename(
            columns={
                "symbol": "symbol",
                "cname": "name",
                "open": "open",
                "price": "close",
                "high": "high",
                "low": "low",
                "volume": "volume",
                "mktcap": "total_value",
                "pe": "pe",
            }
        )

        """ 获取A股数据文件地址 """
        file_name_d = FileInfo(trade_date, "us").get_file_path_latest
        """ 每日一个文件，根据交易日期创建 """
        if os.path.exists(file_name_d):
            os.remove(file_name_d)
        date = datetime.strptime(trade_date, "%Y%m%d")
        df_stock_daily["date"] = date
        df_stock_daily.to_csv(file_name_d, mode="w", index=True, header=True)

        em = EMWebCrawlerUti()
        em.get_daily_gz_info("us", trade_date)
