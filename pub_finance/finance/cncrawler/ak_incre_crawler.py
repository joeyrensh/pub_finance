#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from datetime import datetime
import os
from utility.fileinfo import FileInfo
import akshare as ak
from utility.em_stock_uti import EMWebCrawlerUti


class AKCNWebCrawler:
    def __init__(self):
        """A股日数据url, 东方财经
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH
        """
        self.proxy = None

    """ AKshare获取A股历史数据"""

    def get_cn_daily_stock_info_ak(self, trade_date):
        """
        获取A股数据，上海市场
        """
        stock_sh_a_spot_em = ak.stock_sh_a_spot_em()
        pd_stock_sh = stock_sh_a_spot_em[
            [
                "代码",
                "名称",
                "今开",
                "最新价",
                "最高",
                "最低",
                "成交量",
                "总市值",
                "市盈率-动态",
            ]
        ].copy()
        pd_stock_sh = pd_stock_sh[pd_stock_sh["最新价"] > 0]
        pd_stock_sh = pd_stock_sh.rename(
            columns={
                "代码": "symbol",
                "名称": "name",
                "今开": "open",
                "最新价": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "总市值": "total_value",
                "市盈率-动态": "pe",
            }
        )
        # 给 symbol 字段增加前缀（如 "SH"）
        pd_stock_sh["symbol"] = "SH" + pd_stock_sh["symbol"]

        """ 获取A股数据文件地址 """
        file_name_d = FileInfo(trade_date, "cn").get_file_path_latest
        """ 每日一个文件，根据交易日期创建 """
        if os.path.exists(file_name_d):
            os.remove(file_name_d)
        date = datetime.strptime(trade_date, "%Y%m%d")
        pd_stock_sh["date"] = date
        pd_stock_sh.to_csv(file_name_d, mode="w", index=True, header=True)

        """
        获取A股数据, 深圳市场
        """
        stock_sz_a_spot_em = ak.stock_sz_a_spot_em()
        pd_stock_sz = stock_sz_a_spot_em[
            [
                "代码",
                "名称",
                "今开",
                "最新价",
                "最高",
                "最低",
                "成交量",
                "总市值",
                "市盈率-动态",
            ]
        ].copy()
        pd_stock_sz = pd_stock_sz[pd_stock_sz["最新价"] > 0]
        pd_stock_sz = pd_stock_sz.rename(
            columns={
                "代码": "symbol",
                "名称": "name",
                "今开": "open",
                "最新价": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "总市值": "total_value",
                "市盈率-动态": "pe",
            }
        )
        # 给 symbol 字段增加前缀（如 "SZ"）
        pd_stock_sz["symbol"] = "SZ" + pd_stock_sz["symbol"]
        pd_stock_sz["date"] = date
        pd_stock_sz.to_csv(file_name_d, mode="a", index=True, header=False)

        """
        ETF数据
        """
        fund_etf_spot_em_df = ak.fund_etf_spot_em()
        pd_etf = fund_etf_spot_em_df[
            [
                "代码",
                "名称",
                "开盘价",
                "最新价",
                "最高价",
                "最低价",
                "成交量",
                "总市值",
            ]
        ].copy()
        pd_etf = pd_etf[pd_etf["最新价"] > 0]
        pd_etf = pd_etf.rename(
            columns={
                "代码": "symbol",
                "名称": "name",
                "开盘价": "open",
                "最新价": "close",
                "最高价": "high",
                "最低价": "low",
                "成交量": "volume",
                "总市值": "total_value",
            }
        )
        # 给 symbol 字段增加前缀（如 "ETF"）
        pd_etf["symbol"] = "ETF" + pd_etf["symbol"]
        pd_etf["pe"] = "-"
        pd_etf["date"] = date
        pd_etf.to_csv(file_name_d, mode="a", index=True, header=False)

        em = EMWebCrawlerUti()
        em.get_daily_gz_info("cn", trade_date)
