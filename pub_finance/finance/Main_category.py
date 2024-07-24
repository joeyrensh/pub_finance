#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from uscrawler.EMUsTickerCategoryCrawler import EMUsTickerCategoryCrawler
from cncrawler.EMCNTickerCategoryCrawler import EMCNTickerCategoryCrawler
from utility.ToolKit import ToolKit

# 主程序入口
if __name__ == "__main__":
    """爬取每日最新股票对应行业数据"""
    trade_date = ToolKit("获取最新美股交易日期").get_us_latest_trade_date(0)
    emi = EMUsTickerCategoryCrawler()
    emi.get_us_ticker_category(trade_date)

    """ 爬取每日最新股票对应行业数据 """
    trade_date_cn = ToolKit("获取最新A股交易日期").get_cn_latest_trade_date(0)
    emi_cn = EMCNTickerCategoryCrawler()
    emi_cn.get_cn_ticker_category(trade_date_cn)
