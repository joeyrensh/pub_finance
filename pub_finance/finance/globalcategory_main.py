#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from uscrawler.eastmoney_tickercategory_crawler import EMUsTickerCategoryCrawler
from cncrawler.eastmoney_tickercategory_crawler import EMCNTickerCategoryCrawler
from finance.utility.toolkit import ToolKit

# 主程序入口
if __name__ == "__main__":
    """爬取每日最新股票对应行业数据"""
    trade_date_cn = ToolKit("获取最新A股交易日期").get_cn_latest_trade_date(0)
    emi_cn = EMCNTickerCategoryCrawler()
    # 旧版本，仅能获取二级行业分类
    # emi_cn.get_cn_ticker_category(trade_date_cn)
    # 新版本，获取一级/二级行业分类
    emi_cn.get_cn_ticker_sector_industry(trade_date_cn)
