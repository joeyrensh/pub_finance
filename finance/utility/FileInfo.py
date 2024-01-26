#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os
import re

from utility.ToolKit import ToolKit

""" 获取文件相关信息 """


class FileInfo:

    """
    market:
    美股：us
    A股：cn
    """

    def __init__(self, trade_date=None, market=None):
        self.market = market
        self.trade_date = trade_date
        self._file_path_latest = (
            "./" + market + "stockinfo/stock_" + trade_date + ".csv"
        )
        self._file_path_dir = "./" + market + "stockinfo/"
        self._file_path_sta = "./" + market + "strategy/strategy_" + trade_date + ".csv"
        self._file_path_industry = "./" + market + "stockinfo/industry.csv"
        self._file_path_position = (
            "./" + market + "stockinfo/position_" + trade_date + ".csv"
        )
        self._file_path_position_detail = (
            "./" + market + "stockinfo/position_detail_" + trade_date + ".csv"
        )
        self._file_path_trade = "./" + market + "stockinfo/trade_" + trade_date + ".csv"

    """ 返回某日数据文件路径 """

    @property
    def get_file_path_latest(self):
        return self._file_path_latest

    """ 返回数据文件路径，用来查找数据文件列表 """

    @property
    def get_file_path(self):
        return self._file_path_dir

    """ 返回日数据文件列表，返回List """

    @property
    def get_file_list(self):
        path_list = os.listdir(self._file_path_dir)
        file_list = []
        for file in path_list:
            """返回小于等于当前交易日期的文件列表"""
            if (
                re.search("stock_", file)
                and str(file).replace("stock_", "").replace(".csv", "")
                <= self.trade_date
            ):
                file_list.append(self._file_path_dir + file)
        file_list.sort()
        return file_list

    """ 返回策略数据文件路径 """

    @property
    def get_file_path_sta(self):
        return self._file_path_sta

    """ 返回股票板块文件路径 """

    @property
    def get_file_path_industry(self):
        return self._file_path_industry

    """ 返回股票仓位文件路径 """

    @property
    def get_file_path_position(self):
        return self._file_path_position

    @property
    def get_file_path_position_detail(self):
        return self._file_path_position_detail

    @property
    def get_file_path_trade(self):
        return self._file_path_trade
