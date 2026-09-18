#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os
import re
from finance import FINANCE_ROOT

""" 获取文件相关信息 """


class FileInfo:
    """
    market:
    美股：us
    A股：cn
    """

    def __init__(self, trade_date=None, market=None):
        # 市场编码
        decode_market = {
            "us_special": "us",
            "us_dynamic": "us",
            "cnetf": "cn",
            "cn_dynamic": "cn",
            "cn_backtest": "cn",
            "us_backtest": "us",
        }.get(market, market)
        self.market = market
        # 交易日期
        self.trade_date = trade_date
        # stock每日数据存储文件
        self._file_path_latest = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / f"stock_{trade_date}.csv"
        )
        # 10年期国债每日数据存储文件
        self._file_path_gz = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / f"gz_{trade_date}.csv"
        )
        # stock目录
        self._file_path_dir = FINANCE_ROOT / f"{decode_market}stockinfo"
        # talib策略文件
        self._file_path_sta = (
            FINANCE_ROOT / f"{decode_market}strategy" / f"strategy_{trade_date}.csv"
        )
        # stock行业文件
        self._file_path_industry = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / "industry.csv"
        )
        # 仓位日志文件
        self._file_path_position = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / f"{market}_position_logs.csv"
        )
        # 仓位明细文件
        self._file_path_position_detail = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / f"{market}_position_detail.csv"
        )
        # 交易日志文件
        self._file_path_trade = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / f"{market}_trade_logs.csv"
        )
        # 固定追踪股票列表
        self._file_path_fixed_list = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / "fixed_list.csv"
        )
        # 动态追踪股票列表
        self._file_path_dynamic_list = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / "dynamic_list.csv"
        )
        # 每日现金和total asset记录
        self._file_path_cash_asset = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / f"{market}_cash_asset.csv"
        )
        # 复权因子文件
        self._file_path_actions_history = (
            FINANCE_ROOT / f"{decode_market}stockinfo" / f"{decode_market}_stock_actions_history.csv"
        )        

    """ 返回某日数据文件路径 """

    @property
    def get_file_path_latest(self):
        return self._file_path_latest

    @property
    def get_file_path_gz(self):
        return self._file_path_gz

    """ 返回数据文件路径，用来查找数据文件列表 """

    @property
    def get_file_path(self):
        return self._file_path_dir

    """ 返回日数据文件列表，返回List """

    @property
    def get_file_list(self):
        """
        获取小于等于当前交易日期的合法 stock_*.csv 文件列表
        自动排除 .bk 备份文件、_new 缓存文件等非标准文件
        """
        file_list = []
        if not os.path.exists(self._file_path_dir):
            return file_list

        # 正则精准匹配: 以 stock_ 开头，中间 8 位数字日期，以 .csv 结尾
        pattern = re.compile(r"^stock_(\d{8})\.csv$")

        for file in os.listdir(self._file_path_dir):
            match = pattern.match(file)
            if match:
                file_date = match.group(1)
                # 校验日期范围并保留 Path 对象
                if file_date <= str(self.trade_date):
                    file_list.append(self._file_path_dir / file)

        file_list.sort()
        return file_list

    """ 返回日数据文件列表，返回List """

    @property
    def get_gz_file_list(self):
        path_list = os.listdir(self._file_path_dir)
        file_list = []
        for file in path_list:
            """返回小于等于当前交易日期的文件列表"""
            if (
                re.search("gz_", file)
                and str(file).replace("gz_", "").replace(".csv", "") <= self.trade_date
            ):
                file_list.append(self._file_path_dir / file)
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

    @property
    def get_file_path_fixed_list(self):
        return self._file_path_fixed_list

    @property
    def get_file_path_dynamic_list(self):
        return self._file_path_dynamic_list

    @property
    def get_file_path_cash_asset(self):
        return self._file_path_cash_asset

    @property
    def get_file_path_actions_history(self):
        return self._file_path_actions_history        
