#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import email
from utility.FileInfo import FileInfo
from tabulate import tabulate
from utility.ToolKit import ToolKit
from utility.MyEmail import MyEmail
from datetime import datetime, timedelta
import pandas as pd
import seaborn as sns
from backtraderref.BTStrategy import BTStrategy
import backtrader as bt
from utility.TickerInfo import TickerInfo
from backtraderref.BTPandasDataExt import BTPandasDataExt
import numpy as np
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import sys
from utility.StockProposal import StockProposal

def send_btstrategy_by_email(trade_date, market):
    """发送邮件"""
    """ 取最新一天数据，获取股票名称 """
    file = FileInfo(trade_date, market)
    file_name_day = file.get_file_name_day
    df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
    """ 取仓位数据 """
    file_cur_p = file.get_file_name_position
    df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 8)])
    pre_file_cur_p = file.get_pre_file_name_position
    df_pre_p = pd.read_csv(pre_file_cur_p, usecols=[i for i in range(1, 8)])
    if not df_cur_p.empty:
        df_np = pd.merge(df_cur_p, df_d, how="inner", on="symbol")
        df_np.rename(
            columns={
                "symbol": "股票代码",
                "buy_date": "策略命中时间",
                "price": "买入价",
                "adjbase": "当前价",
                "p&l": "收益金额",
                "p&l_ratio": "收益率",
                "industry": "所属行业",
                "name": "公司名称",
            },
            inplace=True,
        )
        cm = sns.color_palette("Blues", as_cmap=True)
        html = (
            df_np.style.hide(axis=0)
            .hide_columns(["收益金额"])
            .format({"买入价": "{:.2f}", "当前价": "{:.2f}", "收益率": "{:.2f}"})
            .background_gradient(subset=["买入价", "当前价"], cmap=cm)
            .bar(
                subset=["收益率"],
                align="left",
                color=["#5fba7d", "#d65f5f"],
                vmin=0,
                vmax=1,
            )
            .set_properties(**{"text-align": "left", "width": "auto", "border": "1px solid", "cellspacing": "0px", "style": "border-collapse:collapse"})
            .set_table_styles(
                [dict(selector="th", props=[("text-align", "left"), ("width", "auto"), ("white-space", "nowrap"), ("position", "fixed")])]
            )
            .set_sticky(axis="columns")
            .to_html()
        )
        if market == "us":
            subject = "美股行情"
        elif market == "cn":
            subject = "A股行情"
        MyEmail().send_email(subject, html)
        """ 按照行业板块聚合，统计最近成交率最高的行业 """
        df_sum = df_np.groupby(by="所属行业").size().reset_index(name="今日策略")
        df_sum.sort_values(by=["今日策略"], ascending=False, inplace=True)
        df_sum.reset_index(drop=True, inplace=True)
        """ 取上一交易日的行业板块聚合 """
        df_sum_pre = (
            df_pre_p.groupby(by="industry").size().reset_index(name="pre_count")
        )
        df_sum_pre.sort_values(by=["pre_count"], ascending=False, inplace=True)
        df_sum_pre.reset_index(drop=True, inplace=True)
        df_sum_pre.rename(
            columns={"industry": "所属行业", "pre_count": "昨日策略"}, inplace=True
        )
        df_sum_np = pd.merge(df_sum, df_sum_pre, how="left", on="所属行业")
        df_sum_np["变化比"] = (df_sum_np["今日策略"] - df_sum_np["昨日策略"]) / df_sum_np[
            "昨日策略"
        ]
        """ 发送邮件 """
        cm = sns.color_palette("Blues", as_cmap=True)
        html = (
            df_sum_np.style.hide(axis="index")
            .format({"今日策略": "{:.0f}", "昨日策略": "{:.0f}", "变化比": "{:.2f}"})
            .background_gradient(subset=["今日策略", "昨日策略"], cmap=cm)
            .bar(
                subset=["变化比"],
                align="left",
                color=["#5fba7d", "#d65f5f"],
                vmin=-1,
                vmax=1,
            )
            .set_properties(**{"text-align": "left", "width": "auto", "border": "1px solid", "cellspacing": "0px", "style": "border-collapse:collapse"})
            .set_table_styles(
                [dict(selector="th", props=[("text-align", "left"), ("width", "auto"), ("white-space", "nowrap"), ("position", "fixed")])]
            )
            .set_sticky(axis="columns")
            .to_html()
        )
        if market == "us":
            subject = "美股行业行情"
            image_path = './TRdraw.png'
        elif market == "cn":
            subject = "A股行业行情"
            image_path = './CNTRdraw.png'
        # MyEmail().send_email(subject, html)
        MyEmail().send_email_embedded_image(subject, html, image_path)        

# 主程序入口
if __name__ == "__main__":
    trade_date = ToolKit("获取最新A股交易日期").get_cn_latest_trade_date(1)
    send_btstrategy_by_email(trade_date, 'cn')
    """ 发送邮件 """
    # StockProposal("us", '20220927').send_btstrategy_by_email()  
    # print(os.path.abspath(sys.argv[0]))