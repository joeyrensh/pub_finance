#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from utility.MyEmail import MyEmail
from utility.FileInfo import FileInfo
import pandas as pd
import seaborn as sns
import os
import sys
from numpy import size
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from matplotlib.pylab import mpl
import matplotlib.font_manager as fm
from utility.MyEmail import MyEmail
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

mpl.rcParams["font.sans-serif"] = ["SimHei"]  # 用来正常显示中文标签


class StockProposal:
    def __init__(self, market, trade_date):
        self.market = market
        self.trade_date = trade_date

    def send_strategy_df_by_email(self, dataframe):
        file = FileInfo(self.trade_date, self.market)
        file_name_industry = file.get_file_name_industry

        """ 匹配行业信息 """
        df_ind = pd.read_csv(file_name_industry, usecols=[i for i in range(1, 3)])
        df_n = pd.merge(dataframe, df_ind, how="left", on="symbol")
        df_n.sort_values(by=["chg", "amplitude"], ascending=False, inplace=True)
        df_n.reset_index(drop=True, inplace=True)
        df_n.rename(
            columns={
                "symbol": "股票代码",
                "close": "收盘价",
                "chg": "涨幅",
                "volume": "成交量",
                "amplitude": "振幅",
                "tag": "标记",
                "industry": "所属行业",
            },
            inplace=True,
        )
        cm = sns.color_palette("Blues", as_cmap=True)
        html = (
            df_n.style.hide(axis="index")
            .hide_columns(["标记", "成交量"])
            .format({"收盘价": "{:.2f}", "涨幅": "{:.2f}", "成交量": "{:.0f}", "振幅": "{:.2f}%"})
            .background_gradient(subset=["收盘价", "涨幅", "成交量", "振幅"], cmap=cm)
            .bar(
                subset=["涨幅"],
                align="left",
                color=["#5fba7d", "#d65f5f"],
                vmin=0,
                vmax=1,
            )
            .set_properties(
                **{
                    "text-align": "left",
                    "width": "auto",
                    "border": "1px solid",
                    "cellspacing": "0px",
                    "style": "border-collapse:collapse",
                }
            )
            .set_table_styles(
                [
                    dict(
                        selector="th",
                        props=[
                            ("text-align", "left"),
                            ("width", "auto"),
                            ("white-space", "nowrap"),
                            ("position", "fixed"),
                        ],
                    )
                ]
            )
            .set_sticky(axis="columns")
            .to_html(doctype_html=True)
        )
        subject = "美股波动"
        MyEmail().send_email(subject, html)

    def send_btstrategy_by_email(self):
        """发送邮件"""
        """ 取最新一天数据，获取股票名称 """
        file = FileInfo(self.trade_date, self.market)
        file_name_day = file.get_file_name_day
        df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
        """ 取仓位数据 """
        file_cur_p = file.get_file_name_position
        df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 8)])
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
                df_np.style.hide(axis="index")
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
                .set_properties(
                    **{
                        "text-align": "left",
                        "width": "auto",
                        "border": "1px solid",
                        "cellspacing": "0px",
                        "style": "border-collapse:collapse",
                    }
                )
                .set_table_styles(
                    [
                        dict(
                            selector="th",
                            props=[
                                ("text-align", "left"),
                                ("width", "auto"),
                                ("white-space", "nowrap"),
                                ("position", "fixed"),
                            ],
                        )
                    ]
                )
                .set_sticky(axis="columns")
                .to_html(doctype_html=True)
            )
            if self.market == "us":
                subject = "美股行情"
            elif self.market == "cn":
                subject = "A股行情"
            MyEmail().send_email(subject, html)

            """ 按照行业板块聚合，统计最近成交率最高的行业 """
            spark = (
                SparkSession.builder.master("local[1]")
                .appName("SparkTest")
                .getOrCreate()
            )
            df = spark.read.csv(file_cur_p, header=True)
            df.createOrReplaceTempView("temp")
            sqlDF = spark.sql(
                " select industry, cnt from ( \
                    select industry, count(*) as cnt from temp group by industry) \
                    order by cnt desc limit 20"
            )
            df_display = sqlDF.toPandas()
            # recent 1 month
            sqlDF_bydate = spark.sql(
                "select buy_date, count(*) as cnt from temp \
                    where buy_date >= date_add(current_date(), -100) \
                    group by buy_date order by buy_date "
            )
            df_displaybydate = sqlDF_bydate.toPandas()
            fig = px.pie(
                df_display,
                values="cnt",
                names="industry",
                title="Stock Positions by Industry",
                hover_data=["cnt"],
                labels={"Industry": "cnt"},
            )
            fig.update_traces(textposition="inside", textinfo="percent")
            fig.write_image("./postion_byindustry.png")

            fig = px.bar(
                df_displaybydate,
                x="buy_date",
                y="cnt",
                title="Stock Positions by Purchase Date",
                labels={"buy_date": "Purchase Date", "cnt": "Stock Sum"},
                color="cnt",
            )
            fig.write_image("./postion_bydate.png")

            if self.market == "us":
                subject = "美股行业行情"
                image_path = "./TRdraw.png"
            elif self.market == "cn":
                subject = "A股行业行情"
                image_path = "./CNTRdraw.png"
            image_path = [
                "./postion_byindustry.png",
                "./postion_bydate.png",
                "./CNTRdraw.png",
            ]
            html = ""
            MyEmail().send_email_embedded_image(subject, html, image_path)
