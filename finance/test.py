#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import matplotlib.colors as mcolors
from pyspark import StorageLevel
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
from email.mime.text import MIMEText
import gc

spark = SparkSession.builder.master(
    "local").appName("SparkTest").config("spark.driver.memory", "512m").config("spark.executor.memory", "512m").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

file = FileInfo('20240116', 'cn')
file_name_industry = file.get_file_path_industry

file_name_day = file.get_file_path_latest
df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
""" 取仓位数据 """
file_cur_p = file.get_file_path_position
df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 8)])
if not df_cur_p.empty:
    df_np = pd.merge(df_cur_p, df_d, how="inner", on="symbol")
    # df_np = df_np[df_np['p&l_ratio'] > 0].reset_index(drop=True)
    df_np = df_np.reset_index(drop=True)
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
        df_np.style
        .hide(axis=1, subset=["收益金额"])
        .format({"买入价": "{:.2f}", "当前价": "{:.2f}", "收益率": "{:.2f}"})
        .background_gradient(subset=["买入价", "当前价"], cmap=cm)
        .bar(
            subset=["收益率"],
            align="left",
            color=["#5fba7d", "#d65f5f"],
            vmin=-1,
            vmax=1,
        )
        .set_properties(
            **{
                "text-align": "left",
                "border": "1px solid",
                "cellspacing": "0px",
                "style": "border-collapse:collapse;",
            }
        )
        .set_table_styles(
            [
                dict(selector="th", props=[
                    ("border", "5px solid #eee"),
                    ("border-collapse", "collapse"),
                    ("white-space", "nowrap"),
                    ("color", "black")
                ]),
                dict(selector="td", props=[
                    ("border", "5px solid #eee"),
                    ("border-collapse", "collapse"),
                    ("white-space", "nowrap"),
                    ("color", "black")
                ]),
            ],
        )
        .set_sticky(axis="columns")
        .to_html(doctype_html=True)
    )
    css = """
    <style>
        :root {
            color-scheme: light;
            supported-color-schemes: light;
            bgcolor: "#000000" !important;
            color: black;
            display: table !important;        
        }
        @media (prefers-color-scheme: light) {
            /* Your light mode (default) styles: */
            body {
                bgcolor: "#000000" !important;
                color: black;
                display: table !important;
            }
            table {
                bgcolor: "#000000" !important;
                color: black;
            }
        }

        @media (prefers-color-scheme: dark) {
            /* Your dark mode styles: */
            body {
                bgcolor: "#000000" !important;
                color: black;
                display: table !important;
            }
            table {
                bgcolor: "#000000" !important;
                color: black;
            }
        }
    </style>
    """
    html = css + html


file_path_trade = file.get_file_path_trade
cols = ["idx", "symbol", "date", "trade_type", "price", "size"]

df1 = spark.read.csv(file_path_trade,
                     header=None, inferSchema=True)
df1.coalesce(2)
df1 = df1.toDF(*cols)
df1.createOrReplaceTempView("temp1")
df = spark.read.csv(file_cur_p, header=True)
df.coalesce(2)
df.createOrReplaceTempView("temp")
file_path_indus = file.get_file_path_industry
df2 = spark.read.csv(file_path_indus, header=True)
df2.coalesce(2)
df2.createOrReplaceTempView("temp2")
""" 行业盈亏统计分析 """
sparkdata7 = spark.sql(
    """ with tmp as (select industry, count(*) as p_cnt, sum(`p&l`) as p_pnl from temp 
                    where buy_date >= date_add(current_date(), -365)
                    group by industry)
        ,tmp1 as (
                select symbol, date, trade_type, price, size, l_date, l_trade_type, l_price, l_size
                from (
                    select symbol, date, trade_type, price, size
                    ,case when trade_type = 'sell' then lag(date) over (partition by symbol order by date)  
                            else lead(date) over (partition by symbol order by date) end as l_date
                    ,case when trade_type = 'sell' then lag(trade_type) over (partition by symbol order by date) 
                            else lead(trade_type) over (partition by symbol order by date) end as l_trade_type
                    ,case when trade_type = 'sell' then lag(price) over (partition by symbol order by date)
                            else lead(price) over (partition by symbol order by date) end as l_price
                    ,case when trade_type = 'sell' then lag(size) over (partition by symbol order by date) 
                            else lead(size) over (partition by symbol order by date) end as l_size
                    from temp1
                    where date >= date_add(current_date(), -365)
                    order by symbol, date, trade_type
                    ) t
                )
        ,tmp2 as (
                select t1.industry
                ,count(t2.symbol) as his_trade_cnt
                ,sum(datediff(t3.date, t3.l_date)) as his_days
                ,sum(case when t2.l_date is null then datediff(current_date(), t2.date) else 0 end) as lastest_days
                ,sum(case when t3.price - t3.l_price >=0 then 1 else 0 end) as pos_cnt
                ,sum(case when t3.price - t3.l_price < 0 then 1 else 0 end) as neg_cnt
                ,sum(t3.price * (-t3.size) - t3.l_price * t3.l_size) as his_pnl
                from temp2 as t1
                join (select * from tmp1 where trade_type = 'buy' ) as t2
                on t1.symbol = t2.symbol
                join (select * from tmp1 where trade_type = 'sell') as t3
                on t1.symbol = t3.symbol
                group by t1.industry
                )                
        select t1.industry, t2.p_cnt, t2.p_pnl+t1.his_pnl as pnl
        ,t1.his_trade_cnt
        ,(t1.his_days + t1.lastest_days) / t1.his_trade_cnt as avg_days
        ,t1.pos_cnt / (t1.pos_cnt + t1.neg_cnt) as pnl_ratio
        from tmp2 t1 left join tmp t2
        on t1.industry = t2.industry
        order by t2.p_cnt desc
    """
)
dfdata7 = sparkdata7.toPandas()

dfdata7.rename(
    columns={
        "industry": "行业",
        "p_cnt": "当前持仓量",
        "pnl": "盈亏金额",
        "his_trade_cnt": "历史交易次数",
        "avg_days": "平均持仓天数",
        "pnl_ratio": "盈亏比"
    },
    inplace=True,
)
# cm = sns.color_palette("Blues", as_cmap=True)
# 创建稍微调暗的浅色系 colormap
colors = ["#FFD700", "#FFA500", "#FF8C00", "#FF4500"]  # 自定义颜色列表
darkened_colors = mcolors.LinearSegmentedColormap.from_list(
    "darkened_colors", colors, N=256)  # 调暗颜色
html1 = (
    dfdata7.style
    .format({"当前持仓量": "{:.2f}", "盈亏金额": "{:.2f}", "平均持仓天数": "{:.2f}", "盈亏比": "{:.2f}"})
    .background_gradient(subset=["盈亏金额", "当前持仓量"], cmap=darkened_colors)
    .bar(
        subset=["盈亏比"],
        align="left",
        color=["#5fba7d", "#d65f5f"],
        vmin=0,
        vmax=1,
    )
    .set_properties(
        **{
            "text-align": "left",
            "border": "1px solid",
            "cellspacing": "0px",
            "style": "border-collapse:collapse;",
        }
    )
    .set_table_styles(
        [
            dict(selector="th", props=[
                ("border", "5px solid #eee"),
                ("border-collapse", "collapse"),
                ("white-space", "nowrap"),
                ("color", "black")
            ]),
            dict(selector="td", props=[
                ("border", "5px solid #eee"),
                ("border-collapse", "collapse"),
                ("white-space", "nowrap"),
                ("color", "black")
            ]),
        ],
    )
    .set_sticky(axis="columns")
    .to_html(doctype_html=True)
)
css1 = """
<style>
    :root {
        color-scheme: light;
        supported-color-schemes: light;
        bgcolor: "#000000" !important;
        color: black;
        display: table !important;        
    }
    @media (prefers-color-scheme: light) {
        /* Your light mode (default) styles: */
        body {
            bgcolor: "#000000" !important;
            color: black;
            display: table !important;
        }
        table {
            bgcolor: "#000000" !important;
            color: black;
        }
    }

    @media (prefers-color-scheme: dark) {
        /* Your dark mode styles: */
        body {
            bgcolor: "#000000" !important;
            color: black;
            display: table !important;
        }
        table {
            bgcolor: "#000000" !important;
            color: black;
        }
    }
</style>
"""
html1 = css1 + html1

subject = 'test'
image_path = [
    "./images/postion_byindustry.png",
]
MyEmail().send_email_embedded_image(subject, html1 + html, image_path)
