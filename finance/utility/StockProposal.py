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
from email.mime.text import MIMEText


mpl.rcParams["font.sans-serif"] = ["SimHei"]  # 用来正常显示中文标签


class StockProposal:
    def __init__(self, market, trade_date):
        self.market = market
        self.trade_date = trade_date

    def send_strategy_df_by_email(self, dataframe):
        file = FileInfo(self.trade_date, self.market)
        file_name_industry = file.get_file_path_industry

        """ 匹配行业信息 """
        df_ind = pd.read_csv(file_name_industry, usecols=[
                             i for i in range(1, 3)])
        df_n = pd.merge(dataframe, df_ind, how="left", on="symbol")
        df_n.sort_values(by=["chg", "amplitude"],
                         ascending=False, inplace=True)
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
            df_n.style
            .hide(axis=1, subset=["标记", "成交量"])
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
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("SparkTest")
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
            .getOrCreate()
        )
        """输出仓位表格"""
        file = FileInfo(self.trade_date, self.market)
        file_name_day = file.get_file_path_latest
        df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
        """ 取仓位数据 """
        file_cur_p = file.get_file_path_position
        df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 8)])
        if not df_cur_p.empty:
            df_np = pd.merge(df_cur_p, df_d, how="inner", on="symbol")
            df_np = df_np[df_np['p&l_ratio'] > 0].reset_index(drop=True)
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

            """ 按照行业板块聚合，统计最近成交率最高的行业 """
            file_path_trade = file.get_file_path_trade
            cols = ["idx", "symbol", "date", "trade_type"]
            df1 = spark.read.csv(file_path_trade,
                                 header=None, inferSchema=True)
            df1 = df1.toDF(*cols)
            df1.createOrReplaceTempView("temp1")
            df = spark.read.csv(file_cur_p, header=True)
            df.createOrReplaceTempView("temp")
            # TOP10热门行业
            sqlDF = spark.sql(
                """ select industry, cnt from (
                    select industry, count(*) as cnt from temp group by industry)
                    order by cnt desc limit 10 """
            )
            df_display = sqlDF.toPandas()
            fig = go.Figure(data=[go.Pie(labels=df_display['industry'],
                                         values=df_display['cnt'],
                                         pull=0.2)]
                            )
            colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
            fig.update_traces(marker=dict(colors=colors,
                                          line=dict(color='#000000', width=1)),
                              textinfo='value+percent'
                              )
            fig.update_layout(title='Top 10 Stock Position Industry',
                              legend=dict(
                                  orientation="h",
                                  yanchor="bottom",
                                  xanchor="center",
                                  x=0.5,
                                  y=-0.5
                              ),
                              margin=dict(t=50, b=0.2, l=0.2, r=0.2)
                              )
            fig.write_image("./images/postion_byindustry.png",
                            engine='kaleido')

            # TOP10盈利行业
            sqlDF_asc = spark.sql(
                """ select industry, round(pl,2) as pl from ( 
                    select industry, sum(`p&l`) as pl from temp group by industry)
                    order by pl desc limit 10"""
            )
            df_display_asc = sqlDF_asc.toPandas()
            fig = go.Figure(data=[go.Pie(
                labels=df_display_asc['industry'], values=df_display_asc['pl'], pull=0.2)])
            colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
            fig.update_traces(marker=dict(colors=colors,
                                          line=dict(color='#000000', width=1)
                                          ),
                              textinfo='value+percent')
            fig.update_layout(title='Top 10 Profit Industry',
                              legend=dict(
                                  orientation="h",
                                  yanchor="bottom",
                                  xanchor="center",
                                  x=0.5,
                                  y=-0.5
                              ),
                              margin=dict(t=50, b=0.2, l=0.2, r=0.2))
            fig.write_image("./images/postion_byp&l.png", engine='kaleido')
            # recent 60 days
            sqlDF_bydate = spark.sql(
                """select buy_date, cnt, total_cnt 
                from ( 
                    select buy_date, count(*) as cnt,
                    SUM(COUNT(*)) OVER (ORDER BY buy_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_cnt
                    from temp
                    group by buy_date order by buy_date ) t
                where buy_date >= date_add(current_date(), -60)
                """
            )
            df_displaybydate = sqlDF_bydate.toPandas()
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df_displaybydate['buy_date'], y=df_displaybydate['total_cnt'],
                                     mode='lines+markers',
                                     name='total stock',
                                     line=dict(color='darkslateblue',
                                               width=2),
                                     yaxis='y'))
            fig.add_trace(go.Bar(x=df_displaybydate['buy_date'], y=df_displaybydate['cnt'],
                                 name='stock per day',
                                 marker_color='darkorange',
                                 yaxis='y2'))
            fig.update_layout(title='Last 60 days Stock Position Distribution',
                              xaxis_title='Trade Date',
                              yaxis_title='Stock Positions',
                              legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=-0.3,
                                    xanchor="center",
                                    x=0.5
                              ),
                              plot_bgcolor='white',
                              yaxis=dict(title='Total Positions', side='left'),
                              yaxis2=dict(title='Positions per day', side='right', overlaying='y', showgrid=False))
            fig.update_xaxes(
                mirror=True,
                ticks='outside',
                showline=True,
                linecolor='black',
                gridcolor='lightgrey'
            )
            fig.update_yaxes(
                mirror=True,
                ticks='outside',
                showline=True,
                linecolor='black',
                gridcolor='lightgrey'
            )
            fig.write_image("./images/postion_bydate.png", engine='kaleido')

            sqlDF1 = spark.sql(
                """ select date, trade_type, count(symbol) as cnt from temp1 
                    where date >= date_add(current_date(), -60)
                    group by date, trade_type order by date"""
            )
            df1_display = sqlDF1.toPandas()
            fig = px.bar(
                df1_display,
                color='trade_type',
                x="date",
                y="cnt",
                title="Last 60 days trade details",
                labels={"date": "Trade Date", "cnt": "Trade Sum"}
            )
            fig.update_layout(legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5),
                plot_bgcolor='white'
            )
            fig.update_xaxes(
                mirror=True,
                ticks='outside',
                showline=True,
                linecolor='black',
                gridcolor='lightgrey'
            )
            fig.update_yaxes(
                mirror=True,
                ticks='outside',
                showline=True,
                linecolor='black',
                gridcolor='lightgrey'
            )
            fig.write_image("./images/BuySell.png", engine='kaleido')
            # Top5行业仓位变化
            date_range = pd.date_range(
                start='2023-01-01', end=pd.to_datetime('today').strftime("%Y-%m-%d"), freq='D')
            df_timeseries = pd.DataFrame({'buy_date': date_range})
            df_timeseries_spark = spark.createDataFrame(
                df_timeseries.astype({'buy_date': 'string'}))
            df_timeseries_spark.createOrReplaceTempView("temp_timeseries")
            sqlDF_bydate1 = spark.sql(
                """with tmp as (
                    select industry, cnt from (
                        select industry, count(*) as cnt from temp group by industry) t
                        order by cnt desc limit 5
                ), tmp1 as ( select buy_date, industry, total_cnt
                from (
                    select buy_date, industry,
                            SUM(COUNT(*)) OVER (partition by industry ORDER BY buy_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_cnt
                    from temp
                    group by buy_date, industry order by buy_date ) t
                    where buy_date >= date_add(current_date(), -60) 
                ), tmp2 as (select tmp.industry, tmp1.buy_date, tmp1.total_cnt 
                            from tmp left join tmp1 
                            on tmp.industry = tmp1.industry
                ), tmp3 as (select temp_timeseries.buy_date, t1.industry
                            from temp_timeseries left join (select industry from tmp) t1
                            on 1=1
                )  select tmp3.buy_date, tmp3.industry, 
                        case when tmp2.total_cnt > 0 then tmp2.total_cnt
                            else last_value(tmp2.total_cnt) ignore nulls over (partition by tmp3.industry order by tmp3.buy_date)
                        end as total_cnt
                    from tmp3 left join tmp2
                    on tmp3.buy_date = tmp2.buy_date
                    and tmp3.industry = tmp2.industry
                    left join tmp
                    on tmp3.industry = tmp.industry
                    where tmp3.buy_date >= date_add(current_date(), -60)
                    order by tmp.cnt desc
                """
            )
            df_displaybydate1 = sqlDF_bydate1.toPandas()
            fig = px.line(df_displaybydate1,
                          x='buy_date',
                          y='total_cnt',
                          color='industry',
                          color_discrete_sequence=px.colors.qualitative.Plotly)
            fig.update_traces(line=dict(width=2))
            fig.update_xaxes(
                mirror=True,
                ticks='outside',
                showline=True,
                linecolor='black',
                gridcolor='lightgrey'
            )
            fig.update_yaxes(
                mirror=True,
                ticks='outside',
                showline=True,
                linecolor='black',
                gridcolor='lightgrey'
            )
            fig.update_layout(title='Last 60 days Industry Position Distribution No',
                              legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=-0.5,
                                    xanchor="left",
                                    x=0
                              ),
                              plot_bgcolor='white',
                              )
            fig.write_image(
                "./images/postion_byindustry&date.png", engine='kaleido')
            # P&L分析
            sqlDF_bydate2 = spark.sql(
                """with tmp as ( \
                    select industry, pl from ( \
                        select industry, sum(`p&l`) as pl from temp group by industry) \
                        order by pl desc limit 5 \
                ), tmp1 as ( select buy_date, industry, total_cnt
                from (
                    select buy_date, industry,
                            SUM(COUNT(*)) OVER (partition by industry ORDER BY buy_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_cnt
                    from temp
                    group by buy_date, industry order by buy_date ) t
                    where buy_date >= date_add(current_date(), -60) 
                ), tmp2 as (select tmp.industry, tmp1.buy_date, tmp1.total_cnt 
                            from tmp left join tmp1 
                            on tmp.industry = tmp1.industry
                ), tmp3 as (select temp_timeseries.buy_date, t1.industry
                            from temp_timeseries left join (select industry from tmp) t1
                            on 1=1
                )  select tmp3.buy_date, tmp3.industry, 
                        case when tmp2.total_cnt > 0 then tmp2.total_cnt
                            else last_value(tmp2.total_cnt) ignore nulls over (partition by tmp3.industry order by tmp3.buy_date)
                        end as total_cnt
                    from tmp3 left join tmp2
                    on tmp3.buy_date = tmp2.buy_date
                    and tmp3.industry = tmp2.industry
                    left join tmp
                    on tmp3.industry = tmp.industry
                    where tmp3.buy_date >= date_add(current_date(), -60)
                    order by tmp.pl desc
                """
            )
            df_displaybydate2 = sqlDF_bydate2.toPandas()
            fig = px.line(df_displaybydate2,
                          x='buy_date',
                          y='total_cnt',
                          color='industry',
                          color_discrete_sequence=px.colors.qualitative.Plotly)
            fig.update_traces(line=dict(width=2))
            fig.update_xaxes(
                mirror=True,
                ticks='outside',
                showline=True,
                linecolor='black',
                gridcolor='lightgrey'
            )
            fig.update_yaxes(
                mirror=True,
                ticks='outside',
                showline=True,
                linecolor='black',
                gridcolor='lightgrey'
            )
            fig.update_layout(title='Last 60 days Industry Position Distribution P&L',
                              legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=-0.5,
                                    xanchor="left",
                                    x=0
                              ),
                              plot_bgcolor='white',
                              )
            fig.write_image(
                "./images/postion_byindustry&p&l.png", engine='kaleido')
            if self.market == "us":
                subject = "美股行情分析"
                image_path_return = "./images/TRdraw.png"
            elif self.market == "cn":
                subject = "A股行情分析"
                image_path_return = "./images/CNTRdraw.png"
            image_path = [
                "./images/postion_byindustry.png",
                "./images/postion_byp&l.png",
                "./images/postion_bydate.png",
                "./images/BuySell.png",
                "./images/postion_byindustry&date.png",
                "./images/postion_byindustry&p&l.png",
                image_path_return,
            ]
            MyEmail().send_email_embedded_image(subject, html, image_path)
