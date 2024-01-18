#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

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
import matplotlib.colors as mcolors


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
        spark = SparkSession.builder.master(
            "local").appName("SparkTest").config("spark.driver.memory", "512m").config("spark.executor.memory", "512m").getOrCreate()
        # spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        """输出仓位表格"""
        file = FileInfo(self.trade_date, self.market)
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

            del df_np
            gc.collect()

            """ 按照行业板块聚合，统计最近成交率最高的行业 """
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

            # TOP10热门行业
            sparkdata1 = spark.sql(
                """ select industry, cnt from (
                    select industry, count(*) as cnt from temp group by industry)
                    order by cnt desc limit 10 """
            )
            # sqlDF.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
            dfdata1 = sparkdata1.toPandas()
            fig = go.Figure(data=[go.Pie(labels=dfdata1['industry'],
                                         values=dfdata1['cnt'],
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
            del dfdata1
            gc.collect()

            # TOP10盈利行业
            sparkdata2 = spark.sql(
                """ select industry, round(pl,2) as pl from ( 
                    select industry, sum(`p&l`) as pl from temp group by industry)
                    order by pl desc limit 10"""
            )
            # sparkdata2.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
            dfdata2 = sparkdata2.toPandas()
            fig = go.Figure(data=[go.Pie(
                labels=dfdata2['industry'], values=dfdata2['pl'], pull=0.2)])
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

            del dfdata2
            gc.collect()

            # recent 60 days
            sparkdata3 = spark.sql(
                """select buy_date, cnt, total_cnt 
                from ( 
                    select buy_date, count(*) as cnt,
                    SUM(COUNT(*)) OVER (ORDER BY buy_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_cnt
                    from temp
                    group by buy_date order by buy_date ) t
                where buy_date >= date_add(current_date(), -60)
                """
            )
            # sparkdata3.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
            dfdata3 = sparkdata3.toPandas()
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=dfdata3['buy_date'], y=dfdata3['total_cnt'],
                                     mode='lines+markers',
                                     name='total stock',
                                     line=dict(color='darkslateblue',
                                               width=2),
                                     yaxis='y'))
            fig.add_trace(go.Bar(x=dfdata3['buy_date'], y=dfdata3['cnt'],
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

            del dfdata3
            gc.collect()

            sparkdata4 = spark.sql(
                """ select date, trade_type, count(symbol) as cnt from temp1 
                    where date >= date_add(current_date(), -60)
                    group by date, trade_type order by date"""
            )
            # sparkdata4.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
            dfdata4 = sparkdata4.toPandas()
            fig = px.bar(
                dfdata4,
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

            del dfdata4
            gc.collect()

            # Top5行业仓位变化
            end_date = pd.to_datetime('today').strftime("%Y-%m-%d")
            start_date = pd.to_datetime(end_date) - pd.DateOffset(days=60)
            date_range = pd.date_range(
                start=start_date.strftime("%Y-%m-%d"), end=end_date, freq='D')
            df_timeseries = pd.DataFrame({'buy_date': date_range})
            df_timeseries_spark = spark.createDataFrame(
                df_timeseries.astype({'buy_date': 'string'}))
            df_timeseries_spark.createOrReplaceTempView("temp_timeseries")

            sparkdata5 = spark.sql(
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
                ), tmp3 as (select temp_timeseries.buy_date, tmp.industry, tmp.cnt
                            from temp_timeseries left join tmp
                            on 1=1
                )  select tmp3.buy_date, tmp3.industry, 
                        case when tmp2.total_cnt > 0 then tmp2.total_cnt
                            else last_value(tmp2.total_cnt) ignore nulls over (partition by tmp3.industry order by tmp3.buy_date)
                        end as total_cnt
                    from tmp3 left join tmp2
                    on tmp3.buy_date = tmp2.buy_date
                    and tmp3.industry = tmp2.industry
                    order by tmp3.cnt desc
                """
            )
            dfdata5 = sparkdata5.toPandas()
            fig = px.line(dfdata5,
                          x='buy_date',
                          y='total_cnt',
                          color='industry',
                          color_discrete_sequence=px.colors.qualitative.Plotly)
            fig.update_traces(line=dict(width=3))
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

            del dfdata5
            gc.collect()
            # P&L分析
            sparkdata6 = spark.sql(
                """with tmp as ( 
                    select industry, pl from ( 
                        select industry, sum(`p&l`) as pl from temp group by industry) 
                        order by pl desc limit 5 
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
                ), tmp3 as (select temp_timeseries.buy_date, tmp.industry, tmp.pl
                            from temp_timeseries left join tmp
                            on 1=1
                )  select tmp3.buy_date, tmp3.industry, 
                        case when tmp2.total_cnt > 0 then tmp2.total_cnt
                            else last_value(tmp2.total_cnt) ignore nulls over (partition by tmp3.industry order by tmp3.buy_date)
                        end as total_cnt
                    from tmp3 left join tmp2
                    on tmp3.buy_date = tmp2.buy_date
                    and tmp3.industry = tmp2.industry
                    order by tmp3.pl desc
                """
            )
            # sparkdata6.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
            dfdata6 = sparkdata6.toPandas()
            fig = px.line(dfdata6,
                          x='buy_date',
                          y='total_cnt',
                          color='industry',
                          color_discrete_sequence=px.colors.qualitative.Plotly)
            fig.update_traces(line=dict(width=3))
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
            del dfdata6
            gc.collect()

            """ 行业分析 """
            file_path_indus = file.get_file_path_industry
            df2 = spark.read.csv(file_path_indus, header=True)
            df2.coalesce(2)
            df2.createOrReplaceTempView("temp2")
            """ 行业盈亏统计分析 """
            sparkdata7 = spark.sql(
                """ with tmp as (select 
                                    industry
                                    ,sum(case when `p&l` >= 0 then 1 else 0 end) as pos_cnt
                                    ,sum(case when `p&l` < 0 then 1 else 0 end) as neg_cnt
                                    ,count(*) as p_cnt
                                    ,sum(`p&l`) as p_pnl
                                from temp 
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
                                ,count(distinct t2.symbol) as his_symbol_cnt
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
                    select 
                    t1.industry
                    ,t2.p_cnt
                    ,t2.p_pnl + t1.his_pnl as pnl
                    ,t1.his_trade_cnt / t1.his_symbol_cnt as avg_his_trade_cnt
                    ,(t1.his_days + t1.lastest_days) / t1.his_trade_cnt as avg_days
                    ,(t1.pos_cnt + COALESCE(t2.pos_cnt,0)) / (t1.pos_cnt + COALESCE(t2.pos_cnt,0) + t1.neg_cnt + COALESCE(t2.neg_cnt,0)) as pnl_ratio
                    from tmp2 t1 left join tmp t2
                    on t1.industry = t2.industry
                    order by t2.p_pnl+t1.his_pnl desc
                """
            )
            dfdata7 = sparkdata7.toPandas()

            dfdata7.rename(
                columns={
                    "industry": "行业",
                    "p_cnt": "当前持仓量",
                    "pnl": "盈亏金额",
                    "avg_his_trade_cnt": "平均交易次数",
                    "avg_days": "平均持仓天数",
                    "pnl_ratio": "盈亏比"
                },
                inplace=True,
            )
            cm = sns.color_palette("Wistia", as_cmap=True)
            html1 = (
                dfdata7.style
                .format({"当前持仓量": "{:.2f}", "盈亏金额": "{:.2f}",
                         "平均交易次数": "{:.0f}", "平均持仓天数": "{:.2f}", "盈亏比": "{:.2f}"})
                .background_gradient(subset=["盈亏金额", "当前持仓量"], cmap=cm)
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
            del dfdata7
            gc.collect()

            spark.stop()

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
            MyEmail().send_email_embedded_image(subject, html1 + html, image_path)
