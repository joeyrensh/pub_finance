import base64
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
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from email.mime.text import MIMEText
import plotly.io as pio

# 读取图像文件并转换为 Base64 编码


file = FileInfo('20231227', 'cn')

spark = (
    SparkSession.builder.master("local[1]")
    .appName("SparkTest")
    .getOrCreate()
)
file_name_day = file.get_file_path_latest
file_cur_p = file.get_file_path_position
""" 按照行业板块聚合，统计最近成交率最高的行业 """
file_path_trade = file.get_file_path_trade
cols = ["idx", "symbol", "date", "trade_type"]
df1 = spark.read.csv(file_path_trade,
                     header=None, inferSchema=True)
df1 = df1.toDF(*cols)
df1.createOrReplaceTempView("temp1")
df = spark.read.csv(file_cur_p, header=True)
df.createOrReplaceTempView("temp")
# TOP15热门行业
sqlDF = spark.sql(
    " select industry, cnt from ( \
        select industry, count(*) as cnt from temp group by industry) \
        order by cnt desc limit 10 "
)
df_display = sqlDF.toPandas()
fig = go.Figure(data=[go.Pie(labels=df_display['industry'],
                             values=df_display['cnt'],
                             pull=0.2)]
                )
colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
fig.update_traces(marker=dict(colors=colors,
                              line=dict(color='#000000',
                                        width=1)),
                  textinfo='value+percent')
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

# TOP15盈利行业
sqlDF_asc = spark.sql(
    " select industry, pl from ( \
        select industry, sum(`p&l`) as pl from temp group by industry) \
        order by pl desc limit 10"
)
df_display_asc = sqlDF_asc.toPandas()
fig = go.Figure(data=[go.Pie(labels=df_display_asc['industry'],
                             values=df_display_asc['pl'],
                             pull=0.2)])
colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
fig.update_traces(marker=dict(colors=colors,
                              line=dict(color='#000000',
                                        width=1)
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
fig.write_image("./images/postion_byp&l.png",
                engine='kaleido')
# recent 100 days
sqlDF_bydate = spark.sql(
    "select buy_date, count(*) as cnt, \
        SUM(COUNT(*)) OVER (ORDER BY buy_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_cnt \
        from temp \
        where buy_date >= date_add(current_date(), -100) \
        group by buy_date order by buy_date "
)
df_displaybydate = sqlDF_bydate.toPandas()
fig = go.Figure()
fig.add_trace(go.Scatter(x=df_displaybydate['buy_date'],
                         y=df_displaybydate['total_cnt'],
                         mode='lines+markers',
                         name='total stock',
                         line=dict(color='blueviolet', width=2)))
fig.add_trace(go.Bar(x=df_displaybydate['buy_date'],
                     y=df_displaybydate['cnt'],
                     name='stock per day',
                     marker_color='red'))
fig.update_layout(title='Last 100 days Stock Position Distribution',
                  xaxis_title='Trade Date',
                  yaxis_title='Stock Positions',
                  legend=dict(
                      orientation="h",
                      yanchor="bottom",
                      y=-0.3,
                      xanchor="center",
                      x=0.5
                  ),
                  plot_bgcolor='white')
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
    " select date, trade_type, count(symbol) as cnt from temp1  \
        where date >= date_add(current_date(), -100) \
        group by date, trade_type order by date"
)
df1_display = sqlDF1.toPandas()
fig = px.bar(
    df1_display,
    # color_discrete_sequence=px.colors.sequential.RdBu,
    color='trade_type',
    x="date",
    y="cnt",
    title="Last 100 days trade details",
    labels={"date": "Trade Date", "cnt": "Trade Sum"}
)
fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=-0.3,
    xanchor="center",
    x=0.5), plot_bgcolor='white'
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
subject = "test"
html = ""
image_path = [
    "./images/postion_byindustry.png",
    "./images/postion_byp&l.png",
    "./images/postion_bydate.png",
    "./images/BuySell.png",
]
MyEmail().send_email_embedded_image(subject, html, image_path)
