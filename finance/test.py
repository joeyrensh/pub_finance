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
import pandas as pd
import plotly.graph_objects as go

# 假设数据存储在 DataFrame 中，包含 'timestamp' 和 'value' 列
df = pd.DataFrame({'date': ['2023-01-01', '2023-01-02', '2023-01-04'],
                   'value': [10, 15, None]})

# df.sort_values(by='date', inplace=True)
# df.ffill(inplace=True)
# print(df)
date_range = pd.date_range(
    start='2023-01-01', end=pd.to_datetime('today').strftime("%Y-%m-%d"), freq='D')
df12 = pd.DataFrame({'buy_date': date_range})
spark = (
    SparkSession.builder.master("local[1]")
    .appName("SparkTest")
    .getOrCreate()
)
"""输出仓位表格"""
file = FileInfo('20231229', 'cn')
file_name_day = file.get_file_path_latest
df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
""" 取仓位数据 """
file_cur_p = file.get_file_path_position
df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 8)])
df = spark.read.csv(file_cur_p, header=True)
df.createOrReplaceTempView("temp")
sqlDF_bydate2 = spark.sql(
    "with tmp as ( \
        select industry, pl from ( \
            select industry, sum(`p&l`) as pl from temp group by industry) \
            order by pl desc limit 5 \
    ), tmp1 as ( select buy_date, industry, total_cnt \
                from ( \
                    select buy_date, industry, \
                    SUM(COUNT(*)) OVER (partition by industry ORDER BY buy_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_cnt \
                    from temp \
                    group by buy_date, industry order by buy_date ) t \
                where buy_date >= date_add(current_date(), -60) \
    )  select tmp.industry, tmp1.buy_date, tmp1.total_cnt \
    from tmp left join tmp1 \
    on tmp.industry = tmp1.industry \
    "
)
joined_df = sqlDF_bydate2.toPandas()
fig = px.area(joined_df,
              x='buy_date',
              y='total_cnt',
              facet_col='industry',
              facet_col_wrap=2
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
fig.write_image(
    "./images/postion_byindustry&p&l.png", engine='kaleido')
subject = 'test'
html = ''
image_path = [
    "./images/postion_byindustry&p&l.png",
]
MyEmail().send_email_embedded_image(subject, html, image_path)

# where buy_date >= date_add(current_date(), -60) \

# df12_spark = spark.createDataFrame(df12.astype({'buy_date': 'string'}))
# df12_spark.createOrReplaceTempView("df12_view")
# sqlDF_bydate2.createOrReplaceTempView("sqlDF_bydate2_view")
# # Perform the join operation using Spark SQL
# joined_df = spark.sql("""
#     SELECT df12.buy_date, sqlDF_bydate2.industry, sqlDF_bydate2.total_cnt
#     FROM df12_view AS df12
#     LEFT JOIN sqlDF_bydate2_view AS sqlDF_bydate2 ON df12.buy_date = sqlDF_bydate2.buy_date
# """)
# joined_df1 = joined_df.toPandas()
# # joined_df1.sort_values(by=['industry', 'buy_date'])
# # joined_df1.ffill(inplace=True)

# joined_df1 = joined_df.infer_objects(copy=False)

# fig = px.line(joined_df,
#               x='buy_date',
#               y='total_cnt',
#               color='industry',
#               color_discrete_sequence=px.colors.qualitative.Plotly)
# fig.update_traces(line=dict(width=2), connectgaps=True)
# fig.update_xaxes(
#     mirror=True,
#     ticks='outside',
#     showline=True,
#     linecolor='black',
#     gridcolor='lightgrey'
# )
# fig.update_yaxes(
#     mirror=True,
#     ticks='outside',
#     showline=True,
#     linecolor='black',
#     gridcolor='lightgrey'
# )
# fig.update_layout(title='Last 60 days Industry Position Distribution P&L',
#                   legend=dict(
#                         orientation="h",
#                         yanchor="bottom",
#                         y=-0.5,
#                         xanchor="left",
#                         x=0
#                   ),
#                   plot_bgcolor='white',
#                   )
# fig.write_image(
#     "./images/postion_byindustry&p&l.png", engine='kaleido')
# subject = 'test'
# html = ''
# image_path = [
#     "./images/postion_byindustry&p&l.png",
# ]
# MyEmail().send_email_embedded_image(subject, html, image_path)
