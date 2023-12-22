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


def read_image_file(image_path):
    with open(image_path, 'rb') as f:
        image_data = f.read()
    return base64.b64encode(image_data).decode('utf-8')


spark = (
    SparkSession.builder.master("local[1]")
    .appName("SparkTest")
    .getOrCreate()
)
file = FileInfo('20231222', 'cn')
file_path_trade = file.get_file_path_trade
cols = ["idx", "symbol", "date", "trade_type"]
df1 = spark.read.csv(file_path_trade,
                     header=None, inferSchema=True)
df1 = df1.toDF(*cols)
df1.createOrReplaceTempView("temp1")

sqlDF1 = spark.sql(
    " select date, trade_type, count(symbol) as cnt from temp1  \
        where date >= date_add(current_date(), -100) \
        group by date, trade_type order by date"
)
df1_display = sqlDF1.toPandas()
fig = px.bar(
    df1_display,
    color='trade_type',
    x="date",
    y="cnt",
    title="Last 100 days trade details",
    labels={"date": "Trade Date", "cnt": "Trade Sum"}
)
# fig.write_image("./images/BuySell.png")
fig_html = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')

# 创建 HTML 正文
html = fig_html
print(html)

subject = "test111"
# image_path = ["./images/BuySell.png"]
# 发送电子邮件
MyEmail().send_email(subject, html)
