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


file = FileInfo('20231225', 'cn')
file_name_day = file.get_file_path_latest
df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
""" 取仓位数据 """
file_cur_p = file.get_file_path_position
df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 8)])
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
            # "width": "auto"
        }
    )
    .set_table_styles(
        [
            dict(selector="th", props=[
                ("border", "5px solid #eee"),
                ("border-collapse", "collapse"),
                ("white-space", "nowrap"),
                ("color", "black")
                # ("width", "auto")
            ]),
            dict(selector="td", props=[
                ("border", "5px solid #eee"),
                ("border-collapse", "collapse"),
                ("white-space", "nowrap"),
                ("color", "black")
                # ("width", "auto")
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
image_path = [
    "./images/postion_byindustry.png"
]

subject = "testing"
MyEmail().send_email_embedded_image(subject, html, image_path)
