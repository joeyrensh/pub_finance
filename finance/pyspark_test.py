from numpy import size
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.pylab import mpl
import matplotlib.font_manager as fm
from utility.MyEmail import MyEmail
import pandas_bokeh
import plotly.graph_objects as go
import plotly.express as px
from utility.StockProposal import StockProposal

# mpl.rcParams["font.sans-serif"] = ["SimHei"]  # 用来正常显示中文标签

# Create SparkSession
# spark = SparkSession.builder.master("local[1]").appName("SparkTest").getOrCreate()

# performance test
# df = spark.read.csv("./usstockinfo/stock_20210630.csv", header=True)
# df.createOrReplaceTempView("temp")
# sqlDF = spark.sql(
#     "select count(*) as cnt from temp"
# )
# sqlDF.show()

# line chart test
# stock_20210630.csv
# df = spark.read.csv("./usstockinfo/position_20221010.csv", header=True)
# df.createOrReplaceTempView("temp")
# sqlDF = spark.sql(
#     "select buy_date, industry, count(*) as cnt from temp group by buy_date, industry"
# )
# sqlDF.createOrReplaceTempView("temp_latest")
# sqlDF1 = spark.sql(
#     "select * from temp_latest where buy_date = (select max(buy_date) from temp_latest) order by cnt desc limit 3"
# )
# sqlDF1.createOrReplaceTempView("temp_lastday")
# sqlDF2 = spark.sql(
#     "select a.industry, coalesce(b.cnt, 0, b.cnt) as cnt, b.buy_date from temp_lastday a left outer join temp_latest b on a.industry = b.industry order by b.buy_date\
#                 "
# )
# df1 = sqlDF2.toPandas().set_index("buy_date")

# sqlDF3 = spark.sql("select distinct industry from temp_lastday")
# df2 = sqlDF3.toPandas()
# col1 = df2._get_value(0, "industry")
# col2 = df2._get_value(1, "industry")
# col3 = df2._get_value(2, "industry")
# df2 = df1.groupby("industry").get_group(col1)
# df3 = df1.groupby("industry").get_group(col2)
# df4 = df1.groupby("industry").get_group(col3)
# df5 = pd.DataFrame(
#     {col1: df2["cnt"], col2: df3["cnt"], col3: df4["cnt"]}).fillna(0)
# # print(df5)
# df5.plot.line()
# plt.savefig("./pyspark_test.png")
# subject = "this is a test"
# html = ""
# image_path = "./pyspark_test.png"


# # pie chart test
# # stock_20210630.csv
# df = spark.read.csv("./usstockinfo/position_20221010.csv", header=True)
# df.createOrReplaceTempView("temp")
# sqlDF = spark.sql(
#     " select industry, cnt from ( \
#         select industry, count(*) as cnt from temp group by industry) \
#         order by cnt desc limit 10"
# )
# df_display = sqlDF.toPandas()
# sqlDF_bydate = spark.sql(
#     " select buy_date, cnt from ( \
#         select buy_date, count(*) as cnt from temp group by buy_date) \
#         order by cnt desc limit 10"
# )
# df_displaybydate = sqlDF_bydate.toPandas()
# # pandas_bokeh.output_notebook()
# # df_display.plot_bokeh(
# #     kind="pie", x="industry", y="cnt", xlabel="Industry", ylabel="Stock cnt"
# # )
# # plotting the pie chart

# fig = px.pie(
#     df_display,
#     values="cnt",
#     names="industry",
#     title="Stock Positions by Industry",
#     hover_data=["cnt"],
#     labels={"Industry": "cnt"},
# )
# fig.update_traces(textposition="inside", textinfo="percent")
# fig.write_image("./pyspark_test.png")

# fig = px.pie(
#     df_displaybydate,
#     values="cnt",
#     names="buy_date",
#     title="Stock Positions by Purchase Date",
#     hover_data=["cnt"],
#     labels={"Purchase Date": "cnt"},
# )
# fig.update_traces(textposition="inside", textinfo="percent")
# fig.write_image("./pyspark_test1.png")
# subject = "this is a test"
# html = ""
# image_path = ["./pyspark_test.png", "./pyspark_test1.png"]
# MyEmail().send_email_embedded_image(subject, html, image_path)
StockProposal("us", "20221011").send_btstrategy_by_email()
