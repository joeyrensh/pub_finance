import plotly.graph_objects as go
import pandas as pd
from utility.fileinfo import FileInfo
from pyspark.sql import SparkSession
from utility.toolkit import ToolKit

""" 
读取交易相关数据，交易明细，持仓明细，仓位日志明细，行业信息
"""
# 启动Spark Session
spark = (
    SparkSession.builder.master("local")
    .appName("SparkTest")
    .config("spark.driver.memory", "450m")
    .config("spark.executor.memory", "450m")
    .config("spark.memory.fraction", "0.8")
    .config("spark.executor.instances", "1")
    .config("spark.executor.cores", "2")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

trade_date = "20241028"
# 生成时间序列，用于时间序列补齐
end_date = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
start_date = pd.to_datetime(end_date) - pd.DateOffset(days=60)

date_range = pd.date_range(
    start=start_date.strftime("%Y-%m-%d"), end=end_date, freq="D"
)
df_timeseries = pd.DataFrame({"buy_date": date_range})
# 将日期转换为字符串格式 'YYYYMMDD'
df_timeseries["trade_date"] = df_timeseries["buy_date"].dt.strftime("%Y%m%d")
toolkit = ToolKit("identify trade date")

df_timeseries = df_timeseries[
    df_timeseries["trade_date"].apply(toolkit.is_cn_trade_date)
]

df_timeseries_spark = spark.createDataFrame(
    df_timeseries.astype({"buy_date": "string"})
)

df_timeseries_spark.createOrReplaceTempView("temp_timeseries")

file = FileInfo(trade_date, "cn")

# 行业明细
file_path_indus = file.get_file_path_industry
df2 = spark.read.csv(file_path_indus, header=True)
df2.createOrReplaceTempView("temp2")

# 仓位日志明细
file_path_position_detail = file.get_file_path_position_detail
cols = ["idx", "symbol", "date", "price", "adjbase", "pnl"]
df3 = spark.read.csv(file_path_position_detail, header=None, inferSchema=True)
df3 = df3.toDF(*cols)
df3.createOrReplaceTempView("temp3")

sparkdata100 = spark.sql(
    """
    WITH tmp AS (
        SELECT 
             t1.date
            ,t2.industry
            ,SUM(t1.pnl) AS pnl
        FROM temp3 t1 JOIN temp2 t2 ON t1.symbol = t2.symbol
        WHERE t1.date >= (
            SELECT buy_date FROM (
            SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
            FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
            WHERE row_num = 20 )
        GROUP BY t1.date, t2.industry
    ), tmp1 AS (
    SELECT
         date
        ,industry
        ,pnl
        ,LAG(pnl) OVER (PARTITION BY industry ORDER BY date) AS l_pnl
    FROM tmp
    ORDER BY date, industry
    ), tmp2 AS (
    SELECT 
        date
        ,SUM(s_pnl) AS s_pnl
    FROM (
        SELECT
            date
            ,industry
            ,SUM(pnl - COALESCE(l_pnl, 0)) as s_pnl
        FROM tmp1
        GROUP BY date, industry
        ) t
    GROUP BY date
    ), tmp3 AS (
    SELECT 
        tmp1.date
        ,tmp1.industry
        ,tmp1.pnl
        ,tmp1.l_pnl
        ,tmp1.pnl - COALESCE(tmp1.l_pnl, 0) AS pnl_incre
        ,tmp2.s_pnl
        ,ROW_NUMBER() OVER(PARTITION BY tmp1.date ORDER BY tmp1.pnl - COALESCE(tmp1.l_pnl, 0) DESC) AS rn
    FROM tmp1 LEFT JOIN tmp2 ON tmp1.date = tmp2.date
    ORDER BY tmp1.date, tmp1.industry
    )
    SELECT 
        date
        ,COLLECT_LIST(industry) AS industry_top3
        ,MAX(s_pnl) AS s_pnl
    FROM (SELECT * FROM tmp3 WHERE rn <= 3 ORDER BY date, rn) t
    GROUP BY date
    """
)

dfdata100 = sparkdata100.toPandas()
print(dfdata100["s_pnl"].max())
print(dfdata100["s_pnl"].min())

# 计算每个日期的周和星期几
dfdata100["date"] = pd.to_datetime(dfdata100["date"])
dfdata100["week"] = dfdata100["date"].dt.isocalendar().week
dfdata100["day_of_week"] = dfdata100["date"].dt.dayofweek

# 重新排列数据，使其按日期顺序排列
dfdata100 = dfdata100.sort_values(by="date").reset_index(drop=True)

# 计算每周的起始日期
dfdata100["week_start"] = dfdata100["date"] - pd.to_timedelta(
    dfdata100["day_of_week"], unit="d"
)

# 确定每周的顺序
unique_weeks = (
    dfdata100["week_start"].drop_duplicates().sort_values().reset_index(drop=True)
)
week_mapping = {date: i for i, date in enumerate(unique_weeks)}
dfdata100["week_order"] = dfdata100["week_start"].map(week_mapping)

# 创建日历图
fig = go.Figure()

# 假设 s_pnl 的最小值为负，最大值为正
min_val = dfdata100["s_pnl"].min()
max_val = dfdata100["s_pnl"].max()
mid_val = 0  # 中间值，用于白色

# 添加热力图
fig.add_trace(
    go.Heatmap(
        x=dfdata100["day_of_week"],  # 每行显示7天
        y=dfdata100["week_order"],  # 每7天增加一行
        z=dfdata100["s_pnl"],
        # 定义自定义颜色比例
        colorscale=[
            [0, "#003f00"],
            [
                (mid_val - min_val) / (max_val - min_val) / 2,
                "#008B00",
            ],
            [
                (mid_val - min_val) / (max_val - min_val),
                "white",
            ],
            [
                1 - (max_val - mid_val) / (max_val - min_val) / 2,
                "#FF0000",
            ],
            [1, "#b30000"],
        ],
        zmin=min_val,
        zmax=max_val,
        colorbar=dict(
            title="PnL",
            titleside="top",  # 将颜色条标题放在顶部
            tickfont=dict(size=16),  # 调整颜色条刻度字体大小
            thickness=20,  # 增加颜色条厚度
            len=0.5,  # 调整颜色条长度以适应布局
        ),
        text=dfdata100["industry_top3"].apply(lambda x: "<br>".join(x)),
    )
)

# 设置图表布局
fig.update_layout(
    # title="Calendar",
    xaxis=dict(
        tickmode="array",
        tickvals=[0, 1, 2, 3, 4, 5, 6],
        ticktext=[
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ],
        showgrid=True,
        gridcolor="rgba(0, 0, 0, 0.5)",
        zeroline=False,
        showticklabels=True,
        dtick=1,  # 每天显示一个刻度
        tickfont=dict(size=20),  # 调整x轴刻度字体大小
    ),
    yaxis=dict(
        showgrid=True,
        gridcolor="rgba(0, 0, 0, 0.5)",
        showticklabels=False,
        # title="Week",
        autorange="reversed",  # 反转Y轴，使得最新的一周在最下方
    ),
    plot_bgcolor="rgba(0, 0, 0, 0)",
    paper_bgcolor="rgba(0, 0, 0, 0)",
    margin=dict(
        l=10,  # 左边距
        r=10,  # 右边距
        t=10,  # 上边距（为颜色条留出空间）
        b=10,  # 下边距
    ),
)

# 在每个单元格中添加文本
for i, row in dfdata100.iterrows():
    day_of_week = row["day_of_week"]
    week_order = row["week_order"]
    col2_values = row["industry_top3"]
    col3_value = row["s_pnl"]
    date = row["date"].strftime("%Y-%m-%d")

    # 将col2的list按3行展示
    text = f"<b>{date}</b><br>" + "<br>".join(col2_values[:3])
    fig.add_annotation(
        x=day_of_week,
        y=week_order,
        text=text,
        showarrow=False,
        font=dict(color="black", size=16),  # 调整文本字体大小和颜色
        align="center",
        xanchor="center",
        yanchor="middle",
        # bordercolor="white",  # 添加白色边框以提高可读性（可选）
        # borderwidth=1,  # 设置边框宽度（可选）
        # bgcolor="rgba(255, 255, 255, 0.7)",  # 设置背景色为半透明白色（可选）
    )
# 设置图像的宽度和高度（例如，1920x1080像素）
fig_width, fig_height = 1440, 1000
# 设置缩放系数，例如2，3等，这将相应地增加图像的分辨率
scale_factor = 2

# 显示图表
# fig.show()
fig.write_image(
    "./images/test.png",
    width=fig_width,
    height=fig_height,
    scale=scale_factor,
)
