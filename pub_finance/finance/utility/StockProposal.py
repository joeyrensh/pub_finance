#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from utility.MyEmail import MyEmail
from utility.FileInfo import FileInfo
from utility.TickerInfo import TickerInfo
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession
from utility.MyEmail import MyEmail
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import gc
from utility.ToolKit import ToolKit


# mpl.rcParams["font.sans-serif"] = ["SimHei"]  # 用来正常显示中文标签


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
            df_n.style.hide(axis=1, subset=["标记", "成交量"])
            .format(
                {
                    "收盘价": "{:.2f}",
                    "涨幅": "{:.2f}",
                    "成交量": "{:.0f}",
                    "振幅": "{:.2f}%",
                }
            )
            .background_gradient(subset=["收盘价", "涨幅", "成交量", "振幅"], cmap=cm)
            .bar(
                subset=["涨幅"],
                align="left",
                color=["#5fba7d", "#d65f5f"],
                vmin=0,
                vmax=0.8,
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

    def send_btstrategy_by_email(self, cash, final_value):
        """
        发送邮件
        """
        # 启动Spark Session
        spark = (
            SparkSession.builder.master("local")
            .appName("SparkTest")
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        """ 
        读取交易相关数据，交易明细，持仓明细，仓位日志明细，行业信息
        """
        file = FileInfo(self.trade_date, self.market)
        # 最新一日股票信息
        file_name_day = file.get_file_path_latest
        df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
        # 持仓明细, pandas读取
        file_cur_p = file.get_file_path_position
        df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 9)])
        # 交易明细
        file_path_trade = file.get_file_path_trade
        cols = ["idx", "symbol", "date",
                "trade_type", "price", "size", "strategy"]
        df1 = spark.read.csv(file_path_trade, header=None, inferSchema=True)
        df1 = df1.toDF(*cols)
        df1.createOrReplaceTempView("temp1")
        # 持仓明细, spark读取
        df = spark.read.csv(file_cur_p, header=True)
        df.createOrReplaceTempView("temp")
        # 行业明细
        file_path_indus = file.get_file_path_industry
        df2 = spark.read.csv(file_path_indus, header=True)
        df2.createOrReplaceTempView("temp2")
        # 仓位日志明细
        file_path_position_detail = file.get_file_path_position_detail
        cols = ["idx", "symbol", "date", "price", "adjbase", "pnl"]
        df3 = spark.read.csv(file_path_position_detail,
                             header=None, inferSchema=True)
        df3 = df3.toDF(*cols)
        df3.createOrReplaceTempView("temp3")
        # 当日股票信息
        cols = [
            "idx",
            "symbol",
            "name",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "turnover",
            "chg",
            "change",
            "amplitude",
            "preclose",
            "total_value",
            "circulation_value",
            "date",
        ]
        df4 = spark.read.csv(file_name_day, header=None, inferSchema=True)
        df4 = df4.toDF(*cols)
        df4.createOrReplaceTempView("temp4")

        # 获取回测股票列表
        stock_list = TickerInfo(self.trade_date, self.market).get_stock_list()
        stock_list_tuples = [(symbol,) for symbol in stock_list]
        df5 = spark.createDataFrame(stock_list_tuples, schema=['symbol'])
        df5.createOrReplaceTempView("temp5")

        # 生成时间序列，用于时间序列补齐
        end_date = pd.to_datetime(self.trade_date).strftime("%Y-%m-%d")
        start_date = pd.to_datetime(end_date) - pd.DateOffset(days=60)
        date_range = pd.date_range(
            start=start_date.strftime("%Y-%m-%d"), end=end_date, freq="D"
        )
        df_timeseries = pd.DataFrame({"buy_date": date_range})
        # 将日期转换为字符串格式 'YYYYMMDD'
        df_timeseries["trade_date"] = df_timeseries["buy_date"].dt.strftime(
            "%Y%m%d")

        # 假设您的ToolKit类已经定义好了，并且可以检查交易日期
        toolkit = ToolKit("identify trade date")

        # 根据市场类型过滤非交易日
        if self.market == "us":
            df_timeseries = df_timeseries[
                df_timeseries["trade_date"].apply(toolkit.is_us_trade_date)
            ]
        elif self.market == "cn":
            df_timeseries = df_timeseries[
                df_timeseries["trade_date"].apply(toolkit.is_cn_trade_date)
            ]

        df_timeseries_spark = spark.createDataFrame(
            df_timeseries.astype({"buy_date": "string"})
        )

        df_timeseries_spark.createOrReplaceTempView("temp_timeseries")

        """ 
        行业板块历史数据分析
        """
        sparkdata7 = spark.sql(
            """ 
            WITH tmp AS (
                SELECT industry
                    ,SUM(IF(`p&l` >= 0, 1, 0)) AS pos_cnt
                    ,SUM(IF(`p&l` < 0, 1, 0)) AS neg_cnt
                    ,COUNT(*) AS p_cnt
                    ,SUM(IF(buy_date >= (SELECT buy_date FROM ( SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                                         FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt WHERE row_num = 5), 1, 0)) AS l5_p_cnt
                    ,SUM(`p&l`) AS p_pnl
                    ,SUM(adjbase * size) AS adjbase
                    ,SUM(price * size) AS base
                FROM temp 
                GROUP BY industry
                ), tmp1 AS (
                SELECT symbol
                    ,date
                    ,trade_type
                    ,price
                    ,size
                    ,l_date
                    ,l_trade_type
                    ,l_price
                    ,l_size
                FROM (
                    SELECT symbol
                        ,date
                        ,trade_type
                        ,price
                        ,size
                        ,IF(trade_type = 'sell', LAG(date) OVER (PARTITION BY symbol ORDER BY date)  
                            ,LEAD(date) OVER (PARTITION BY symbol ORDER BY date)) AS l_date
                        ,IF(trade_type = 'sell', LAG(trade_type) OVER (PARTITION BY symbol ORDER BY date) 
                            ,LEAD(trade_type) OVER (PARTITION BY symbol ORDER BY date)) AS l_trade_type
                        ,IF(trade_type = 'sell', LAG(price) OVER (PARTITION BY symbol ORDER BY date)
                            ,LEAD(price) OVER (PARTITION BY symbol ORDER BY date)) AS l_price
                        ,IF(trade_type = 'sell', LAG(size) OVER (PARTITION BY symbol ORDER BY date) 
                            ,LEAD(size) OVER (PARTITION BY symbol ORDER BY date)) AS l_size
                    FROM temp1
                    ORDER BY symbol
                        ,date
                        ,trade_type) t
            ), tmp11 AS (
                SELECT symbol
                    ,l_date as buy_date
                    ,l_price as base_price
                    ,l_size as base_size
                    ,date as sell_date
                    ,price as adj_price
                    ,size as adj_size
                FROM 
                tmp1 WHERE trade_type = 'sell' and date >= DATE_ADD('{}', -180)
                UNION ALL
                SELECT symbol
                    ,date as buy_date
                    ,price as base_price
                    ,size as base_size
                    ,null as sell_date
                    ,null as adj_price
                    ,null as adj_size
                FROM 
                tmp1 WHERE trade_type = 'buy' AND l_date IS NULL
            ), tmp2 AS (
                SELECT t1.industry
                    ,COUNT(t2.symbol) * 1.00 AS his_trade_cnt
                    ,COUNT(DISTINCT t2.symbol) AS his_symbol_cnt
                    ,COUNT(DISTINCT IF(t2.sell_date >= (SELECT buy_date FROM ( SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                                         FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt WHERE row_num = 5), t2.symbol, null)) AS l5_close
                    ,SUM(IF(t2.sell_date IS NOT NULL, DATEDIFF(t2.sell_date, t2.buy_date), DATEDIFF('{}', t2.buy_date))) AS his_days
                    ,SUM(IF(t2.sell_date IS NOT NULL AND t2.adj_price - t2.base_price >=0, 1, 0)) AS pos_cnt
                    ,SUM(IF(t2.sell_date IS NOT NULL AND t2.adj_price - t2.base_price < 0, 1, 0)) AS neg_cnt
                    ,SUM(IF(t2.sell_date IS NOT NULL, t2.adj_price * (-t2.adj_size) - t2.base_price * t2.base_size, 0)) AS his_pnl
                    ,SUM(IF(t2.sell_date IS NOT NULL, t2.adj_price * (-t2.adj_size), 0)) AS his_adjbase
                    ,SUM(IF(t2.sell_date IS NOT NULL, t2.base_price * t2.base_size, 0)) AS his_base
                FROM temp2 t1 JOIN tmp11 t2 ON t1.symbol = t2.symbol
                GROUP BY t1.industry
            ), tmp3 AS (
                SELECT industry
                    , COLLECT_LIST(pnl) AS pnl_array
                FROM (
                    SELECT t2.industry, t1.buy_date AS date
                        , SUM(IF(t1.buy_date = t2.date, COALESCE(t2.pnl, 0), 0)) AS pnl
                    FROM temp_timeseries t1 
                    LEFT JOIN  (SELECT t3.industry, t2.date, SUM(t2.pnl) AS pnl FROM temp3 t2 JOIN temp2 t3 ON t2.symbol = t3.symbol 
                                GROUP BY t3.industry, t2.date) t2 ON 1 = 1
                    GROUP BY t2.industry, t1.buy_date
                    ORDER BY t2.industry, t1.buy_date ASC
                    ) t
                GROUP BY industry
            ), tmp4 AS (
                SELECT temp2.industry, COUNT(temp2.symbol) AS ticker_cnt
                FROM temp2 JOIN temp5 ON temp2.symbol = temp5.symbol
                GROUP BY temp2.industry
            )                
            SELECT t1.industry
                ,COALESCE(t2.p_cnt,0) AS p_cnt
                ,COALESCE(t2.p_cnt,0) / t4.ticker_cnt AS long_ratio
                ,COALESCE(t2.l5_p_cnt,0) AS l5_p_cnt
                ,COALESCE(t1.l5_close,0) AS l5_close
                ,COALESCE(t2.p_pnl,0) AS pnl
                ,IF(COALESCE(t2.base,0) + COALESCE(t1.his_base,0) = 0, 0, (COALESCE(t2.adjbase,0) + COALESCE(t1.his_adjbase,0) - COALESCE(t2.base,0) - COALESCE(t1.his_base,0)) / (COALESCE(t2.base,0) + COALESCE(t1.his_base,0))) AS pnl_ratio
                ,COALESCE(t3.pnl_array,ARRAY(0)) AS pnl_array
                ,IF(COALESCE(t1.his_symbol_cnt,0) = 0, 0, COALESCE(t1.his_trade_cnt,0) / COALESCE(t1.his_symbol_cnt,0) ) AS avg_his_trade_cnt
                ,IF(COALESCE(t1.his_trade_cnt,0) = 0, 0, COALESCE(t1.his_days,0) / COALESCE(t1.his_trade_cnt,0) ) AS avg_days
                ,IF((COALESCE(t1.pos_cnt,0) + COALESCE(t2.pos_cnt,0) + COALESCE(t1.neg_cnt,0) + COALESCE(t2.neg_cnt,0)) > 0, (COALESCE(t1.pos_cnt,0) + COALESCE(t2.pos_cnt,0)) / (COALESCE(t1.pos_cnt,0) + COALESCE(t2.pos_cnt,0) + COALESCE(t1.neg_cnt,0) + COALESCE(t2.neg_cnt,0)), 0) AS win_rate
            FROM tmp2 t1 LEFT JOIN tmp t2 ON t1.industry = t2.industry
            LEFT JOIN tmp3 t3 ON t1.industry = t3.industry
            LEFT JOIN tmp4 t4 ON t1.industry = t4.industry
            ORDER BY COALESCE(t2.p_pnl,0) DESC
            """.format(
                end_date, end_date
            )
        )
        # 5日前行业盈亏情况
        sparkdata71 = spark.sql(
            """
            WITH tmp AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp3 t1 JOIN temp2 t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
                    WHERE row_num = 5 )
                GROUP BY t2.industry
            )   
            SELECT industry
                ,pnl
            FROM tmp
            ORDER BY pnl DESC
            """
        )
        dfdata71 = sparkdata71.toPandas()
        dfdata7 = sparkdata7.toPandas()

        # Create a dictionary to store the index differences
        index_diff_dict = {}

        # Iterate over each row in df1
        for index, row in dfdata7.iterrows():
            industry = row["industry"]

            # Check if the industry value exists in df2
            if industry in dfdata71["industry"].values:
                # Get the corresponding index differences from df2
                index_diff = (
                    dfdata71[dfdata71["industry"] ==
                             industry].index.values - index
                )[0]
                index_diff_dict[index] = index_diff

        # Add the index differences to df1 as a new column
        dfdata7["index_diff"] = dfdata7.index.map(index_diff_dict)

        # Define a function to create the trend arrows
        def create_arrow(value):
            if pd.isnull(value):
                return ""
            elif value > 0:
                return (
                    f"<span style='color:red'; font-size:32px>↑{abs(value):.0f}</span>"
                )
            elif value < 0:
                return f"<span style='color:green'; font-size:32px>↓{abs(value):.0f}</span>"
            else:
                return ""

        # Apply the create_arrow function to the index_diff column and update the "industry" column
        dfdata7["industry_new"] = dfdata7["industry"]
        dfdata7["industry"] = dfdata7.apply(
            lambda row: f"{row['industry']} {create_arrow(row['index_diff'])}",
            axis=1,
        )

        dfdata7["pnl_trend"] = dfdata7["pnl_array"].apply(
            ToolKit("draw line").create_line
        )
        dfdata7.rename(
            columns={
                "industry": "IND",
                "p_cnt": "OPEN",
                "l5_p_cnt": "L5 OPEN",
                "l5_close": "L5 CLOSE",
                "pnl": "PROFIT",
                "pnl_ratio": "PNL RATIO",
                "long_ratio": "LRATIO",
                "avg_his_trade_cnt": "AVG TRANS",
                "avg_days": "AVG DAYS",
                "win_rate": "WIN RATE",
                "pnl_trend": "PROFIT TREND",
            },
            inplace=True,
        )
        cm = sns.color_palette("coolwarm", as_cmap=True)
        html = (
            "<h2>Industry Overview</h2>"
            "<table>"
            + dfdata7.style.hide(
                axis=1, subset=["pnl_array", "index_diff", "industry_new", "AVG TRANS"]
            )
            .format(
                {
                    "OPEN": "{:.2f}",
                    "L5 OPEN": "{:.2f}",
                    "L5 CLOSE": "{:.2f}",
                    "PROFIT": "{:.2f}",
                    "LRATIO": "{:.2%}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.2f}",
                    "AVG DAYS": "{:.0f}",
                    "WIN RATE": "{:.2%}",
                }
            )
            .background_gradient(
                subset=["PROFIT", "OPEN", "L5 OPEN", "L5 CLOSE"], cmap=cm
            )
            .bar(
                subset=["WIN RATE"],
                align="left",
                color=["#99CC66", "#FF6666"],
                vmin=0,
                vmax=0.8,
                # width=50,
            )
            .bar(
                subset=["LRATIO"],
                align="left",
                color=["#99CC66", "#FF6666"],
                vmin=0,
                vmax=0.8,
            )
            .bar(
                subset=["PNL RATIO"],
                align="mid",
                color=["#99CC66", "#FF6666"],
                vmin=-0.8,
                vmax=0.8,
            )
            .set_properties(
                **{
                    "text-align": "left",
                    "border": "1px solid #ccc",
                    "cellspacing": "0",
                    "style": "border-collapse: collapse; ",
                }
            )
            .set_table_styles(
                [
                    # 表头样式
                    dict(
                        selector="th",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "5px"),  # 增加填充以便更易点击和阅读
                            ("font-size", "24px"),  # 在PC端使用较大字体
                        ],
                    ),
                    # 表格数据单元格样式
                    dict(
                        selector="td",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "5px"),
                            ("font-size", "24px"),
                        ],
                    ),
                ],
            )
            .set_table_styles(
                {
                    # 针对盈亏趋势列增加列宽
                    "PROFIT TREND": [
                        {
                            "selector": "th",
                            "props": [
                                ("min-width", "300px"),
                                ("max-width", "100%"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [
                                ("min-width", "300px"),
                                ("max-width", "100%"),
                                ("padding", "0"),
                            ],
                        },
                    ],
                    "IND": [
                        {
                            "selector": "th",
                            "props": [
                                ("min-width", "200px"),
                                ("max-width", "500px"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [
                                ("min-width", "200px"),
                                ("max-width", "500px"),
                                # ("white-space", "nowrap"),
                            ],
                        },
                    ],
                    "PNL RATIO": [
                        {
                            "selector": "th",
                            "props": [("min-width", "50px"), ("max-width", "100px")],
                        },
                        {
                            "selector": "td",
                            "props": [("min-width", "50px"), ("max-width", "100px")],
                        },
                    ],
                    "WIN RATE": [
                        {
                            "selector": "th",
                            "props": [("min-width", "50px"), ("max-width", "100px")],
                        },
                        {
                            "selector": "td",
                            "props": [("min-width", "50px"), ("max-width", "100px")],
                        },
                    ],
                },
                overwrite=False,
            )
            .set_sticky(axis="columns")
            .to_html(doctype_html=True, escape=False)
            + "<table>"
        )

        css = """
            <style>
                :root {
                    color-scheme: dark light;
                    supported-color-schemes: dark light;
                    background-color: white;
                    color: black;
                    display: table ;
                }
                /* Your light mode (default) styles: */
                body {
                    background-color: white;
                    color: black;
                    display: table ;
                    width: 100%;
                }
                table {
                    background-color: white;
                    color: black;
                    width: 100%;
                }
                @media (prefers-color-scheme: dark) {            
                    body {
                        background-color: black;
                        color: white;
                        display: table ;
                        width: 100%;                        
                    }
                    table {
                        background-color: black;
                        color: white;
                        width: 100%;
                    }
                }
            </style>
        """

        html = css + html

        """
        持仓明细历史交易情况分析
        """
        df_np = pd.merge(df_cur_p, df_d, how="inner", on="symbol")
        # df_np = df_np[df_np['p&l_ratio'] > 0].reset_index(drop=True)
        df_np["pnlsum"] = df_np.groupby("industry")["p&l"].transform("sum")
        df_np = (
            df_np.sort_values(["pnlsum", "buy_date"], ascending=[False, False])
            .drop(columns="pnlsum")
            .reset_index(drop=True)
        )

        df_np_spark = spark.createDataFrame(df_np)
        df_np_spark.createOrReplaceTempView("temp_symbol")

        sparkdata8 = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT symbol
                    ,date
                    ,trade_type
                    ,price
                    ,size
                    ,strategy
                    ,l_date
                    ,l_trade_type
                    ,l_price
                    ,l_size
                    ,l_strategy
                FROM (
                    SELECT symbol
                        ,date
                        ,trade_type
                        ,price
                        ,size
                        ,strategy
                        ,IF(trade_type = 'sell', LAG(date) OVER (PARTITION BY symbol ORDER BY date)  
                            , LEAD(date) OVER (PARTITION BY symbol ORDER BY date)) AS l_date
                        ,IF(trade_type = 'sell', LAG(trade_type) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(trade_type) OVER (PARTITION BY symbol ORDER BY date)) AS l_trade_type
                        ,IF(trade_type = 'sell', LAG(price) OVER (PARTITION BY symbol ORDER BY date)
                            , LEAD(price) OVER (PARTITION BY symbol ORDER BY date)) AS l_price
                        ,IF(trade_type = 'sell', LAG(size) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(size) OVER (PARTITION BY symbol ORDER BY date)) AS l_size
                        ,IF(trade_type = 'sell', LAG(strategy) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(strategy) OVER (PARTITION BY symbol ORDER BY date)) AS l_strategy                               
                    FROM temp1
                    ORDER BY symbol
                        ,date
                        ,trade_type) t
            ), tmp11 AS (
                SELECT symbol
                    ,l_date AS buy_date
                    ,l_price AS base_price
                    ,l_size AS base_size
                    ,l_strategy AS buy_strategy
                    ,date AS sell_date
                    ,price AS adj_price
                    ,size AS adj_size
                    ,strategy AS sell_strategy
                FROM tmp1 WHERE trade_type = 'sell' AND date >= DATE_ADD('{}', -180)
                UNION ALL
                SELECT symbol
                    ,date AS buy_date
                    ,price AS base_price
                    ,size AS base_size
                    ,strategy AS buy_strategy
                    ,null AS sell_date
                    ,null AS adj_price
                    ,null AS adj_size
                    ,null AS sell_strategy
                FROM tmp1 WHERE trade_type = 'buy' AND l_date IS NULL
            ), tmp2 AS (
                SELECT symbol
                    ,COUNT(symbol) AS his_trade_cnt
                    ,SUM(IF(sell_date IS NOT NULL, DATEDIFF(sell_date, buy_date), DATEDIFF('{}', buy_date))) AS his_days
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price >=0, 1, 0)) AS pos_cnt
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price < 0, 1, 0)) AS neg_cnt
                    ,SUM(IF(sell_date IS NOT NULL, adj_price * (-adj_size) - base_price * base_size, 0)) AS his_pnl
                    ,SUM(IF(sell_date IS NOT NULL, base_price * base_size, 0)) AS his_base_price
                    ,MAX(IF(sell_date IS NULL, buy_strategy, null)) AS buy_strategy
                FROM  tmp11
                GROUP BY symbol
            ), tmp3 AS (
                SELECT symbol
                    , buy_date
                    , price
                    , adjbase
                    , size
                    , `p&l` as pnl
                    , `p&l_ratio` as pnl_ratio
                    , industry
                    , name
                FROM temp_symbol
            )
            SELECT t1.symbol
                , t1.buy_date
                , t1.price
                , t1.adjbase
                , t1.pnl
                , t1.pnl_ratio
                , t1.industry
                , t1.name
                , COALESCE(t2.his_trade_cnt, 0) AS avg_trans
                , COALESCE(t2.his_days, 0) / t2.his_trade_cnt AS avg_days
                , (t1.pos_cnt + COALESCE(t2.pos_cnt,0)) / ( COALESCE(t2.pos_cnt,0) + COALESCE(t2.neg_cnt,0) + t1.pos_cnt + t1.neg_cnt) AS win_rate
                , (COALESCE(t2.his_pnl,0) + (t1.adjbase - t1.price) * t1.size) / (COALESCE(t2.his_base_price,0) + t1.price * t1.size) AS total_pnl_ratio
                , t2.buy_strategy
            FROM (
                SELECT symbol
                , buy_date
                , price
                , adjbase
                , size
                , pnl
                , pnl_ratio
                , industry
                , name
                , IF(adjbase >= price, 1, 0) AS pos_cnt
                , IF(adjbase < price, 1, 0) AS neg_cnt
                FROM tmp3
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol
            """.format(
                end_date, end_date
            )
        )

        dfdata8 = sparkdata8.toPandas()

        # 将df2的索引和'ind'列的值拼接起来
        dfdata7["combined"] = (
            dfdata7["IND"].astype(str) + "(" + dfdata7.index.astype(str) + ")"
        )

        # 使用merge来找到df1和df2中'ind'相等的行，并保留df1的所有行
        dfdata8 = pd.merge(
            dfdata8,
            dfdata7[["industry_new", "combined"]],
            left_on="industry",
            right_on="industry_new",
            how="inner",
        )
        dfdata8["industry"] = dfdata8["combined"]

        # 删除添加的'combined_df2'列
        dfdata8.drop(columns=["combined", "industry_new"], inplace=True)

        dfdata8.rename(
            columns={
                "symbol": "SYMBOL",
                "buy_date": "OPEN DATE",
                "price": "BASE",
                "adjbase": "ADJBASE",
                "pnl": "PNL",
                "pnl_ratio": "PNL RATIO",
                "avg_trans": "AVG TRANS",
                "avg_days": "AVG DAYS",
                "win_rate": "WIN RATE",
                "total_pnl_ratio": "TOTAL PNL RATIO",
                "industry": "IND",
                "name": "NAME",
                "buy_strategy": "Strategy",
            },
            inplace=True,
        )
        cm = sns.color_palette("coolwarm", as_cmap=True)

        # 创建样式函数，用于突出显示具有特定日期值的行
        old_date = datetime.strptime(self.trade_date, "%Y%m%d").date()

        # 将日期减去一天
        new_date = old_date - timedelta(days=5)

        # 将新日期转换为字符串
        df_timeseries_sorted = df_timeseries.sort_values(
            by="buy_date", ascending=False)
        new_date_str = str(
            df_timeseries_sorted.iloc[4]["buy_date"].strftime("%Y-%m-%d"))
        # new_date_str = new_date.strftime("%Y-%m-%d")

        def highlight_row(row):
            if row["OPEN DATE"] >= new_date_str:
                return ["background-color: orange"] * len(row)
            else:
                return [""] * len(row)

        # 将样式函数应用于DataFrame
        # styled_df = df.style.apply(highlight_row, axis=1)

        html1 = (
            "<h2>Open Position List</h2>"  # 添加标题
            "<table>"
            + dfdata8.style.hide(axis=1, subset=["PNL"])
            .format(
                {
                    "BASE": "{:.2f}",
                    "ADJBASE": "{:.2f}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.2f}",
                    "AVG DAYS": "{:.0f}",
                    "WIN RATE": "{:.2%}",
                    "TOTAL PNL RATIO": "{:.2%}",
                }
            )
            .apply(highlight_row, axis=1)
            .background_gradient(subset=["BASE", "ADJBASE"], cmap=cm)
            .bar(
                subset=["PNL RATIO", "TOTAL PNL RATIO"],
                align="mid",
                color=["#99CC66", "#FF6666"],
                vmin=-0.8,
                vmax=0.8,
            )
            .bar(
                subset=["WIN RATE"],
                align="left",
                color=["#99CC66", "#FF6666"],
                vmin=0,
                vmax=0.8,
            )
            .set_properties(
                **{
                    "text-align": "left",
                    "border": "1px solid #ccc",
                    "cellspacing": "0",
                    "style": "border-collapse: collapse; ",
                }
            )
            .set_table_styles(
                [
                    # 表头样式
                    dict(
                        selector="th",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "8px"),  # 增加填充以便更易点击和阅读
                            ("font-size", "18px"),  # 在PC端使用较大字体
                        ],
                    ),
                    # 表格数据单元格样式
                    dict(
                        selector="td",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "8px"),
                            (
                                "font-size",
                                "18px",
                            ),  # 同样适用较大字体以提高移动端可读性
                        ],
                    ),
                ]
            )
            .set_table_styles(
                {
                    "IND": [
                        {
                            "selector": "th",
                            "props": [
                                ("min-width", "150px"),
                                ("max-width", "300px"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [
                                ("min-width", "150px"),
                                ("max-width", "300px"),
                            ],
                        },
                    ],
                    "NAME": [
                        {
                            "selector": "th",
                            "props": [
                                ("min-width", "150px"),
                                ("max-width", "300px"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [
                                ("min-width", "150px"),
                                ("max-width", "300px"),
                            ],
                        },
                    ],
                },
                overwrite=False,
            )
            .set_sticky(axis="columns")
            .to_html(doctype_html=True, escape=False)
            + "<table>"
        )

        css1 = """
            <style>
                :root {
                    color-scheme: dark light;
                    supported-color-schemes: dark light;
                    background-color: white;
                    color: black;
                    display: table ;
                }                
                /* Your light mode (default) styles: */
                body {
                    background-color: white;
                    color: black;
                    display: table ;
                    width: 100%;                          
                }
                table {
                    background-color: white;
                    color: black;
                    width: 100%;
                }
                @media (prefers-color-scheme: dark) {            
                    body {
                        background-color: black;
                        color: white;
                        display: table ;
                        width: 100%;                              
                    }
                    table {
                        background-color: black;
                        color: white;
                        width: 100%;
                    }
                }
            </style>
        """

        html1 = css1 + html1

        del df_np
        gc.collect()

        """
        减仓情况分析
        """
        sparkdata9 = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT symbol
                    ,date
                    ,trade_type
                    ,price
                    ,size
                    ,strategy
                    ,l_date
                    ,l_trade_type
                    ,l_price
                    ,l_size
                    ,l_strategy
                FROM (
                    SELECT symbol
                        ,date
                        ,trade_type
                        ,price
                        ,size
                        ,strategy
                        ,IF(trade_type = 'sell', LAG(date) OVER (PARTITION BY symbol ORDER BY date)  
                            , LEAD(date) OVER (PARTITION BY symbol ORDER BY date)) AS l_date
                        ,IF(trade_type = 'sell', LAG(trade_type) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(trade_type) OVER (PARTITION BY symbol ORDER BY date)) AS l_trade_type
                        ,IF(trade_type = 'sell', LAG(price) OVER (PARTITION BY symbol ORDER BY date)
                            , LEAD(price) OVER (PARTITION BY symbol ORDER BY date)) AS l_price
                        ,IF(trade_type = 'sell', LAG(size) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(size) OVER (PARTITION BY symbol ORDER BY date)) AS l_size
                        ,IF(trade_type = 'sell', LAG(strategy) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(strategy) OVER (PARTITION BY symbol ORDER BY date)) AS l_strategy                               
                    FROM temp1
                    ORDER BY symbol
                        ,date
                        ,trade_type) t
                WHERE date >= DATE_ADD('{}', -180)
            ), tmp11 AS (
                SELECT symbol
                    ,l_date AS buy_date
                    ,l_price AS base_price
                    ,l_size AS base_size
                    ,l_strategy AS buy_strategy
                    ,date AS sell_date
                    ,price AS adj_price
                    ,size AS adj_size
                    ,strategy AS sell_strategy
                    ,ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY l_date DESC) AS row_num
                FROM tmp1 WHERE trade_type = 'sell'
            ), tmp2 AS (
                SELECT symbol
                    ,COUNT(symbol) AS his_trade_cnt
                    ,SUM(DATEDIFF(sell_date, buy_date)) AS his_days
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price >=0, 1, 0)) AS pos_cnt
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price < 0, 1, 0)) AS neg_cnt
                    ,SUM(IF(sell_date IS NOT NULL, adj_price * (-adj_size) - base_price * base_size, 0)) AS his_pnl
                    ,SUM(IF(sell_date IS NOT NULL, base_price * base_size, 0)) AS his_base_price
                    ,MAX(sell_strategy) AS sell_strategy
                FROM  tmp11
                GROUP BY symbol
            ), tmp3 AS (
                SELECT t1.symbol
                    ,t1.name
                    ,t2.industry
                FROM temp4 t1 LEFT JOIN temp2 t2 ON t1.symbol = t2.symbol
                GROUP BY t1.symbol
                    ,t1.name
                    ,t2.industry
            )
            SELECT t1.symbol
                , t1.buy_date
                , t1.sell_date
                , t1.base_price AS price
                , t1.adj_price AS adjbase
                , t1.pnl
                , t1.pnl_ratio
                , t3.industry
                , t3.name
                , COALESCE(t2.his_trade_cnt, 0) AS avg_trans
                , COALESCE(t2.his_days, 0) / t2.his_trade_cnt AS avg_days
                , COALESCE(t2.pos_cnt,0) / (COALESCE(t2.pos_cnt,0) + COALESCE(t2.neg_cnt,0)) AS win_rate
                , COALESCE(t2.his_pnl,0) / COALESCE(t2.his_base_price,0) AS total_pnl_ratio
                , t2.sell_strategy
            FROM (
                SELECT symbol
                    , buy_date
                    , sell_date
                    , base_price
                    , adj_price
                    , adj_price * (-adj_size) - base_price * base_size AS pnl
                    , (adj_price - base_price) / base_price AS pnl_ratio
                FROM tmp11 WHERE sell_date >= (SELECT buy_date FROM (
                                                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                                                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t 
                                                ) tt WHERE row_num = 5)
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol 
                LEFT JOIN tmp3 t3 ON t1.symbol = t3.symbol
            """.format(
                end_date
            )
        )

        dfdata9 = sparkdata9.toPandas()

        if not dfdata9.empty:

            # 使用merge来找到df1和df2中'ind'相等的行，并保留df1的所有行
            dfdata9 = pd.merge(
                dfdata9,
                dfdata7[["industry_new", "combined"]],
                left_on="industry",
                right_on="industry_new",
                how="inner",
            )
            dfdata9["industry"] = dfdata9["combined"]

            # 删除添加的'combined_df2'列
            dfdata9.drop(columns=["combined", "industry_new"], inplace=True)
            # 提取括号内的数字
            dfdata9['col_sort'] = dfdata9['industry'].str.extract(
                r'\((\d+)\)').astype(int)

            dfdata9 = (
                dfdata9.sort_values(
                    ["col_sort", "sell_date"], ascending=[True, False])
                .drop(columns="col_sort")
                .reset_index(drop=True)
            )

            dfdata9.rename(
                columns={
                    "symbol": "SYMBOL",
                    "buy_date": "OPEN DATE",
                    "sell_date": "CLOSE DATE",
                    "price": "BASE",
                    "adjbase": "ADJBASE",
                    "pnl": "PNL",
                    "pnl_ratio": "PNL RATIO",
                    "avg_trans": "AVG TRANS",
                    "avg_days": "AVG DAYS",
                    "win_rate": "WIN RATE",
                    "total_pnl_ratio": "TOTAL PNL RATIO",
                    "industry": "IND",
                    "name": "NAME",
                    "sell_strategy": "Strategy",
                },
                inplace=True,
            )
            cm = sns.color_palette("coolwarm", as_cmap=True)
            html2 = (
                "<h2>Close Position List Last 5 Days</h2>"  # 添加标题
                "<table>"
                + dfdata9.style.hide(axis=1, subset=["PNL"])
                .format(
                    {
                        "BASE": "{:.2f}",
                        "ADJBASE": "{:.2f}",
                        "PNL RATIO": "{:.2%}",
                        "AVG TRANS": "{:.2f}",
                        "AVG DAYS": "{:.0f}",
                        "WIN RATE": "{:.2%}",
                        "TOTAL PNL RATIO": "{:.2%}",
                    }
                )
                .background_gradient(subset=["BASE", "ADJBASE"], cmap=cm)
                .bar(
                    subset=["PNL RATIO", "TOTAL PNL RATIO"],
                    align="mid",
                    color=["#99CC66", "#FF6666"],
                    vmin=-0.8,
                    vmax=0.8,
                )
                .bar(
                    subset=["WIN RATE"],
                    align="left",
                    color=["#99CC66", "#FF6666"],
                    vmin=0,
                    vmax=0.8,
                )
                .set_properties(
                    **{
                        "text-align": "left",
                        "border": "1px solid #ccc",
                        "cellspacing": "0",
                        "style": "border-collapse: collapse; ",
                    }
                )
                .set_table_styles(
                    [
                        # 表头样式
                        dict(
                            selector="th",
                            props=[
                                ("border", "1px solid #ccc"),
                                ("text-align", "left"),
                                ("padding", "8px"),  # 增加填充以便更易点击和阅读
                                ("font-size", "18px"),  # 在PC端使用较大字体
                            ],
                        ),
                        # 表格数据单元格样式
                        dict(
                            selector="td",
                            props=[
                                ("border", "1px solid #ccc"),
                                ("text-align", "left"),
                                ("padding", "8px"),
                                (
                                    "font-size",
                                    "18px",
                                ),  # 同样适用较大字体以提高移动端可读性
                            ],
                        ),
                    ]
                )
                .set_table_styles(
                    {
                        "IND": [
                            {
                                "selector": "th",
                                "props": [
                                    ("min-width", "150px"),
                                    ("max-width", "300px"),
                                ],
                            },
                            {
                                "selector": "td",
                                "props": [
                                    ("min-width", "150px"),
                                    ("max-width", "300px"),
                                ],
                            },
                        ],
                        "NAME": [
                            {
                                "selector": "th",
                                "props": [
                                    ("min-width", "150px"),
                                    ("max-width", "300px"),
                                ],
                            },
                            {
                                "selector": "td",
                                "props": [
                                    ("min-width", "150px"),
                                    ("max-width", "300px"),
                                ],
                            },
                        ],
                    },
                    overwrite=False,
                )
                .set_sticky(axis="columns")
                .to_html(doctype_html=True, escape=False)
                + "</table>"
            )

            css2 = """
                <style>
                    :root {
                        color-scheme: dark light;
                        supported-color-schemes: dark light;
                        background-color: white;
                        color: black;
                        display: table ;
                    }                
                    /* Your light mode (default) styles: */
                    body {
                        background-color: white;
                        color: black;
                        display: table ;
                        width: 100%;                          
                    }
                    table {
                        background-color: white;
                        color: black;
                        width: 100%;
                    }
                    @media (prefers-color-scheme: dark) {            
                        body {
                            background-color: black;
                            color: white;
                            display: table ;
                            width: 100%;                              
                        }
                        table {
                            background-color: black;
                            color: white;
                            width: 100%;
                        }
                    }
                </style>
            """
            html2 = css2 + html2
        else:
            html2 = ""

        del dfdata9
        gc.collect()
        del dfdata7
        gc.collect()

        # TOP10热门行业
        sparkdata1 = spark.sql(
            """ 
            SELECT industry, cnt 
            FROM (
                SELECT industry, count(*) AS cnt FROM temp GROUP BY industry)
            ORDER BY cnt DESC LIMIT 10
            """
        )

        dfdata1 = sparkdata1.toPandas()
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=dfdata1["industry"],
                    values=dfdata1["cnt"],
                    pull=[0.2, 0.2, 0.2, 0.2, 0.2],
                )
            ]
        )
        colors = ["gold", "mediumturquoise", "darkorange", "lightgreen"]
        # light mode
        fig.update_traces(
            marker=dict(colors=colors, line=dict(width=1)),
            textinfo="value+percent",
            textfont=dict(
                size=20,
                color="black",
            ),
            textposition="inside",
        )
        fig.update_layout(
            title="Top10 Position",
            title_font=dict(size=20, color="black"),
            legend=dict(
                orientation="v",
                yanchor="top",
                xanchor="left",
                x=-0.3,
                y=1,
                font=dict(size=20, color="black"),  # 调整图例字体大小
                bgcolor="rgba(0,0,0,0)",  # 设置图例背景为完全透明
            ),
            # margin=dict(t=50, b=0.2, l=0.2, r=0.2),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        # 设置图像的宽度和高度（例如，1920x1080像素）
        fig_width, fig_height = 1280, 720
        # 设置缩放系数，例如2，3等，这将相应地增加图像的分辨率
        scale_factor = 1

        # htmltest = io.to_html(fig, full_html=False)

        fig.write_image(
            "./images/postion_byindustry_light.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        fig.update_traces(
            marker=dict(colors=colors, line=dict(width=1)),
            textinfo="value+percent",
            textfont=dict(
                size=20,
                color="white",
            ),
            textposition="inside",
        )
        fig.update_layout(
            title="Top10 Position",
            title_font=dict(
                size=20,
                color="white",
            ),
            legend=dict(
                orientation="v",
                yanchor="top",
                xanchor="left",
                x=-0.3,
                y=1,
                font=dict(size=20, color="white"),  # 调整图例字体大小
                bgcolor="rgba(0,0,0,0)",  # 设置图例背景为完全透明
            ),
            # margin=dict(t=50, b=0.2, l=0.2, r=0.2),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        # 设置图像的宽度和高度（例如，1920x1080像素）
        fig_width, fig_height = 1280, 720
        # 设置缩放系数，例如2，3等，这将相应地增加图像的分辨率
        scale_factor = 1

        fig.write_image(
            "./images/postion_byindustry_dark.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        del dfdata1
        gc.collect()

        # TOP10盈利行业
        sparkdata2 = spark.sql(
            """ 
            SELECT industry, ROUND(pl,2) AS pl 
            FROM (
                SELECT industry, sum(`p&l`) as pl FROM temp GROUP BY industry)
            ORDER BY pl DESC LIMIT 10
            """
        )

        dfdata2 = sparkdata2.toPandas()
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=dfdata2["industry"],
                    values=dfdata2["pl"],
                    pull=[0.2, 0.2, 0.2, 0.2, 0.2],
                )
            ]
        )
        colors = ["gold", "mediumturquoise", "darkorange", "lightgreen"]
        # light mode
        fig.update_traces(
            marker=dict(colors=colors, line=dict(width=1)),
            textinfo="value+percent",
            textfont=dict(
                size=20,
                color="black",
            ),
            textposition="inside",
        )
        fig.update_layout(
            title="Top10 Profit",
            title_font=dict(
                size=20,
                color="black",
            ),
            legend=dict(
                orientation="v",
                yanchor="top",
                xanchor="left",
                x=-0.3,
                y=1,
                font=dict(
                    size=20,
                    color="black",
                ),  # 调整图例字体大小
                bgcolor="rgba(0,0,0,0)",  # 设置图例背景为完全透明
            ),
            # margin=dict(t=50, b=0.2, l=0.2, r=0.2),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/postion_byp&l_light.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        fig.update_traces(
            marker=dict(colors=colors, line=dict(width=1)),
            textinfo="value+percent",
            textfont=dict(
                size=20,
                color="white",
            ),
            textposition="inside",
        )
        fig.update_layout(
            title="Top10 Profit",
            title_font=dict(
                size=20,
                color="white",
            ),
            legend=dict(
                orientation="v",
                yanchor="top",
                xanchor="left",
                x=-0.3,
                y=1,
                font=dict(
                    size=20,
                    color="white",
                ),  # 调整图例字体大小
                bgcolor="rgba(0,0,0,0)",  # 设置图例背景为完全透明
            ),
            # margin=dict(t=50, b=0.2, l=0.2, r=0.2),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/postion_byp&l_dark.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )

        del dfdata2
        gc.collect()

        # 60天内交易明细分析
        sparkdata3 = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT date
                    ,COUNT(symbol) AS total_cnt
                FROM temp3
                WHERE date >= DATE_ADD('{}', -60)
                GROUP BY date
            ), tmp11 AS (
                SELECT temp_timeseries.buy_date
                    ,IF(tmp1.total_cnt > 0, tmp1.total_cnt
                        ,LAST_VALUE(tmp1.total_cnt) IGNORE NULLS OVER (PARTITION BY 'AAA' ORDER BY temp_timeseries.buy_date)) AS total_cnt
                FROM temp_timeseries LEFT JOIN tmp1 ON temp_timeseries.buy_date = tmp1.date
            ), tmp5 AS (
                SELECT date
                    ,SUM(IF(trade_type = 'buy', 1, 0)) AS buy_cnt
                    ,SUM(IF(trade_type = 'sell', 1, 0)) AS sell_cnt
                FROM temp1 
                WHERE date >= DATE_ADD('{}', -60)
                GROUP BY date
            )
            SELECT t1.buy_date AS buy_date
                ,t1.total_cnt AS total_cnt
                ,t2.buy_cnt AS buy_cnt
                ,t2.sell_cnt AS sell_cnt
            FROM tmp11 t1 LEFT JOIN tmp5 t2 ON t1.buy_date = t2.date
            """.format(
                end_date, end_date
            )
        )
        dfdata3 = sparkdata3.toPandas()
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=dfdata3["buy_date"],
                y=dfdata3["total_cnt"],
                mode="lines+markers",
                name="total stock",
                line=dict(color="red", width=3),
                yaxis="y",
            )
        )
        fig.add_trace(
            go.Bar(
                x=dfdata3["buy_date"],
                y=dfdata3["buy_cnt"],
                name="long",
                marker_color="red",
                yaxis="y2",
            )
        )
        fig.add_trace(
            go.Bar(
                x=dfdata3["buy_date"],
                y=dfdata3["sell_cnt"],
                name="short",
                marker_color="green",
                yaxis="y2",
            )
        )
        # light mode
        fig.update_layout(
            title={
                "text": "Last 60 days trade info",
                "y": 0.95,
                "x": 0.05,
                "xanchor": "left",
                "yanchor": "top",
                "font": dict(family="Courier", size=20, color="black"),
            },
            xaxis=dict(
                title="Trade Date",
                titlefont=dict(family="Courier", size=20, color="black"),
                mirror=True,
                ticks="outside",
                tickfont=dict(color="black"),
                showline=True,
                gridcolor="rgba(0, 0, 0, 0.5)",
            ),
            yaxis=dict(
                title="Total Positions",
                titlefont=dict(family="Courier", size=20, color="black"),
                side="left",
                mirror=True,
                ticks="outside",
                tickfont=dict(color="black"),
                showline=True,
                gridcolor="rgba(0, 0, 0, 0.5)",
            ),
            yaxis2=dict(
                title="Positions per day",
                titlefont=dict(family="Courier", size=20, color="black"),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="outside",
                tickfont=dict(color="black"),
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(family="Courier", size=20, color="black"),
            ),
            barmode="stack",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/postion_bydate_light.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        fig.update_layout(
            title={
                "text": "Last 60 days trade info",
                "y": 0.95,
                "x": 0.05,
                "xanchor": "left",
                "yanchor": "top",
                "font": dict(family="Courier", size=20, color="white"),
            },
            xaxis=dict(
                title="Trade Date",
                titlefont=dict(family="Courier", size=20, color="white"),
                mirror=True,
                ticks="outside",
                tickfont=dict(color="white"),
                showline=True,
                gridcolor="rgba(255, 255, 255, 0.5)",
            ),
            yaxis=dict(
                title="Total Positions",
                titlefont=dict(family="Courier", size=20, color="white"),
                side="left",
                mirror=True,
                ticks="outside",
                tickfont=dict(color="white"),
                showline=True,
                gridcolor="rgba(255, 255, 255, 0.5)",
            ),
            yaxis2=dict(
                title="Positions per day",
                titlefont=dict(family="Courier", size=20, color="white"),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="outside",
                tickfont=dict(color="white"),
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(family="Courier", size=20, color="white"),
            ),
            barmode="stack",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/postion_bydate_dark.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )

        del dfdata3
        gc.collect()

        # TOP5行业仓位变化趋势
        sparkdata5 = spark.sql(
            """
            WITH tmp AS ( 
                SELECT industry
                    ,cnt 
                FROM ( 
                    SELECT industry, count(*) AS cnt FROM temp GROUP BY industry) t
                ORDER BY cnt DESC LIMIT 5
            ), tmp1 AS (
                SELECT temp_timeseries.buy_date
                    ,tmp.industry
                    ,tmp.cnt
                FROM temp_timeseries JOIN tmp ON 1=1
            ), tmp2 AS (
                SELECT t1.symbol
                    ,t2.industry
                    ,t1.date
                    ,t1.pnl
                FROM temp3 t1 JOIN temp2 t2 ON t1.symbol = t2.symbol
                WHERE t1.date >= DATE_ADD('{}', -60)
            ) 
            SELECT t1.buy_date
                ,t1.industry
                ,SUM(IF(t2.symbol IS NOT NULL, 1, 0)) AS total_cnt
            FROM tmp1 t1 LEFT JOIN tmp2 t2 ON t1.industry = t2.industry AND t1.buy_date = t2.date
            GROUP BY t1.buy_date, t1.industry
            """.format(
                end_date
            )
        )
        dfdata5 = sparkdata5.toPandas()
        dfdata5.sort_values(
            by=["buy_date", "total_cnt"], ascending=[False, False], inplace=True
        )

        fig = px.area(
            dfdata5,
            x="buy_date",
            y="total_cnt",
            color="industry",
            line_group="industry",
        )
        # light mode
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color="black"),
            showline=True,
            gridcolor="rgba(0, 0, 0, 0.5)",
            title_font=dict(size=20, family="Courier", color="black"),
        )
        fig.update_yaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color="black"),
            showline=True,
            gridcolor="rgba(0, 0, 0, 0.5)",
            title_font=dict(size=20, family="Courier", color="black"),
        )
        fig.update_layout(
            title="Last 60 days top5 positions ",
            title_font=dict(size=20, color="black"),
            legend_title_text="",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=20, family="Courier", color="black"),
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/postion_byindustry&date_light.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color="white"),
            showline=True,
            gridcolor="rgba(255, 255, 255, 0.5)",
            title_font=dict(size=20, family="Courier", color="white"),
        )
        fig.update_yaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color="white"),
            showline=True,
            gridcolor="rgba(255, 255, 255, 0.5)",
            title_font=dict(size=20, family="Courier", color="white"),
        )
        fig.update_layout(
            title="Last 60 days top5 positions ",
            title_font=dict(size=20, color="white"),
            legend_title_text="",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=20, family="Courier", color="white"),
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/postion_byindustry&date_dark.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )

        del dfdata5
        gc.collect()
        # TOP5行业PnL变化趋势
        sparkdata6 = spark.sql(
            """
            WITH tmp AS ( 
                SELECT industry
                    ,pl 
                FROM ( 
                    SELECT industry, sum(`p&l`) AS pl FROM temp GROUP BY industry) t 
                ORDER BY pl DESC LIMIT 5
            ), tmp1 AS (
                SELECT temp_timeseries.buy_date
                    ,tmp.industry
                    ,tmp.pl
                FROM temp_timeseries JOIN tmp ON 1=1
            ), tmp2 AS (
                SELECT t1.symbol
                    ,t2.industry
                    ,t1.date
                    ,t1.pnl
                FROM temp3 t1 JOIN temp2 t2 ON t1.symbol = t2.symbol
                WHERE t1.date >= DATE_ADD('{}', -60)
            )   
            SELECT t1.buy_date
                ,t1.industry
                ,SUM(COALESCE(t2.pnl, 0)) AS pnl
            FROM tmp1 t1 LEFT JOIN tmp2 t2 ON t1.industry = t2.industry AND t1.buy_date = t2.date
            GROUP BY t1.buy_date, t1.industry
            """.format(
                end_date
            )
        )
        dfdata6 = sparkdata6.toPandas()
        dfdata6.sort_values(
            by=["buy_date", "pnl"], ascending=[False, False], inplace=True
        )

        fig = px.area(
            dfdata6, x="buy_date", y="pnl", color="industry", line_group="industry"
        )
        # light mode
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color="black"),
            showline=True,
            gridcolor="rgba(0, 0, 0, 0.5)",
            title_font=dict(size=20, family="Courier", color="black"),
        )
        fig.update_yaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color="black"),
            showline=True,
            gridcolor="rgba(0, 0, 0, 0.5)",
            title_font=dict(size=20, family="Courier", color="black"),
        )
        fig.update_layout(
            title="Last 60 days top5 pnl ",
            title_font=dict(size=20, color="black"),
            legend_title_text="",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=20, family="Courier", color="black"),
            ),
            plot_bgcolor="rgba(0, 0, 0, 0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/postion_byindustry&p&l_light.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color="white"),
            showline=True,
            gridcolor="rgba(255, 255, 255, 0.5)",
            title_font=dict(size=20, family="Courier", color="white"),
        )
        fig.update_yaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color="white"),
            showline=True,
            gridcolor="rgba(255, 255, 255, 0.5)",
            title_font=dict(size=20, family="Courier", color="white"),
        )
        fig.update_layout(
            title="Last 60 days top5 pnl ",
            title_font=dict(size=20, color="white"),
            legend_title_text="",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=20, family="Courier", color="white"),
            ),
            plot_bgcolor="rgba(0, 0, 0, 0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/postion_byindustry&p&l_dark.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        del dfdata6
        gc.collect()

        spark.stop()

        if self.market == "us":
            subject = "US Stock Market Trends"
            image_path_return_light = "./images/TRdraw_light.png"
            image_path_return_dark = "./images/TRdraw_dark.png"
        elif self.market == "cn":
            subject = "CN Stock Market Trends"
            image_path_return_light = "./images/CNTRdraw_light.png"
            image_path_return_dark = "./images/CNTRdraw_dark.png"
        image_path = [
            "./images/postion_byindustry_light.png",
            "./images/postion_byindustry_dark.png",
            "./images/postion_byp&l_light.png",
            "./images/postion_byp&l_dark.png",
            "./images/postion_bydate_light.png",
            "./images/postion_bydate_dark.png",
            "./images/postion_byindustry&date_light.png",
            "./images/postion_byindustry&date_dark.png",
            "./images/postion_byindustry&p&l_light.png",
            "./images/postion_byindustry&p&l_dark.png",
            image_path_return_light,
            image_path_return_dark,
        ]
        html_img = """
                <html>
                    <head>
                        <style>
                            body {
                                font-family: Arial, sans-serif;
                                background-color: white; /* 设置默认背景颜色为白色 */
                                color: black; /* 设置默认字体颜色为黑色 */
                            }

                            figure {
                                margin: 0;
                                padding: 5px;
                                border-radius: 8px;
                                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                            }

                            img {
                                width: 100%;
                                height: auto;
                                border-radius: 2px;
                            }

                            figcaption {
                                padding: 5px;
                                text-align: center;
                                font-style: italic;
                                font-size: 24px;
                            }

                            /* Light Mode */
                            @media (prefers-color-scheme: light) {
                                body {
                                    background-color: white;
                                    color: black;
                                }

                                figure {
                                    border: 1px solid #ddd;
                                }
                            }

                            /* Dark Mode */
                            @media (prefers-color-scheme: dark) {
                                body {
                                    background-color: black;
                                    color: white;
                                }

                                figure {
                                    border: 1px solid #444;
                                }
                            }
                        </style>
                    </head>
                    <body>
                        <picture>
                            <!-- 深色模式下的图片 -->
                            <source srcset="cid:image1" media="(prefers-color-scheme: dark)" alt="The industry distribution of current positions is as follows:" style="width:100%"/>
                            <!-- 默认模式下的图片 -->
                            <img src="cid:image0" alt="The industry distribution of current positions is as follows:" style="width:100%">
                            <figcaption> The industry position distribution of the top 10 shows the current distribution of industry
                                        positions that meet the strategy.</figcaption>
                        </picture>
                        <picture>
                            <!-- 深色模式下的图片 -->
                            <source srcset="cid:image3" media="(prefers-color-scheme: dark)" alt="The industry distribution of current pnl is as follows:" style="width:100%"/>
                            <!-- 默认模式下的图片 -->
                            <img src="cid:image2" alt="The industry distribution of current pnl is as follows:" style="width:100%">
                            <figcaption>The industry pnl distribution of the top 10 shows the current distribution of industry
                                        pnl that meet the strategy.</figcaption>
                        </picture>
                        <picture>
                            <!-- 深色模式下的图片 -->
                            <source srcset="cid:image5" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days trade detail info:" style="width:100%"/>
                            <!-- 默认模式下的图片 -->
                            <img src="cid:image4" alt="The diagram shows the last x days trade detail info:" style="width:100%">
                            <figcaption>The diagram shows the last x days trade detail info, which include the short/long/position
                                        info every day.</figcaption>
                        </picture>
                        <picture>
                            <!-- 深色模式下的图片 -->
                            <source srcset="cid:image7" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days top5 industry position info:" style="width:100%"/>
                            <!-- 默认模式下的图片 -->
                            <img src="cid:image6" alt="The diagram shows the last x days top5 industry position info:" style="width:100%">
                            <figcaption>The diagram shows the last x days top5 industry position trend, to stat last x days
                                        the top5 industry positions change status</figcaption>
                        </picture>
                        <picture>
                            <!-- 深色模式下的图片 -->
                            <source srcset="cid:image9" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days top5 industry pnl info:" style="width:100%"/>
                            <!-- 默认模式下的图片 -->
                            <img src="cid:image8" alt="The diagram shows the last x days top5 industry pnl info:" style="width:100%">
                            <figcaption>The diagram shows the last x days top5 industry pnl trend, to stat last x days
                                        the top5 industry pnl change status</figcaption>
                        </picture>
                        <picture>
                            <!-- 深色模式下的图片 -->
                            <source srcset="cid:image11" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%"/>
                            <!-- 默认模式下的图片 -->
                            <img src="cid:image10" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%">
                            <figcaption>The diagram shows the last x years cumulative return and max drawdown trend,
                                        to track the stock market and stategy execution information</figcaption>
                        </picture>
                    </body>
                </html>
                """
        html_txt = """
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <style>
                            h1 {{
                                font-style: italic;
                            }}
                        </style>
                    </head>                    
                    <body>
                        <h1>The current cash is {cash}, the final portfolio value is {final_value}, the number of backtesting list is {stock_cnt}</h1>
                    </body>
                    </html>
                    """.format(
            cash=cash, final_value=final_value, stock_cnt = len(stock_list)
        )

        MyEmail().send_email_embedded_image(
            subject, html_txt + html + html_img + html1 + html2, image_path
        )

    def send_etf_btstrategy_by_email(self, cash, final_value):
        """
        发送邮件
        """
        # 启动Spark Session
        spark = (
            SparkSession.builder.master("local")
            .appName("SparkTest")
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", "1")

        """ 
        读取交易相关数据，交易明细，持仓明细，仓位日志明细，行业信息
        """
        file = FileInfo(self.trade_date, self.market)
        # 最新一日股票信息
        file_name_day = file.get_file_path_latest
        df_d = pd.read_csv(file_name_day, usecols=[i for i in range(1, 3)])
        # 持仓明细, pandas读取
        file_cur_p = file.get_file_path_etf_position
        df_cur_p = pd.read_csv(file_cur_p, usecols=[i for i in range(1, 8)])
        # 交易明细
        file_path_trade = file.get_file_path_etf_trade
        cols = ["idx", "symbol", "date",
                "trade_type", "price", "size", "strategy"]
        df1 = spark.read.csv(file_path_trade, header=None, inferSchema=True)
        df1 = df1.toDF(*cols)
        df1.createOrReplaceTempView("temp1")
        # 持仓明细, spark读取
        df = spark.read.csv(file_cur_p, header=True)
        df.createOrReplaceTempView("temp")
        # 仓位日志明细
        file_path_position_detail = file.get_file_path_etf_position_detail
        cols = ["idx", "symbol", "date", "price", "adjbase", "pnl"]
        df3 = spark.read.csv(file_path_position_detail,
                             header=None, inferSchema=True)
        df3 = df3.toDF(*cols)
        df3.createOrReplaceTempView("temp3")
        # 当日股票信息
        cols = [
            "idx",
            "symbol",
            "name",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "turnover",
            "chg",
            "change",
            "amplitude",
            "preclose",
            "total_value",
            "circulation_value",
            "date",
        ]
        df4 = spark.read.csv(file_name_day, header=None, inferSchema=True)
        df4 = df4.toDF(*cols)
        df4.createOrReplaceTempView("temp4")

        # 生成时间序列，用于时间序列补齐
        end_date = pd.to_datetime(self.trade_date).strftime("%Y-%m-%d")
        start_date = pd.to_datetime(end_date) - pd.DateOffset(days=60)
        date_range = pd.date_range(
            start=start_date.strftime("%Y-%m-%d"), end=end_date, freq="D"
        )
        df_timeseries = pd.DataFrame({"buy_date": date_range})
        # 将日期转换为字符串格式 'YYYYMMDD'
        df_timeseries["trade_date"] = df_timeseries["buy_date"].dt.strftime(
            "%Y%m%d")

        # 假设您的ToolKit类已经定义好了，并且可以检查交易日期
        toolkit = ToolKit("identify trade date")

        # 根据市场类型过滤非交易日
        if self.market == "us":
            df_timeseries = df_timeseries[
                df_timeseries["trade_date"].apply(toolkit.is_us_trade_date)
            ]
        elif self.market == "cn":
            df_timeseries = df_timeseries[
                df_timeseries["trade_date"].apply(toolkit.is_cn_trade_date)
            ]

        df_timeseries_spark = spark.createDataFrame(
            df_timeseries.astype({"buy_date": "string"})
        )

        df_timeseries_spark.createOrReplaceTempView("temp_timeseries")

        """
        持仓明细历史交易情况分析
        """
        df_np = pd.merge(df_cur_p, df_d, how="inner", on="symbol")

        df_np_spark = spark.createDataFrame(df_np)
        df_np_spark.createOrReplaceTempView("temp_symbol")

        sparkdata8 = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT symbol
                    ,date
                    ,trade_type
                    ,price
                    ,size
                    ,strategy
                    ,l_date
                    ,l_trade_type
                    ,l_price
                    ,l_size
                    ,l_strategy
                FROM (
                    SELECT symbol
                        ,date
                        ,trade_type
                        ,price
                        ,size
                        ,strategy
                        ,IF(trade_type = 'sell', LAG(date) OVER (PARTITION BY symbol ORDER BY date)  
                            , LEAD(date) OVER (PARTITION BY symbol ORDER BY date)) AS l_date
                        ,IF(trade_type = 'sell', LAG(trade_type) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(trade_type) OVER (PARTITION BY symbol ORDER BY date)) AS l_trade_type
                        ,IF(trade_type = 'sell', LAG(price) OVER (PARTITION BY symbol ORDER BY date)
                            , LEAD(price) OVER (PARTITION BY symbol ORDER BY date)) AS l_price
                        ,IF(trade_type = 'sell', LAG(size) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(size) OVER (PARTITION BY symbol ORDER BY date)) AS l_size
                        ,IF(trade_type = 'sell', LAG(strategy) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(strategy) OVER (PARTITION BY symbol ORDER BY date)) AS l_strategy                               
                    FROM temp1
                    ORDER BY symbol
                        ,date
                        ,trade_type) t
            ), tmp11 AS (
                SELECT symbol
                    ,l_date AS buy_date
                    ,l_price AS base_price
                    ,l_size AS base_size
                    ,l_strategy AS buy_strategy
                    ,date AS sell_date
                    ,price AS adj_price
                    ,size AS adj_size
                    ,strategy AS sell_strategy
                FROM tmp1 WHERE trade_type = 'sell' AND date >= DATE_ADD('{}', -180)
                UNION ALL
                SELECT symbol
                    ,date AS buy_date
                    ,price AS base_price
                    ,size AS base_size
                    ,strategy AS buy_strategy
                    ,null AS sell_date
                    ,null AS adj_price
                    ,null AS adj_size
                    ,null AS sell_strategy
                FROM tmp1 WHERE trade_type = 'buy' AND l_date IS NULL
            ), tmp2 AS (
                SELECT symbol
                    ,COUNT(symbol) AS his_trade_cnt
                    ,SUM(IF(sell_date IS NOT NULL, DATEDIFF(sell_date, buy_date), DATEDIFF('{}', buy_date))) AS his_days
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price >=0, 1, 0)) AS pos_cnt
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price < 0, 1, 0)) AS neg_cnt
                    ,SUM(IF(sell_date IS NOT NULL, adj_price * (-adj_size) - base_price * base_size, 0)) AS his_pnl
                    ,SUM(IF(sell_date IS NOT NULL, base_price * base_size, 0)) AS his_base_price
                    ,MAX(IF(sell_date IS NULL, buy_strategy, null)) AS buy_strategy
                FROM  tmp11
                GROUP BY symbol
            ), tmp3 AS (
                SELECT symbol
                    , buy_date
                    , price
                    , adjbase
                    , size
                    , `p&l` as pnl
                    , `p&l_ratio` as pnl_ratio
                    , name
                FROM temp_symbol
            )
            SELECT t1.symbol
                , t1.buy_date
                , t1.price
                , t1.adjbase
                , t1.pnl
                , t1.pnl_ratio
                , t1.name
                , COALESCE(t2.his_trade_cnt, 0) AS avg_trans
                , COALESCE(t2.his_days, 0) / t2.his_trade_cnt AS avg_days
                , (t1.pos_cnt + COALESCE(t2.pos_cnt,0)) / ( COALESCE(t2.pos_cnt,0) + COALESCE(t2.neg_cnt,0) + t1.pos_cnt + t1.neg_cnt) AS win_rate
                , (COALESCE(t2.his_pnl,0) + (t1.adjbase - t1.price) * t1.size) / (COALESCE(t2.his_base_price,0) + t1.price * t1.size) AS total_pnl_ratio
                , t2.buy_strategy
            FROM (
                SELECT symbol
                , buy_date
                , price
                , adjbase
                , size
                , pnl
                , pnl_ratio
                , name
                , IF(adjbase >= price, 1, 0) AS pos_cnt
                , IF(adjbase < price, 1, 0) AS neg_cnt
                FROM tmp3
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol
            """.format(
                end_date, end_date
            )
        )

        dfdata8 = sparkdata8.toPandas()

        dfdata8.rename(
            columns={
                "symbol": "SYMBOL",
                "buy_date": "OPEN DATE",
                "price": "BASE",
                "adjbase": "ADJBASE",
                "pnl": "PNL",
                "pnl_ratio": "PNL RATIO",
                "avg_trans": "AVG TRANS",
                "avg_days": "AVG DAYS",
                "win_rate": "WIN RATE",
                "total_pnl_ratio": "TOTAL PNL RATIO",
                "name": "NAME",
                "buy_strategy": "Strategy",
            },
            inplace=True,
        )
        cm = sns.color_palette("coolwarm", as_cmap=True)
        # 创建样式函数，用于突出显示具有特定日期值的行
        old_date = datetime.strptime(self.trade_date, "%Y%m%d").date()

        # 将日期减去一天
        new_date = old_date - timedelta(days=5)

        df_timeseries_sorted = df_timeseries.sort_values(
            by="buy_date", ascending=False)
        new_date_str = str(
            df_timeseries_sorted.iloc[4]["buy_date"].strftime("%Y-%m-%d"))

        # 将新日期转换为字符串
        # new_date_str = new_date.strftime("%Y-%m-%d")

        def highlight_row(row):
            if row["OPEN DATE"] >= new_date_str:
                return ["background-color: orange"] * len(row)
            else:
                return [""] * len(row)

        html = (
            "<h2>Open Position List</h2>"  # 添加标题
            "<table>"
            + dfdata8.style.hide(axis=1, subset=["PNL"])
            .format(
                {
                    "BASE": "{:.2f}",
                    "ADJBASE": "{:.2f}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.2f}",
                    "AVG DAYS": "{:.0f}",
                    "WIN RATE": "{:.2%}",
                    "TOTAL PNL RATIO": "{:.2%}",
                }
            )
            .apply(highlight_row, axis=1)
            .background_gradient(subset=["BASE", "ADJBASE"], cmap=cm)
            .bar(
                subset=["PNL RATIO", "TOTAL PNL RATIO"],
                align="mid",
                color=["#99CC66", "#FF6666"],
                vmin=-0.8,
                vmax=0.8,
            )
            .bar(
                subset=["WIN RATE"],
                align="left",
                color=["#99CC66", "#FF6666"],
                vmin=0,
                vmax=0.8,
            )
            .set_properties(
                **{
                    "text-align": "left",
                    "border": "1px solid #ccc",
                    "cellspacing": "0",
                    "style": "border-collapse: collapse; ",
                }
            )
            .set_table_styles(
                [
                    # 表头样式
                    dict(
                        selector="th",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "8px"),  # 增加填充以便更易点击和阅读
                            ("font-size", "18px"),  # 在PC端使用较大字体
                        ],
                    ),
                    # 表格数据单元格样式
                    dict(
                        selector="td",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "8px"),
                            (
                                "font-size",
                                "18px",
                            ),  # 同样适用较大字体以提高移动端可读性
                        ],
                    ),
                ]
            )
            .set_table_styles(
                {
                    "NAME": [
                        {
                            "selector": "th",
                            "props": [
                                ("min-width", "150px"),
                                ("max-width", "300px"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [
                                ("min-width", "150px"),
                                ("max-width", "300px"),
                            ],
                        },
                    ],
                },
                overwrite=False,
            )
            .set_sticky(axis="columns")
            .to_html(doctype_html=True, escape=False)
            + "<table>"
        )

        css = """
            <style>
                :root {
                    color-scheme: dark light;
                    supported-color-schemes: dark light;
                    background-color: white;
                    color: black;
                    display: table ;
                }                
                /* Your light mode (default) styles: */
                body {
                    background-color: white;
                    color: black;
                    display: table ;
                    width: 100%;                          
                }
                table {
                    background-color: white;
                    color: black;
                    width: 100%;
                }
                @media (prefers-color-scheme: dark) {            
                    body {
                        background-color: black;
                        color: white;
                        display: table ;
                        width: 100%;                              
                    }
                    table {
                        background-color: black;
                        color: white;
                        width: 100%;
                    }
                }
            </style>
        """

        html = css + html

        del df_np
        gc.collect()

        """
        减仓情况分析
        """
        sparkdata9 = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT symbol
                    ,date
                    ,trade_type
                    ,price
                    ,size
                    ,strategy
                    ,l_date
                    ,l_trade_type
                    ,l_price
                    ,l_size
                    ,l_strategy
                FROM (
                    SELECT symbol
                        ,date
                        ,trade_type
                        ,price
                        ,size
                        ,strategy
                        ,IF(trade_type = 'sell', LAG(date) OVER (PARTITION BY symbol ORDER BY date)  
                            , LEAD(date) OVER (PARTITION BY symbol ORDER BY date)) AS l_date
                        ,IF(trade_type = 'sell', LAG(trade_type) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(trade_type) OVER (PARTITION BY symbol ORDER BY date)) AS l_trade_type
                        ,IF(trade_type = 'sell', LAG(price) OVER (PARTITION BY symbol ORDER BY date)
                            , LEAD(price) OVER (PARTITION BY symbol ORDER BY date)) AS l_price
                        ,IF(trade_type = 'sell', LAG(size) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(size) OVER (PARTITION BY symbol ORDER BY date)) AS l_size
                        ,IF(trade_type = 'sell', LAG(strategy) OVER (PARTITION BY symbol ORDER BY date) 
                            , LEAD(strategy) OVER (PARTITION BY symbol ORDER BY date)) AS l_strategy                               
                    FROM temp1
                    ORDER BY symbol
                        ,date
                        ,trade_type) t
                WHERE date >= DATE_ADD('{}', -180)
            ), tmp11 AS (
                SELECT symbol
                    ,l_date AS buy_date
                    ,l_price AS base_price
                    ,l_size AS base_size
                    ,l_strategy AS buy_strategy
                    ,date AS sell_date
                    ,price AS adj_price
                    ,size AS adj_size
                    ,strategy AS sell_strategy
                    ,ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY l_date DESC) AS row_num
                FROM tmp1 WHERE trade_type = 'sell'
            ), tmp2 AS (
                SELECT symbol
                    ,COUNT(symbol) AS his_trade_cnt
                    ,SUM(DATEDIFF(sell_date, buy_date)) AS his_days
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price >=0, 1, 0)) AS pos_cnt
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price < 0, 1, 0)) AS neg_cnt
                    ,SUM(IF(sell_date IS NOT NULL, adj_price * (-adj_size) - base_price * base_size, 0)) AS his_pnl
                    ,SUM(IF(sell_date IS NOT NULL, base_price * base_size, 0)) AS his_base_price
                    ,MAX(sell_strategy) AS sell_strategy
                FROM  tmp11
                GROUP BY symbol
            ), tmp3 AS (
                SELECT symbol
                    ,name
                FROM temp4
                GROUP BY symbol
                    ,name
            )
            SELECT t1.symbol
                , t1.buy_date
                , t1.sell_date
                , t1.base_price AS price
                , t1.adj_price AS adjbase
                , t1.pnl
                , t1.pnl_ratio
                , t3.name
                , COALESCE(t2.his_trade_cnt, 0) AS avg_trans
                , COALESCE(t2.his_days, 0) / t2.his_trade_cnt AS avg_days
                , COALESCE(t2.pos_cnt,0) / (COALESCE(t2.pos_cnt,0) + COALESCE(t2.neg_cnt,0)) AS win_rate
                , COALESCE(t2.his_pnl,0) / COALESCE(t2.his_base_price,0) AS total_pnl_ratio
                , t2.sell_strategy
            FROM (
                SELECT symbol
                    , buy_date
                    , sell_date
                    , base_price
                    , adj_price
                    , adj_price * (-adj_size) - base_price * base_size AS pnl
                    , (adj_price - base_price) / base_price AS pnl_ratio
                FROM tmp11 WHERE sell_date >= (SELECT buy_date FROM (
                                                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                                                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t 
                                                ) tt WHERE row_num = 5)
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol 
                LEFT JOIN tmp3 t3 ON t1.symbol = t3.symbol
            """.format(
                end_date, end_date
            )
        )

        dfdata9 = sparkdata9.toPandas()

        if not dfdata9.empty:

            dfdata9.rename(
                columns={
                    "symbol": "SYMBOL",
                    "buy_date": "OPEN DATE",
                    "sell_date": "CLOSE DATE",
                    "price": "BASE",
                    "adjbase": "ADJBASE",
                    "pnl": "PNL",
                    "pnl_ratio": "PNL RATIO",
                    "avg_trans": "AVG TRANS",
                    "avg_days": "AVG DAYS",
                    "win_rate": "WIN RATE",
                    "total_pnl_ratio": "TOTAL PNL RATIO",
                    "name": "NAME",
                    "sell_strategy": "Strategy",
                },
                inplace=True,
            )
            cm = sns.color_palette("coolwarm", as_cmap=True)

            html2 = (
                "<h2>Close Position List Last 5 Days</h2>"  # 添加标题
                "<table>"
                + dfdata9.style.hide(axis=1, subset=["PNL"])
                .format(
                    {
                        "BASE": "{:.2f}",
                        "ADJBASE": "{:.2f}",
                        "PNL RATIO": "{:.2%}",
                        "AVG TRANS": "{:.2f}",
                        "AVG DAYS": "{:.0f}",
                        "WIN RATE": "{:.2%}",
                        "TOTAL PNL RATIO": "{:.2%}",
                    }
                )
                .background_gradient(subset=["BASE", "ADJBASE"], cmap=cm)
                .bar(
                    subset=["PNL RATIO", "TOTAL PNL RATIO"],
                    align="mid",
                    color=["#99CC66", "#FF6666"],
                    vmin=-0.8,
                    vmax=0.8,
                )
                .bar(
                    subset=["WIN RATE"],
                    align="left",
                    color=["#99CC66", "#FF6666"],
                    vmin=0,
                    vmax=0.8,
                )
                .set_properties(
                    **{
                        "text-align": "left",
                        "border": "1px solid #ccc",
                        "cellspacing": "0",
                        "style": "border-collapse: collapse; ",
                    }
                )
                .set_table_styles(
                    [
                        # 表头样式
                        dict(
                            selector="th",
                            props=[
                                ("border", "1px solid #ccc"),
                                ("text-align", "left"),
                                ("padding", "8px"),  # 增加填充以便更易点击和阅读
                                ("font-size", "18px"),  # 在PC端使用较大字体
                            ],
                        ),
                        # 表格数据单元格样式
                        dict(
                            selector="td",
                            props=[
                                ("border", "1px solid #ccc"),
                                ("text-align", "left"),
                                ("padding", "8px"),
                                (
                                    "font-size",
                                    "18px",
                                ),  # 同样适用较大字体以提高移动端可读性
                            ],
                        ),
                    ]
                )
                .set_table_styles(
                    {
                        "NAME": [
                            {
                                "selector": "th",
                                "props": [
                                    ("min-width", "150px"),
                                    ("max-width", "300px"),
                                ],
                            },
                            {
                                "selector": "td",
                                "props": [
                                    ("min-width", "150px"),
                                    ("max-width", "300px"),
                                ],
                            },
                        ],
                    },
                    overwrite=False,
                )
                .set_sticky(axis="columns")
                .to_html(doctype_html=True, escape=False)
                + "</table>"
            )

            css2 = """
                <style>
                    :root {
                        color-scheme: dark light;
                        supported-color-schemes: dark light;
                        background-color: white;
                        color: black;
                        display: table ;
                    }                
                    /* Your light mode (default) styles: */
                    body {
                        background-color: white;
                        color: black;
                        display: table ;
                        width: 100%;                          
                    }
                    table {
                        background-color: white;
                        color: black;
                        width: 100%;
                    }
                    @media (prefers-color-scheme: dark) {            
                        body {
                            background-color: black;
                            color: white;
                            display: table ;
                            width: 100%;                              
                        }
                        table {
                            background-color: black;
                            color: white;
                            width: 100%;
                        }
                    }
                </style>
            """
            html2 = css2 + html2
        else:
            html2 = ""

        del dfdata9
        del dfdata8
        gc.collect()

        # 60天内交易明细分析
        sparkdata3 = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT date
                    ,COUNT(symbol) AS total_cnt
                FROM temp3
                WHERE date >= DATE_ADD('{}', -60)
                GROUP BY date
            ), tmp11 AS (
                SELECT temp_timeseries.buy_date
                    ,IF(tmp1.total_cnt > 0
                    ,tmp1.total_cnt
                    ,LAST_VALUE(tmp1.total_cnt) IGNORE NULLS OVER (PARTITION BY "AAA" ORDER BY temp_timeseries.buy_date)) AS total_cnt
                FROM temp_timeseries LEFT JOIN tmp1 ON temp_timeseries.buy_date = tmp1.date
            ), tmp5 AS (
                SELECT date
                    ,SUM(IF(trade_type = 'buy', 1, 0)) AS buy_cnt
                    ,SUM(IF(trade_type = 'sell', 1, 0)) AS sell_cnt
                FROM temp1 
                WHERE date >= DATE_ADD('{}', -60)
                GROUP BY date
            )
            SELECT t1.buy_date AS buy_date
                ,t1.total_cnt AS total_cnt
                ,t2.buy_cnt AS buy_cnt
                ,t2.sell_cnt AS sell_cnt
            FROM tmp11 t1 LEFT JOIN tmp5 t2 ON t1.buy_date = t2.date
            """.format(
                end_date, end_date
            )
        )
        dfdata3 = sparkdata3.toPandas()

        # 设置图像的宽度和高度（例如，1920x1080像素）
        fig_width, fig_height = 1280, 720
        # 设置缩放系数，例如2，3等，这将相应地增加图像的分辨率
        scale_factor = 1
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=dfdata3["buy_date"],
                y=dfdata3["total_cnt"],
                mode="lines+markers",
                name="total stock",
                line=dict(color="red", width=3),
                yaxis="y",
            )
        )
        fig.add_trace(
            go.Bar(
                x=dfdata3["buy_date"],
                y=dfdata3["buy_cnt"],
                name="long",
                marker_color="red",
                yaxis="y2",
            )
        )
        fig.add_trace(
            go.Bar(
                x=dfdata3["buy_date"],
                y=dfdata3["sell_cnt"],
                name="short",
                marker_color="green",
                yaxis="y2",
            )
        )
        # light mode
        fig.update_layout(
            title={
                "text": "Last 60 days trade info",
                "y": 0.95,
                "x": 0.05,
                "xanchor": "left",
                "yanchor": "top",
                "font": dict(family="Courier", size=20, color="black"),
            },
            xaxis=dict(
                title="Trade Date",
                titlefont=dict(family="Courier", size=20, color="black"),
                mirror=True,
                ticks="outside",
                tickfont=dict(color="black"),
                showline=True,
                gridcolor="rgba(0, 0, 0, 0.5)",
            ),
            yaxis=dict(
                title="Total Positions",
                titlefont=dict(family="Courier", size=20, color="black"),
                side="left",
                mirror=True,
                ticks="outside",
                tickfont=dict(color="black"),
                showline=True,
                gridcolor="rgba(0, 0, 0, 0.5)",
            ),
            yaxis2=dict(
                title="Positions per day",
                titlefont=dict(family="Courier", size=20, color="black"),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="outside",
                tickfont=dict(color="black"),
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(family="Courier", size=20, color="black"),
            ),
            barmode="stack",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/cnetf_postion_bydate_light.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        fig.update_layout(
            title={
                "text": "Last 60 days trade info",
                "y": 0.95,
                "x": 0.05,
                "xanchor": "left",
                "yanchor": "top",
                "font": dict(family="Courier", size=20, color="white"),
            },
            xaxis=dict(
                title="Trade Date",
                titlefont=dict(family="Courier", size=20, color="white"),
                mirror=True,
                ticks="outside",
                tickfont=dict(color="white"),
                showline=True,
                gridcolor="rgba(255, 255, 255, 0.5)",
            ),
            yaxis=dict(
                title="Total Positions",
                titlefont=dict(family="Courier", size=20, color="white"),
                side="left",
                mirror=True,
                ticks="outside",
                tickfont=dict(color="white"),
                showline=True,
                gridcolor="rgba(255, 255, 255, 0.5)",
            ),
            yaxis2=dict(
                title="Positions per day",
                titlefont=dict(family="Courier", size=20, color="white"),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="outside",
                tickfont=dict(color="white"),
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(family="Courier", size=20, color="white"),
            ),
            barmode="stack",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        fig.write_image(
            "./images/cnetf_postion_bydate_dark.png",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )

        del dfdata3
        gc.collect()

        spark.stop()

        if self.market == "us":
            subject = "US Stock Market ETF Trends"
            image_path_return_light = "./images/ETFTRdraw_light.png"
            image_path_return_dark = "./images/ETFTRdraw_dark.png"
        elif self.market == "cn":
            subject = "CN Stock Market ETF Trends"
            image_path_return_light = "./images/CNETFTRdraw_light.png"
            image_path_return_dark = "./images/CNETFTRdraw_dark.png"
        image_path = [
            "./images/cnetf_postion_bydate_light.png",
            "./images/cnetf_postion_bydate_dark.png",
            image_path_return_light,
            image_path_return_dark,
        ]
        html_img = """
                <html>
                    <head>
                        <style>
                            body {
                                font-family: Arial, sans-serif;
                                background-color: white; /* 设置默认背景颜色为白色 */
                                color: black; /* 设置默认字体颜色为黑色 */
                            }

                            figure {
                                margin: 0;
                                padding: 5px;
                                border-radius: 8px;
                                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                            }

                            img {
                                width: 100%;
                                height: auto;
                                border-radius: 2px;
                            }

                            figcaption {
                                padding: 5px;
                                text-align: center;
                                font-style: italic;
                                font-size: 24px;
                            }

                            /* Light Mode */
                            @media (prefers-color-scheme: light) {
                                body {
                                    background-color: white;
                                    color: black;
                                }

                                figure {
                                    border: 1px solid #ddd;
                                }
                            }

                            /* Dark Mode */
                            @media (prefers-color-scheme: dark) {
                                body {
                                    background-color: black;
                                    color: white;
                                }

                                figure {
                                    border: 1px solid #444;
                                }
                            }
                        </style>
                    </head>
                    <body>
                        <picture>
                            <!-- 深色模式下的图片 -->
                            <source srcset="cid:image1" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days trade detail info:" style="width:100%"/>
                            <!-- 默认模式下的图片 -->
                            <img src="cid:image0" alt="The diagram shows the last x days trade detail info:" style="width:100%">
                            <figcaption>The diagram shows the last x days trade detail info, which include the short/long/position
                                        info every day.</figcaption>
                        </picture>
                        <picture>
                            <!-- 深色模式下的图片 -->
                            <source srcset="cid:image3" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%"/>
                            <!-- 默认模式下的图片 -->
                            <img src="cid:image2" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%">
                            <figcaption>The diagram shows the last x years cumulative return and max drawdown trend,
                                        to track the stock market and stategy execution information</figcaption>
                        </picture>                        
                    </body>
                </html>
                """
        html_txt = """
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <style>
                            h1 {{
                                font-style: italic;
                            }}
                        </style>
                    </head>                    
                    <body>
                        <h1>The current cash is {cash}, the final portfolio value is {final_value}</h1>
                    </body>
                    </html>
                    """.format(
            cash=cash, final_value=final_value
        )

        MyEmail().send_email_embedded_image(
            subject, html_txt + html + html2 + html_img, image_path
        )
