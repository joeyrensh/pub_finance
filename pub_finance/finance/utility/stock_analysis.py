#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from utility.email_uti import MyEmail
from utility.fileinfo import FileInfo
from utility.tickerinfo import TickerInfo
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession
import plotly.graph_objects as go
import plotly.express as px
import gc
from utility.toolkit import ToolKit
import numpy as np
from matplotlib.colors import to_rgb
import re
from plotly.colors import sample_colorscale
from datetime import datetime

# mpl.rcParams["font.sans-serif"] = ["SimHei"]  # 用来正常显示中文标签


def initialize_spark(app_name: str, memory: str = "512m", partitions: int = 1):
    """
    初始化 SparkSession
    """
    return (
        SparkSession.builder.master("local")
        .appName(app_name)
        .config("spark.driver.memory", memory)
        .config("spark.executor.memory", memory)
        .config("spark.sql.shuffle.partitions", str(partitions))
        .getOrCreate()
    )


class StockProposal:
    def __init__(self, market, trade_date):
        self.market = market
        self.trade_date = trade_date

    def send_btstrategy_by_email(self, cash, final_value):
        """
        发送邮件
        """
        # 启动Spark Session
        spark = initialize_spark("StockAnalysis", memory="450m", partitions=1)
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        """ 
        读取交易相关数据，交易明细，持仓明细，仓位日志明细，行业信息
        """
        file = FileInfo(self.trade_date, self.market)
        # 国债信息
        file_gz = file.get_file_path_gz
        cols = ["code", "name", "date", "new"]
        spark_gz = spark.read.csv(file_gz, header=True, inferSchema=True)
        spark_gz = spark_gz.toDF(*cols)
        spark_gz.createOrReplaceTempView("temp_gz")
        # 交易明细
        file_path_trade = file.get_file_path_trade
        cols = ["idx", "symbol", "date", "trade_type", "price", "size", "strategy"]
        spark_transaction_detail = spark.read.csv(
            file_path_trade, header=None, inferSchema=True
        )
        spark_transaction_detail = spark_transaction_detail.toDF(*cols)
        spark_transaction_detail.createOrReplaceTempView("temp_transaction_detail")
        # 持仓明细, spark读取
        file_cur_p = file.get_file_path_position
        spark_cur_position = spark.read.csv(file_cur_p, header=True, inferSchema=True)
        cols = [
            "idx",
            "symbol",
            "buy_date",
            "price",
            "adjbase",
            "size",
            "p&l",
            "p&l_ratio",
            "industry",
        ]
        spark_cur_position = spark_cur_position.toDF(*cols)
        spark_cur_position.createOrReplaceTempView("temp_cur_position")
        pd_cur_position = spark_cur_position[
            [
                "symbol",
                "buy_date",
                "price",
                "adjbase",
                "size",
                "p&l",
                "p&l_ratio",
                "industry",
            ]
        ].toPandas()
        # 行业明细
        file_path_indus = file.get_file_path_industry
        spark_industry_info = spark.read.csv(file_path_indus, header=True)
        spark_industry_info.createOrReplaceTempView("temp_industry_info")
        # 仓位日志明细
        file_path_position_detail = file.get_file_path_position_detail
        cols = ["idx", "symbol", "date", "price", "adjbase", "pnl"]
        spark_position_detail = spark.read.csv(
            file_path_position_detail, header=None, inferSchema=True
        )
        spark_position_detail = spark_position_detail.toDF(*cols)
        spark_position_detail.createOrReplaceTempView("temp_position_detail")
        # 最新一日股票信息
        file_name_day = file.get_file_path_latest
        cols = [
            "symbol",
            "name",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "total_value",
            "pe",
            "date",
        ]
        spark_latest_stock_info = spark.read.csv(
            file_name_day, header=True, inferSchema=True
        )
        spark_latest_stock_info = spark_latest_stock_info.select(cols)
        spark_latest_stock_info.createOrReplaceTempView("temp_latest_stock_info")
        pd_latest_stock_info = spark_latest_stock_info[
            ["name", "symbol", "total_value"]
        ].toPandas()

        # 获取回测股票列表
        stock_list = TickerInfo(self.trade_date, self.market).get_stock_list()
        stock_list_tuples = [(symbol,) for symbol in stock_list]
        spark_stock_list = spark.createDataFrame(stock_list_tuples, schema=["symbol"])
        spark_stock_list.createOrReplaceTempView("temp_stock_list")

        # 生成时间序列，用于时间序列补齐
        end_date = pd.to_datetime(self.trade_date).strftime("%Y-%m-%d")
        start_date = pd.to_datetime(end_date) - pd.DateOffset(days=120)
        date_range = pd.date_range(
            start=start_date.strftime("%Y-%m-%d"), end=end_date, freq="D"
        )
        pd_timeseries = pd.DataFrame({"buy_date": date_range})
        # 将日期转换为字符串格式 'YYYYMMDD'
        pd_timeseries["trade_date"] = pd_timeseries["buy_date"].dt.strftime("%Y%m%d")

        # 根据市场类型过滤非交易日
        toolkit = ToolKit("identify trade date")
        if self.market == "us":
            pd_timeseries = pd_timeseries[
                pd_timeseries["trade_date"].apply(toolkit.is_us_trade_date)
            ]
        elif self.market == "cn":
            pd_timeseries = pd_timeseries[
                pd_timeseries["trade_date"].apply(toolkit.is_cn_trade_date)
            ]

        spark_timeseries = spark.createDataFrame(
            pd_timeseries.astype({"buy_date": "string"})
        )

        spark_timeseries.createOrReplaceTempView("temp_timeseries")

        """ 
        行业板块历史数据分析
        """
        spark_industry_history_tracking = spark.sql(
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
                FROM temp_cur_position
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
                    FROM temp_transaction_detail
                    ORDER BY symbol
                        ,date
                        ,trade_type) t
            ), tmp11 AS (
                SELECT symbol
                    ,l_date AS buy_date
                    ,l_price AS base_price
                    ,l_size AS base_size
                    ,date AS sell_date
                    ,price AS adj_price
                    ,size AS adj_size
                FROM 
                tmp1 WHERE trade_type = 'sell' AND date >= DATE_ADD('{}', -120)
                UNION ALL
                SELECT symbol
                    ,date AS buy_date
                    ,price AS base_price
                    ,size AS base_size
                    ,null AS sell_date
                    ,null AS adj_price
                    ,null AS adj_size
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
                FROM temp_industry_info t1 JOIN tmp11 t2 ON t1.symbol = t2.symbol
                GROUP BY t1.industry
            ), tmp3 AS (
                SELECT industry
                    , COLLECT_LIST(pnl) AS pnl_array
                FROM (
                    SELECT t2.industry, t1.buy_date AS date
                        , SUM(IF(t1.buy_date = t2.date, COALESCE(t2.pnl, 0), 0)) AS pnl
                    FROM temp_timeseries t1 
                    LEFT JOIN  (SELECT t3.industry, t2.date, SUM(t2.pnl) AS pnl FROM temp_position_detail t2 JOIN temp_industry_info t3 ON t2.symbol = t3.symbol 
                                GROUP BY t3.industry, t2.date) t2 ON 1 = 1
                    GROUP BY t2.industry, t1.buy_date
                    ORDER BY t2.industry, t1.buy_date ASC
                    ) t
                GROUP BY industry
            ), tmp4 AS (
                SELECT temp_industry_info.industry, COUNT(temp_industry_info.symbol) AS ticker_cnt
                FROM temp_industry_info JOIN temp_stock_list ON temp_industry_info.symbol = temp_stock_list.symbol
                GROUP BY temp_industry_info.industry
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
            """.format(end_date, end_date)
        )

        spark_industry_history_tracking_lst5days = spark.sql(
            """
            WITH tmp AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
                    WHERE row_num = 1 )
                GROUP BY t2.industry
            ), tmp1 AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
                    WHERE row_num = 5 )
                GROUP BY t2.industry
            )   
            SELECT tmp.industry
                ,tmp.pnl - COALESCE(tmp1.pnl, 0) AS pnl_growth
            FROM tmp LEFT JOIN tmp1 ON tmp.industry = tmp1.industry
            ORDER BY tmp.pnl - COALESCE(tmp1.pnl, 0) DESC
            """
        )
        spark_industry_history_tracking_5daysbeforeyesterday = spark.sql(
            """
            WITH tmp AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
                    WHERE row_num = 1 )
                GROUP BY t2.industry
            ), tmp1 AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
                    WHERE row_num = 2)
                GROUP BY t2.industry
            )
            , tmp2 AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
                    WHERE row_num = 6)
                GROUP BY t2.industry
            )   
            SELECT tmp.industry
                ,COALESCE(tmp1.pnl, 0) - COALESCE(tmp2.pnl, 0) AS pnl_growth
            FROM tmp 
            LEFT JOIN tmp1 ON tmp.industry = tmp1.industry
            LEFT JOIN tmp2 ON tmp.industry = tmp2.industry
            ORDER BY COALESCE(tmp1.pnl, 0) - COALESCE(tmp2.pnl, 0) DESC
            """
        )
        pd_industry_history_tracking_lst5days = (
            spark_industry_history_tracking_lst5days.toPandas()
        )
        pd_industry_history_tracking_5daysbeforeyesterday = (
            spark_industry_history_tracking_5daysbeforeyesterday.toPandas()
        )
        pd_industry_history_tracking = spark_industry_history_tracking.toPandas()

        result_df = (
            pd_industry_history_tracking.merge(
                pd_industry_history_tracking_lst5days, on="industry", how="inner"
            )
            .sort_values(by="pnl_growth", ascending=False)
            .reset_index(drop=True)
        )
        pd_industry_history_tracking = result_df[
            [
                "industry",
                "p_cnt",
                "l5_p_cnt",
                "l5_close",
                "pnl",
                "pnl_ratio",
                "long_ratio",
                "avg_his_trade_cnt",
                "avg_days",
                "win_rate",
                "pnl_array",
                "pnl_growth",
            ]
        ].copy()
        index_diff_dict = {}
        for index, row in pd_industry_history_tracking.iterrows():
            industry = row["industry"]

            if (
                industry
                in pd_industry_history_tracking_5daysbeforeyesterday["industry"].values
            ):
                index_diff = (
                    pd_industry_history_tracking_5daysbeforeyesterday[
                        pd_industry_history_tracking_5daysbeforeyesterday["industry"]
                        == industry
                    ].index.values
                    - index
                )[0]
                index_diff_dict[index] = index_diff

        pd_industry_history_tracking["index_diff"] = (
            pd_industry_history_tracking.index.map(index_diff_dict)
        )

        def create_arrow(value):
            if pd.isnull(value):
                return ""
            elif value > 0:
                return f"<span style='color:red;'>↑{abs(value):.0f}</span>"
            elif value < 0:
                return f"<span style='color:green;'>↓{abs(value):.0f}</span>"
            else:
                return ""

        pd_industry_history_tracking["industry_new"] = pd_industry_history_tracking[
            "industry"
        ]
        pd_industry_history_tracking["industry"] = pd_industry_history_tracking.apply(
            lambda row: f"{row['industry']} {create_arrow(row['index_diff'])}",
            axis=1,
        )
        pd_industry_history_tracking["pnl_trend"] = pd_industry_history_tracking[
            "pnl_array"
        ].apply(ToolKit("draw line").create_line)
        pd_industry_history_tracking.rename(
            columns={
                "industry": "IND",
                "p_cnt": "OPEN",
                "long_ratio": "LRATIO",
                "l5_p_cnt": "L5 OPEN",
                "l5_close": "L5 CLOSE",
                "pnl": "PROFIT",
                "pnl_ratio": "PNL RATIO",
                "avg_his_trade_cnt": "AVG TRANS",
                "avg_days": "AVG DAYS",
                "win_rate": "WIN RATE",
                "pnl_trend": "PROFIT TREND",
            },
            inplace=True,
        )
        if self.market == "us":
            pd_industry_history_tracking.to_csv("./data/us_category.csv", header=True)
        else:
            pd_industry_history_tracking.to_csv("./data/cn_category.csv", header=True)
        cm = sns.light_palette("seagreen", as_cmap=True)

        total_rows = len(pd_industry_history_tracking)
        rows_per_page = 20
        total_pages = (total_rows + rows_per_page - 1) // rows_per_page  # 计算总页数
        html_pages = []

        for page in range(total_pages):
            start_row = page * rows_per_page
            end_row = min(start_row + rows_per_page, total_rows)
            page_data = pd_industry_history_tracking.iloc[start_row:end_row]

            # 生成表格 HTML
            table_html = "<h2>Industry Overview</h2>" + page_data.style.hide(
                axis=1,
                subset=[
                    "pnl_array",
                    "index_diff",
                    "industry_new",
                    "pnl_growth",
                ],
            ).format(
                {
                    "OPEN": "{:.2f}",
                    "LRATIO": "{:.2%}",
                    "L5 OPEN": "{:.2f}",
                    "L5 CLOSE": "{:.2f}",
                    "PROFIT": "{:.2f}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.2f}",
                    "AVG DAYS": "{:.0f}",
                    "WIN RATE": "{:.2%}",
                }
            ).background_gradient(
                subset=["PROFIT", "OPEN", "L5 OPEN", "L5 CLOSE"], cmap="Greens"
            ).bar(
                subset=["WIN RATE"],
                align="left",
                color=["#99CC66", "#FF6666"],
                vmin=0,
                vmax=0.8,
            ).bar(
                subset=["LRATIO"],
                align="left",
                color=["#99CC66", "#FF6666"],
                vmin=0,
                vmax=0.8,
            ).bar(
                subset=["PNL RATIO"],
                align="mid",
                color=["#99CC66", "#FF6666"],
                vmin=-0.8,
                vmax=0.8,
            ).set_properties(
                **{
                    "text-align": "left",
                    "border": "1px solid #ccc",
                    "cellspacing": "0",
                    "style": "border-collapse: collapse; ",
                }
            ).set_table_styles(
                [
                    dict(
                        selector="th",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "5px"),
                            ("font-size", "24px"),
                        ],
                    ),
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
            ).set_table_styles(
                {
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
                            ],
                        },
                    ],
                },
                overwrite=False,
            ).set_sticky(axis="columns").to_html(doctype_html=True, escape=False)

            # 添加分页导航
            navigation_html = "<div style='text-align: center; margin: 32px;'>"
            if page > 0:
                navigation_html += (
                    f"<a href='#page-{page}' style='margin-right: 32px;'>Previous</a>"
                )
            if page < total_pages - 1:
                navigation_html += (
                    f"<a href='#page-{page + 2}' style='margin-left: 32px;'>Next</a>"
                )
            navigation_html += "</div>"

            # 包装每页内容
            page_html = f"""
            <div id='page-{page + 1}' style='margin-bottom: 20px;'>
                <h3 style='text-align: center;'>Page {page + 1} of {total_pages}</h3>
                {table_html}
                {navigation_html}
            </div>
            """
            html_pages.append(page_html)
        paged_html = "\n".join(html_pages)

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

        html = css + paged_html

        """
        持仓明细历史交易情况分析
        """
        pd_cur_position_with_latest_stock_info = pd.merge(
            pd_cur_position,
            pd_latest_stock_info,
            how="inner",
            on="symbol",
        )

        spark_cur_position_with_latest_stock_info = spark.createDataFrame(
            pd_cur_position_with_latest_stock_info
        )
        spark_cur_position_with_latest_stock_info.createOrReplaceTempView(
            "temp_cur_position_with_latest_stock_info"
        )

        spark_position_history = spark.sql(
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
                    FROM temp_transaction_detail
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
                FROM tmp1 WHERE trade_type = 'sell' AND date >= DATE_ADD('{}', -120)
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
                    , `p&l` AS pnl
                    , `p&l_ratio` AS pnl_ratio
                    , industry
                    , name
                    , total_value
                FROM temp_cur_position_with_latest_stock_info
            )
            SELECT t1.symbol
                , t1.buy_date
                , t1.price
                , t1.adjbase
                , t1.pnl
                , t1.pnl_ratio
                , t1.industry
                , t1.name
                , ROUND(t1.total_value / 100000000, 1) AS total_value
                , COALESCE(t2.his_trade_cnt, 0) AS avg_trans
                , COALESCE(t2.his_days, 0) / t2.his_trade_cnt AS avg_days
                , (t1.pos_cnt + COALESCE(t2.pos_cnt,0)) / ( COALESCE(t2.pos_cnt,0) + COALESCE(t2.neg_cnt,0) + t1.pos_cnt + t1.neg_cnt) AS win_rate
                , (COALESCE(t2.his_pnl,0) + (t1.adjbase - t1.price) * t1.size) / (COALESCE(t2.his_base_price,0) + t1.price * t1.size) AS total_pnl_ratio
                , t2.buy_strategy
                , CASE WHEN t3.pe IS NULL OR t3.pe = '' OR t3.pe = '-'
                    OR NOT t3.pe RLIKE '^-?[0-9]+(\\.[0-9]+)?$' OR t4.new IS NULL THEN '-'
                  ELSE ROUND((1/CAST(t3.pe AS INT) - t4.new / 100) * 100, 1) END AS epr
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
                , total_value
                , IF(adjbase >= price, 1, 0) AS pos_cnt
                , IF(adjbase < price, 1, 0) AS neg_cnt
                FROM tmp3
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol
                LEFT JOIN temp_latest_stock_info t3 ON t1.symbol = t3.symbol
                LEFT JOIN temp_gz t4 ON 1=1
            """.format(end_date, end_date)
        )

        pd_position_history = spark_position_history.toPandas()

        # 将df2的索引和'ind'列的值拼接起来
        pd_industry_history_tracking["combined"] = (
            pd_industry_history_tracking["IND"].astype(str)
            + "("
            + pd_industry_history_tracking.index.astype(str)
            + ")"
        )

        # 使用merge来找到df1和df2中'ind'相等的行，并保留df1的所有行
        pd_position_history = (
            pd_position_history.merge(
                pd_industry_history_tracking[
                    ["industry_new", "combined", "pnl_growth"]
                ],
                left_on="industry",
                right_on="industry_new",
                how="inner",
            )
            .sort_values(
                by=["pnl_growth", "buy_date", "pnl"], ascending=[False, False, False]
            )
            .reset_index(drop=True)
        )
        pd_position_history["industry"] = pd_position_history["combined"]

        # 删除添加的'combined_df2'列
        pd_position_history.drop(columns=["combined", "industry_new"], inplace=True)

        pd_position_history.rename(
            columns={
                "symbol": "SYMBOL",
                "industry": "IND",
                "name": "NAME",
                "total_value": "TOTAL VALUE",
                "epr": "EPR",
                "buy_date": "OPEN DATE",
                "price": "BASE",
                "adjbase": "ADJBASE",
                "pnl": "PNL",
                "pnl_ratio": "PNL RATIO",
                "avg_trans": "AVG TRANS",
                "avg_days": "AVG DAYS",
                "win_rate": "WIN RATE",
                "total_pnl_ratio": "TOTAL PNL RATIO",
                "buy_strategy": "STRATEGY",
            },
            inplace=True,
        )
        if self.market == "us":
            pd_position_history.to_csv("./data/us_stockdetail.csv", header=True)
        else:
            pd_position_history.to_csv("./data/cn_stockdetail.csv", header=True)
        cm = sns.light_palette("seagreen", as_cmap=True)

        # 将新日期转换为字符串
        pd_timeseries_sorted = pd_timeseries.sort_values(by="buy_date", ascending=False)
        new_date_str = str(
            pd_timeseries_sorted.iloc[4]["buy_date"].strftime("%Y-%m-%d")
        )
        new_date = datetime.strptime(new_date_str, "%Y-%m-%d").date()

        def highlight_row(row):
            if row["OPEN DATE"] >= new_date:
                return ["background-color: orange"] * len(row)
            else:
                return [""] * len(row)

        pd_position_history = pd_position_history.head(100)
        total_rows = len(pd_position_history)
        rows_per_page = 20
        total_pages = (total_rows + rows_per_page - 1) // rows_per_page  # 计算总页数
        html_pages = []

        for page in range(total_pages):
            start_row = page * rows_per_page
            end_row = min(start_row + rows_per_page, total_rows)
            page_data = pd_position_history.iloc[start_row:end_row]

            # 生成表格 HTML
            table_html = "<h2>Open Position List</h2>" + page_data.style.hide(
                axis=1,
                subset=["PNL", "pnl_growth", "TOTAL VALUE"],
            ).format(
                {
                    "BASE": "{:.2f}",
                    "ADJBASE": "{:.2f}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.2f}",
                    "AVG DAYS": "{:.0f}",
                    "WIN RATE": "{:.2%}",
                    "TOTAL PNL RATIO": "{:.2%}",
                }
            ).apply(highlight_row, axis=1).background_gradient(
                subset=["BASE", "ADJBASE"], cmap=cm
            ).bar(
                subset=["PNL RATIO", "TOTAL PNL RATIO"],
                align="mid",
                color=["#99CC66", "#FF6666"],
                vmin=-0.8,
                vmax=0.8,
            ).bar(
                subset=["WIN RATE"],
                align="left",
                color=["#99CC66", "#FF6666"],
                vmin=0,
                vmax=0.8,
            ).set_properties(
                **{
                    "text-align": "left",
                    "border": "1px solid #ccc",
                    "cellspacing": "0",
                    "style": "border-collapse: collapse; ",
                }
            ).set_table_styles(
                [
                    # 表头样式
                    dict(
                        selector="th",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "8px"),
                            ("font-size", "18px"),
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
                            ),
                        ],
                    ),
                ]
            ).set_table_styles(
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
            ).set_sticky(axis="columns").to_html(doctype_html=True, escape=False)

            # 添加分页导航
            navigation_html = "<div style='text-align: center; margin: 32px;'>"
            if page > 0:
                navigation_html += (
                    f"<a href='#page-{page}' style='margin-right: 32px;'>Previous</a>"
                )
            if page < total_pages - 1:
                navigation_html += (
                    f"<a href='#page-{page + 2}' style='margin-left: 32px;'>Next</a>"
                )
            navigation_html += "</div>"

            # 包装每页内容
            page_html = f"""
            <div id='page-{page + 1}' style='margin-bottom: 20px;'>
                <h3 style='text-align: center;'>Page {page + 1} of {total_pages}</h3>
                {table_html}
                {navigation_html}
            </div>
            """
            html_pages.append(page_html)
        paged_html1 = "\n".join(html_pages)

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
        html1 = css1 + paged_html1

        pd_cur_position_with_latest_stock_info = None
        pd_position_history = None
        gc.collect()

        """
        减仓情况分析
        """
        spark_position_reduction = spark.sql(
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
                    FROM temp_transaction_detail
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
                    ,ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY l_date DESC) AS row_num
                FROM tmp1 WHERE trade_type = 'sell' AND date >= DATE_ADD('{}', -120)
                AND  symbol NOT IN (SELECT symbol FROM tmp1 WHERE trade_type = 'buy' AND l_date IS NULL)
            ), tmp2 AS (
                SELECT symbol
                    ,sell_date
                    ,SUM(DATEDIFF(sell_date, buy_date)) AS his_days
                    ,SUM(IF(sell_date IS NOT NULL, adj_price * (-adj_size) - base_price * base_size, 0)) AS his_pnl
                    ,MAX(sell_strategy) AS sell_strategy
                FROM  tmp11
                GROUP BY symbol, sell_date
            ), tmp3 AS (
                SELECT t1.symbol
                    ,t1.name
                    ,t2.industry
                FROM temp_latest_stock_info t1 LEFT JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
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
                , COALESCE(t2.his_days, 0) AS his_days
                , t2.sell_strategy
                , CASE WHEN t4.pe IS NULL OR t4.pe = '' OR t4.pe = '-'
                    OR NOT t4.pe RLIKE '^-?[0-9]+(\\.[0-9]+)?$' OR t5.new IS NULL THEN '-'
                  ELSE ROUND((1/CAST(t4.pe AS INT) - t5.new / 100) * 100, 1) END AS epr                
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
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol AND t1.sell_date = t2.sell_date
                LEFT JOIN tmp3 t3 ON t1.symbol = t3.symbol
                LEFT JOIN temp_latest_stock_info t4 ON t1.symbol = t4.symbol
                LEFT JOIN temp_gz t5 ON 1=1             
            """.format(end_date)
        )

        pd_position_reduction = spark_position_reduction.toPandas()

        if not pd_position_reduction.empty:
            # 使用merge来找到df1和df2中'ind'相等的行，并保留df1的所有行
            pd_position_reduction = (
                pd_position_reduction.merge(
                    pd_industry_history_tracking[
                        ["industry_new", "combined", "pnl_growth"]
                    ],
                    left_on="industry",
                    right_on="industry_new",
                    how="inner",
                )
                .sort_values(
                    by=["pnl_growth", "sell_date", "pnl"],
                    ascending=[False, False, False],
                )
                .reset_index(drop=True)
            )
            pd_position_reduction["industry"] = pd_position_reduction["combined"]

            # 删除添加的'combined_df2'列
            pd_position_reduction.drop(
                columns=["combined", "industry_new"], inplace=True
            )

            pd_position_reduction.rename(
                columns={
                    "symbol": "SYMBOL",
                    "epr": "EPR",
                    "buy_date": "OPEN DATE",
                    "sell_date": "CLOSE DATE",
                    "price": "BASE",
                    "adjbase": "ADJBASE",
                    "pnl": "PNL",
                    "pnl_ratio": "PNL RATIO",
                    "his_days": "HIS DAYS",
                    "industry": "IND",
                    "name": "NAME",
                    "sell_strategy": "STRATEGY",
                },
                inplace=True,
            )

            if self.market == "us":
                pd_position_reduction.to_csv(
                    "./data/us_stockdetail_short.csv", header=True
                )
            else:
                pd_position_reduction.to_csv(
                    "./data/cn_stockdetail_short.csv", header=True
                )
            cm = sns.light_palette("seagreen", as_cmap=True)
            pd_position_reduction = pd_position_reduction.head(100)
            total_rows = len(pd_position_reduction)
            rows_per_page = 20
            total_pages = (
                total_rows + rows_per_page - 1
            ) // rows_per_page  # 计算总页数
            html_pages = []

            for page in range(total_pages):
                start_row = page * rows_per_page
                end_row = min(start_row + rows_per_page, total_rows)
                page_data = pd_position_reduction.iloc[start_row:end_row]

                # 生成表格 HTML
                table_html = (
                    "<h2>Close Position List Last 5 Days</h2>"
                    + page_data.style.hide(axis=1, subset=["PNL", "pnl_growth"])
                    .format(
                        {
                            "BASE": "{:.2f}",
                            "ADJBASE": "{:.2f}",
                            "HIS DAYS": "{:.2f}",
                            "PNL RATIO": "{:.2%}",
                        }
                    )
                    .background_gradient(subset=["BASE", "ADJBASE"], cmap=cm)
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
                                    ("padding", "8px"),
                                    ("font-size", "18px"),
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
                                    ),
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
                )

                # 添加分页导航
                navigation_html = "<div style='text-align: center; margin: 32px;'>"
                if page > 0:
                    navigation_html += f"<a href='#page-{page}' style='margin-right: 32px;'>Previous</a>"
                if page < total_pages - 1:
                    navigation_html += f"<a href='#page-{page + 2}' style='margin-left: 32px;'>Next</a>"
                navigation_html += "</div>"

                # 包装每页内容
                page_html = f"""
                <div id='page-{page + 1}' style='margin-bottom: 20px;'>
                    <h3 style='text-align: center;'>Page {page + 1} of {total_pages}</h3>
                    {table_html}
                    {navigation_html}
                </div>
                """
                html_pages.append(page_html)
            paged_html2 = "\n".join(html_pages)
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
            html2 = css2 + paged_html2
        else:
            html2 = ""

        pd_position_reduction = None
        pd_industry_history_tracking = None
        pd_industry_history_tracking_lst5days = None
        pd_industry_history_tracking_5daysbeforeyesterday = None
        gc.collect()

        # 样式变量
        font_size = 32
        title_font_size = 35
        # 设置图像的宽度和高度（例如，1920x1080像素）
        fig_width, fig_height = 1440, 900
        scale_factor = 1
        light_text_color = "rgba(255, 255, 255, 0.77)"
        dark_text_color = "#000000"
        pie_sequential_color = px.colors.diverging.RdYlGn
        # 策略颜色
        strategy_colors_light = [
            "#228B22",
            "#32CD32",
            "#20B2AA",
            "#FF6B6B",
            "#FF4500",
            "#e60606",
            "#8B0000",
        ]

        strategy_colors_dark = [
            "#00FA9A",
            "#7CFC00",
            "#00FF7F",
            "#FFA07A",
            "#DC143C",
            "#dc5037",
            "#FF6347",
        ]

        # 多样化颜色
        diverse_colors5_light = [
            "#C53030",
            "#318231",
            "#63B363",
            "#90EE90",
            "#F56565",
        ]
        diverse_colors5_dark = [
            "#C53030",
            "#318231",
            "#63B363",
            "#90EE90",
            "#F56565",
        ]

        # 热图颜色
        heatmap_colors4_light = [
            "rgba(34, 139, 34, 0.7)",
            "rgba(50, 205, 50, 0.5)",
            "rgba(212, 55, 55, 0.5)",
            "rgba(207, 12, 12, 0.7)",
        ]
        heatmap_colors4_dark = [
            "rgba(34, 139, 34, 0.8)",
            "rgba(50, 205, 50, 0.6)",
            "rgba(240, 80, 80, 0.6)",
            "rgba(220, 40, 40, 0.8)",
        ]
        # TOP20热门行业
        spark_top20_industry = spark.sql(
            """ 
            SELECT industry, cnt 
            FROM (
                SELECT industry, count(*) AS cnt FROM temp_cur_position GROUP BY industry)
            ORDER BY cnt DESC LIMIT 20
            """
        )

        pd_top20_industry = spark_top20_industry.toPandas()

        def get_text_color(hex_color, theme):
            """根据颜色亮度返回黑色或白色文本"""
            try:
                # 处理 RGB/RGBA 字符串（如 'rgb(235,74,64)' 或 'rgba(235,74,64,0.5)'）
                if isinstance(hex_color, str) and hex_color.startswith(("rgb", "rgba")):
                    # 提取数值部分
                    values = re.findall(r"\d+\.?\d*", hex_color)
                    # 转换为归一化 RGB 元组（忽略 Alpha 通道）
                    rgb = tuple(float(x) / 255 for x in values[:3])
                else:
                    # 直接转换其他格式（十六进制、颜色名称等）
                    rgb = to_rgb(hex_color)
                brightness = np.dot(rgb, [0.299, 0.587, 0.114])  # RGB亮度公式
                if brightness > 0.7:
                    return dark_text_color
                elif brightness < 0.3:
                    return light_text_color
                else:
                    return dark_text_color if theme == "light" else light_text_color
            except ValueError:
                # 如果颜色格式无效，返回默认颜色
                if theme == "light":
                    return dark_text_color
                else:
                    return light_text_color

        # light mode
        # 生成动态文本颜色列表（适用于所有区块）
        hex_colors = sample_colorscale(
            pie_sequential_color,  # 原始色阶
            samplepoints=np.linspace(0, 1, 20),  # 生成 20 个等间距点
            colortype="rgb",  # 输出为十六进制
        )
        alpha = 0.4
        # 将 RGB 转换为 RGBA，添加透明度
        rgba_colors = [
            f"rgba({int(color[4:-1].split(',')[0])}, {int(color[4:-1].split(',')[1])}, {int(color[4:-1].split(',')[2])}, {alpha})"
            for color in hex_colors
        ]
        # 文本颜色
        # text_colors = [get_text_color(color, "light") for color in rgba_colors]

        # 创建 Treemap 图
        fig = go.Figure(
            go.Treemap(
                labels=pd_top20_industry["industry"],
                parents=[None] * len(pd_top20_industry),  # 顶层节点为空
                values=pd_top20_industry["cnt"],
                texttemplate="%{label}<br>%{value}<br>%{percentParent:.0%}",  # 自定义文本模板，强制换行
                insidetextfont=dict(
                    size=font_size, color=dark_text_color, family="Arial"
                ),
                textposition="middle center",
                marker=dict(
                    colors=rgba_colors,
                    line=dict(color=rgba_colors, width=2),
                    showscale=False,
                    pad=dict(t=0, b=0, l=0, r=0),
                ),
                opacity=1,
                tiling=dict(
                    squarifyratio=1.8,
                    pad=10,
                ),
            )
        )
        # 更新布局
        fig.update_layout(
            title=dict(
                text=None,  # 标题文本
            ),
            showlegend=False,
            margin=dict(t=0, b=20, l=0, r=0),
            autosize=False,
            width=fig_width,
            height=fig_height,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            treemapcolorway=rgba_colors,  # 确保颜色一致
        )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_postion_byindustry_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_postion_byindustry_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        # dark mode
        # text_colors = [get_text_color(color, "dark") for color in rgba_colors]
        fig.update_traces(
            insidetextfont=dict(color=light_text_color),
            marker=dict(colors=rgba_colors, line=dict(color=rgba_colors)),
        )
        fig.update_layout(
            title_font=dict(color=light_text_color),
        )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_postion_byindustry_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_postion_byindustry_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        pd_top20_industry = None
        gc.collect()

        # TOP20盈利行业
        spark_top20_profit_industry = spark.sql(
            """ 
            SELECT industry, ROUND(pl,2) AS pl 
            FROM (
                SELECT industry, sum(`p&l`) as pl FROM temp_cur_position GROUP BY industry)
            ORDER BY pl DESC LIMIT 20
            """
        )

        pd_top20_profit_industry = spark_top20_profit_industry.toPandas()
        # 创建 Treemap 图
        fig = go.Figure(
            go.Treemap(
                labels=pd_top20_profit_industry["industry"],
                parents=[""] * len(pd_top20_profit_industry),  # 顶层节点为空
                values=pd_top20_profit_industry["pl"],
                texttemplate="%{label}<br>%{value}<br>%{percentParent:.0%}",  # 自定义文本模板，强制换行
                insidetextfont=dict(
                    size=font_size, color=dark_text_color, family="Arial"
                ),
                outsidetextfont=dict(color="grey"),
                textposition="middle center",
                marker=dict(
                    colors=rgba_colors,
                    line=dict(color=rgba_colors, width=2),
                    showscale=False,
                    pad=dict(t=0, b=0, l=0, r=0),
                ),
                opacity=1,
                tiling=dict(
                    squarifyratio=1.8,
                    pad=10,
                ),
            )
        )
        # 更新布局
        fig.update_layout(
            title=dict(
                text=None,  # 标题文本
            ),
            showlegend=False,
            margin=dict(t=0, b=20, l=0, r=0),
            autosize=False,
            width=fig_width,
            height=fig_height,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            treemapcolorway=rgba_colors,  # 确保颜色一致
        )
        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_pl_byindustry_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_pl_byindustry_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        # dark mode
        # text_colors = [get_text_color(color, "dark") for color in hex_colors]
        fig.update_traces(
            insidetextfont=dict(color=light_text_color),
            marker=dict(colors=rgba_colors, line=dict(color=rgba_colors)),
        )
        fig.update_layout(
            title_font=dict(color=light_text_color),
        )
        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_pl_byindustry_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_pl_byindustry_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )

        pd_top20_profit_industry = None
        gc.collect()

        # 120天内策略交易概率
        spark_strategy_tracking_lst120days = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT t1.date
                    ,t1.symbol
                    ,t1.pnl
                    ,t2.strategy
                    ,ROW_NUMBER() OVER(PARTITION BY t1.date, t1.symbol ORDER BY ABS(t1.date - t2.date) ASC, t2.date DESC) AS rn
                FROM temp_position_detail t1 LEFT JOIN temp_transaction_detail t2 ON t1.symbol = t2.symbol AND t1.date >= t2.date AND t2.trade_type = 'buy'
                WHERE t1.date >= DATE_ADD('{}', -120) 
            )
            SELECT date
                    ,strategy
                    ,SUM(pnl) AS pnl
                    ,IF(COUNT(symbol) > 0, SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) / COUNT(symbol), 0) AS success_rate
            FROM tmp1
            WHERE rn = 1
            GROUP BY date, strategy
            ORDER BY date, pnl
            """.format(end_date)
        )
        pd_strategy_tracking_lst120days = spark_strategy_tracking_lst120days.toPandas()
        pd_strategy_tracking_lst120days["date"] = pd.to_datetime(
            pd_strategy_tracking_lst120days["date"]
        )
        pd_strategy_tracking_lst120days["ema_success_rate"] = (
            pd_strategy_tracking_lst120days["success_rate"]
            .ewm(span=5, adjust=False)
            .mean()
        )
        pd_strategy_tracking_lst120days_group = (
            pd_strategy_tracking_lst120days.groupby("date")["pnl"].sum().reset_index()
        )  # 按日期分组并求和
        max_pnl = pd_strategy_tracking_lst120days_group["pnl"].max()

        # 创建带有两个 y 轴的子图布局
        fig = go.Figure()
        # 遍历每个策略并添加数据
        for i, (strategy, data) in enumerate(
            pd_strategy_tracking_lst120days.groupby("strategy")
        ):
            fig.add_trace(
                go.Scatter(
                    x=data["date"],
                    y=data["ema_success_rate"],
                    mode="lines",
                    name=strategy,
                    line=dict(width=2, color=strategy_colors_light[i], shape="hv"),
                    yaxis="y",
                )
            )
            fig.add_trace(
                go.Bar(
                    x=data["date"],
                    y=data["pnl"],
                    name="long",
                    marker=dict(color=strategy_colors_light[i]),
                    marker_line_color=strategy_colors_light[i],
                    yaxis="y2",
                    showlegend=False,
                )
            )
        # light mode
        fig.update_layout(
            title={
                "text": None,
            },
            xaxis=dict(
                mirror=True,
                ticks="outside",
                tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
                showline=False,
                gridcolor="rgba(0, 0, 0, 0.5)",
            ),
            yaxis=dict(
                title=dict(
                    text=None,
                    font=dict(
                        size=title_font_size, color=dark_text_color, family="Arial"
                    ),
                ),
                side="left",
                mirror=True,
                ticks="inside",
                tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
                showline=False,
                gridcolor="rgba(0, 0, 0, 0.5)",
            ),
            yaxis2=dict(
                title=dict(
                    text=None,
                    font=dict(
                        size=title_font_size, color=dark_text_color, family="Arial"
                    ),
                ),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="inside",
                tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
                range=[0, max_pnl * 2],
            ),
            legend=dict(
                orientation="v",
                yanchor="top",
                xanchor="left",
                y=0.95,  # 将 y 设置为 1，表示顶部
                x=0,  # 将 x 设置为 1，表示右侧
                font=dict(size=font_size, color=dark_text_color, family="Arial"),
                bgcolor="rgba(255,255,255,0.5)",
                itemwidth=30,  # 控制图例项宽度
                itemsizing="constant",  # 保持图例符号大小一致
            ),
            barmode="stack",
            bargap=0.5,
            bargroupgap=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
        )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_strategy_tracking_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_strategy_tracking_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )

        # dark mode
        fig = go.Figure()
        # 遍历每个策略并添加数据
        for i, (strategy, data) in enumerate(
            pd_strategy_tracking_lst120days.groupby("strategy")
        ):
            fig.add_trace(
                go.Scatter(
                    x=data["date"],
                    y=data["ema_success_rate"],
                    mode="lines",
                    name=strategy,
                    line=dict(width=2, color=strategy_colors_dark[i], shape="hv"),
                    yaxis="y",
                )
            )
            fig.add_trace(
                go.Bar(
                    x=data["date"],
                    y=data["pnl"],
                    name="long",
                    marker=dict(color=strategy_colors_dark[i]),
                    marker_line_color=strategy_colors_dark[i],
                    yaxis="y2",
                    showlegend=False,
                )
            )
        fig.update_layout(
            title={
                "text": None,
            },
            xaxis=dict(
                title=None,
                mirror=True,
                ticks="outside",
                tickfont=dict(color=light_text_color, size=font_size, family="Arial"),
                showline=False,
                gridcolor="rgba(255, 255, 255, 0.5)",
            ),
            yaxis=dict(
                title=dict(
                    text=None,
                    font=dict(
                        size=title_font_size, color=light_text_color, family="Arial"
                    ),
                ),
                side="left",
                mirror=True,
                ticks="inside",
                tickfont=dict(color=light_text_color, size=font_size, family="Arial"),
                showline=False,
                gridcolor="rgba(255, 255, 255, 0.5)",
            ),
            yaxis2=dict(
                title=dict(
                    text=None,
                    font=dict(
                        size=title_font_size, color=light_text_color, family="Arial"
                    ),
                ),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="inside",
                tickfont=dict(color=light_text_color, size=font_size, family="Arial"),
                range=[0, max_pnl * 2],
            ),
            legend=dict(
                orientation="v",
                yanchor="top",
                xanchor="left",
                y=0.95,  # 将 y 设置为 1，表示顶部
                x=0,  # 将 x 设置为 1，表示右侧
                font=dict(size=font_size, color=light_text_color, family="Arial"),
                bgcolor="rgba(0,0,0,0.5)",
                itemwidth=30,  # 控制图例项宽度
                itemsizing="constant",  # 保持图例符号大小一致
            ),
            barmode="stack",
            bargap=0.5,
            bargroupgap=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
        )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_strategy_tracking_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_strategy_tracking_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )

        pd_strategy_tracking_lst120days = None
        gc.collect()

        # 120天内交易明细分析
        spark_trade_info_lst120days = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT date
                    ,COUNT(symbol) AS total_cnt
                FROM temp_position_detail
                WHERE date >= DATE_ADD('{}', -120)
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
                FROM temp_transaction_detail
                WHERE date >= DATE_ADD('{}', -120)
                GROUP BY date
            )
            SELECT t1.buy_date AS buy_date
                ,t1.total_cnt AS total_cnt
                ,t2.buy_cnt AS buy_cnt
                ,t2.sell_cnt AS sell_cnt
            FROM tmp11 t1 LEFT JOIN tmp5 t2 ON t1.buy_date = t2.date
            """.format(end_date, end_date)
        )
        pd_trade_info_lst120days = spark_trade_info_lst120days.toPandas()
        # df_grouped = pd_trade_info_lst120days.groupby("buy_date")[
        #     ["buy_cnt", "sell_cnt"]
        # ].sum()

        # max_sum = df_grouped.sum(axis=1).max()
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=pd_trade_info_lst120days["buy_date"],
                y=pd_trade_info_lst120days["total_cnt"],
                mode="lines+markers",
                name="Total",
                line=dict(color="red", width=3),
                yaxis="y",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pd_trade_info_lst120days["buy_date"],
                y=pd_trade_info_lst120days["buy_cnt"],
                name="Long",
                marker_color="red",
                marker_line_color="red",
                yaxis="y2",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pd_trade_info_lst120days["buy_date"],
                y=pd_trade_info_lst120days["sell_cnt"],
                name="Short",
                marker_color="green",
                marker_line_color="green",
                yaxis="y2",
            )
        )
        # light mode
        fig.update_layout(
            title={
                "text": "Last 120 days trade info",
                "y": 0.9,
                "x": 0.5,
                "font": dict(
                    size=title_font_size, color=dark_text_color, family="Arial"
                ),
            },
            xaxis=dict(
                title=dict(
                    text="Trade Date",
                    font=dict(
                        size=title_font_size, color=dark_text_color, family="Arial"
                    ),
                ),
                mirror=True,
                ticks="outside",
                tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
                showline=False,
                gridcolor="rgba(0, 0, 0, 0.5)",
                domain=[0, 1],  # 强制x轴占据全部可用宽度
                automargin=False,  # 关闭自动边距计算
            ),
            yaxis=dict(
                title=dict(
                    text=None,
                    font=dict(
                        size=title_font_size, color=dark_text_color, family="Arial"
                    ),
                ),
                side="left",
                mirror=True,
                ticks="inside",
                tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
                showline=False,
                gridcolor="rgba(0, 0, 0, 0.5)",
                ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
                tickangle=0,  # 确保刻度标签水平显示
                # automargin=False,  # 关闭自动边距计算
            ),
            yaxis2=dict(
                title=dict(
                    text=None,
                    font=dict(
                        size=title_font_size, color=dark_text_color, family="Arial"
                    ),
                ),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="inside",
                tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
                ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
                tickangle=0,  # 确保刻度标签水平显示
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=font_size, color=dark_text_color, family="Arial"),
                itemwidth=30,  # 控制图例项宽度
                itemsizing="constant",  # 保持图例符号大小一致
            ),
            barmode="stack",
            bargap=0.5,
            bargroupgap=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
        )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_trade_trend_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_trade_trend_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        # dark mode
        fig.update_layout(
            title={
                "font": dict(color=light_text_color),
            },
            xaxis=dict(
                title=dict(font=dict(color=light_text_color)),
                tickfont=dict(color=light_text_color),
                gridcolor="rgba(255, 255, 255, 0.5)",
            ),
            yaxis=dict(
                title=dict(font=dict(color=light_text_color)),
                tickfont=dict(color=light_text_color),
                gridcolor="rgba(255, 255, 255, 0.5)",
            ),
            yaxis2=dict(
                title=dict(font=dict(color=light_text_color)),
                tickfont=dict(color=light_text_color),
            ),
            legend=dict(font=dict(color=light_text_color)),
        )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_trade_trend_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_trade_trend_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )

        pd_trade_info_lst120days = None
        gc.collect()

        # TOP5行业仓位变化趋势
        spark_top5_industry_position_trend = spark.sql(
            """
            WITH tmp AS ( 
                SELECT industry
                    ,cnt 
                FROM ( 
                    SELECT industry, count(*) AS cnt FROM temp_cur_position GROUP BY industry) t
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
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date >= DATE_ADD('{}', -120)
            ) 
            SELECT t1.buy_date
                ,t1.industry
                ,SUM(IF(t2.symbol IS NOT NULL, 1, 0)) AS total_cnt
            FROM tmp1 t1 LEFT JOIN tmp2 t2 ON t1.industry = t2.industry AND t1.buy_date = t2.date
            GROUP BY t1.buy_date, t1.industry
            """.format(end_date)
        )
        pd_top5_industry_position_trend = spark_top5_industry_position_trend.toPandas()
        pd_top5_industry_position_trend.sort_values(
            by=["buy_date", "total_cnt"], ascending=[False, False], inplace=True
        )

        fig = px.line(
            pd_top5_industry_position_trend,
            x="buy_date",
            y="total_cnt",
            color="industry",
            line_group="industry",
            color_discrete_sequence=diverse_colors5_light,
        )
        # light mode
        fig.update_traces(line=dict(width=3))  # 设置线条宽度为2
        # light mode
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
            showline=False,
            gridcolor="rgba(0, 0, 0, 0.5)",
            title=dict(
                text="Open Date",
                font=dict(size=title_font_size, color=dark_text_color, family="Arial"),
            ),
        )
        fig.update_yaxes(
            mirror=True,
            ticks="inside",
            tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
            showline=False,
            gridcolor="rgba(0, 0, 0, 0.5)",
            title=None,  # 设置为空字符串以隐藏y轴标题
            ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
            tickangle=0,  # 确保刻度标签水平显示
        )
        fig.update_layout(
            title="Last 120 days top5 Positions",
            title_font=dict(
                size=title_font_size, color=dark_text_color, family="Arial"
            ),
            title_x=0.5,
            title_y=0.9,
            legend_title_text=None,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=font_size, color=dark_text_color, family="Arial"),
                itemwidth=30,  # 控制图例项宽度
                itemsizing="constant",  # 保持图例符号大小一致
            ),
            plot_bgcolor="rgba(0, 0, 0, 0)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
            xaxis=dict(
                domain=[0, 1],  # 强制x轴占据全部可用宽度
                automargin=False,  # 关闭自动边距计算
            ),
            yaxis=dict(
                automargin=False  # 关闭自动边距计算
            ),
        )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_top_industry_position_trend_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_top_industry_position_trend_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        # dark mode
        fig.update_xaxes(
            tickfont=dict(color=light_text_color),
            gridcolor="rgba(255, 255, 255, 0.5)",
            title_font=dict(color=light_text_color),
        )
        fig.update_yaxes(
            tickfont=dict(color=light_text_color),
            gridcolor="rgba(255, 255, 255, 0.5)",
            title_font=dict(color=light_text_color),
        )
        fig.update_layout(
            title_font=dict(color=light_text_color),
            legend_title_text="",
            legend=dict(font=dict(color=light_text_color)),
        )
        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_top_industry_position_trend_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_top_industry_position_trend_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )

        pd_top5_industry_position_trend = None
        gc.collect()
        # TOP5行业PnL变化趋势
        spark_industry_history_tracking_lst5days.createOrReplaceTempView(
            "temp_industry_history_tracking_lst5days"
        )
        spark_top5_industry_profit_trend = spark.sql(
            """
            WITH tmp AS ( 
                SELECT industry
                    ,pl 
                FROM ( 
                    SELECT industry, sum(`p&l`) AS pl FROM temp_cur_position GROUP BY industry) t 
                ORDER BY pl DESC
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
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date >= DATE_ADD('{}', -120)
            ), tmp3 AS (
            SELECT t1.buy_date
                ,t1.industry
                ,SUM(COALESCE(t2.pnl, 0)) AS pnl
            FROM tmp1 t1 LEFT JOIN tmp2 t2 ON t1.industry = t2.industry AND t1.buy_date = t2.date
            GROUP BY t1.buy_date, t1.industry
            )  SELECT t2.buy_date
                ,t1.industry
                ,t2.pnl
            FROM (SELECT * FROM temp_industry_history_tracking_lst5days ORDER BY pnl_growth DESC LIMIT 5) t1
            LEFT JOIN tmp3 t2 ON t1.industry = t2.industry
            ORDER BY t2.buy_date ASC, t1.pnl_growth DESC
            """.format(end_date)
        )
        pd_top5_industry_profit_trend = spark_top5_industry_profit_trend.toPandas()
        pd_top5_industry_profit_trend.sort_values(
            by=["buy_date", "pnl"], ascending=[False, False], inplace=True
        )

        fig = px.line(
            pd_top5_industry_profit_trend,
            x="buy_date",
            y="pnl",
            color="industry",
            line_group="industry",
            color_discrete_sequence=diverse_colors5_light,
        )
        fig.update_traces(line=dict(width=3))  # 设置线条宽度为2
        # light mode
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
            showline=False,
            gridcolor="rgba(0, 0, 0, 0.5)",
            title=dict(
                text="Open Date",
                font=dict(size=title_font_size, color=dark_text_color, family="Arial"),
            ),
        )
        fig.update_yaxes(
            mirror=True,
            ticks="inside",
            tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
            showline=False,
            gridcolor="rgba(0, 0, 0, 0.5)",
            title=None,  # 设置为空字符串以隐藏y轴标题
            ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
            tickangle=0,  # 确保刻度标签水平显示
        )
        fig.update_layout(
            title="Last 120 days top5 pnl",
            title_font=dict(
                size=title_font_size, color=dark_text_color, family="Arial"
            ),
            title_x=0.5,
            title_y=0.9,
            legend_title_text=None,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=font_size, color=dark_text_color, family="Arial"),
                itemwidth=30,  # 控制图例项宽度
                itemsizing="constant",  # 保持图例符号大小一致
            ),
            plot_bgcolor="rgba(0, 0, 0, 0)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
            xaxis=dict(
                domain=[0, 1],  # 强制x轴占据全部可用宽度
                automargin=False,  # 关闭自动边距计算
            ),
            yaxis=dict(
                automargin=False  # 关闭自动边距计算
            ),
        )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_top_industry_pl_trend_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_top_industry_pl_trend_light.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        # dark mode
        fig = px.line(
            pd_top5_industry_profit_trend,
            x="buy_date",
            y="pnl",
            color="industry",
            line_group="industry",
            color_discrete_sequence=diverse_colors5_dark,
        )
        fig.update_traces(line=dict(width=3))  # 设置线条宽度为2
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color=light_text_color, size=font_size, family="Arial"),
            showline=False,
            gridcolor="rgba(255, 255, 255, 0.5)",
            title=dict(
                text="Open Date",
                font=dict(size=title_font_size, color=light_text_color, family="Arial"),
            ),
        )
        fig.update_yaxes(
            mirror=True,
            ticks="inside",
            tickfont=dict(color=light_text_color, size=font_size),
            showline=False,
            gridcolor="rgba(255, 255, 255, 0.5)",
            title_font=dict(
                size=title_font_size, color=light_text_color, family="Arial"
            ),
            title=None,  # 设置为空字符串以隐藏y轴标题
            ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
            tickangle=0,  # 确保刻度标签水平显示
        )
        fig.update_layout(
            title="Last 120 days top5 pnl",
            title_font=dict(
                size=title_font_size, color=light_text_color, family="Arial"
            ),
            title_x=0.5,
            title_y=0.9,
            legend_title_text=None,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=font_size, color=light_text_color, family="Arial"),
                itemwidth=30,  # 控制图例项宽度
                itemsizing="constant",  # 保持图例符号大小一致
            ),
            plot_bgcolor="rgba(0, 0, 0, 0)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
            xaxis=dict(
                domain=[0, 1],  # 强制x轴占据全部可用宽度
                automargin=False,  # 关闭自动边距计算
            ),
            yaxis=dict(
                automargin=False  # 关闭自动边距计算
            ),
        )
        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_top_industry_pl_trend_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_top_industry_pl_trend_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=scale_factor,
            )
        pd_top5_industry_profit_trend = None
        gc.collect()

        spark_calendar_heatmap = spark.sql(
            """
            WITH tmp AS (
                SELECT 
                    t1.date
                    ,t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date >= (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
                    WHERE row_num = 26 )
                GROUP BY t1.date, t2.industry
            ), tmp1 AS (
            SELECT t.date
                ,t.industry
                ,t.pnl
                ,t.l_pnl
            FROM (
                SELECT
                    date
                    ,industry
                    ,pnl
                    ,LAG(pnl) OVER (PARTITION BY industry ORDER BY date) AS l_pnl
                FROM tmp
                ORDER BY date, industry
            ) t WHERE t.date >= (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY 'AAA' ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date FROM temp_timeseries) t ) tt
                    WHERE row_num = 25 )
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

        pd_calendar_heatmap = spark_calendar_heatmap.toPandas()

        # 计算每个日期的周和星期几
        pd_calendar_heatmap["date"] = pd.to_datetime(pd_calendar_heatmap["date"])
        pd_calendar_heatmap["week"] = pd_calendar_heatmap["date"].dt.isocalendar().week
        pd_calendar_heatmap["day_of_week"] = pd_calendar_heatmap["date"].dt.dayofweek

        # 重新排列数据，使其按日期顺序排列
        pd_calendar_heatmap = pd_calendar_heatmap.sort_values(by="date").reset_index(
            drop=True
        )

        # 获取数据中的最新日期
        latest_date = pd_calendar_heatmap["date"].max()

        # 获取最新日期是周几（0=周一，1=周二，..., 6=周日）
        weekday = latest_date.weekday()

        # 根据周几动态调整交易日数量
        # 周五 -> 25 个交易日，周四 -> 24 个交易日，...，周一 -> 21 个交易日
        trading_days = 21 + weekday

        # 生成最近的交易日范围
        filtered_dates = pd.date_range(end=latest_date, periods=trading_days, freq="B")

        # 过滤数据
        pd_calendar_heatmap = pd_calendar_heatmap[
            pd_calendar_heatmap["date"].isin(filtered_dates)
        ].reset_index(drop=True)

        # 计算每周的起始日期
        pd_calendar_heatmap["week_start"] = pd_calendar_heatmap[
            "date"
        ] - pd.to_timedelta(pd_calendar_heatmap["day_of_week"], unit="d")

        # 确定每周的顺序
        unique_weeks = (
            pd_calendar_heatmap["week_start"]
            .drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
        )
        week_mapping = {date: i for i, date in enumerate(unique_weeks)}
        pd_calendar_heatmap["week_order"] = pd_calendar_heatmap["week_start"].map(
            week_mapping
        )

        # 创建日历图
        fig = go.Figure()

        # 假设 s_pnl 的最小值为负，最大值为正
        min_val = pd_calendar_heatmap["s_pnl"].min()
        max_val = pd_calendar_heatmap["s_pnl"].max()
        mid_val = 0  # 中间值，用于白色

        # 添加热力图
        fig.add_trace(
            go.Heatmap(
                x=pd_calendar_heatmap["day_of_week"],  # 每行显示7天
                y=pd_calendar_heatmap["week_order"],  # 每7天增加一行
                z=pd_calendar_heatmap["s_pnl"],
                xgap=10,  # 设置列之间的间隙为5像素
                ygap=10,  # 设置行之间的间隙为10像素
                # 定义自定义颜色比例
                colorscale=[
                    [0, heatmap_colors4_light[0]],
                    [
                        (mid_val - min_val) / (max_val - min_val) / 2,
                        heatmap_colors4_light[1],
                    ],
                    [
                        (mid_val - min_val) / (max_val - min_val),
                        "rgba(255, 255, 255, 0)",
                    ],
                    [
                        1 - (max_val - mid_val) / (max_val - min_val) / 2,
                        heatmap_colors4_light[2],
                    ],
                    [1, heatmap_colors4_light[3]],
                ],
                zmin=min_val,
                zmax=max_val,
                showscale=False,
                # colorbar=dict(
                #     title=dict(
                #         text="PnL",
                #         font=dict(
                #             color=dark_text_color, size=font_size, family="Arial"
                #         ),  # 设置颜色条标题的颜色和字体大小
                #     ),
                #     tickfont=dict(
                #         size=font_size, color=dark_text_color, family="Arial"
                #     ),
                #     thickness=10,  # 增加颜色条厚度
                #     len=0.5,  # 调整颜色条长度以适应布局
                #     xpad=0,
                #     x=1,  # 靠近热图
                # ),
                text=pd_calendar_heatmap["industry_top3"].apply(
                    lambda x: "<br>".join(x)
                ),
            )
        )

        # light mode
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
                showgrid=False,
                gridcolor="rgba(0, 0, 0, 0.5)",
                zeroline=False,
                showticklabels=True,
                dtick=1,  # 每天显示一个刻度
                tickfont=dict(size=font_size, family="Arial"),
            ),
            yaxis=dict(
                showgrid=False,
                gridcolor="rgba(0, 0, 0, 0.5)",
                zeroline=False,
                showticklabels=False,
                # title="Week",
                autorange="reversed",  # 反转Y轴，使得最新的一周在最下方
            ),
            plot_bgcolor="rgba(0, 0, 0, 0)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
            margin=dict(
                l=0,
                r=0,
                t=0,
                b=0,
            ),
            autosize=True,
        )
        # 在每个单元格中添加文本
        for i, row in pd_calendar_heatmap.iterrows():
            day_of_week = row["day_of_week"]
            week_order = row["week_order"]
            col2_values = row["industry_top3"]
            col3_value = row["s_pnl"]
            date = row["date"].strftime("%Y-%m-%d")

            # 根据s_pnl的值确定文本颜色
            # 假设当s_pnl接近min_val时，我们使用深色背景（绿色系），接近max_val时，我们使用浅色背景（红色系）
            # 这里简单地使用阈值来判断，但您可以根据实际需求调整逻辑
            text_color = dark_text_color  # 默认黑色
            if (
                col3_value <= min_val + (max_val - min_val) * 0.05
            ):  # 当s_pnl非常小时（接近最小值）
                text_color = (
                    light_text_color if max_val > 0 else dark_text_color
                )  # 如果最大值大于0，则使用白色，否则保持黑色（避免全黑背景）
            elif (
                col3_value >= max_val - (max_val - min_val) * 0.05
            ):  # 当s_pnl非常大时（接近最大值）
                text_color = light_text_color  # 使用白色

            # 将col2的list按3行展示（这里可能需要根据实际情况调整）
            text = f"<b>{date}</b><br>" + "<br>".join(
                col2_values[:3]
            )  # 假设我们只展示前三个行业

            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=text,
                showarrow=False,
                font=dict(
                    color=text_color,
                    family="Arial",
                    size=font_size - 5,
                ),  # 根据s_pnl值动态设置字体颜色
                align="center",
                xanchor="center",
                yanchor="middle",
                # 以下选项是可选的，用于提高可读性
                # bordercolor="white",  # 添加白色边框
                # borderwidth=1,  # 设置边框宽度
                # bgcolor="rgba(255, 255, 255, 0.7)",  # 设置背景色为半透明白色
            )

        # 在每周之间添加横向分隔线
        unique_weeks = pd_calendar_heatmap["week_order"].unique()
        for week in unique_weeks[1:]:  # 跳过最后一个 week
            fig.add_hline(
                y=week - 0.5,
                line_dash="solid",
                line_color="gray",
                opacity=0.5,
                layer="below",
            )

        # 在每列之间添加纵向分割线
        unique_days = sorted(
            pd_calendar_heatmap["day_of_week"].unique()
        )  # 确保按0-6排序
        for day in unique_days[1:]:  # 跳过最后一个 day
            fig.add_vline(
                x=day - 0.5,
                line_dash="solid",
                line_color="gray",
                opacity=0.5,
                layer="below",
            )

        # 找出没有数据的日期
        missing_dates = set(filtered_dates) - set(pd_calendar_heatmap["date"])
        # 排除周六和周日
        missing_dates = [date for date in missing_dates if date.weekday() < 5]

        # 为每个缺失日期添加 annotation
        for missing_date in missing_dates:
            # 计算对应的 day_of_week 和 week_order
            day_of_week = missing_date.dayofweek  # 星期几 (0=Monday, 6=Sunday)
            week_start = missing_date - pd.to_timedelta(day_of_week, unit="d")
            week_order = week_mapping[
                week_start
            ]  # 根据已有的 week_mapping 获取 week_order

            # 添加 annotation
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text="休市",
                showarrow=False,
                font=dict(
                    color=dark_text_color,
                    family="Arial",
                    size=font_size - 5,
                ),  # 根据s_pnl值动态设置字体颜色
                align="center",
                xanchor="center",
                yanchor="middle",
            )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_industry_trend_heatmap_light.svg",
                width=fig_width,
                height=fig_height,
                scale=2,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_industry_trend_heatmap_light.svg",
                width=fig_width,
                height=fig_height,
                scale=2,
            )

        # dark mode
        # 创建日历图
        fig = go.Figure()

        # 假设 s_pnl 的最小值为负，最大值为正
        min_val = pd_calendar_heatmap["s_pnl"].min()
        max_val = pd_calendar_heatmap["s_pnl"].max()
        mid_val = 0  # 中间值，用于白色

        # 添加热力图
        fig.add_trace(
            go.Heatmap(
                x=pd_calendar_heatmap["day_of_week"],  # 每行显示7天
                y=pd_calendar_heatmap["week_order"],  # 每7天增加一行
                z=pd_calendar_heatmap["s_pnl"],
                xgap=10,  # 设置列之间的间隙为5像素
                ygap=10,  # 设置行之间的间隙为10像素
                # 定义自定义颜色比例
                colorscale=[
                    [0, heatmap_colors4_dark[0]],
                    [
                        (mid_val - min_val) / (max_val - min_val) / 2,
                        heatmap_colors4_dark[1],
                    ],
                    [
                        (mid_val - min_val) / (max_val - min_val),
                        "rgba(0, 0, 0, 0)",
                    ],
                    [
                        1 - (max_val - mid_val) / (max_val - min_val) / 2,
                        heatmap_colors4_dark[2],
                    ],
                    [1, heatmap_colors4_dark[3]],
                ],
                zmin=min_val,
                zmax=max_val,
                showscale=False,
                # colorbar=dict(
                #     title=dict(
                #         text="PnL",
                #         font=dict(
                #             color=light_text_color, size=font_size, family="Arial"
                #         ),  # 设置颜色条标题的颜色和字体大小
                #     ),
                #     tickfont=dict(
                #         size=font_size, color=light_text_color, family="Arial"
                #     ),
                #     thickness=10,  # 增加颜色条厚度
                #     len=0.5,  # 调整颜色条长度以适应布局
                #     xpad=0,
                #     x=1,  # 靠近热图
                # ),
                text=pd_calendar_heatmap["industry_top3"].apply(
                    lambda x: "<br>".join(x)
                ),
            )
        )
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
                showgrid=False,
                gridcolor="rgba(255, 255, 255, 0.5)",
                zeroline=False,
                showticklabels=True,
                dtick=1,  # 每天显示一个刻度
                tickfont=dict(size=font_size, color=light_text_color, family="Arial"),
            ),
            yaxis=dict(
                showgrid=False,
                gridcolor="rgba(255, 255, 255, 0.5)",
                zeroline=False,
                showticklabels=False,
                # title="Week",
                autorange="reversed",  # 反转Y轴，使得最新的一周在最下方
            ),
            plot_bgcolor="rgba(0, 0, 0, 0)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
            margin=dict(
                l=0,
                r=0,
                t=0,
                b=0,
            ),
            autosize=True,
        )
        # 在每个单元格中添加文本
        for i, row in pd_calendar_heatmap.iterrows():
            day_of_week = row["day_of_week"]
            week_order = row["week_order"]
            col2_values = row["industry_top3"]
            col3_value = row["s_pnl"]
            date = row["date"].strftime("%Y-%m-%d")

            # 根据s_pnl的值确定文本颜色
            # 假设当s_pnl接近min_val时，我们使用深色背景（绿色系），接近max_val时，我们使用浅色背景（红色系）
            # 这里简单地使用阈值来判断，但您可以根据实际需求调整逻辑
            text_color = light_text_color  # 默认白色

            # 将col2的list按3行展示（这里可能需要根据实际情况调整）
            text = f"<b>{date}</b><br>" + "<br>".join(
                col2_values[:3]
            )  # 假设我们只展示前三个行业

            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=text,
                showarrow=False,
                font=dict(
                    color=text_color,
                    family="Arial",
                    size=font_size - 5,
                ),  # 根据s_pnl值动态设置字体颜色
                align="center",
                xanchor="center",
                yanchor="middle",
                # 以下选项是可选的，用于提高可读性
                # bordercolor="white",  # 添加白色边框
                # borderwidth=1,  # 设置边框宽度
                # bgcolor="rgba(255, 255, 255, 0.7)",  # 设置背景色为半透明白色
            )

        # 在每周之间添加横向分隔线
        unique_weeks = pd_calendar_heatmap["week_order"].unique()
        for week in unique_weeks[1:]:  # 跳过最后一个 week
            fig.add_hline(
                y=week - 0.5,
                line_dash="solid",
                line_color="white",
                opacity=0.5,
                layer="below",
            )

        # 在每列之间添加纵向分割线
        unique_days = sorted(
            pd_calendar_heatmap["day_of_week"].unique()
        )  # 确保按0-6排序
        for day in unique_days[1:]:  # 跳过最后一个 day
            fig.add_vline(
                x=day - 0.5,
                line_dash="solid",
                line_color="white",
                opacity=0.5,
                layer="below",
            )

        # 找出没有数据的日期
        missing_dates = set(filtered_dates) - set(pd_calendar_heatmap["date"])
        # 排除周六和周日
        missing_dates = [date for date in missing_dates if date.weekday() < 5]

        # 为每个缺失日期添加 annotation
        for missing_date in missing_dates:
            # 计算对应的 day_of_week 和 week_order
            day_of_week = missing_date.dayofweek  # 星期几 (0=Monday, 6=Sunday)
            week_start = missing_date - pd.to_timedelta(day_of_week, unit="d")
            week_order = week_mapping[
                week_start
            ]  # 根据已有的 week_mapping 获取 week_order

            # 添加 annotation
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text="休市",
                showarrow=False,
                font=dict(
                    color=light_text_color,
                    family="Arial",
                    size=font_size - 5,
                ),  # 根据s_pnl值动态设置字体颜色
                align="center",
                xanchor="center",
                yanchor="middle",
            )

        if self.market == "us":
            fig.write_image(
                "./dashreport/assets/images/us_industry_trend_heatmap_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=2,
            )
        else:
            fig.write_image(
                "./dashreport/assets/images/cn_industry_trend_heatmap_dark.svg",
                width=fig_width,
                height=fig_height,
                scale=2,
            )

        spark.stop()

        if self.market == "us":
            subject = "US Stock Market Trends"
            image_path_return_light = "./dashreport/assets/images/us_tr_light.svg"
            image_path_return_dark = "./dashreport/assets/images/us_tr_dark.svg"
            image_path = [
                "./dashreport/assets/images/us_postion_byindustry_light.svg",
                "./dashreport/assets/images/us_postion_byindustry_dark.svg",
                "./dashreport/assets/images/us_pl_byindustry_light.svg",
                "./dashreport/assets/images/us_pl_byindustry_dark.svg",
                "./dashreport/assets/images/us_trade_trend_light.svg",
                "./dashreport/assets/images/us_trade_trend_dark.svg",
                "./dashreport/assets/images/us_top_industry_position_trend_light.svg",
                "./dashreport/assets/images/us_top_industry_position_trend_dark.svg",
                "./dashreport/assets/images/us_top_industry_pl_trend_light.svg",
                "./dashreport/assets/images/us_top_industry_pl_trend_dark.svg",
                "./dashreport/assets/images/us_industry_trend_heatmap_light.svg",
                "./dashreport/assets/images/us_industry_trend_heatmap_dark.svg",
                "./dashreport/assets/images/us_strategy_tracking_light.svg",
                "./dashreport/assets/images/us_strategy_tracking_dark.svg",
                image_path_return_light,
                image_path_return_dark,
            ]
        elif self.market == "cn":
            subject = "CN Stock Market Trends"
            image_path_return_light = "./dashreport/assets/images/cn_tr_light.svg"
            image_path_return_dark = "./dashreport/assets/images/cn_tr_dark.svg"
            image_path = [
                "./dashreport/assets/images/cn_postion_byindustry_light.svg",
                "./dashreport/assets/images/cn_postion_byindustry_dark.svg",
                "./dashreport/assets/images/cn_pl_byindustry_light.svg",
                "./dashreport/assets/images/cn_pl_byindustry_dark.svg",
                "./dashreport/assets/images/cn_trade_trend_light.svg",
                "./dashreport/assets/images/cn_trade_trend_dark.svg",
                "./dashreport/assets/images/cn_top_industry_position_trend_light.svg",
                "./dashreport/assets/images/cn_top_industry_position_trend_dark.svg",
                "./dashreport/assets/images/cn_top_industry_pl_trend_light.svg",
                "./dashreport/assets/images/cn_top_industry_pl_trend_dark.svg",
                "./dashreport/assets/images/cn_industry_trend_heatmap_light.svg",
                "./dashreport/assets/images/cn_industry_trend_heatmap_dark.svg",
                "./dashreport/assets/images/cn_strategy_tracking_light.svg",
                "./dashreport/assets/images/cn_strategy_tracking_dark.svg",
                image_path_return_light,
                image_path_return_dark,
            ]
        html_content = """
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <meta charset="UTF-8">
                        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
                        <style>
                            /* 基础样式 - 确保兼容性 */
                            * {{
                                box-sizing: border-box;
                                margin: 0;
                                padding: 0;
                                -webkit-text-size-adjust: 100%;
                                text-size-adjust: 100%;
                            }}
                            
                            body {{
                                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                                line-height: 1.5;
                                min-width: 100% !important;
                                width: 100% !important;
                            }}
                            
                            .email-container {{
                                max-width: 600px;
                                min-width: 100% !important;
                                width: 100%;
                                margin: 0 auto;
                                padding: 0px;
                            }}
                            
                            h1 {{
                                font-style: italic;
                                font-size: 24px;
                                margin-bottom: 20px;
                                text-align: center;
                            }}
                            
                            .image-container {{
                                margin-bottom: 10px;
                                border-radius: 0px;
                                overflow: hidden;
                                width: 100% !important;
                                min-width: 100% !important;
                            }}
                            
                            img {{
                                display: block;
                                height: auto !important;
                                margin: 0 auto !important;
                                width: 100% !important;
                                min-width: 100% !important;
                            }}
                            
                            figcaption {{
                                padding: 12px;
                                text-align: center;
                                font-style: italic;
                                font-size: 18px;
                                line-height: 1.4;
                            }}

                            /* 浅色模式 */
                            @media (prefers-color-scheme: light) {{
                                body {{
                                    background-color: #ffffff;
                                    color: #333333;
                                }}
                                
                                .image-container {{
                                    border: 1px solid #e0e0e0;
                                    background-color: #f8f8f8;
                                }}
                                
                                figcaption {{
                                    background-color: #f0f0f0;
                                    color: #555555;
                                }}
                            }}

                            /* 深色模式 */
                            @media (prefers-color-scheme: dark) {{
                                body {{
                                    background-color: #121212;
                                    color: #e0e0e0;
                                }}
                                
                                .image-container {{
                                    border: 1px solid #333333;
                                    background-color: #1a1a1a;
                                }}
                                
                                figcaption {{
                                    background-color: #222222;
                                    color: #cccccc;
                                }}
                            }}
                            
                            /* 移动设备优化 */
                            @media screen and (max-width: 480px) {{
                                .email-container {{
                                    padding: 10px;
                                }}
                                
                                h1 {{
                                    font-size: 20px;
                                }}
                                
                                figcaption {{
                                    font-size: 16px;
                                    padding: 10px;
                                }}
                            }}
                        </style>
                    </head>
                    <body>
                        <div class="email-container">
                            <h1>The current cash is {cash}, the final portfolio value is {final_value}, the number of backtesting list is {stock_cnt}</h1>
                            <div class="image-container">
                                <picture>
                                    <!-- 深色模式下的图片 -->
                                    <source srcset="cid:image15" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image14" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%">
                                </picture>
                                <figcaption>The diagram shows the last x years cumulative return and max drawdown trend,
                                            to track the stock market and stategy execution information
                                </figcaption>
                            </div>
                            <div class="image-container">
                                <picture>
                                    <!-- 深色模式下的图片 -->
                                    <source srcset="cid:image11" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days top3 industries:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image10" alt="The diagram shows the last x days top3 industries:" style="width:100%">
                                </picture>
                                <figcaption>The diagram shows the last x days top3 industries, to track last x days
                                            the top3 industries
                                </figcaption>
                            </div>
                            <div class="image-container">
                                <picture>
                                    <!-- 深色模式下的图片 -->
                                    <source srcset="cid:image13" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days strategy success ratio:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image12" alt="The diagram shows the last x days strategy success ratio:" style="width:100%">
                                </picture>
                                <figcaption>The diagram shows the last x days strategy success ratio, to track last x days
                                            the strategy success ratio
                                </figcaption>
                            </div>
                            <div class="image-container">                                          
                                <picture>
                                    <!-- 深色模式下的图片 -->
                                    <source srcset="cid:image1" media="(prefers-color-scheme: dark)" alt="The industry distribution of current positions is as follows:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image0" alt="The industry distribution of current positions is as follows:" style="width:100%">
                                </picture>
                                <figcaption> The industry position distribution of the top 10 shows the current distribution of industry
                                            positions that meet the strategy.
                                </figcaption>
                            </div>
                            <div class="image-container">
                                <picture>
                                    <!-- 深色模式下的图片 -->
                                    <source srcset="cid:image3" media="(prefers-color-scheme: dark)" alt="The industry distribution of current pnl is as follows:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image2" alt="The industry distribution of current pnl is as follows:" style="width:100%">
                                </picture>
                                <figcaption>The industry pnl distribution of the top 10 shows the current distribution of industry
                                            pnl that meet the strategy.
                                </figcaption>
                            </div>
                            <div class="image-container">
                                <picture>
                                    <!-- 深色模式下的图片 -->
                                    <source srcset="cid:image5" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days trade detail info:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image4" alt="The diagram shows the last x days trade detail info:" style="width:100%">
                                </picture>
                                <figcaption>The diagram shows the last x days trade detail info, which include the short/long/position
                                            info every day.
                                </figcaption>
                            </div>
                            <div class="image-container">
                                <picture>
                                    <!-- 深色模式下的图片 -->
                                    <source srcset="cid:image7" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days top5 industry position info:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image6" alt="The diagram shows the last x days top5 industry position info:" style="width:100%">
                                </picture>
                                <figcaption>The diagram shows the last x days top5 industry position trend, to stat last x days
                                            the top5 industry positions change status
                                </figcaption>
                            </div>
                            <div class="image-container">
                                <picture>
                                    <!-- 深色模式下的图片 -->
                                    <source srcset="cid:image9" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days top5 industry pnl info:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image8" alt="The diagram shows the last x days top5 industry pnl info:" style="width:100%">
                                </picture>
                                <figcaption>The diagram shows the last x days top5 industry pnl trend, to stat last x days
                                            the top5 industry pnl change status
                                </figcaption>
                            </div>
                        </div>
                    </body>
                    </html>
                    """.format(
            cash=cash, final_value=final_value, stock_cnt=len(stock_list)
        )

        result = [
            {
                "cash": cash,
                "final_value": final_value,
                "stock_cnt": len(stock_list),
                "end_date": end_date,
            }
        ]
        df_result = pd.DataFrame.from_dict(result)
        df_result.to_csv(f"./data/{self.market}_df_result.csv", header=True)

        MyEmail().send_email_embedded_image(
            subject, html_content + html + html1 + html2, image_path
        )

    def send_etf_btstrategy_by_email(self, cash, final_value):
        """
        发送邮件
        """
        # 启动Spark Session
        spark = initialize_spark("StockAnalysis", memory="512m", partitions=1)
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        """ 
        读取交易相关数据，交易明细，持仓明细，仓位日志明细，行业信息
        """
        file = FileInfo(self.trade_date, self.market)
        # 交易明细
        file_path_trade = file.get_file_path_etf_trade
        cols = ["idx", "symbol", "date", "trade_type", "price", "size", "strategy"]
        spark_transaction_detail = spark.read.csv(
            file_path_trade, header=None, inferSchema=True
        )
        spark_transaction_detail = spark_transaction_detail.toDF(*cols)
        spark_transaction_detail.createOrReplaceTempView("temp_transaction_detail")
        # 持仓明细, spark读取
        file_cur_p = file.get_file_path_etf_position
        cols = [
            "idx",
            "symbol",
            "buy_date",
            "price",
            "adjbase",
            "size",
            "p&l",
            "p&l_ratio",
        ]
        spark_cur_position = spark.read.csv(file_cur_p, header=True, inferSchema=True)
        spark_cur_position = spark_cur_position.toDF(*cols)
        spark_cur_position.createOrReplaceTempView("temp_cur_position")
        pd_cur_position = spark_cur_position[
            [
                "symbol",
                "buy_date",
                "price",
                "adjbase",
                "size",
                "p&l",
                "p&l_ratio",
            ]
        ].toPandas()
        # 仓位日志明细
        file_path_position_detail = file.get_file_path_etf_position_detail
        cols = ["idx", "symbol", "date", "price", "adjbase", "pnl"]
        spark_position_detail = spark.read.csv(
            file_path_position_detail, header=None, inferSchema=True
        )
        spark_position_detail = spark_position_detail.toDF(*cols)
        spark_position_detail.createOrReplaceTempView("temp_position_detail")
        # 当日股票信息
        file_name_day = file.get_file_path_latest
        cols = [
            "symbol",
            "name",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "total_value",
            "pe",
            "date",
        ]
        spark_latest_stock_info = spark.read.csv(
            file_name_day,
            header=True,
            inferSchema=True,
        )
        spark_latest_stock_info = spark_latest_stock_info.select(cols)
        spark_latest_stock_info.createOrReplaceTempView("temp_latest_stock_info")
        pd_latest_stock_info = spark_latest_stock_info[
            ["name", "symbol", "total_value"]
        ].toPandas()

        # 生成时间序列，用于时间序列补齐
        end_date = pd.to_datetime(self.trade_date).strftime("%Y-%m-%d")
        start_date = pd.to_datetime(end_date) - pd.DateOffset(days=120)
        date_range = pd.date_range(
            start=start_date.strftime("%Y-%m-%d"), end=end_date, freq="D"
        )
        pd_timeseries = pd.DataFrame({"buy_date": date_range})
        # 将日期转换为字符串格式 'YYYYMMDD'
        pd_timeseries["trade_date"] = pd_timeseries["buy_date"].dt.strftime("%Y%m%d")

        # 根据市场类型过滤非交易日
        toolkit = ToolKit("identify trade date")

        if self.market == "us":
            pd_timeseries = pd_timeseries[
                pd_timeseries["trade_date"].apply(toolkit.is_us_trade_date)
            ]
        elif self.market == "cn":
            pd_timeseries = pd_timeseries[
                pd_timeseries["trade_date"].apply(toolkit.is_cn_trade_date)
            ]

        spark_timeseries = spark.createDataFrame(
            pd_timeseries.astype({"buy_date": "string"})
        )

        spark_timeseries.createOrReplaceTempView("temp_timeseries")

        """
        持仓明细历史交易情况分析
        """
        pd_cur_position_with_latest_stock_info = pd.merge(
            pd_cur_position, pd_latest_stock_info, how="inner", on="symbol"
        )

        spark_cur_position_with_latest_stock_info = spark.createDataFrame(
            pd_cur_position_with_latest_stock_info
        )
        spark_cur_position_with_latest_stock_info.createOrReplaceTempView(
            "temp_cur_position_with_latest_stock_info"
        )

        spark_position_history = spark.sql(
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
                    FROM temp_transaction_detail
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
                FROM tmp1 WHERE trade_type = 'sell' AND date >= DATE_ADD('{}', -120)
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
                    , total_value
                FROM temp_cur_position_with_latest_stock_info
            )
            SELECT t1.symbol
                , t1.buy_date
                , t1.price
                , t1.adjbase
                , t1.pnl
                , t1.pnl_ratio
                , t1.name
                , ROUND(t1.total_value / 100000000, 1) AS total_value
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
                , total_value
                , IF(adjbase >= price, 1, 0) AS pos_cnt
                , IF(adjbase < price, 1, 0) AS neg_cnt
                FROM tmp3
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol
            """.format(end_date, end_date)
        )

        pd_position_history = spark_position_history.toPandas()

        pd_position_history.rename(
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
                "total_value": "TOTAL VALUE",
                "buy_strategy": "STRATEGY",
            },
            inplace=True,
        )
        if self.market == "us":
            pd_position_history.to_csv("./data/us_etf.csv", header=True)
        else:
            pd_position_history.to_csv("./data/cn_etf.csv", header=True)
        cm = sns.light_palette("seagreen", as_cmap=True)

        pd_timeseries_sorted = pd_timeseries.sort_values(by="buy_date", ascending=False)
        new_date_str = str(
            pd_timeseries_sorted.iloc[4]["buy_date"].strftime("%Y-%m-%d")
        )
        new_date = datetime.strptime(new_date_str, "%Y-%m-%d").date()

        def highlight_row(row):
            if row["OPEN DATE"] >= new_date:
                return ["background-color: orange"] * len(row)
            else:
                return [""] * len(row)

        pd_position_history = pd_position_history.head(100)
        total_rows = len(pd_position_history)
        rows_per_page = 20
        total_pages = (total_rows + rows_per_page - 1) // rows_per_page  # 计算总页数
        html_pages = []

        for page in range(total_pages):
            start_row = page * rows_per_page
            end_row = min(start_row + rows_per_page, total_rows)
            page_data = pd_position_history.iloc[start_row:end_row]

            # 生成表格 HTML
            table_html = "<h2>Open Position List</h2>" + page_data.style.hide(
                axis=1, subset=["PNL", "TOTAL VALUE"]
            ).format(
                {
                    "BASE": "{:.2f}",
                    "ADJBASE": "{:.2f}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.2f}",
                    "AVG DAYS": "{:.0f}",
                    "WIN RATE": "{:.2%}",
                    "TOTAL PNL RATIO": "{:.2%}",
                }
            ).apply(highlight_row, axis=1).background_gradient(
                subset=["BASE", "ADJBASE"], cmap=cm
            ).bar(
                subset=["PNL RATIO", "TOTAL PNL RATIO"],
                align="mid",
                color=["#99CC66", "#FF6666"],
                vmin=-0.8,
                vmax=0.8,
            ).bar(
                subset=["WIN RATE"],
                align="left",
                color=["#99CC66", "#FF6666"],
                vmin=0,
                vmax=0.8,
            ).set_properties(
                **{
                    "text-align": "left",
                    "border": "1px solid #ccc",
                    "cellspacing": "0",
                    "style": "border-collapse: collapse; ",
                }
            ).set_table_styles(
                [
                    # 表头样式
                    dict(
                        selector="th",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "8px"),
                            ("font-size", "18px"),
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
                            ),
                        ],
                    ),
                ]
            ).set_table_styles(
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
            ).set_sticky(axis="columns").to_html(doctype_html=True, escape=False)

            # 添加分页导航
            navigation_html = "<div style='text-align: center; margin: 32px;'>"
            if page > 0:
                navigation_html += (
                    f"<a href='#page-{page}' style='margin-right: 32px;'>Previous</a>"
                )
            if page < total_pages - 1:
                navigation_html += (
                    f"<a href='#page-{page + 2}' style='margin-left: 32px;'>Next</a>"
                )
            navigation_html += "</div>"

            # 包装每页内容
            page_html = f"""
            <div id='page-{page + 1}' style='margin-bottom: 20px;'>
                <h3 style='text-align: center;'>Page {page + 1} of {total_pages}</h3>
                {table_html}
                {navigation_html}
            </div>
            """
            html_pages.append(page_html)
        paged_html = "\n".join(html_pages)

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

        html = css + paged_html

        pd_cur_position_with_latest_stock_info = None
        gc.collect()

        """
        减仓情况分析
        """
        spark_position_reduction = spark.sql(
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
                    FROM temp_transaction_detail
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
                    ,ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY l_date DESC) AS row_num
                FROM tmp1 WHERE trade_type = 'sell' AND date >= DATE_ADD('{}', -120)
                AND  symbol NOT IN (SELECT symbol FROM tmp1 WHERE trade_type = 'buy' AND l_date IS NULL)
            ), tmp2 AS (
                SELECT symbol
                    ,sell_date
                    ,SUM(DATEDIFF(sell_date, buy_date)) AS his_days
                    ,SUM(IF(sell_date IS NOT NULL, adj_price * (-adj_size) - base_price * base_size, 0)) AS his_pnl
                    ,MAX(sell_strategy) AS sell_strategy
                FROM  tmp11
                GROUP BY symbol, sell_date
            ), tmp3 AS (
                SELECT symbol
                    ,name
                FROM temp_latest_stock_info
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
                , COALESCE(t2.his_days, 0) AS his_days
                , t2.sell_strategy AS sell_strategy           
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
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol AND t1.sell_date = t2.sell_date
                LEFT JOIN tmp3 t3 ON t1.symbol = t3.symbol
            """.format(end_date)
        )

        pd_position_reduction = spark_position_reduction.toPandas()

        if not pd_position_reduction.empty:
            pd_position_reduction.rename(
                columns={
                    "symbol": "SYMBOL",
                    "buy_date": "OPEN DATE",
                    "sell_date": "CLOSE DATE",
                    "price": "BASE",
                    "adjbase": "ADJBASE",
                    "pnl": "PNL",
                    "pnl_ratio": "PNL RATIO",
                    "his_days": "HIS DAYS",
                    "name": "NAME",
                    "sell_strategy": "STRATEGY",
                },
                inplace=True,
            )
            cm = sns.light_palette("seagreen", as_cmap=True)

            pd_position_reduction = pd_position_reduction.head(100)
            total_rows = len(pd_position_reduction)
            rows_per_page = 20
            total_pages = (
                total_rows + rows_per_page - 1
            ) // rows_per_page  # 计算总页数
            html_pages = []

            for page in range(total_pages):
                start_row = page * rows_per_page
                end_row = min(start_row + rows_per_page, total_rows)
                page_data = pd_position_reduction.iloc[start_row:end_row]

                # 生成表格 HTML
                table_html = (
                    "<h2>Close Position List Last 5 Days</h2>"
                    + page_data.style.hide(axis=1, subset=["PNL"])
                    .format(
                        {
                            "BASE": "{:.2f}",
                            "ADJBASE": "{:.2f}",
                            "PNL RATIO": "{:.2%}",
                            "HIS DAYS": "{:.0f}",
                        }
                    )
                    .background_gradient(subset=["BASE", "ADJBASE"], cmap=cm)
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
                                    ("padding", "8px"),
                                    ("font-size", "18px"),
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
                                    ),
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
                )

                # 添加分页导航
                navigation_html = "<div style='text-align: center; margin: 32px;'>"
                if page > 0:
                    navigation_html += f"<a href='#page-{page}' style='margin-right: 32px;'>Previous</a>"
                if page < total_pages - 1:
                    navigation_html += f"<a href='#page-{page + 2}' style='margin-left: 32px;'>Next</a>"
                navigation_html += "</div>"

                # 包装每页内容
                page_html = f"""
                <div id='page-{page + 1}' style='margin-bottom: 20px;'>
                    <h3 style='text-align: center;'>Page {page + 1} of {total_pages}</h3>
                    {table_html}
                    {navigation_html}
                </div>
                """
                html_pages.append(page_html)
            paged_html2 = "\n".join(html_pages)

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
            html2 = css2 + paged_html2
        else:
            html2 = ""

        pd_position_reduction = None
        pd_position_history = None
        gc.collect()

        # 120天内交易明细分析
        spark_trade_info_lst120days = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT date
                    ,COUNT(symbol) AS total_cnt
                FROM temp_position_detail
                WHERE date >= DATE_ADD('{}', -120)
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
                FROM temp_transaction_detail
                WHERE date >= DATE_ADD('{}', -120)
                GROUP BY date
            )
            SELECT t1.buy_date AS buy_date
                ,t1.total_cnt AS total_cnt
                ,t2.buy_cnt AS buy_cnt
                ,t2.sell_cnt AS sell_cnt
            FROM tmp11 t1 LEFT JOIN tmp5 t2 ON t1.buy_date = t2.date
            """.format(end_date, end_date)
        )
        pd_trade_info_lst120days = spark_trade_info_lst120days.toPandas()

        # 设置图像的宽度和高度（例如，1920x1080像素）
        fig_width, fig_height = 1440, 900
        scale_factor = 1.2
        font_size = 28
        title_font_size = 32
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=pd_trade_info_lst120days["buy_date"],
                y=pd_trade_info_lst120days["total_cnt"],
                mode="lines+markers",
                name="total stock",
                line=dict(color="red", width=3),
                yaxis="y",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pd_trade_info_lst120days["buy_date"],
                y=pd_trade_info_lst120days["buy_cnt"],
                name="long",
                marker_color="red",
                marker_line_color="red",
                yaxis="y2",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pd_trade_info_lst120days["buy_date"],
                y=pd_trade_info_lst120days["sell_cnt"],
                name="short",
                marker_color="green",
                marker_line_color="green",
                yaxis="y2",
            )
        )
        # light mode
        fig.update_layout(
            title={
                "text": "Last 120 days trade info",
                "y": 0.9,
                "x": 0.5,
                "font": dict(size=title_font_size, color="black", family="Arial"),
            },
            xaxis=dict(
                title=dict(
                    text="Trade Date",
                    font=dict(size=title_font_size, color="black", family="Arial"),
                ),
                mirror=True,
                ticks="outside",
                tickfont=dict(color="black", family="Arial", size=font_size),
                showline=False,
                gridcolor="rgba(0, 0, 0, 0.5)",
                domain=[0, 1],  # 强制x轴占据全部可用宽度
                automargin=False,  # 关闭自动边距计算
            ),
            yaxis=dict(
                title=dict(
                    text=None,
                    font=dict(size=title_font_size, color="black", family="Arial"),
                ),
                side="left",
                mirror=True,
                ticks="inside",
                tickfont=dict(color="black", family="Arial", size=font_size),
                showline=False,
                gridcolor="rgba(0, 0, 0, 0.5)",
                ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
                tickangle=0,  # 确保刻度标签水平显示
            ),
            yaxis2=dict(
                title=dict(
                    text=None,
                    font=dict(size=title_font_size, color="black", family="Arial"),
                ),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="inside",
                tickfont=dict(color="black", family="Arial", size=font_size),
                ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
                tickangle=0,  # 确保刻度标签水平显示
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=font_size, color="black", family="Arial"),
            ),
            barmode="stack",
            bargap=0.5,
            bargroupgap=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
        )

        fig.write_image(
            "./dashreport/assets/images/cnetf_trade_trend_light.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        text_color = "rgba(255, 255, 255, 0.77)"
        fig.update_layout(
            title={
                "text": "Last 120 days trade info",
                "y": 0.9,
                "x": 0.5,
                "font": dict(size=title_font_size, color=text_color, family="Arial"),
            },
            xaxis=dict(
                title=dict(
                    text="Trade Date",
                    font=dict(size=title_font_size, color=text_color, family="Arial"),
                ),
                mirror=True,
                ticks="outside",
                tickfont=dict(color=text_color, family="Arial", size=font_size),
                showline=False,
                gridcolor="rgba(255, 255, 255, 0.5)",
                domain=[0, 1],  # 强制x轴占据全部可用宽度
                automargin=False,  # 关闭自动边距计算
            ),
            yaxis=dict(
                title=dict(
                    text=None,
                    font=dict(size=title_font_size, color=text_color, family="Arial"),
                ),
                side="left",
                mirror=True,
                ticks="outside",
                tickfont=dict(color=text_color, family="Arial", size=font_size),
                showline=False,
                gridcolor="rgba(255, 255, 255, 0.5)",
                ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
                tickangle=0,  # 确保刻度标签水平显示
            ),
            yaxis2=dict(
                title=dict(
                    text=None,
                    font=dict(size=title_font_size, color=text_color, family="Arial"),
                ),
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="outside",
                tickfont=dict(color=text_color, family="Arial", size=font_size),
                ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
                tickangle=0,  # 确保刻度标签水平显示
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
                font=dict(size=font_size, color=text_color, family="Arial"),
            ),
            barmode="stack",
            bargap=0.5,
            bargroupgap=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
        )

        fig.write_image(
            "./dashreport/assets/images/cnetf_trade_trend_dark.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )

        pd_trade_info_lst120days = None
        gc.collect()

        spark.stop()

        if self.market == "us":
            subject = "US Stock Market ETF Trends"
            image_path_return_light = "./dashreport/assets/images/etf_tr_light.svg"
            image_path_return_dark = "./dashreport/assets/images/etf_tr_dark.svg"
        elif self.market == "cn":
            subject = "CN Stock Market ETF Trends"
            image_path_return_light = "./dashreport/assets/images/cnetf_tr_light.svg"
            image_path_return_dark = "./dashreport/assets/images/cnetf_tr_dark.svg"
        image_path = [
            "./dashreport/assets/images/cnetf_trade_trend_light.svg",
            "./dashreport/assets/images/cnetf_trade_trend_dark.svg",
            image_path_return_light,
            image_path_return_dark,
        ]
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
            <style>
                /* 基础样式 - 确保兼容性 */
                * {{
                    box-sizing: border-box;
                    margin: 0;
                    padding: 0;
                    -webkit-text-size-adjust: 100%;
                    text-size-adjust: 100%;
                }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    line-height: 1.5;
                    min-width: 100% !important;
                    width: 100% !important;
                }}
                
                .email-container {{
                    max-width: 600px;
                    min-width: 100% !important;
                    width: 100%;
                    margin: 0 auto;
                    padding: 0px;
                }}
                
                h1 {{
                    font-style: italic;
                    font-size: 24px;
                    margin-bottom: 20px;
                    text-align: center;
                }}
                
                .image-container {{
                    margin-bottom: 10px;
                    border-radius: 0px;
                    overflow: hidden;
                    width: 100% !important;
                    min-width: 100% !important;
                }}
                
                img {{
                    display: block;
                    height: auto !important;
                    margin: 0 auto !important;
                    width: 100% !important;
                    min-width: 100% !important;
                }}
                
                figcaption {{
                    padding: 0px;
                    text-align: center;
                    font-style: italic;
                    font-size: 18px;
                    line-height: 1.4;
                }}

                /* 浅色模式 */
                @media (prefers-color-scheme: light) {{
                    body {{
                        background-color: #ffffff;
                        color: #333333;
                    }}
                    
                    .image-container {{
                        border: 1px solid #e0e0e0;
                        background-color: #f8f8f8;
                    }}
                    
                    figcaption {{
                        background-color: #f0f0f0;
                        color: #555555;
                    }}
                }}

                /* 深色模式 */
                @media (prefers-color-scheme: dark) {{
                    body {{
                        background-color: #121212;
                        color: #e0e0e0;
                    }}
                    
                    .image-container {{
                        border: 1px solid #333333;
                        background-color: #1a1a1a;
                    }}
                    
                    figcaption {{
                        background-color: #222222;
                        color: #cccccc;
                    }}
                }}
                
                /* 移动设备优化 */
                @media screen and (max-width: 480px) {{
                    .email-container {{
                        padding: 10px;
                    }}
                    
                    h1 {{
                        font-size: 20px;
                    }}
                    
                    figcaption {{
                        font-size: 16px;
                        padding: 10px;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="email-container">
                <h1>The current cash is {cash}, the final portfolio value is {final_value}</h1>
                <div class="image-container">
                    <picture>                                    
                        <source srcset="cid:image3" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%"/>                                    
                        <img src="cid:image2" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%">
                    </picture>
                    <figcaption>The diagram shows the last x years cumulative return and max drawdown trend,
                                to track the stock market and strategy execution information
                    </figcaption>                                
                </div>
                <div class="image-container">             
                    <picture>                                    
                        <source srcset="cid:image1" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x days trade detail info:" style="width:100%"/>                                    
                        <img src="cid:image0" alt="The diagram shows the last x days trade detail info:" style="width:100%">
                    </picture>
                    <figcaption>The diagram shows the last x days trade detail info, which include the short/long/position
                                info every day.
                    </figcaption>
                </div>
            </div>
        </body>
        </html>
        """.format(cash=cash, final_value=final_value)
        MyEmail().send_email_embedded_image(
            subject, html_content + html + html2, image_path
        )
