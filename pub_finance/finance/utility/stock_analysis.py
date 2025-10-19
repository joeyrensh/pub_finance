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
        start_date = pd.to_datetime(end_date) - pd.DateOffset(days=180)
        date_range = pd.date_range(
            start=start_date.strftime("%Y-%m-%d"), end=end_date, freq="D"
        )
        pd_timeseries = pd.DataFrame({"buy_date": date_range})
        # 将日期转换为字符串格式 'YYYYMMDD'
        pd_timeseries["trade_date"] = pd_timeseries["buy_date"].dt.strftime("%Y%m%d")

        # 根据市场类型过滤非交易日
        toolkit = ToolKit("identify trade date")
        if self.market in ("us", "us_special"):
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
                    ,SUM(IF(buy_date >= (SELECT buy_date FROM ( SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                                         FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t ) tt WHERE row_num = 5), 1, 0)) AS l5_p_cnt
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
                tmp1 WHERE trade_type = 'sell' AND l_date >= DATE_ADD('{}', -180)
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
                    ,COUNT(DISTINCT IF(t2.sell_date >= (SELECT buy_date FROM ( SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                                         FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t ) tt WHERE row_num = 5), t2.symbol, null)) AS l5_close
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
            """.format(
                end_date, end_date
            )
        )

        spark_industry_history_tracking_lst5days = spark.sql(
            """
            WITH tmp AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t ) tt
                    WHERE row_num = 1 )
                GROUP BY t2.industry
            ), tmp1 AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date,1 AS partition_key FROM temp_timeseries) t ) tt
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
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t ) tt
                    WHERE row_num = 1 )
                GROUP BY t2.industry
            ), tmp1 AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t ) tt
                    WHERE row_num = 2)
                GROUP BY t2.industry
            )
            , tmp2 AS (
                SELECT t2.industry
                    ,SUM(t1.pnl) AS pnl
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                WHERE t1.date = (
                    SELECT buy_date FROM (
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t ) tt
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
                return f"<span style='color:red;'><b>↑</b>{abs(value):.0f}</span>"
            elif value < 0:
                return f"<span style='color:green;'><b>↓</b>{abs(value):.0f}</span>"
            else:
                return ""

        pd_industry_history_tracking["industry_new"] = pd_industry_history_tracking[
            "industry"
        ]
        pd_industry_history_tracking["industry"] = pd_industry_history_tracking.apply(
            lambda row: f"{row['industry']}{create_arrow(row['index_diff'])}",
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
        pd_industry_history_tracking.to_csv(
            f"./data/{self.market}_category.csv", header=True
        )
        cm = sns.light_palette("seagreen", as_cmap=True)

        total_rows = len(pd_industry_history_tracking)
        rows_per_page = 20
        total_pages = (total_rows + rows_per_page - 1) // rows_per_page  # 计算总页数
        html_pages = []
        counter = total_pages

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
                    "OPEN": "{:.0f}",
                    "LRATIO": "{:.2%}",
                    "L5 OPEN": "{:.0f}",
                    "L5 CLOSE": "{:.0f}",
                    "PROFIT": "{:.2f}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.0f}",
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
                            ("padding", "2px"),
                            # ("font-size", "20px"),
                        ],
                    ),
                    dict(
                        selector="td",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "2px"),
                            # ("font-size", "20px"),
                        ],
                    ),
                ],
            ).set_properties(
                subset=["PROFIT TREND", "IND"],
                **{
                    "min-width": "150px !important",
                    # "max-width": "100%",
                    # "width": "150px",
                    "padding": "0",
                },
                overwrite=False,
            ).set_properties(
                subset=["LRATIO", "WIN RATE", "PNL RATIO"],
                **{
                    "width": "100px",
                    "padding": "0",
                },
                overwrite=False,
            ).set_properties(
                subset=[
                    "OPEN",
                    "L5 OPEN",
                    "L5 CLOSE",
                    "PROFIT",
                    "AVG TRANS",
                    "AVG DAYS",
                ],
                **{
                    "width": "70px",
                    "padding": "0",
                },
                overwrite=False,
            ).set_sticky(
                axis="columns"
            ).to_html(
                doctype_html=False,
                escape=False,
                table_attributes='style="border-collapse: collapse; border: 0.2px solid #ccc"; width: 100%;',
            )

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
                FROM tmp1 WHERE trade_type = 'sell' AND l_date >= DATE_ADD('{}', -180)
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
                , t1.industry
                , t1.name
                , ROUND(t1.total_value / 100000000, 1) AS total_value
                , CASE WHEN t3.pe_double IS NULL OR t4.new IS NULL OR t3.pe_double = 0 THEN null
                  ELSE ROUND((1.0 / t3.pe_double - t4.new / 100.0) * 100, 1) END AS erp      
                , t1.buy_date
                , t1.price
                , t1.adjbase
                , t1.pnl
                , t1.pnl_ratio
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
                , total_value
                , IF(adjbase >= price, 1, 0) AS pos_cnt
                , IF(adjbase < price, 1, 0) AS neg_cnt
                FROM tmp3
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol
                LEFT JOIN (
                    SELECT symbol,
                        CASE 
                            WHEN pe IS NULL OR pe IN ('', '-', 'NULL', 'N/A') THEN NULL
                            WHEN TRY_CAST(pe AS DOUBLE) IS NOT NULL THEN TRY_CAST(pe AS DOUBLE)
                            ELSE NULL
                        END AS pe_double
                    FROM temp_latest_stock_info
                ) t3 ON t1.symbol = t3.symbol
                LEFT JOIN temp_gz t4 ON 1=1
            """.format(
                end_date, end_date
            )
        )

        pd_position_history = spark_position_history.toPandas()

        # 将df2的索引和'ind'列的值拼接起来
        pd_industry_history_tracking["combined"] = (
            pd_industry_history_tracking["IND"].astype(str)
            + "/"
            + pd_industry_history_tracking.index.astype(str)
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
                "erp": "ERP",
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
        pd_position_history.to_csv(f"./data/{self.market}_stockdetail.csv", header=True)
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

        # pd_position_history = pd_position_history.head(100)
        # 按IND分组，每组取最多20条记录
        pd_position_history = (
            pd_position_history.groupby("IND").head(10).reset_index(drop=True).head(100)
        )
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
                subset=["PNL", "pnl_growth"],
            ).format(
                {
                    "TOTAL VALUE": "{:.2f}",
                    "BASE": "{:.2f}",
                    "ADJBASE": "{:.2f}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.0f}",
                    "AVG DAYS": "{:.0f}",
                    "WIN RATE": "{:.2%}",
                    "TOTAL PNL RATIO": "{:.2%}",
                }
            ).apply(
                highlight_row, axis=1
            ).background_gradient(
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
                            ("padding", "2px"),
                            # ("font-size", "20px"),
                        ],
                    ),
                    # 表格数据单元格样式
                    dict(
                        selector="td",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "2px"),
                            # (
                            #     "font-size",
                            #     "20px",
                            # ),
                        ],
                    ),
                ]
            ).set_properties(
                subset=["NAME", "IND"],
                **{
                    "min-width": "100px !important",
                    "max-width": "100%",
                    "padding": "0",
                },
                overwrite=False,
            ).set_sticky(
                axis="columns"
            ).to_html(
                doctype_html=False,
                escape=False,
                table_attributes='style="border-collapse: collapse; border: 0.2px solid #ccc"; width: 100%;',
            )

            # 添加分页导航
            navigation_html = "<div style='text-align: center; margin: 32px;'>"
            if page > 0:
                navigation_html += f"<a href='#page-{page + counter}' style='margin-right: 32px;'>Previous</a>"
            if page < total_pages - 1:
                navigation_html += f"<a href='#page-{page + 2 + counter}' style='margin-left: 32px;'>Next</a>"
            navigation_html += "</div>"

            # 包装每页内容
            page_html = f"""
            <div id='page-{page + 1 + counter}' style='margin-bottom: 20px;'>
                <h3 style='text-align: center;'>Page {page + 1} of {total_pages}</h3>
                {table_html}
                {navigation_html}
            </div>
            """
            html_pages.append(page_html)
        paged_html1 = "\n".join(html_pages)
        counter += total_pages

        css1 = """
            <style>
                :root {
                    color-scheme: dark light;
                    supported-color-schemes: dark light;
                    background-color: transparent;
                    color: black;
                    display: table ;
                    -webkit-text-size-adjust: 100%;
                    text-size-adjust: 100%;
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;                         
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
                /* 移动设备优化 */
                @media screen and (max-width: 480px) {
                    body {
                        width: 100%;                              
                    }
                    table {
                        width: 100%;
                        margin: 0 auto;  /* 居中显示 */
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
                FROM tmp1 WHERE trade_type = 'sell' AND l_date >= DATE_ADD('{}', -180)
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
                    ,ROUND(t1.total_value / 100000000, 1) AS total_value  
                FROM temp_latest_stock_info t1 LEFT JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                GROUP BY t1.symbol
                    ,t1.name
                    ,t2.industry
                    ,t1.total_value
            )
            SELECT t1.symbol
                , t3.industry
                , t3.name
                , t3.total_value
                , CASE WHEN t4.pe_double IS NULL OR t5.new IS NULL OR t4.pe_double = 0 THEN null
                  ELSE ROUND((1.0 / t4.pe_double - t5.new / 100.0) * 100, 1) END AS erp
                , t1.buy_date
                , t1.sell_date
                , t1.base_price AS price
                , t1.adj_price AS adjbase
                , t1.pnl
                , t1.pnl_ratio
                , COALESCE(t2.his_days, 0) AS his_days
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
                                                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                                                    FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t 
                                                ) tt WHERE row_num = 5)
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol AND t1.sell_date = t2.sell_date
                LEFT JOIN tmp3 t3 ON t1.symbol = t3.symbol
                LEFT JOIN (
                    SELECT symbol,
                        CASE 
                            WHEN pe IS NULL OR pe IN ('', '-', 'NULL', 'N/A') THEN NULL
                            WHEN TRY_CAST(pe AS DOUBLE) IS NOT NULL THEN TRY_CAST(pe AS DOUBLE)
                            ELSE NULL
                        END AS pe_double
                    FROM temp_latest_stock_info
                ) t4 ON t1.symbol = t4.symbol
                LEFT JOIN temp_gz t5 ON 1=1             
            """.format(
                end_date
            )
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
                    "erp": "ERP",
                    "buy_date": "OPEN DATE",
                    "sell_date": "CLOSE DATE",
                    "price": "BASE",
                    "adjbase": "ADJBASE",
                    "pnl": "PNL",
                    "pnl_ratio": "PNL RATIO",
                    "his_days": "HIS DAYS",
                    "industry": "IND",
                    "name": "NAME",
                    "total_value": "TOTAL VALUE",
                    "sell_strategy": "STRATEGY",
                },
                inplace=True,
            )

            pd_position_reduction.to_csv(
                f"./data/{self.market}_stockdetail_short.csv", header=True
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
                table_html = "<h2>Close Position List Last 5 Days</h2>" + page_data.style.hide(
                    axis=1, subset=["PNL", "pnl_growth"]
                ).format(
                    {
                        "TOTAL VALUE": "{:.2f}",
                        "BASE": "{:.2f}",
                        "ADJBASE": "{:.2f}",
                        "HIS DAYS": "{:.0f}",
                        "PNL RATIO": "{:.2%}",
                    }
                ).background_gradient(
                    subset=["BASE", "ADJBASE"], cmap=cm
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
                        # 表头样式
                        dict(
                            selector="th",
                            props=[
                                ("border", "1px solid #ccc"),
                                ("text-align", "left"),
                                ("padding", "2px"),
                                # ("font-size", "20px"),
                            ],
                        ),
                        # 表格数据单元格样式
                        dict(
                            selector="td",
                            props=[
                                ("border", "1px solid #ccc"),
                                ("text-align", "left"),
                                ("padding", "2px"),
                                # (
                                #     "font-size",
                                #     "20px",
                                # ),
                            ],
                        ),
                    ]
                ).set_properties(
                    subset=["NAME", "IND"],
                    **{
                        "min-width": "150px !important",
                        "max-width": "100%",
                        "padding": "0",
                    },
                    overwrite=False,
                ).set_sticky(
                    axis="columns"
                ).to_html(
                    doctype_html=False,
                    escape=False,
                    table_attributes='style="border-collapse: collapse; border: 0.2px solid #ccc"; width: 100%;',
                )

                # 添加分页导航
                navigation_html = "<div style='text-align: center; margin: 32px;'>"
                if page > 0:
                    navigation_html += f"<a href='#page-{page + counter}' style='margin-right: 32px;'>Previous</a>"
                if page < total_pages - 1:
                    navigation_html += f"<a href='#page-{page + 2 + counter}' style='margin-left: 32px;'>Next</a>"
                navigation_html += "</div>"

                # 包装每页内容
                page_html = f"""
                <div id='page-{page + 1 + counter}' style='margin-bottom: 20px;'>
                    <h3 style='text-align: center;'>Page {page + 1} of {total_pages}</h3>
                    {table_html}
                    {navigation_html}
                </div>
                """
                html_pages.append(page_html)
            paged_html2 = "\n".join(html_pages)
            counter += total_pages
            css2 = """
                <style>
                    :root {
                        color-scheme: dark light;
                        supported-color-schemes: dark light;
                        background-color: transparent;
                        color: black;
                        display: table ;
                        -webkit-text-size-adjust: 100%;
                        text-size-adjust: 100%;
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;                             
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
                    /* 移动设备优化 */
                    @media screen and (max-width: 480px) {
                        body {
                            width: 100%;                              
                        }
                        table {
                            width: 100%;
                            margin: 0 auto;  /* 居中显示 */
                        }
                    }                           
                </style>
            """
            html2 = css2 + paged_html2
        else:
            html2 = ""

        pd_position_reduction = None
        # pd_industry_history_tracking = None
        pd_industry_history_tracking_lst5days = None
        pd_industry_history_tracking_5daysbeforeyesterday = None
        gc.collect()

        # 样式变量
        font_size = 32
        title_font_size = 35
        # 设置图像的宽度和高度（例如，1920x1080像素）
        fig_width, fig_height = 1440, 900
        scale_factor = 1
        light_text_color = "rgba(255, 255, 255, 0.8)"
        dark_text_color = "#000000"
        pie_sequential_color = px.colors.sequential.Reds_r
        # customized_sequential_color = [
        #     "#ab0d34",
        #     "#ffa700",
        #     "#00a380",
        #     "#0d876d",
        # ]
        customized_sequential_color = [
            "#d60a22",
            "#ea7034",
            "#81a949",
            "#037b66",
        ]
        # 策略颜色
        strategy_colors_light = [
            "#0c6552",
            "#0d876d",
            "#00a380",
            "#ffa700",
            "#d50b3e",
            "#a90a3f",
            "#7a0925",
        ]

        strategy_colors_dark = [
            "#0d7b67",
            "#0e987f",
            "#01b08f",
            "#ffa700",
            "#e90c4a",
            "#cf1745",
            "#b6183d",
        ]

        # 多样化颜色
        # diverse_colors5_light = [
        #     "#80233b",
        #     "#af2d4e",
        #     "#de3761",
        #     "#0d876d",
        #     "#0c6552",
        # ]
        # diverse_colors5_dark = [
        #     "#b52748",
        #     "#d1315a",
        #     "#f44577",
        #     "#0e987f",
        #     "#0d7b67",
        # ]
        diverse_colors5_light = [
            "#d60a22",
            "#ea7034",
            "#ffd747",
            "#81a949",
            "#037b66",
        ]
        diverse_colors5_dark = [
            "#d60a22",
            "#ea7034",
            "#ffd747",
            "#81a949",
            "#037b66",
        ]

        # 热图颜色
        heatmap_colors4_light = [
            "rgba(3, 123, 102, 0.9)",
            "rgba(129, 169, 73, 0.3)",
            "rgba(234, 112, 52, 0.3)",
            "rgba(214, 10, 34, 0.9)",
        ]
        heatmap_colors4_dark = [
            "rgba(3, 123, 102, 0.9)",
            "rgba(129, 169, 73, 0.3)",
            "rgba(234, 112, 52, 0.3)",
            "rgba(214, 10, 34, 0.9)",
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
        replace_dict = pd_industry_history_tracking.set_index("industry_new")[
            "combined"
        ].to_dict()
        pd_top20_industry["industry"] = (
            pd_top20_industry["industry"]
            .map(replace_dict)
            .combine_first(pd_top20_industry["industry"])
        )

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
        # hex_colors = sample_colorscale(
        #     # pie_sequential_color,  # 原始色阶
        #     customized_sequential_color,
        #     samplepoints=np.linspace(0, 1, 20),  # 生成 20 个等间距点
        #     colortype="rgb",  # 输出为十六进制
        # )
        # 生成20个颜色，分段填充
        # hex_colors = [customized_sequential_color[i // 5] for i in range(20)]
        hex_colors = ["rgba(0,0,0,0)" for _ in range(20)]
        alpha = 1
        # 将 RGB 转换为 RGBA，添加透明度
        rgba_colors = []
        for color in hex_colors:
            if isinstance(color, str) and color.startswith("rgb"):
                values = [
                    float(x)
                    for x in color[color.find("(") + 1 : color.find(")")].split(",")[:3]
                ]
                rgba_colors.append(
                    f"rgba({int(values[0])}, {int(values[1])}, {int(values[2])}, {alpha})"
                )
            else:
                r, g, b = [int(x * 255) for x in to_rgb(color)]
                rgba_colors.append(f"rgba({r}, {g}, {b}, {alpha})")
        # 文本颜色
        text_colors = [get_text_color(color, "light") for color in rgba_colors]

        # 创建 Treemap 图
        fig = go.Figure(
            go.Treemap(
                labels=pd_top20_industry["industry"],
                parents=[None] * len(pd_top20_industry),  # 顶层节点为空
                values=pd_top20_industry["cnt"],
                texttemplate="%{label}<br>%{percentParent:.0%}",  # 自定义文本模板，强制换行
                insidetextfont=dict(
                    size=font_size, color=dark_text_color, family="Arial"
                ),
                textposition="middle center",
                marker=dict(
                    colors=hex_colors,
                    line=dict(color="rgba(200, 200, 200, 0.8)", width=1),
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
            treemapcolorway=hex_colors,  # 确保颜色一致
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_postion_byindustry_light.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        text_colors = [get_text_color(color, "dark") for color in rgba_colors]
        fig.update_traces(
            insidetextfont=dict(color=light_text_color),
            marker=dict(
                colors=hex_colors,
                line=dict(color="rgba(240, 240, 240, 0.9)", width=1),
            ),
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_postion_byindustry_dark.svg",
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
        pd_top20_profit_industry["industry"] = (
            pd_top20_profit_industry["industry"]
            .map(replace_dict)
            .combine_first(pd_top20_profit_industry["industry"])
        )
        # 文本颜色
        text_colors = [get_text_color(color, "light") for color in rgba_colors]
        # 创建 Treemap 图
        fig = go.Figure(
            go.Treemap(
                labels=pd_top20_profit_industry["industry"],
                parents=[""] * len(pd_top20_profit_industry),  # 顶层节点为空
                values=pd_top20_profit_industry["pl"],
                texttemplate="%{label}<br>%{percentParent:.0%}",  # 自定义文本模板，强制换行
                insidetextfont=dict(
                    size=font_size, color=dark_text_color, family="Arial"
                ),
                outsidetextfont=dict(color="grey"),
                textposition="middle center",
                marker=dict(
                    colors=hex_colors,
                    line=dict(color="rgba(200, 200, 200, 0.8)", width=1),
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
            treemapcolorway=hex_colors,  # 确保颜色一致
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_pl_byindustry_light.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        text_colors = [get_text_color(color, "dark") for color in hex_colors]
        fig.update_traces(
            insidetextfont=dict(color=light_text_color),
            marker=dict(
                colors=hex_colors,
                line=dict(color="rgba(240, 240, 240, 0.9)", width=1),
            ),
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_pl_byindustry_dark.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )

        pd_top20_profit_industry = None
        gc.collect()

        # 180天内策略交易概率
        spark_strategy_tracking_lst180days = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT t1.date
                    ,t1.symbol
                    ,t1.pnl
                    ,t2.strategy
                    ,ROW_NUMBER() OVER(PARTITION BY t1.date, t1.symbol ORDER BY ABS(t1.date - t2.date) ASC, t2.date DESC) AS rn
                FROM temp_position_detail t1 LEFT JOIN temp_transaction_detail t2 ON t1.symbol = t2.symbol AND t1.date >= t2.date AND t2.trade_type = 'buy'
                WHERE t1.date >= DATE_ADD('{}', -180) 
            )
            SELECT date
                    ,strategy
                    ,SUM(pnl) AS pnl
                    ,IF(COUNT(symbol) > 0, SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) / COUNT(symbol), 0) AS success_rate
            FROM tmp1
            WHERE rn = 1
            GROUP BY date, strategy
            ORDER BY date, pnl
            """.format(
                end_date
            )
        )
        pd_strategy_tracking_lst180days = spark_strategy_tracking_lst180days.toPandas()
        pd_strategy_tracking_lst180days["date"] = pd.to_datetime(
            pd_strategy_tracking_lst180days["date"]
        ).dt.date
        print("date:", pd_strategy_tracking_lst180days["date"])
        pd_strategy_tracking_lst180days["ema_success_rate"] = (
            pd_strategy_tracking_lst180days["success_rate"]
            .ewm(span=5, adjust=False)
            .mean()
        )
        pd_strategy_tracking_lst180days_group = (
            pd_strategy_tracking_lst180days.groupby("date")["pnl"].sum().reset_index()
        )  # 按日期分组并求和
        max_pnl = pd_strategy_tracking_lst180days_group["pnl"].max()

        # 创建带有两个 y 轴的子图布局
        fig = go.Figure()
        # 遍历每个策略并添加数据
        for i, (strategy, data) in enumerate(
            pd_strategy_tracking_lst180days.groupby("strategy")
        ):
            fig.add_trace(
                go.Scatter(
                    x=data["date"],
                    y=data["ema_success_rate"],
                    mode="lines",
                    name=strategy,
                    line=dict(width=3, color=strategy_colors_light[i], shape="hv"),
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
                gridcolor="rgba(0, 0, 0, 0.2)",
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
                gridcolor="rgba(0, 0, 0, 0.2)",
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
            bargap=0.2,
            bargroupgap=0.2,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_strategy_tracking_light.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        fig = go.Figure()
        # 遍历每个策略并添加数据
        for i, (strategy, data) in enumerate(
            pd_strategy_tracking_lst180days.groupby("strategy")
        ):
            fig.add_trace(
                go.Scatter(
                    x=data["date"],
                    y=data["ema_success_rate"],
                    mode="lines",
                    name=strategy,
                    line=dict(width=3, color=strategy_colors_dark[i], shape="hv"),
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
                gridcolor="rgba(255, 255, 255, 0.2)",
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
                gridcolor="rgba(255, 255, 255, 0.2)",
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
            bargap=0.2,
            bargroupgap=0.2,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_strategy_tracking_dark.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )

        pd_strategy_tracking_lst180days = None
        gc.collect()

        # 180天内交易明细分析
        spark_trade_info_lst180days = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT date
                    ,COUNT(symbol) AS total_cnt
                FROM temp_position_detail
                WHERE date >= DATE_ADD('{}', -180)
                GROUP BY date
            ), tmp11 AS (
                SELECT temp_timeseries.buy_date
                    ,IF(tmp1.total_cnt > 0, tmp1.total_cnt
                        ,LAST_VALUE(tmp1.total_cnt) IGNORE NULLS OVER (PARTITION BY temp_timeseries.partition_key ORDER BY temp_timeseries.buy_date)) AS total_cnt
                FROM (SELECT *, 1 AS partition_key FROM temp_timeseries) AS temp_timeseries LEFT JOIN tmp1 ON temp_timeseries.buy_date = tmp1.date
            ), tmp5 AS (
                SELECT date
                    ,SUM(IF(trade_type = 'buy', 1, 0)) AS buy_cnt
                    ,SUM(IF(trade_type = 'sell', 1, 0)) AS sell_cnt
                FROM temp_transaction_detail
                WHERE date >= DATE_ADD('{}', -180)
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
        pd_trade_info_lst180days = spark_trade_info_lst180days.toPandas()
        # df_grouped = pd_trade_info_lst180days.groupby("buy_date")[
        #     ["buy_cnt", "sell_cnt"]
        # ].sum()

        # max_sum = df_grouped.sum(axis=1).max()
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=pd_trade_info_lst180days["buy_date"],
                y=pd_trade_info_lst180days["total_cnt"],
                mode="lines+markers",
                name="Total",
                line=dict(color="#e01c3a", width=3),
                yaxis="y",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pd_trade_info_lst180days["buy_date"],
                y=pd_trade_info_lst180days["buy_cnt"],
                name="Long",
                marker_color="#e01c3a",
                marker_line_color="#e01c3a",
                yaxis="y2",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pd_trade_info_lst180days["buy_date"],
                y=pd_trade_info_lst180days["sell_cnt"],
                name="Short",
                marker_color="#0d876d",
                marker_line_color="#0d876d",
                yaxis="y2",
            )
        )
        # light mode
        fig.update_layout(
            title={
                "text": "Last 180 days trade info",
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
                gridcolor="rgba(0, 0, 0, 0.2)",
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
                gridcolor="rgba(0, 0, 0, 0.2)",
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
            bargap=0.2,
            bargroupgap=0.2,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=False,  # 自动调整大小
            width=fig_width,
            height=fig_height,
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_trade_trend_light.svg",
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
                gridcolor="rgba(255, 255, 255, 0.2)",
            ),
            yaxis=dict(
                title=dict(font=dict(color=light_text_color)),
                tickfont=dict(color=light_text_color),
                gridcolor="rgba(255, 255, 255, 0.2)",
            ),
            yaxis2=dict(
                title=dict(font=dict(color=light_text_color)),
                tickfont=dict(color=light_text_color),
            ),
            legend=dict(font=dict(color=light_text_color)),
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_trade_trend_dark.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )

        pd_trade_info_lst180days = None
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
                WHERE t1.date >= DATE_ADD('{}', -180)
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
        fig.update_traces(line=dict(width=3))
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
            showline=False,
            gridcolor="rgba(0, 0, 0, 0.2)",
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
            gridcolor="rgba(0, 0, 0, 0.2)",
            title=None,  # 设置为空字符串以隐藏y轴标题
            ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
            tickangle=0,  # 确保刻度标签水平显示
        )
        fig.update_layout(
            title="Last 180 days top5 Positions",
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
            yaxis=dict(automargin=False),  # 关闭自动边距计算
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_top_industry_position_trend_light.svg",
            width=fig_width,
            height=fig_height,
            scale=scale_factor,
        )
        # dark mode
        fig.update_xaxes(
            tickfont=dict(color=light_text_color),
            gridcolor="rgba(255, 255, 255, 0.2)",
            title_font=dict(color=light_text_color),
        )
        fig.update_yaxes(
            tickfont=dict(color=light_text_color),
            gridcolor="rgba(255, 255, 255, 0.2)",
            title_font=dict(color=light_text_color),
        )
        fig.update_layout(
            title_font=dict(color=light_text_color),
            legend_title_text="",
            legend=dict(font=dict(color=light_text_color)),
        )
        fig.write_image(
            f"./dashreport/assets/images/{self.market}_top_industry_position_trend_dark.svg",
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
                WHERE t1.date >= DATE_ADD('{}', -180)
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
            """.format(
                end_date
            )
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
        # light mode
        fig.update_traces(line=dict(width=3))
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color=dark_text_color, size=font_size, family="Arial"),
            showline=False,
            gridcolor="rgba(0, 0, 0, 0.2)",
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
            gridcolor="rgba(0, 0, 0, 0.2)",
            title=None,  # 设置为空字符串以隐藏y轴标题
            ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
            tickangle=0,  # 确保刻度标签水平显示
        )
        fig.update_layout(
            title="Last 180 days top5 pnl",
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
            yaxis=dict(automargin=False),  # 关闭自动边距计算
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_top_industry_pl_trend_light.svg",
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
        fig.update_traces(line=dict(width=3))
        fig.update_xaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(color=light_text_color, size=font_size, family="Arial"),
            showline=False,
            gridcolor="rgba(255, 255, 255, 0.2)",
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
            gridcolor="rgba(255, 255, 255, 0.2)",
            title_font=dict(
                size=title_font_size, color=light_text_color, family="Arial"
            ),
            title=None,  # 设置为空字符串以隐藏y轴标题
            ticklabelposition="inside",  # 将刻度标签移到坐标轴内部
            tickangle=0,  # 确保刻度标签水平显示
        )
        fig.update_layout(
            title="Last 180 days top5 pnl",
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
            yaxis=dict(automargin=False),  # 关闭自动边距计算
        )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_top_industry_pl_trend_dark.svg",
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
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t ) tt
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
                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                    FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t ) tt
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
                    [0, "rgba(0, 0, 0, 0)"],  # 完全透明
                    [1, "rgba(0, 0, 0, 0)"],  # 完全透明
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
                gridcolor="rgba(0, 0, 0, 0.2)",
                zeroline=False,
                showticklabels=True,
                dtick=1,  # 每天显示一个刻度
                tickfont=dict(size=font_size, family="Arial"),
            ),
            yaxis=dict(
                showgrid=False,
                gridcolor="rgba(0, 0, 0, 0.2)",
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

            # 基础字体大小和最大增量
            base_font_size = font_size - 8
            max_size_increase = 30  # 增加最大增量以增强反差

            # 计算绝对值的分位点
            abs_values = np.abs(pd_calendar_heatmap["s_pnl"])
            quantiles = np.quantile(abs_values, [0.2, 0.4, 0.6, 0.8])

            # 计算每档的字体大小
            font_steps = np.linspace(
                base_font_size, base_font_size + max_size_increase, 5
            )

            # 计算每档的字体粗细
            font_weights = [300, 350, 400, 450, 500]  # 5个档位的字体粗细

            abs_col3 = abs(col3_value)
            if abs_col3 <= quantiles[0]:
                dynamic_font_size = font_steps[0]
                dynamic_font_weight = font_weights[0]
            elif abs_col3 <= quantiles[1]:
                dynamic_font_size = font_steps[1]
                dynamic_font_weight = font_weights[1]
            elif abs_col3 <= quantiles[2]:
                dynamic_font_size = font_steps[2]
                dynamic_font_weight = font_weights[2]
            elif abs_col3 <= quantiles[3]:
                dynamic_font_size = font_steps[3]
                dynamic_font_weight = font_weights[3]
            else:
                dynamic_font_size = font_steps[4]
                dynamic_font_weight = font_weights[4]
            dynamic_font_size = int(dynamic_font_size)
            # 创建文本内容，显示日期和行业
            # text = f"<b>{date}</b><br>" + "<br>".join(col2_values[:3])
            # 创建文本内容 - 日期竖向展示，行业横向展示
            # 将日期拆分为年、月、日
            date_str = row["date"].strftime("%Y-%m-%d")
            year, month, day = date_str.split("-")
            if col3_value > 0:
                text_color = "#d60a22"
                vertical_date = f"{month}<b>↑</b><br>{day}"
            elif col3_value < 0:
                text_color = "#037b66"
                vertical_date = f"{month}<b>↓</b><br>{day}"
            else:
                # 零值 - 使用灰色
                text_color = dark_text_color
                dynamic_font_size = base_font_size
                vertical_date = f"{month}<br>{day}"

            # 行业信息
            industry_text = "<br>".join(col2_values[:2])

            # 添加日期注解（左侧）
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=vertical_date,
                showarrow=False,
                font=dict(
                    # color="rgba(100, 100, 100, 0.7)",
                    # size=font_size,
                    color=text_color,
                    size=dynamic_font_size,
                    weight=dynamic_font_weight,
                ),
                align="left",
                xanchor="center",
                yanchor="middle",
                xshift=-100,  # 向左偏移，使日期靠左
                # yshift=-100,
            )

            # 添加行业信息注解（右侧）
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=industry_text,
                showarrow=False,
                font=dict(
                    color=dark_text_color,
                    size=font_size,
                    # color=text_color,
                    # size=dynamic_font_size,
                ),
                align="center",
                xanchor="center",
                yanchor="middle",
                xshift=10,  # 向右偏移，使行业信息靠右
            )

        # 在每周之间添加横向分隔线
        unique_weeks = pd_calendar_heatmap["week_order"].unique()
        for week in unique_weeks[1:]:  # 跳过最后一个 week
            fig.add_hline(
                y=week - 0.5,
                line_dash="solid",
                line_color="gray",
                opacity=0.2,
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
                opacity=0.2,
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
                yanchor="top",
            )

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_industry_trend_heatmap_light.svg",
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
                    [0, "rgba(0, 0, 0, 0)"],  # 完全透明
                    [1, "rgba(0, 0, 0, 0)"],  # 完全透明
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
                gridcolor="rgba(255, 255, 255, 0.2)",
                zeroline=False,
                showticklabels=True,
                dtick=1,  # 每天显示一个刻度
                tickfont=dict(size=font_size, color=light_text_color, family="Arial"),
            ),
            yaxis=dict(
                showgrid=False,
                gridcolor="rgba(255, 255, 255, 0.2)",
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

            # 基础字体大小和最大增量
            base_font_size = font_size - 8
            max_size_increase = 30  # 增加最大增量以增强反差

            # 计算绝对值的分位点
            abs_values = np.abs(pd_calendar_heatmap["s_pnl"])
            quantiles = np.quantile(abs_values, [0.2, 0.4, 0.6, 0.8])

            # 计算每档的字体大小
            font_steps = np.linspace(
                base_font_size, base_font_size + max_size_increase, 5
            )

            # 计算每档的字体粗细
            font_weights = [300, 350, 400, 450, 500]  # 5个档位的字体粗细

            abs_col3 = abs(col3_value)
            if abs_col3 <= quantiles[0]:
                dynamic_font_size = font_steps[0]
                dynamic_font_weight = font_weights[0]
            elif abs_col3 <= quantiles[1]:
                dynamic_font_size = font_steps[1]
                dynamic_font_weight = font_weights[1]
            elif abs_col3 <= quantiles[2]:
                dynamic_font_size = font_steps[2]
                dynamic_font_weight = font_weights[2]
            elif abs_col3 <= quantiles[3]:
                dynamic_font_size = font_steps[3]
                dynamic_font_weight = font_weights[3]
            else:
                dynamic_font_size = font_steps[4]
                dynamic_font_weight = font_weights[4]
            dynamic_font_size = int(dynamic_font_size)

            date_str = row["date"].strftime("%Y-%m-%d")
            year, month, day = date_str.split("-")
            if col3_value > 0:
                text_color = "#d60a22"
                vertical_date = f"{month}<b>↑</b><br>{day}"
            elif col3_value < 0:
                text_color = "#037b66"
                vertical_date = f"{month}<b>↓</b><br>{day}"
            else:
                # 零值 - 使用灰色
                text_color = light_text_color
                dynamic_font_size = base_font_size
                vertical_date = f"{month}<br>{day}"

            # 行业信息
            industry_text = "<br>".join(col2_values[:2])

            # 添加日期注解（左侧）
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=vertical_date,
                showarrow=False,
                font=dict(
                    # color="rgba(200, 200, 200, 0.7)",
                    # size=font_size,
                    color=text_color,
                    size=dynamic_font_size,
                    weight=dynamic_font_weight,
                ),
                align="left",
                xanchor="center",
                yanchor="middle",
                xshift=-100,  # 向左偏移，使日期靠左
                # yshift=-100,
            )

            # 添加行业信息注解（右侧）
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=industry_text,
                showarrow=False,
                font=dict(
                    color=light_text_color,
                    size=font_size,
                ),
                align="center",  # 左对齐
                xanchor="center",
                yanchor="middle",
                xshift=10,  # 向右偏移，使行业信息靠右
            )

        # 在每周之间添加横向分隔线
        unique_weeks = pd_calendar_heatmap["week_order"].unique()
        for week in unique_weeks[1:]:  # 跳过最后一个 week
            fig.add_hline(
                y=week - 0.5,
                line_dash="solid",
                line_color="white",
                opacity=0.2,
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
                opacity=0.2,
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

        fig.write_image(
            f"./dashreport/assets/images/{self.market}_industry_trend_heatmap_dark.svg",
            width=fig_width,
            height=fig_height,
            scale=2,
        )

        spark.stop()
        subject = f"""{self.market.upper()} Stock Market Trends - {end_date}""".format(
            end_date=end_date
        )
        image_path_return_light = (
            f"./dashreport/assets/images/{self.market}_tr_light.svg"
        )
        image_path_return_dark = f"./dashreport/assets/images/{self.market}_tr_dark.svg"
        image_path = [
            f"./dashreport/assets/images/{self.market}_postion_byindustry_light.svg",
            f"./dashreport/assets/images/{self.market}_postion_byindustry_dark.svg",
            f"./dashreport/assets/images/{self.market}_pl_byindustry_light.svg",
            f"./dashreport/assets/images/{self.market}_pl_byindustry_dark.svg",
            f"./dashreport/assets/images/{self.market}_trade_trend_light.svg",
            f"./dashreport/assets/images/{self.market}_trade_trend_dark.svg",
            f"./dashreport/assets/images/{self.market}_top_industry_position_trend_light.svg",
            f"./dashreport/assets/images/{self.market}_top_industry_position_trend_dark.svg",
            f"./dashreport/assets/images/{self.market}_top_industry_pl_trend_light.svg",
            f"./dashreport/assets/images/{self.market}_top_industry_pl_trend_dark.svg",
            f"./dashreport/assets/images/{self.market}_industry_trend_heatmap_light.svg",
            f"./dashreport/assets/images/{self.market}_industry_trend_heatmap_dark.svg",
            f"./dashreport/assets/images/{self.market}_strategy_tracking_light.svg",
            f"./dashreport/assets/images/{self.market}_strategy_tracking_dark.svg",
            image_path_return_light,
            image_path_return_dark,
        ]
        html_content = """
                    <html>
                    <head>
                        <style>
                            /* 基础样式 - 确保兼容性 */
                            * {{
                                box-sizing: border-box;
                                margin: 0;
                                padding: 0;
                                -webkit-text-size-adjust: 100%;
                                text-size-adjust: 100%;
                                -ms-text-size-adjust: 100%;
                            }}
                            
                            body {{
                                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                                line-height: 1.5;
                            }}
                            
                            .email-container {{
                                width: 100%;
                                margin: 0 auto;
                                padding: 0px;
                            }}
                            
                            h2 {{
                                font-size: 24px;
                                margin-bottom: 20px;
                                text-align: left ;
                                word-break: break-all;
                            }}
                            
                            .image-container {{
                                margin-bottom: 10px;
                                border-radius: 0px;
                                overflow: hidden;
                                width: 100% ;
                                min-width: 100% ;
                            }}
                            
                            img {{
                                display: block;
                                height: auto ;
                                margin: 0 auto ;
                                width: 100% ;
                                min-width: 100% ;
                            }}
                            
                            figcaption {{
                                padding: 12px;
                                text-align: center;
                                font-style: italic;
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
                                    margin: 0 auto;
                                    padding: 0px;
                                    width: 100%;
                                    min-width: 100%; 
                                }}

                                .image-container {{
                                    margin-bottom: 10px;
                                    border-radius: 0px;
                                    overflow: hidden;
                                    width: 100%;
                                    min-width: 100%;
                                }} 
                                
                                h2 {{
                                    font-size: 35px;
                                    word-break: break-all;
                                }}
                                
                                figcaption {{
                                    padding: 10px;
                                }}
                            }}
                        </style>
                    </head>
                    <body>
                        <div class="email-container">
                            <h2>The current cash is {cash}, the final portfolio value is {final_value}, the number of backtesting list is {stock_cnt}</h2>
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

        final_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
            <style>
                /* 合并所有样式到这里 */
                body {{
                    margin: 0;
                    padding: 0;
                    width: 100vw;
                    min-width: 100vw;
                }}            
                .email-wrapper {{
                    width: 800px !important;
                    margin: 0 auto !important;
                    padding: 0;                    
                }}               
                @media screen and (max-width: 480px) {{
                    .email-wrapper {{
                        width: 100% !important;
                        min-width: 100% !important;
                    }}
                }}
                /* 其它样式... */
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                {html_content}
                {html}
                {html1}
                {html2}
            </div>
        </body>
        </html>
        """

        MyEmail().send_email_embedded_image(subject, final_html, image_path)

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
        start_date = pd.to_datetime(end_date) - pd.DateOffset(days=180)
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
                FROM tmp1 WHERE trade_type = 'sell' AND l_date >= DATE_ADD('{}', -180)
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
                , t1.name
                , ROUND(t1.total_value / 100000000, 1) AS total_value            
                , t1.buy_date
                , t1.price
                , t1.adjbase
                , t1.pnl
                , t1.pnl_ratio
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
            """.format(
                end_date, end_date
            )
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
        counter = total_pages

        for page in range(total_pages):
            start_row = page * rows_per_page
            end_row = min(start_row + rows_per_page, total_rows)
            page_data = pd_position_history.iloc[start_row:end_row]

            # 生成表格 HTML
            table_html = "<h2>Open Position List</h2>" + page_data.style.hide(
                axis=1, subset=["PNL"]
            ).format(
                {
                    "TOTAL VALUE": "{:.2f}",
                    "BASE": "{:.2f}",
                    "ADJBASE": "{:.2f}",
                    "PNL RATIO": "{:.2%}",
                    "AVG TRANS": "{:.0f}",
                    "AVG DAYS": "{:.0f}",
                    "WIN RATE": "{:.2%}",
                    "TOTAL PNL RATIO": "{:.2%}",
                }
            ).apply(
                highlight_row, axis=1
            ).background_gradient(
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
                            ("padding", "2px"),
                            # ("font-size", "20px"),
                        ],
                    ),
                    # 表格数据单元格样式
                    dict(
                        selector="td",
                        props=[
                            ("border", "1px solid #ccc"),
                            ("text-align", "left"),
                            ("padding", "2px"),
                            # (
                            #     "font-size",
                            #     "20px",
                            # ),
                        ],
                    ),
                ]
            ).set_properties(
                subset=["NAME"],
                **{
                    "min-width": "100px !important",
                    "max-width": "100%",
                    "padding": "0",
                },
                overwrite=False,
            ).set_sticky(
                axis="columns"
            ).to_html(
                doctype_html=True,
                escape=False,
                table_attributes='style="border-collapse: collapse; border: 0.2px solid #ccc"; width: 100%;',
            )

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
                    background-color: transparent;
                    color: black;
                    display: table ;
                    -webkit-text-size-adjust: 100%;
                    text-size-adjust: 100%;
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;                            
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
                    margin: 0 auto;  /* 居中显示 */
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
                        margin: 0 auto;  /* 居中显示 */
                    }
                }
                /* 移动设备优化 */
                @media screen and (max-width: 480px) {
                    body {
                        width: 100%;                              
                    }
                    table {
                        width: 100%;
                        margin: 0 auto;  /* 居中显示 */
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
                FROM tmp1 WHERE trade_type = 'sell' AND l_date >= DATE_ADD('{}', -180)
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
                    ,ROUND(total_value / 100000000, 1) AS total_value
                FROM temp_latest_stock_info
                GROUP BY symbol
                    ,name
                    ,total_value
            )
            SELECT t1.symbol
                , t3.name
                , t3.total_value            
                , t1.buy_date
                , t1.sell_date
                , t1.base_price AS price
                , t1.adj_price AS adjbase
                , t1.pnl
                , t1.pnl_ratio
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
                                                    SELECT buy_date, ROW_NUMBER() OVER(PARTITION BY partition_key ORDER BY buy_date DESC) AS row_num
                                                    FROM (SELECT DISTINCT buy_date, 1 AS partition_key FROM temp_timeseries) t 
                                                ) tt WHERE row_num = 5)
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol AND t1.sell_date = t2.sell_date
                LEFT JOIN tmp3 t3 ON t1.symbol = t3.symbol
            """.format(
                end_date
            )
        )

        pd_position_reduction = spark_position_reduction.toPandas()

        if not pd_position_reduction.empty:
            pd_position_reduction.rename(
                columns={
                    "symbol": "SYMBOL",
                    "name": "NAME",
                    "total_value": "TOTAL VALUE",
                    "buy_date": "OPEN DATE",
                    "sell_date": "CLOSE DATE",
                    "price": "BASE",
                    "adjbase": "ADJBASE",
                    "pnl": "PNL",
                    "pnl_ratio": "PNL RATIO",
                    "his_days": "HIS DAYS",
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
                table_html = "<h2>Close Position List Last 5 Days</h2>" + page_data.style.hide(
                    axis=1, subset=["PNL"]
                ).format(
                    {
                        "TOTAL VALUE": "{:.2f}",
                        "BASE": "{:.2f}",
                        "ADJBASE": "{:.2f}",
                        "PNL RATIO": "{:.2%}",
                        "HIS DAYS": "{:.0f}",
                    }
                ).background_gradient(
                    subset=["BASE", "ADJBASE"], cmap=cm
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
                        # 表头样式
                        dict(
                            selector="th",
                            props=[
                                ("border", "1px solid #ccc"),
                                ("text-align", "left"),
                                ("padding", "2px"),
                                # ("font-size", "20px"),
                            ],
                        ),
                        # 表格数据单元格样式
                        dict(
                            selector="td",
                            props=[
                                ("border", "1px solid #ccc"),
                                ("text-align", "left"),
                                ("padding", "2px"),
                                # (
                                #     "font-size",
                                #     "20px",
                                # ),
                            ],
                        ),
                    ]
                ).set_properties(
                    subset=["NAME"],
                    **{
                        "min-width": "100px !important",
                        "max-width": "100%",
                        "padding": "0",
                    },
                    overwrite=False,
                ).set_sticky(
                    axis="columns"
                ).to_html(
                    doctype_html=True,
                    escape=False,
                    table_attributes='style="border-collapse: collapse; border: 0.2px solid #ccc"; width: 100%;',
                )

                # 添加分页导航
                navigation_html = "<div style='text-align: center; margin: 32px;'>"
                if page > 0:
                    navigation_html += f"<a href='#page-{page + counter}' style='margin-right: 32px;'>Previous</a>"
                if page < total_pages - 1:
                    navigation_html += f"<a href='#page-{page + 2 + counter}' style='margin-left: 32px;'>Next</a>"
                navigation_html += "</div>"

                # 包装每页内容
                page_html = f"""
                <div id='page-{page + 1 + counter}' style='margin-bottom: 20px;'>
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
                        background-color: transparent;
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
                    /* 移动设备优化 */
                    @media screen and (max-width: 480px) {
                        body {
                            width: 100%;                              
                        }
                        table {
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

        # 180天内交易明细分析
        spark_trade_info_lst180days = spark.sql(
            """ 
            WITH tmp1 AS (
                SELECT date
                    ,COUNT(symbol) AS total_cnt
                FROM temp_position_detail
                WHERE date >= DATE_ADD('{}', -180)
                GROUP BY date
            ), tmp11 AS (
                SELECT temp_timeseries.buy_date
                    ,IF(tmp1.total_cnt > 0
                    ,tmp1.total_cnt
                    ,LAST_VALUE(tmp1.total_cnt) IGNORE NULLS OVER (PARTITION BY temp_timeseries.partition_key ORDER BY temp_timeseries.buy_date)) AS total_cnt
                FROM (SELECT *, 1 AS partition_key FROM temp_timeseries) AS temp_timeseries LEFT JOIN tmp1 ON temp_timeseries.buy_date = tmp1.date
            ), tmp5 AS (
                SELECT date
                    ,SUM(IF(trade_type = 'buy', 1, 0)) AS buy_cnt
                    ,SUM(IF(trade_type = 'sell', 1, 0)) AS sell_cnt
                FROM temp_transaction_detail
                WHERE date >= DATE_ADD('{}', -180)
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
        pd_trade_info_lst180days = spark_trade_info_lst180days.toPandas()

        # 设置图像的宽度和高度（例如，1920x1080像素）
        fig_width, fig_height = 1440, 900
        scale_factor = 1.2
        font_size = 32
        title_font_size = 35
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=pd_trade_info_lst180days["buy_date"],
                y=pd_trade_info_lst180days["total_cnt"],
                mode="lines+markers",
                name="total stock",
                line=dict(color="#e01c3a", width=3),
                yaxis="y",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pd_trade_info_lst180days["buy_date"],
                y=pd_trade_info_lst180days["buy_cnt"],
                name="long",
                marker_color="#e01c3a",
                marker_line_color="#e01c3a",
                yaxis="y2",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pd_trade_info_lst180days["buy_date"],
                y=pd_trade_info_lst180days["sell_cnt"],
                name="short",
                marker_color="#0d876d",
                marker_line_color="#0d876d",
                yaxis="y2",
            )
        )
        # light mode
        fig.update_layout(
            title={
                "text": "Last 180 days trade info",
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
                gridcolor="rgba(0, 0, 0, 0.2)",
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
                gridcolor="rgba(0, 0, 0, 0.2)",
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
            bargap=0.2,
            bargroupgap=0.2,
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
                "text": "Last 180 days trade info",
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
                gridcolor="rgba(255, 255, 255, 0.2)",
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
                gridcolor="rgba(255, 255, 255, 0.2)",
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
            bargap=0.2,
            bargroupgap=0.2,
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

        pd_trade_info_lst180days = None
        gc.collect()

        spark.stop()

        if self.market == "us":
            subject = "US Stock Market ETF Trends"
            image_path_return_light = "./dashreport/assets/images/etf_tr_light.svg"
            image_path_return_dark = "./dashreport/assets/images/etf_tr_dark.svg"
        elif self.market == "cn":
            subject = f"""CN Stock Market ETF Trends - {end_date}""".format(
                end_date=end_date
            )
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
                }}
                
                .email-container {{
                    width: 100%;
                    margin: 0 auto;
                    padding: 0px;
                }}
                
                h2 {{
                    font-size: 24px;
                    margin-bottom: 20px;
                    text-align: left ;
                    word-break: break-all;
                }}
                
                .image-container {{
                    margin-bottom: 10px;
                    border-radius: 0px;
                    overflow: hidden;
                    width: 100% ;
                    min-width: 100% ;
                }}
                
                img {{
                    display: block;
                    height: auto ;
                    margin: 0 auto ;
                    width: 100% ;
                    min-width: 100% ;
                }}
                
                figcaption {{
                    padding: 12px;
                    text-align: center;
                    font-style: italic;
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
                        margin: 0 auto;
                        padding: 0px;
                        width: 100%;
                        min-width: 100%; 
                    }}

                    .image-container {{
                        margin-bottom: 10px;
                        border-radius: 0px;
                        overflow: hidden;
                        width: 100%;
                        min-width: 100%;
                    }} 
                    
                    h2 {{
                        font-size: 35px;
                        word-break: break-all;
                    }}
                    
                    figcaption {{
                        padding: 10px;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="email-container">
                <h2>The current cash is {cash}, the final portfolio value is {final_value}</h2>
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
        """.format(
            cash=cash, final_value=final_value
        )

        final_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
            <style>
                /* 合并所有样式到这里 */
                body {{
                    margin: 0;
                    padding: 0;
                    width: 100vw;
                    min-width: 100vw;
                }}            
                .email-wrapper {{
                    width: 800px !important;
                    margin: 0 auto !important;
                    padding: 0;                    
                }}               
                @media screen and (max-width: 480px) {{
                    .email-wrapper {{
                        width: 100% !important;
                        min-width: 100% !important;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                {html_content}
                {html}
                {html2}
            </div>
        </body>
        </html>
        """

        MyEmail().send_email_embedded_image(subject, final_html, image_path)
