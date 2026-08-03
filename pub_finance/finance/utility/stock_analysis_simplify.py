#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from finance.utility.email_uti import MyEmail
from finance.utility.fileinfo import FileInfo
from finance.utility.tickerinfo import TickerInfo
import pandas as pd
from pyspark.sql import SparkSession
import os
import gc
from finance.utility.toolkit import ToolKit
import os
from finance import FINANCE_ROOT

# mpl.rcParams["font.sans-serif"] = ["SimHei"]  # 用来正常显示中文标签


def initialize_spark(app_name: str, memory: str = "512m", partitions: int = 1):
    """
    初始化 SparkSession
    """
    # 如果外部环境设置了 JAVA_TOOL_OPTIONS，会在 JVM 启动时打印，尽量在这里移除
    os.environ.pop("JAVA_TOOL_OPTIONS", None)

    # 指定仓库内的 log4j2 配置文件，避免 Spark 使用默认配置并打印提示
    log4j_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "conf", "log4j2.properties")
    )
    # 不再通过 --add-modules 向 JVM 注入 incubator 模块，以避免启动时警告
    extra_java_opts = f"-Dlog4j2.configurationFile=file:{log4j_path}"

    return (
        SparkSession.builder.master("local")
        .appName(app_name)
        .config("spark.driver.memory", memory)
        .config("spark.executor.memory", memory)
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.driver.extraJavaOptions", extra_java_opts)
        .config("spark.executor.extraJavaOptions", extra_java_opts)
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
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        spark.sparkContext.setLogLevel("ERROR")

        """ 
        读取交易相关数据，交易明细，持仓明细，仓位日志明细，行业信息
        """
        file = FileInfo(self.trade_date, self.market)
        # 国债信息
        file_gz = file.get_file_path_gz
        cols = ["code", "name", "date", "new"]
        spark_gz = spark.read.csv(str(file_gz), header=True, inferSchema=True)
        spark_gz = spark_gz.toDF(*cols)
        spark_gz.createOrReplaceTempView("temp_gz")
        # 交易明细
        file_path_trade = file.get_file_path_trade
        cols = ["idx", "symbol", "date", "trade_type", "price", "size", "strategy"]
        spark_transaction_detail = spark.read.csv(
            str(file_path_trade), header=None, inferSchema=True
        )
        spark_transaction_detail = spark_transaction_detail.toDF(*cols)
        spark_transaction_detail.createOrReplaceTempView("temp_transaction_detail")
        # 持仓明细, spark读取
        file_cur_p = file.get_file_path_position
        spark_cur_position = spark.read.csv(
            str(file_cur_p), header=True, inferSchema=True
        )
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
        spark_industry_info = spark.read.csv(str(file_path_indus), header=True)
        spark_industry_info.createOrReplaceTempView("temp_industry_info")
        # 仓位日志明细
        file_path_position_detail = file.get_file_path_position_detail
        cols = [
            "idx",
            "symbol",
            "date",
            "price",
            "adjbase",
            "shares",
            "pnl",
            "volume",
            "daily_return",
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown",
            "strategy",
        ]
        spark_position_detail = spark.read.csv(
            str(file_path_position_detail), header=None, inferSchema=True
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
            str(file_name_day), header=True, inferSchema=True
        )
        spark_latest_stock_info = spark_latest_stock_info.select(cols)
        spark_latest_stock_info.createOrReplaceTempView("temp_latest_stock_info")
        pd_latest_stock_info = spark_latest_stock_info[
            ["name", "symbol", "total_value"]
        ].toPandas()

        # 获取pe历史数据
        pe_df = TickerInfo(self.trade_date, self.market).get_recent_pe_data()

        # 如果pdf为空，创建一个空的Spark DataFrame，确保列名和类型一致
        if pe_df.empty:
            # 创建一个空的Spark DataFrame，具有相同的结构
            schema = "symbol string, date date, pe string, total_value double"
            spark_pe_df = spark.createDataFrame([], schema)
        else:
            spark_pe_df = spark.createDataFrame(pe_df)

        spark_pe_df.createOrReplaceTempView("temp_pe_trend")

        # 获取gz历史数据
        gz_df = TickerInfo(self.trade_date, self.market).get_recent_gz_data()
        # 如果pdf为空，创建一个空的Spark DataFrame，确保列名和类型一致
        if gz_df.empty:
            # 创建一个空的Spark DataFrame，具有相同的结构
            schema = "date date, new double"
            spark_gz_df = spark.createDataFrame([], schema)
        else:
            spark_gz_df = spark.createDataFrame(gz_df)
        spark_gz_df.createOrReplaceTempView("temp_gz_trend")

        # 获取回测股票列表
        stock_list = TickerInfo(self.trade_date, self.market).get_stock_list()
        stock_list_tuples = [(symbol,) for symbol in stock_list]
        spark_stock_list = spark.createDataFrame(stock_list_tuples, schema=["symbol"])
        spark_stock_list.createOrReplaceTempView("temp_stock_list")

        # 现金资产日志记录
        file_path_cash_asset = file.get_file_path_cash_asset
        cols = [
            "idx",
            "date",
            "cash",
            "final_value",
        ]
        spark_cash_asset = spark.read.csv(
            str(file_path_cash_asset), header=None, inferSchema=True
        )
        spark_cash_asset = spark_cash_asset.toDF(*cols)
        spark_cash_asset.createOrReplaceTempView("temp_cash_asset")

        # 生成时间序列，用于时间序列补齐
        end_date = pd.to_datetime(self.trade_date).strftime("%Y-%m-%d")
        date_range = pd.date_range(
            start=(pd.to_datetime(end_date) - pd.DateOffset(days=360)).strftime(
                "%Y-%m-%d"
            ),
            end=end_date,
            freq="D",
        )
        pd_timeseries = pd.DataFrame({"buy_date": date_range})
        # 将日期转换为字符串格式 'YYYYMMDD'
        pd_timeseries["trade_date"] = pd_timeseries["buy_date"].dt.strftime("%Y%m%d")

        # 根据市场类型过滤非交易日
        toolkit = ToolKit("identify trade date")
        if self.market in ("us", "us_special", "us_dynamic"):
            pd_timeseries = pd_timeseries[
                pd_timeseries["trade_date"].apply(toolkit.is_us_trade_date)
            ]
        elif self.market in ("cn", "cn_dynamic"):
            pd_timeseries = pd_timeseries[
                pd_timeseries["trade_date"].apply(toolkit.is_cn_trade_date)
            ]
        pd_timeseries = pd_timeseries.sort_values("buy_date").reset_index(drop=True)
        # 获取时间窗口配置
        chart_time_range = max(
            180, ToolKit.get_config("chart_display.chart_time_range", default=120)
        )
        minichart_time_range = max(
            60, ToolKit.get_config("chart_display.minichart_time_range", default=60)
        )
        start_date = pd_timeseries.iloc[-chart_time_range]["buy_date"]
        start_date_minichart = pd_timeseries.iloc[-minichart_time_range]["buy_date"]
        pd_timeseries = pd_timeseries.tail(chart_time_range)

        spark_timeseries = spark.createDataFrame(
            pd_timeseries.astype({"buy_date": "string"})
        )

        spark_timeseries.createOrReplaceTempView("temp_timeseries")

        """
        临时视图，方便复用
        """
        spark_transaction_logs = spark.sql("""
            WITH tmp AS (
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
            )
            SELECT symbol
                ,l_date AS buy_date
                ,l_price AS base_price
                ,l_size AS base_size
                ,l_strategy AS buy_strategy
                ,date AS sell_date
                ,price AS adj_price
                ,size AS adj_size
                ,strategy AS sell_strategy
            FROM tmp WHERE trade_type = 'sell' AND l_date >= '{}'
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
            FROM tmp WHERE trade_type = 'buy' AND l_date IS NULL          
            """.format(start_date))
        spark_transaction_logs.createOrReplaceTempView("temp_transaction_logs")

        """
        辅助日期函数，获取第N个交易日
        """

        def get_date_rank_subquery(rank_num):
            return f"""
            (SELECT buy_date FROM (
                SELECT buy_date, ROW_NUMBER() OVER(ORDER BY buy_date DESC) AS rn
                FROM (SELECT DISTINCT buy_date FROM temp_timeseries)
            ) WHERE rn = {rank_num})
            """

        """
        股票最新市值以及pe
        """
        spark_temp_symbol_pe = spark.sql("""
            SELECT t1.symbol
                , t1.total_value
                , t1.pe_double as pe                
                , CASE WHEN t1.pe_double IS NULL OR t2.new IS NULL OR t1.pe_double = 0 THEN 0
                    ELSE ROUND((1.0 / t1.pe_double - t2.new / 100.0) * 100, 1) END AS erp
            FROM
                (
                SELECT symbol,
                        total_value,
                        COALESCE(
                            TRY_CAST(
                                CASE 
                                    WHEN pe IS NULL OR TRIM(pe) IN ('', '-', 'NULL', 'N/A') THEN NULL
                                    ELSE TRIM(pe)
                                END AS DOUBLE
                            ),
                            NULL
                        ) AS pe_double
                FROM temp_latest_stock_info
                ) t1 LEFT JOIN temp_gz t2 ON 1=1
            """)
        spark_temp_symbol_pe.createOrReplaceTempView("temp_symbol_pe")

        """ 
        行业板块历史数据分析
        """
        spark_industry_history_tracking = spark.sql(f"""
            WITH tmp AS (
                SELECT industry
                    ,SUM(IF(`p&l` >= 0, 1, 0)) AS pos_cnt
                    ,SUM(IF(`p&l` < 0, 1, 0)) AS neg_cnt
                    ,COUNT(*) AS p_cnt
                    ,SUM(IF(buy_date >= {get_date_rank_subquery(5)}, 1, 0)) AS l5_p_cnt
                    ,SUM(`p&l`) AS p_pnl
                    ,SUM(adjbase * size) AS adjbase
                    ,SUM(price * size) AS base
                FROM temp_cur_position
                GROUP BY industry
            ), tmp2 AS (
                SELECT t1.industry
                    ,COUNT(t2.symbol) * 1.00 AS his_trade_cnt
                    ,COUNT(DISTINCT t2.symbol) AS his_symbol_cnt
                    ,COUNT(DISTINCT IF(t2.sell_date >= {get_date_rank_subquery(5)}, t2.symbol, null)) AS l5_close
                    ,SUM(IF(t2.sell_date IS NOT NULL, DATEDIFF(t2.sell_date, t2.buy_date), DATEDIFF('{end_date}', t2.buy_date))) AS his_days
                    ,SUM(IF(t2.sell_date IS NOT NULL AND t2.adj_price - t2.base_price >=0, 1, 0)) AS pos_cnt
                    ,SUM(IF(t2.sell_date IS NOT NULL AND t2.adj_price - t2.base_price < 0, 1, 0)) AS neg_cnt
                    ,SUM(IF(t2.sell_date IS NOT NULL, t2.adj_price * (-t2.adj_size) - t2.base_price * t2.base_size, 0)) AS his_pnl
                    ,SUM(IF(t2.sell_date IS NOT NULL, t2.adj_price * (-t2.adj_size), 0)) AS his_adjbase
                    ,SUM(IF(t2.sell_date IS NOT NULL, t2.base_price * t2.base_size, 0)) AS his_base
                FROM temp_industry_info t1 JOIN temp_transaction_logs t2 ON t1.symbol = t2.symbol
                GROUP BY t1.industry
            ),  industry_daily_pnl AS (
                -- 1. 预先算出每个行业每天的 PNL 和成交量（干净的日线数据）
                SELECT 
                    t3.industry, 
                    t2.date, 
                    SUM(t2.pnl) AS pnl, 
                    SUM(t2.volume) AS volume 
                FROM temp_position_detail t2 
                JOIN temp_industry_info t3 ON t2.symbol = t3.symbol 
                GROUP BY t3.industry, t2.date
            ),  industry_date_matrix AS (
                -- 2. 核心对齐：用时间轴和行业做网格对齐，如果没有数据，用 0.0 兜底，确保每个行业都拥有完美的 180 天
                SELECT 
                    m.industry,
                    t1.buy_date,
                    COALESCE(i.pnl, 0.0) AS pnl,
                    COALESCE(i.volume, 0.0) AS volume
                FROM temp_timeseries t1
                -- 交叉生成“所有行业 x 所有日期”的标本网格，这才是对齐时序的标准做法
                CROSS JOIN (SELECT DISTINCT industry FROM temp_industry_info) m
                LEFT JOIN industry_daily_pnl i ON m.industry = i.industry AND t1.buy_date = i.date
                WHERE t1.buy_date >= '{start_date_minichart}'
            ),  tmp3 AS (
                -- 3. 最终聚合：打包成结构体强行排序，彻底解决分布式乱序问题
                SELECT 
                    industry,
                    transform(sort_array(collect_list(struct_data)), x -> x.pnl) AS pnl_array,
                    transform(sort_array(collect_list(struct_data)), x -> x.volume) AS volume_array
                FROM (
                    SELECT 
                        industry,
                        NAMED_STRUCT(
                            'buy_date', buy_date,
                            'pnl', pnl,
                            'volume', volume
                        ) AS struct_data
                    FROM industry_date_matrix
                ) t
                GROUP BY industry
            ), tmp4 AS (
                SELECT temp_industry_info.industry, COUNT(temp_industry_info.symbol) AS ticker_cnt
                FROM temp_industry_info JOIN temp_stock_list ON temp_industry_info.symbol = temp_stock_list.symbol
                GROUP BY temp_industry_info.industry
            ), tmp5 AS (
                SELECT
                    temp_industry_info.industry AS industry,
                    CASE 
                        WHEN SUM(COALESCE(t3.total_value,0)) > 0 
                        THEN ROUND(SUM(t3.erp * COALESCE(t3.total_value,0)) / SUM(COALESCE(t3.total_value,0)), 1)
                        ELSE 0 
                    END AS industry_erp
                FROM temp_industry_info 
                LEFT JOIN temp_symbol_pe t3
                ON temp_industry_info.symbol = t3.symbol
                WHERE t3.erp > -50 and t3.erp < 50
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
                ,COALESCE(t3.volume_array,ARRAY(0)) AS volume_array
                ,IF(COALESCE(t1.his_symbol_cnt,0) = 0, 0, COALESCE(t1.his_trade_cnt,0) / COALESCE(t1.his_symbol_cnt,0) ) AS avg_his_trade_cnt
                ,IF(COALESCE(t1.his_trade_cnt,0) = 0, 0, COALESCE(t1.his_days,0) / COALESCE(t1.his_trade_cnt,0) ) AS avg_days
                ,IF((COALESCE(t1.pos_cnt,0) + COALESCE(t2.pos_cnt,0) + COALESCE(t1.neg_cnt,0) + COALESCE(t2.neg_cnt,0)) > 0, (COALESCE(t1.pos_cnt,0) + COALESCE(t2.pos_cnt,0)) / (COALESCE(t1.pos_cnt,0) + COALESCE(t2.pos_cnt,0) + COALESCE(t1.neg_cnt,0) + COALESCE(t2.neg_cnt,0)), 0) AS win_rate
                ,COALESCE(t5.industry_erp, 0) AS industry_erp
            FROM tmp2 t1 LEFT JOIN tmp t2 ON t1.industry = t2.industry
            LEFT JOIN tmp3 t3 ON t1.industry = t3.industry
            LEFT JOIN tmp4 t4 ON t1.industry = t4.industry
            LEFT JOIN tmp5 t5 ON t1.industry = t5.industry
            WHERE t2.p_cnt > 0
            ORDER BY COALESCE(t2.p_pnl,0) DESC
            """)

        """
        近10日行业加权平均涨幅
        """
        spark_industry_history_tracking_lstndays = spark.sql(f"""
            WITH stock_daily_flat AS (
                -- 核心融合层：只查一次表，同时把单股在最新日（1天前）和10天前的 pnl、adjbase 拉平到同一行
                SELECT
                    t1.symbol,
                    t2.industry,
                    t3.total_value,
                    -- 新逻辑所需字段（1天前是区间终点，10天前是区间起点）
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(1)} THEN t1.adjbase END) AS adjbase_1,
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(1)} THEN t1.price END) AS base_1, -- 1天前的买入价
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(10)} THEN t1.adjbase END) AS adjbase_10,
                    -- 老逻辑所需字段
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(1)} THEN t1.pnl END) AS pnl_1,
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(10)} THEN t1.pnl END) AS pnl_10
                FROM temp_position_detail t1
                JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                LEFT JOIN temp_latest_stock_info t3 ON t1.symbol = t3.symbol
                WHERE t1.date IN ({get_date_rank_subquery(1)}, {get_date_rank_subquery(10)})
                GROUP BY t1.symbol, t2.industry, t3.total_value
            )
            -- 最终聚合层：直接在最外层按行业分组，分流输出新老两套指标
            SELECT
                industry,
                -- 1. 老逻辑：(最新 1 天前总 pnl - 10 日前总 pnl) / 总市值
                -- (SUM(pnl_1 * total_value) - COALESCE(SUM(pnl_10 * total_value), 0)) / SUM(total_value) AS pnl_growth,

                -- 2. 新逻辑：个股涨幅 (由10天前持有至1天前，新老股平替) 的市值加权平均
                SUM(
                  -- 修正此处：终点 (adjbase_1) 减去 起点 (COALESCE(adjbase_10, base_1))
                  ( (adjbase_1 - COALESCE(adjbase_10, base_1)) / COALESCE(adjbase_10, base_1) )
                  * total_value
                ) / SUM(total_value) AS pnl_growth
            FROM stock_daily_flat
            -- 修正过滤：确保最新日有持仓，且计算起点的价格大于 0
            WHERE adjbase_1 IS NOT NULL AND COALESCE(adjbase_10, base_1) > 0
            GROUP BY industry
            HAVING SUM(total_value) > 0
            ORDER BY pnl_growth DESC
            """)
        """
        近5日 排名 vs 近10日 排名 ：短期对长期趋势的修正，寻找趋势共振/黄金交叉。适合做顺势突破或趋势跟踪。
        """
        spark_industry_history_tracking_ndaysbeforeyesterday = spark.sql(f"""
            WITH stock_daily_flat AS (
                -- 核心融合层：只查一次表，同时把单股在最新日（1天前）和5天前的 pnl、adjbase 拉平到同一行
                SELECT
                    t1.symbol,
                    t2.industry,
                    t3.total_value,
                    -- 新逻辑所需字段（1天前是区间终点，5天前是区间起点）
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(1)} THEN t1.adjbase END) AS adjbase_1,
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(1)} THEN t1.price END) AS base_1, -- 1天前的买入价
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(5)} THEN t1.adjbase END) AS adjbase_5,
                    -- 老逻辑所需字段
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(1)} THEN t1.pnl END) AS pnl_1,
                    MAX(CASE WHEN t1.date = {get_date_rank_subquery(5)} THEN t1.pnl END) AS pnl_5
                FROM temp_position_detail t1
                JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                LEFT JOIN temp_latest_stock_info t3 ON t1.symbol = t3.symbol
                WHERE t1.date IN ({get_date_rank_subquery(1)}, {get_date_rank_subquery(5)})
                GROUP BY t1.symbol, t2.industry, t3.total_value
            )
            -- 最终聚合层：直接在最外层按行业分组，分流输出新老两套指标
            SELECT
                industry,
                -- 1. 老逻辑：(最新 1 天前总 pnl - 5 日前总 pnl) / 总市值
                -- (SUM(pnl_1 * total_value) - COALESCE(SUM(pnl_5 * total_value), 0)) / SUM(total_value) AS pnl_growth,
                
                -- 2. 新逻辑：个股涨幅 (由5天前持有至1天前，新老股平替) 的市值加权平均
                SUM(
                  -- 修正此处：终点 (adjbase_1) 减去 起点 (COALESCE(adjbase_5, base_1)) 
                  ( (adjbase_1 - COALESCE(adjbase_5, base_1)) / COALESCE(adjbase_5, base_1) ) 
                  * total_value
                ) / SUM(total_value) AS pnl_growth
            FROM stock_daily_flat
            -- 修正过滤：确保最新日有持仓，且计算起点的价格大于 0
            WHERE adjbase_1 IS NOT NULL AND COALESCE(adjbase_5, base_1) > 0
            GROUP BY industry
            HAVING SUM(total_value) > 0
            ORDER BY pnl_growth DESC
            """)

        """
        [1-5日] 排名 vs [6-10日] 排名：速度的绝对改变（加速度），寻找极速反转。适合做超跌反弹或极速动量破位。
        """
        pd_industry_history_tracking_lstndays = (
            spark_industry_history_tracking_lstndays.toPandas()
        )
        pd_industry_history_tracking_ndaysbeforeyesterday = (
            spark_industry_history_tracking_ndaysbeforeyesterday.toPandas()
        )
        pd_industry_history_tracking_ndaysbeforeyesterday = (
            pd_industry_history_tracking_ndaysbeforeyesterday.sort_values(
                by="pnl_growth", ascending=False
            ).reset_index(drop=True)
        )

        pd_industry_history_tracking = spark_industry_history_tracking.toPandas()

        result_df = (
            pd_industry_history_tracking.merge(
                pd_industry_history_tracking_lstndays, on="industry", how="left"
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
                "industry_erp",
                "pnl",
                "pnl_ratio",
                "long_ratio",
                "avg_his_trade_cnt",
                "avg_days",
                "win_rate",
                "pnl_array",
                "volume_array",
                "pnl_growth",
            ]
        ].copy()
        index_diff_dict = {}
        for index, row in pd_industry_history_tracking.iterrows():
            industry = row["industry"]

            if (
                industry
                in pd_industry_history_tracking_ndaysbeforeyesterday["industry"].values
            ):
                # 核心修正：颠倒相减顺序，改为 (现在索引 - 过去索引)
                # 汽车行业：现在的 index 是 81（第82位），过去的 index 是 34（第35位）
                # 计算结果：81 - 34 = +47 (正数) -> 完美触发红色上箭头 ↑47
                past_index = pd_industry_history_tracking_ndaysbeforeyesterday[
                    pd_industry_history_tracking_ndaysbeforeyesterday["industry"]
                    == industry
                ].index.values[0]

                index_diff = index - past_index
                index_diff_dict[index] = index_diff

        pd_industry_history_tracking["index_diff"] = (
            pd_industry_history_tracking.index.map(index_diff_dict)
        )

        def create_arrow(value):
            if pd.isnull(value):
                return ""
            elif value > 0:
                return f"<span style='color:#ff4444;'><b>↑{abs(value):.0f}</b></span>"
            elif value < 0:
                return f"<span style='color:#00a859;'><b>↓{abs(value):.0f}</b></span>"
            else:
                return ""

        pd_industry_history_tracking["industry_new"] = pd_industry_history_tracking[
            "industry"
        ]
        pd_industry_history_tracking["industry"] = pd_industry_history_tracking.apply(
            lambda row: f"{row['industry']}{create_arrow(row['index_diff'])}",
            axis=1,
        )
        # 双列叠加绘图 - 正确的调用方式
        toolkit = ToolKit("draw line")
        pd_industry_history_tracking["pnl_trend"] = pd_industry_history_tracking.apply(
            lambda row: toolkit.create_line(
                row["pnl_array"],  # 作为第一个参数
                row["volume_array"],  # 作为第二个参数
            ),
            axis=1,
        )
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
                "industry_erp": "ERP",
            },
            inplace=True,
        )
        pd_industry_history_tracking.to_csv(
            FINANCE_ROOT / f"data/{self.market}_category.csv",
            columns=[
                "IND",
                "OPEN",
                "L5 OPEN",
                "L5 CLOSE",
                "ERP",
                "PROFIT",
                "PNL RATIO",
                "LRATIO",
                "AVG TRANS",
                "AVG DAYS",
                "WIN RATE",
                "pnl_growth",
                "index_diff",
                "industry_new",
                "PROFIT TREND",
            ],
            header=True,
        )
        print("板块数据生成完成...")
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

        spark_position_history = spark.sql(f""" 
            WITH tmp2 AS (
                SELECT symbol
                    ,COUNT(symbol) AS his_trade_cnt
                    ,SUM(IF(sell_date IS NOT NULL, DATEDIFF(sell_date, buy_date), DATEDIFF('{end_date}', buy_date))) AS his_days
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price >=0, 1, 0)) AS pos_cnt
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price < 0, 1, 0)) AS neg_cnt
                    ,SUM(IF(sell_date IS NOT NULL, adj_price * (-adj_size) - base_price * base_size, 0)) AS his_pnl
                    ,SUM(IF(sell_date IS NOT NULL, base_price * base_size, 0)) AS his_base_price
                    ,MAX(IF(sell_date IS NULL, buy_strategy, null)) AS buy_strategy
                FROM  temp_transaction_logs
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
                , t3.erp
                , t5.sharpe_ratio    
                , t5.sortino_ratio
                , t5.max_drawdown
                , t5.strategy_cnt
                , t6.daily_return_array
                , t1.buy_date
                , t1.price
                , t1.adjbase
                , t1.pnl
                , t1.pnl_ratio
                , COALESCE(t2.his_trade_cnt, 0) AS avg_trans
                , COALESCE(t2.his_days, 0) / t2.his_trade_cnt AS avg_days
                , (t1.pos_cnt + COALESCE(t2.pos_cnt,0)) / ( COALESCE(t2.pos_cnt,0) + COALESCE(t2.neg_cnt,0) + t1.pos_cnt + t1.neg_cnt) AS win_rate
                , (COALESCE(t2.his_pnl,0) + (t1.adjbase - t1.price) * t1.size) / (COALESCE(t2.his_base_price,0) + t1.price * t1.size) AS total_pnl_ratio
                , COALESCE(t5.strategy, t2.buy_strategy) AS buy_strategy
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
                LEFT JOIN temp_symbol_pe t3 ON t1.symbol = t3.symbol
                LEFT JOIN (
                    SELECT 
                        symbol,
                        COALESCE(sharpe_ratio, 0) AS sharpe_ratio,
                        COALESCE(sortino_ratio, 0) AS sortino_ratio,
                        COALESCE(max_drawdown, 0) AS max_drawdown,
                        COALESCE(strategy_cnt, 0) AS strategy_cnt,
                        strategy
                    FROM (
                        SELECT
                            c.symbol,
                            p.sharpe_ratio,
                            p.sortino_ratio,
                            p.max_drawdown,
                            p.strategy,
                            ROW_NUMBER() OVER (PARTITION BY c.symbol ORDER BY p.date DESC NULLS LAST) AS rn,
                            SUM(
                                CASE 
                                    WHEN p.strategy IS NULL THEN 0
                                    WHEN LAG(p.strategy) OVER (PARTITION BY c.symbol ORDER BY p.date ASC NULLS FIRST) = p.strategy 
                                    THEN 0 
                                    ELSE 1 
                                END
                            ) OVER (PARTITION BY c.symbol ORDER BY p.date ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS strategy_cnt
                        FROM temp_cur_position_with_latest_stock_info c
                        LEFT JOIN temp_position_detail p
                            ON p.symbol = c.symbol AND p.date >= c.buy_date
                    ) t
                    WHERE rn = 1
                ) t5 ON t1.symbol = t5.symbol
                LEFT JOIN (
                    SELECT 
                        symbol,
                        -- 3. 终极修复：使用 sort_array 依据结构体首个字段 (buy_date) 严格正序排列
                        -- 排序完成后，再使用 transform 表达式将各个时序指标抽离为独立的单列 Array
                        transform(sort_array(collect_list(struct_data)), x -> x.daily_return) AS daily_return_array,
                        transform(sort_array(collect_list(struct_data)), x -> x.sortino_ratio) AS sortino_ratio_array,
                        transform(sort_array(collect_list(struct_data)), x -> x.max_drawdown) AS max_drawdown_array,
                        transform(sort_array(collect_list(struct_data)), x -> x.adjbase) AS adjbase_array,
                        transform(sort_array(collect_list(struct_data)), x -> x.volume) AS volume_array
                    FROM (
                        SELECT
                            t2.symbol,
                            -- 1. 结构化绑定：将时间戳作为第一个字段打包进 Struct
                            -- 当 Spark 对包含 Struct 的 List 进行排序时，会物理强制按字段定义的先后顺序（即 buy_date ASC）进行对齐
                            NAMED_STRUCT(
                                'buy_date', t1.buy_date,
                                -- 2. 槽位对齐：通过 COALESCE 兜底 NULL 值，防止 collect_list 漏掉未建仓的日期，确保数组长度等长
                                'daily_return', COALESCE(t2.daily_return, 0.0),
                                'sortino_ratio', COALESCE(t2.sortino_ratio, 0.0),
                                'max_drawdown', COALESCE(t2.max_drawdown, 0.0),
                                'adjbase', COALESCE(t2.adjbase, 0.0),
                                'volume', COALESCE(t2.volume, 0.0)
                            ) AS struct_data
                        FROM temp_timeseries t1
                        JOIN temp_position_detail t2 ON t1.buy_date = t2.date
                        JOIN temp_cur_position_with_latest_stock_info t3 ON t2.symbol = t3.symbol AND t2.date >= t3.buy_date
                    ) t
                    GROUP BY symbol
                ) t6 ON t1.symbol = t6.symbol
            """)

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
                "daily_return_array": "DAILY RETURN",
                "sharpe_ratio": "SHARPE RATIO",
                "sortino_ratio": "SORTINO RATIO",
                "max_drawdown": "MAX DD",
                "strategy_cnt": "STRATEGY CNT",
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
        pd_position_history.to_csv(
            FINANCE_ROOT / f"data/{self.market}_stockdetail.csv", header=True
        )
        print("持仓明细生成完成...")
        column_map_default = {
            "symbol": "SYMBOL",
            "name": "NAME",
            "industry": "IND",
            "erp": "ERP",
            "open_date": "OPEN DATE",
            "daily_return_array": "DAILY RETURN",
            "pnl_ratio": "PNL RATIO",
            "win_rate": "WIN RATE",
            "avg_trans": "AVG TRANS",
            "sortino": "SORTINO RATIO",
            "max_dd": "MAX DD",
            "strategy_cnt": "STRATEGY CNT",
            "strategy": "STRATEGY",
        }
        if self.market in ("cn", "us"):
            toolkit = ToolKit("股票排名导出")
            selected_symbols, _ = toolkit.score_and_select_symbols(
                pd_position_history,
                column_map_default,
                self.market,
                self.trade_date,
            )
            toolkit.export_if_changed(selected_symbols, self.market)

        pd_cur_position_with_latest_stock_info = None
        pd_position_history = None
        gc.collect()

        """
        减仓情况分析
        """
        spark_position_reduction = spark.sql(f""" 
            WITH tmp11 AS (
                SELECT symbol
                    ,buy_date
                    ,base_price
                    ,base_size
                    ,buy_strategy
                    ,sell_date
                    ,adj_price
                    ,adj_size
                    ,sell_strategy
                FROM temp_transaction_logs WHERE buy_date >= '{start_date}'
                AND  symbol NOT IN (SELECT symbol FROM temp_transaction_logs WHERE sell_date IS NULL)
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
                , t4.erp
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
                FROM tmp11 WHERE sell_date >= {get_date_rank_subquery(5)}
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol AND t1.sell_date = t2.sell_date
                LEFT JOIN tmp3 t3 ON t1.symbol = t3.symbol
                LEFT JOIN temp_symbol_pe t4 ON t1.symbol = t4.symbol
            """)

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
                    how="left",
                )
                .sort_values(
                    by=["pnl_growth", "sell_date", "pnl"],
                    ascending=[False, False, False],
                )
                .reset_index(drop=True)
            )
            pd_position_reduction["industry"] = pd_position_reduction[
                "combined"
            ].combine_first(pd_position_reduction["industry"])

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
                FINANCE_ROOT / f"data/{self.market}_stockdetail_short.csv", header=True
            )
            print("减仓明细生成完成...")

        pd_position_reduction = None
        # pd_industry_history_tracking = None
        pd_industry_history_tracking_lstndays = None
        pd_industry_history_tracking_ndaysbeforeyesterday = None
        gc.collect()

        # TOPN热门行业
        spark_topn_industry = spark.sql(""" 
            SELECT industry, cnt 
            FROM (
                SELECT industry, count(symbol) AS cnt FROM temp_cur_position GROUP BY industry)
            ORDER BY cnt DESC LIMIT 10
            """)

        pd_topn_industry = spark_topn_industry.toPandas()
        replace_dict = pd_industry_history_tracking.set_index("industry_new")[
            "combined"
        ].to_dict()
        pd_topn_industry["industry"] = (
            pd_topn_industry["industry"]
            .map(replace_dict)
            .combine_first(pd_topn_industry["industry"])
        )
        # 导出 CSV 供 Dash 使用（带 market 前缀和不带前缀）
        try:
            pd_topn_industry.to_csv(
                FINANCE_ROOT / f"data/{self.market}_pd_topn_industry.csv", index=False
            )
        except Exception:
            pass

        pd_topn_industry = None
        gc.collect()
        print("Tree Map-Position生成完成...")

        # TOPN盈利行业
        spark_topn_profit_industry = spark.sql(""" 
            SELECT industry, ROUND(pl,2) AS pl 
            FROM (
                SELECT industry, SUM(`p&l`) as pl FROM temp_cur_position GROUP BY industry)
            ORDER BY pl DESC LIMIT 10
            """)

        pd_topn_profit_industry = spark_topn_profit_industry.toPandas()
        pd_topn_profit_industry["industry"] = (
            pd_topn_profit_industry["industry"]
            .map(replace_dict)
            .combine_first(pd_topn_profit_industry["industry"])
        )
        # 导出 CSV 供 Dash 使用
        try:
            pd_topn_profit_industry.to_csv(
                FINANCE_ROOT / f"data/{self.market}_pd_topn_profit_industry.csv",
                index=False,
            )
        except Exception:
            pass

        pd_topn_profit_industry = None
        gc.collect()
        print("Tree Map-PnL生成完成...")

        # N天内策略交易概率
        spark_strategy_tracking_lstndays = spark.sql(f"""
            WITH tmp_trades AS (
                SELECT 
                    symbol,
                    buy_date,
                    COALESCE(sell_date, '9999-12-31') AS sell_date,
                    CONCAT(symbol, '_', buy_date) AS trade_id
                FROM temp_transaction_logs
            ),
            tmp_base AS (
                SELECT 
                    p.date,
                    p.symbol,
                    p.strategy,
                    p.price,     -- 开仓买入价
                    p.adjbase,   -- 当日最新价格
                    p.shares,    -- 当日持仓股数
                    t.trade_id,                    
                    -- 1. fwd_adjbase_1d 逻辑：
                    -- 若 p.date 等于 {end_date}，则返回 NULL；否则取未来第 1 天的 adjbase
                    CASE 
                        WHEN p.date = '{end_date}' THEN NULL 
                        ELSE LEAD(p.adjbase, 1) OVER (PARTITION BY p.symbol, t.trade_id ORDER BY p.date) 
                    END AS fwd_adjbase_1d,                    
                    -- 2. fwd_adjbase_5d 动态 Fallback 逻辑：
                    -- 如果未来 5 天内已平仓/触及 end_date，则自动取这期间可用的【最后一个交易日】的价格（有几天算几天）
                    LAST_VALUE(p.adjbase) OVER (
                        PARTITION BY p.symbol, t.trade_id 
                        ORDER BY p.date 
                        ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING
                    ) AS fwd_adjbase_5d
                FROM temp_position_detail p
                INNER JOIN tmp_trades t
                    ON p.symbol = t.symbol
                   AND p.date >= t.buy_date 
                   AND p.date <= t.sell_date
                WHERE p.date >= '{start_date}'
                  AND p.date <= '{end_date}'
            ),
            tmp_signals AS (
                SELECT 
                    date,
                    symbol,
                    strategy,
                    trade_id,
                    -- 1. T 日当天的持仓浮动总 PnL金额 = (当前价 - 买入价) * 股数
                    (adjbase - price) * shares AS pnl_curr,                                        
                    -- 2. T+1 日的前瞻 PnL 变化金额 = (T+1日价格 - T日价格) * 股数
                    (fwd_adjbase_1d - adjbase) * shares AS fwd_pnl_1d_diff,                    
                    -- 3. T+5 日的前瞻 PnL 变化金额 = (T+5日价格 - T日价格) * 股数
                    (fwd_adjbase_5d - adjbase) * shares AS fwd_pnl_5d_diff,                    
                    -- 4. 未来 5 天收益率 % （价格变动百分比，与股数无关）
                    (fwd_adjbase_5d - adjbase) / adjbase AS fwd_ret_5d,                    
                    -- 5. 未来 1 天收益率 % （与股数无关）
                    (fwd_adjbase_1d - adjbase) / adjbase AS fwd_ret_1d,                                        
                    -- 6. 判定未来 5 天是否盈利 (未到期/已平仓无数据时记为 NULL)
                    CASE WHEN fwd_adjbase_5d > adjbase THEN 1 WHEN fwd_adjbase_5d IS NULL THEN NULL ELSE 0 END AS is_win_5d,                    
                    -- 7. 判定未来 1 天是否盈利
                    CASE WHEN fwd_adjbase_1d > adjbase THEN 1 WHEN fwd_adjbase_1d IS NULL THEN NULL ELSE 0 END AS is_win_1d
                FROM tmp_base
            ),
            -- 4. 每日策略维度聚合统计
            tmp_daily_agg AS (
                SELECT 
                    date,
                    strategy,
                    COUNT(symbol) AS cnt,                    
                    -- T日当天的持仓总 PnL
                    SUM(pnl_curr) AS pnl_total_curr,                    
                    -- 未来 1 日该策略带来的【总绝对盈亏金额】与【单票平均绝对盈亏金额】
                    SUM(fwd_pnl_1d_diff) AS pnl_1d_future_sum,
                    AVG(fwd_pnl_1d_diff) AS pnl_1d_future_avg,                    
                    -- 未来 5 日该策略带来的【总绝对盈亏金额】与【单票平均绝对盈亏金额】
                    SUM(fwd_pnl_5d_diff) AS pnl_5d_future_sum,
                    AVG(fwd_pnl_5d_diff) AS pnl_5d_future_avg,    
                    -- 未来 1 日收益率 % (均值 & 累加值)
                    AVG(fwd_ret_1d) AS ret_1d_future_avg,
                    SUM(fwd_ret_1d) AS ret_1d_future_sum,
                    -- 未来 5 日收益率 % (均值 & 累加值)
                    AVG(fwd_ret_5d) AS ret_5d_future_avg,
                    SUM(fwd_ret_5d) AS ret_5d_future_sum,                                    
                    -- 未来胜率统计
                    try_divide(SUM(is_win_5d), NULLIF(COUNT(is_win_5d), 0)) AS success_rate_5d,
                    try_divide(SUM(is_win_1d), NULLIF(COUNT(is_win_1d), 0)) AS success_rate_1d
                FROM tmp_signals
                GROUP BY date, strategy
            )
            SELECT 
                date,
                strategy,
                cnt AS cnt,
                cnt / SUM(cnt) OVER(PARTITION BY date) AS symbol_ratio,
                pnl_total_curr AS pnl,
                pnl_1d_future_sum AS pnl_1d_future_sum,
                pnl_1d_future_avg AS pnl_1d_future_avg,
                pnl_5d_future_sum AS pnl_5d_future_sum,
                pnl_5d_future_avg AS pnl_5d_future_avg,
                ROUND(ret_1d_future_avg, 4) AS ret_1d_future_avg,
                ROUND(ret_1d_future_sum, 4) AS ret_1d_future_sum,
                ROUND(ret_5d_future_avg, 4) AS ret_5d_future_avg,
                ROUND(ret_5d_future_sum, 4) AS ret_5d_future_sum,                
                ROUND(success_rate_1d, 4) AS success_rate_1d,
                ROUND(success_rate_5d, 4) AS success_rate_5d
            FROM tmp_daily_agg
            ORDER BY date, pnl_5d_future_sum DESC
            """)
        pd_strategy_tracking_lstndays = spark_strategy_tracking_lstndays.toPandas()
        pd_strategy_tracking_lstndays["date"] = pd.to_datetime(
            pd_strategy_tracking_lstndays["date"]
        ).dt.date
        pd_strategy_tracking_lstndays = pd_strategy_tracking_lstndays[
            pd_strategy_tracking_lstndays["date"] >= pd.to_datetime(start_date).date()
        ]
        # 导出 CSV 供 Dash 使用
        try:
            pd_strategy_tracking_lstndays.to_csv(
                FINANCE_ROOT / f"data/{self.market}_pd_strategy_tracking_lstndays.csv",
                index=False,
            )
        except Exception:
            pass

        pd_strategy_tracking_lstndays = None
        gc.collect()
        print("Strategy Chart生成完成...")

        # N天内交易明细分析
        spark_trade_info_lstndays = spark.sql(""" 
            WITH tmp1 AS (
                SELECT 
                    date
                    ,COUNT(symbol) AS total_cnt
                FROM temp_position_detail
                WHERE date >='{}'
                GROUP BY date
            ), 
            tmp11 AS (
                -- 直接在外面套 LAST_VALUE，当 tmp1.total_cnt 为 NULL 时，会自动向前找最近的非空持仓数
                SELECT 
                    ts.buy_date
                    ,LAST_VALUE(t1.total_cnt) IGNORE NULLS OVER (
                        PARTITION BY ts.partition_key 
                        ORDER BY ts.buy_date
                    ) AS total_cnt
                FROM (
                    SELECT *, 1 AS partition_key FROM temp_timeseries
                ) AS ts 
                LEFT JOIN tmp1 t1 ON ts.buy_date = t1.date
            ), 
            tmp5 AS (
                SELECT 
                    date
                    ,SUM(IF(trade_type = 'buy', 1, 0)) AS buy_cnt
                    ,SUM(IF(trade_type = 'sell', 1, 0)) AS sell_cnt
                FROM temp_transaction_detail
                WHERE date >= '{}'
                GROUP BY date
            )
            SELECT 
                t1.buy_date AS buy_date
                ,IFNULL(t1.total_cnt, 0) + IFNULL(t2.buy_cnt, 0) - IFNULL(t2.sell_cnt, 0) AS total_cnt
                ,IFNULL(t2.buy_cnt, 0) AS buy_cnt
                ,IFNULL(t2.sell_cnt, 0) AS sell_cnt
            FROM tmp11 t1 
            LEFT JOIN tmp5 t2 ON t1.buy_date = t2.date
            """.format(start_date, start_date))
        pd_trade_info_lstndays = spark_trade_info_lstndays.toPandas()

        # 导出 CSV 供 Dash 使用 (duplicate section for other market blocks)
        try:
            pd_trade_info_lstndays.to_csv(
                FINANCE_ROOT / f"data/{self.market}_pd_trade_info_lstndays.csv",
                index=False,
            )
        except Exception:
            pass

        pd_trade_info_lstndays = None
        gc.collect()
        print("Trade Trend生成完成...")

        # TOPN行业仓位变化趋势
        spark_topn_industry_position_trend = spark.sql("""
            WITH tmp AS ( 
                SELECT industry
                    ,cnt 
                FROM ( 
                    SELECT industry, count(symbol) AS cnt FROM temp_cur_position GROUP BY industry) t
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
                WHERE t1.date >= '{}'
            ) 
            SELECT t1.buy_date
                ,t1.industry
                ,SUM(IF(t2.symbol IS NOT NULL, 1, 0)) AS total_cnt
            FROM tmp1 t1 LEFT JOIN tmp2 t2 ON t1.industry = t2.industry AND t1.buy_date = t2.date
            GROUP BY t1.buy_date, t1.industry
            """.format(start_date))
        pd_topn_industry_position_trend = spark_topn_industry_position_trend.toPandas()
        pd_topn_industry_position_trend.sort_values(
            by=["buy_date", "total_cnt"], ascending=[False, False], inplace=True
        )
        pd_topn_industry_position_trend = None
        gc.collect()
        print("TopN Position Trend生成完成...")
        # TOPN行业PnL变化趋势
        spark_industry_history_tracking_lstndays.createOrReplaceTempView(
            "temp_industry_history_tracking_lstndays"
        )
        spark_topn_industry_profit_trend = spark.sql("""
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
                WHERE t1.date >= '{}'
            ), tmp3 AS (
            SELECT t1.buy_date
                ,t1.industry
                ,SUM(COALESCE(t2.pnl, 0)) AS pnl
            FROM tmp1 t1 LEFT JOIN tmp2 t2 ON t1.industry = t2.industry AND t1.buy_date = t2.date
            GROUP BY t1.buy_date, t1.industry
            )  SELECT t2.buy_date
                ,t1.industry
                ,t2.pnl
            FROM (SELECT * FROM temp_industry_history_tracking_lstndays ORDER BY pnl_growth DESC LIMIT 5) t1
            LEFT JOIN tmp3 t2 ON t1.industry = t2.industry
            ORDER BY t2.buy_date ASC, t1.pnl_growth DESC
            """.format(start_date))
        pd_topn_industry_profit_trend = spark_topn_industry_profit_trend.toPandas()
        pd_topn_industry_profit_trend.sort_values(
            by=["buy_date", "pnl"], ascending=[False, False], inplace=True
        )
        # 导出 CSV 供 Dash 使用
        try:
            pd_topn_industry_profit_trend.to_csv(
                FINANCE_ROOT / f"data/{self.market}_pd_topn_industry_profit_trend.csv",
                index=False,
            )
        except Exception:
            pass

        pd_topn_industry_profit_trend = None
        gc.collect()
        print("TopN Pnl Trend生成完成...")

        spark_calendar_heatmap = spark.sql(f"""
            WITH tmp AS (
                SELECT 
                    t1.date
                    ,t2.industry
                    --,SUM(t1.pnl) / COUNT(t1.symbol) AS pnl --平均收益
                    ,SUM(t1.pnl * t3.total_value) / SUM(t3.total_value) AS pnl --市值加权收益
                FROM temp_position_detail t1 JOIN temp_industry_info t2 ON t1.symbol = t2.symbol
                LEFT JOIN temp_latest_stock_info t3 ON t1.symbol = t3.symbol
                WHERE t1.date >= {get_date_rank_subquery(26)}
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
            ) t WHERE t.date >= {get_date_rank_subquery(25)}
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
                -- ,COLLECT_LIST(industry) AS industry_top3
                ,CONCAT_WS(',', COLLECT_LIST(industry)) AS industry_top3
                ,MAX(s_pnl) AS s_pnl
            FROM (SELECT * FROM tmp3 WHERE rn <= 3 ORDER BY date, rn) t
            GROUP BY date
            """)

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

        # ===== 修改开始：基于 filtered_dates 构建所有周的映射 =====
        # 生成所有可能的周起始（从交易日范围中提取）
        all_week_starts = (
            pd.Series(filtered_dates)
            .apply(lambda d: d - pd.to_timedelta(d.weekday(), unit="d"))
            .unique()
        )
        all_week_starts = sorted(all_week_starts)
        week_mapping = {date: i for i, date in enumerate(all_week_starts)}
        # ===== 修改结束 =====

        # 为数据中的每行分配 week_order
        pd_calendar_heatmap["week_order"] = pd_calendar_heatmap["week_start"].map(
            week_mapping
        )
        # 导出 CSV 供 Dash 使用
        try:
            pd_calendar_heatmap.to_csv(
                FINANCE_ROOT / f"data/{self.market}_pd_calendar_heatmap.csv",
                index=False,
            )
        except Exception:
            pass
        print("Calendar Map生成完成...")
        # 生成kpi section仪表盘最近2天的指标
        spark_kpi_section = spark.sql(f"""
            WITH tmp AS (
                SELECT 
                    date
                    ,COUNT(symbol) AS total_cnt
                FROM temp_position_detail
                WHERE date >= {get_date_rank_subquery(2)} AND date <= '{end_date}'
                GROUP BY date             
            ), tmp1 AS (
                SELECT
                    date
                    ,cash
                    ,final_value
                FROM temp_cash_asset
                WHERE date >= {get_date_rank_subquery(2)} AND date <= '{end_date}'
            ), tmp5 AS (
                SELECT date
                    ,SUM(IF(trade_type = 'buy', 1, 0)) AS buy_cnt
                    ,SUM(IF(trade_type = 'sell', 1, 0)) AS sell_cnt
                FROM temp_transaction_detail
                WHERE date >= {get_date_rank_subquery(2)} AND date <= '{end_date}'
                GROUP BY date
            )
            SELECT
                tmp1.cash
                ,tmp1.final_value
                ,tmp.total_cnt + IFNULL(tmp5.buy_cnt, 0) - IFNULL(tmp5.sell_cnt, 0) AS stock_cnt
                ,tmp.date AS end_date
            FROM tmp JOIN tmp1 ON tmp.date = tmp1.date
            LEFT JOIN tmp5 ON tmp.date = tmp5.date
            ORDER BY tmp.date DESC
            """)
        pd_kpi_section = spark_kpi_section.toPandas()

        pd_kpi_section.to_csv(
            FINANCE_ROOT / f"data/{self.market}_df_result.csv", header=True
        )

        spark.stop()
        subject = f"""{self.market.upper()} Stock Market Trends - {end_date}""".format(
            end_date=end_date
        )
        image_path_return_light = str(
            FINANCE_ROOT / f"dashreport/assets/images/{self.market}_tr_light.svg"
        )
        image_path_return_dark = str(
            FINANCE_ROOT / f"dashreport/assets/images/{self.market}_tr_dark.svg"
        )
        image_path = [
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
                                text-align: left ;
                                word-break: break-all;
                            }}
                            
                            .image-container {{
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
                                    border-radius: 0px;
                                    overflow: hidden;
                                    width: 100%;
                                    min-width: 100%;
                                }} 
                                
                                h2 {{
                                    font-size: 15px;
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
                                    <source srcset="cid:image1" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%"/>
                                    <!-- 默认模式下的图片 -->
                                    <img src="cid:image0" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%">
                                </picture>
                                <figcaption>The diagram shows the last x years cumulative return and max drawdown trend,
                                            to track the stock market and stategy execution information
                                </figcaption>
                            </div>
                        </div>
                    </body>
                    </html>
                    """.format(
            cash=cash, final_value=final_value, stock_cnt=len(stock_list)
        )
        css = """
            <style>
                :root {
                    color-scheme: dark light;
                    supported-color-schemes: dark light;
                    background-color: transparent;
                    color: black;
                    -webkit-text-size-adjust: 100%;
                    text-size-adjust: 100%;
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;                            
                }                
                /* Your light mode (default) styles: */
                body {
                    background-color: white;
                    color: black;
                    width: 100%;                                    
                }
                @media (prefers-color-scheme: dark) {            
                    body {
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
                }                    
            </style>
        """

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
                {css}
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
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

        """ 
        读取交易相关数据，交易明细，持仓明细，仓位日志明细，行业信息
        """
        file = FileInfo(self.trade_date, self.market)
        # 交易明细
        file_path_trade = file.get_file_path_trade
        cols = ["idx", "symbol", "date", "trade_type", "price", "size", "strategy"]
        spark_transaction_detail = spark.read.csv(
            str(file_path_trade), header=None, inferSchema=True
        )
        spark_transaction_detail = spark_transaction_detail.toDF(*cols)
        spark_transaction_detail.createOrReplaceTempView("temp_transaction_detail")
        # 持仓明细, spark读取
        file_cur_p = file.get_file_path_position
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
        spark_cur_position = spark.read.csv(
            str(file_cur_p), header=True, inferSchema=True
        )
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
        file_path_position_detail = file.get_file_path_position_detail
        cols = [
            "idx",
            "symbol",
            "date",
            "price",
            "adjbase",
            "shares",
            "pnl",
            "volume",
            "daily_return",
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown",
            "strategy",
        ]
        spark_position_detail = spark.read.csv(
            str(file_path_position_detail), header=None, inferSchema=True
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
            str(file_name_day),
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
        date_range = pd.date_range(
            start=(pd.to_datetime(end_date) - pd.DateOffset(days=360)).strftime(
                "%Y-%m-%d"
            ),
            end=end_date,
            freq="D",
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
        pd_timeseries = pd_timeseries.sort_values("buy_date").reset_index(drop=True)
        # 获取时间窗口JSON配置
        chart_time_range = max(
            180, ToolKit.get_config("chart_display.chart_time_range", default=120)
        )
        start_date = pd_timeseries.iloc[-chart_time_range]["buy_date"]
        pd_timeseries = pd_timeseries.tail(chart_time_range)

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

        spark_transaction_logs = spark.sql("""
            WITH tmp AS (
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
            )
            SELECT symbol
                ,l_date AS buy_date
                ,l_price AS base_price
                ,l_size AS base_size
                ,l_strategy AS buy_strategy
                ,date AS sell_date
                ,price AS adj_price
                ,size AS adj_size
                ,strategy AS sell_strategy
            FROM tmp WHERE trade_type = 'sell' AND l_date >= '{}'
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
            FROM tmp WHERE trade_type = 'buy' AND l_date IS NULL
            """.format(start_date))
        spark_transaction_logs.createOrReplaceTempView("temp_transaction_logs")

        """
        辅助日期函数，获取第N个交易日
        """

        def get_date_rank_subquery(rank_num):
            return f"""
            (SELECT buy_date FROM (
                SELECT buy_date, ROW_NUMBER() OVER(ORDER BY buy_date DESC) AS rn
                FROM (SELECT DISTINCT buy_date FROM temp_timeseries)
            ) WHERE rn = {rank_num})
            """

        spark_position_history = spark.sql(""" 
            WITH tmp2 AS (
                SELECT symbol
                    ,COUNT(symbol) AS his_trade_cnt
                    ,SUM(IF(sell_date IS NOT NULL, DATEDIFF(sell_date, buy_date), DATEDIFF('{}', buy_date))) AS his_days
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price >=0, 1, 0)) AS pos_cnt
                    ,SUM(IF(sell_date IS NOT NULL AND adj_price - base_price < 0, 1, 0)) AS neg_cnt
                    ,SUM(IF(sell_date IS NOT NULL, adj_price * (-adj_size) - base_price * base_size, 0)) AS his_pnl
                    ,SUM(IF(sell_date IS NOT NULL, base_price * base_size, 0)) AS his_base_price
                    ,MAX(IF(sell_date IS NULL, buy_strategy, null)) AS buy_strategy
                FROM  temp_transaction_logs
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
                , COALESCE(t3.strategy, t2.buy_strategy) AS buy_strategy
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
                LEFT JOIN
                (
                    SELECT 
                        symbol,
                        COALESCE(sharpe_ratio, 0) AS sharpe_ratio,
                        COALESCE(sortino_ratio, 0) AS sortino_ratio,
                        COALESCE(max_drawdown, 0) AS max_drawdown,
                        COALESCE(strategy_cnt, 0) AS strategy_cnt,
                        strategy
                    FROM (
                        SELECT
                            c.symbol,
                            p.sharpe_ratio,
                            p.sortino_ratio,
                            p.max_drawdown,
                            p.strategy,
                            ROW_NUMBER() OVER (PARTITION BY c.symbol ORDER BY p.date DESC NULLS LAST) AS rn,
                            SUM(
                                CASE 
                                    WHEN p.strategy IS NULL THEN 0
                                    WHEN LAG(p.strategy) OVER (PARTITION BY c.symbol ORDER BY p.date ASC NULLS FIRST) = p.strategy 
                                    THEN 0 
                                    ELSE 1 
                                END
                            ) OVER (PARTITION BY c.symbol ORDER BY p.date ASC NULLS FIRST ROWS UNBOUNDED PRECEDING) AS strategy_cnt
                        FROM temp_cur_position_with_latest_stock_info c
                        LEFT JOIN temp_position_detail p
                            ON p.symbol = c.symbol AND p.date >= c.buy_date
                    ) t
                    WHERE rn = 1
                ) t3 ON t1.symbol = t3.symbol
            """.format(end_date))

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

        pd_position_history.to_csv(
            FINANCE_ROOT / f"data/{self.market}.csv", header=True
        )
        pd_cur_position_with_latest_stock_info = None
        gc.collect()

        """
        减仓情况分析
        """
        spark_position_reduction = spark.sql(f""" 
            WITH tmp11 AS (
                SELECT symbol
                    ,buy_date
                    ,base_price
                    ,base_size
                    ,buy_strategy
                    ,sell_date
                    ,adj_price
                    ,adj_size
                    ,sell_strategy
                FROM temp_transaction_logs WHERE buy_date >= '{start_date}'
                AND  symbol NOT IN (SELECT symbol FROM temp_transaction_logs WHERE sell_date IS NULL)
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
                FROM tmp11 WHERE sell_date >= {get_date_rank_subquery(5)}
                ) t1 LEFT JOIN tmp2 t2 ON t1.symbol = t2.symbol AND t1.sell_date = t2.sell_date
                LEFT JOIN tmp3 t3 ON t1.symbol = t3.symbol
            """)

        pd_position_reduction = spark_position_reduction.toPandas()

        pd_position_reduction = None
        pd_position_history = None
        gc.collect()

        # N天内交易明细分析
        spark_trade_info_lstndays = spark.sql(""" 
            WITH tmp1 AS (
                SELECT 
                    date
                    ,COUNT(symbol) AS total_cnt
                FROM temp_position_detail
                WHERE date >='{}'
                GROUP BY date
            ), 
            tmp11 AS (
                -- 直接在外面套 LAST_VALUE，当 tmp1.total_cnt 为 NULL 时，会自动向前找最近的非空持仓数
                SELECT 
                    ts.buy_date
                    ,LAST_VALUE(t1.total_cnt) IGNORE NULLS OVER (
                        PARTITION BY ts.partition_key 
                        ORDER BY ts.buy_date
                    ) AS total_cnt
                FROM (
                    SELECT *, 1 AS partition_key FROM temp_timeseries
                ) AS ts 
                LEFT JOIN tmp1 t1 ON ts.buy_date = t1.date
            ), 
            tmp5 AS (
                SELECT 
                    date
                    ,SUM(IF(trade_type = 'buy', 1, 0)) AS buy_cnt
                    ,SUM(IF(trade_type = 'sell', 1, 0)) AS sell_cnt
                FROM temp_transaction_detail
                WHERE date >= '{}'
                GROUP BY date
            )
            SELECT 
                t1.buy_date AS buy_date
                ,IFNULL(t1.total_cnt, 0) + IFNULL(t2.buy_cnt, 0) - IFNULL(t2.sell_cnt, 0) AS total_cnt
                ,IFNULL(t2.buy_cnt, 0) AS buy_cnt
                ,IFNULL(t2.sell_cnt, 0) AS sell_cnt
            FROM tmp11 t1 
            LEFT JOIN tmp5 t2 ON t1.buy_date = t2.date
            """.format(start_date, start_date))
        pd_trade_info_lstndays = spark_trade_info_lstndays.toPandas()

        pd_trade_info_lstndays = None
        gc.collect()

        spark.stop()

        subject = f"""CN Stock Market ETF Trends - {end_date}""".format(
            end_date=end_date
        )
        image_path_return_light = str(
            FINANCE_ROOT / f"dashreport/assets/images/{self.market}_tr_light.svg"
        )
        image_path_return_dark = str(
            FINANCE_ROOT / f"dashreport/assets/images/{self.market}_tr_dark.svg"
        )
        image_path = [
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
                        border-radius: 0px;
                        overflow: hidden;
                        width: 100%;
                        min-width: 100%;
                    }} 
                    
                    h2 {{
                        font-size:15px;
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
                        <source srcset="cid:image1" media="(prefers-color-scheme: dark)" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%"/>                                    
                        <img src="cid:image0" alt="The diagram shows the last x years cumulative return and max drawdown trend:" style="width:100%">
                    </picture>
                    <figcaption>The diagram shows the last x years cumulative return and max drawdown trend,
                                to track the stock market and strategy execution information
                    </figcaption>                                
                </div>
            </div>
        </body>
        </html>
        """.format(cash=cash, final_value=final_value)

        css = """
            <style>
                :root {
                    color-scheme: dark light;
                    supported-color-schemes: dark light;
                    background-color: transparent;
                    color: black;
                    -webkit-text-size-adjust: 100%;
                    text-size-adjust: 100%;
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;                            
                }                
                /* Your light mode (default) styles: */
                body {
                    background-color: white;
                    color: black;
                    width: 100%;                                    
                }
                @media (prefers-color-scheme: dark) {            
                    body {
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
                }                    
            </style>
        """

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
                {css}
            </div>
        </body>
        </html>
        """

        MyEmail().send_email_embedded_image(subject, final_html, image_path)
