#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from finance.utility.toolkit import ToolKit
import time
import random
import pandas as pd
import requests
import json
from finance.utility.fileinfo import FileInfo
from finance.utility.em_stock_uti import EMWebCrawlerUti
from finance.paths import FINANCE_ROOT

""" 东方财经A股股票对应行业板块数据获取接口 """


class EMCNTickerCategoryCrawler:
    def __init__(self):
        self.item = "http://60.210.40.190:9091"
        self.proxy = {
            "http": self.item,
            "https": self.item,
        }
        self.proxy = None
        self.headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "host": "push2.eastmoney.com",
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

    def get_cn_ticker_category(self, trade_date, batch_size=10):
        """
        市场代码：
        0: 深证/创业板/新三板/ SZ
        1: 上证/科创板		SH

        https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=SH688798
        """
        # 1. 获取输出文件路径
        file = FileInfo(trade_date, "cn")
        file_path_industry = file.get_file_path_industry

        # 2. 读取已处理的股票代码（用于断点续传）及当前最大 idx
        processed_symbols = set()
        next_idx = 0
        if file_path_industry.exists():
            try:
                df_existing = pd.read_csv(file_path_industry, dtype=str)
                if not df_existing.empty:
                    if "symbol" in df_existing.columns:
                        processed_symbols = set(
                            df_existing["symbol"].astype(str).str.strip()
                        )
                    if "idx" in df_existing.columns:
                        max_idx = pd.to_numeric(
                            df_existing["idx"], errors="coerce"
                        ).max()
                        if pd.notna(max_idx):
                            next_idx = int(max_idx) + 1
                    print(
                        f"已加载 {len(processed_symbols)} 条已有记录，下一 idx 从 {next_idx} 开始"
                    )
            except Exception as e:
                print(f"读取已有文件失败: {e}，将重新开始")
                processed_symbols = set()
                next_idx = 0
        else:
            print("输出文件不存在，将创建新文件")

        # 3. 获取所有股票列表
        em = EMWebCrawlerUti()
        tick_list = em.get_stock_list(
            "cn",
            trade_date,
            target_file=FINANCE_ROOT / "cnstockinfo/cn_stock_list_cache.csv",
        )
        # 计算 ETF 数量（symbol 以 "ETF" 开头）
        etf_count = sum(
            1 for item in tick_list if str(item["symbol"]).strip().startswith("ETF")
        )

        # 过滤：排除已处理的股票以及 ETF
        filtered_list = [
            item
            for item in tick_list
            if str(item["symbol"]).strip() not in processed_symbols
            and not str(item["symbol"]).strip().startswith("ETF")
        ]
        total = len(tick_list)  # 实际需要处理的股票数量
        tool = ToolKit("行业下载进度")
        tool.progress_bar(total, len(processed_symbols) + etf_count)

        # 缓存待写入的数据
        batch_data = []
        # 文件表头是否已写入
        header_written = (
            file_path_industry.exists() and file_path_industry.stat().st_size > 0
        )

        def flush_batch():
            nonlocal batch_data, next_idx, header_written
            if not batch_data:
                return
            df_batch = pd.DataFrame(batch_data)
            idx_list = list(range(next_idx, next_idx + len(df_batch)))
            df_batch.insert(0, "idx", idx_list)
            mode = "a" if header_written else "w"
            header = not header_written
            df_batch.to_csv(
                file_path_industry,
                mode=mode,
                index=False,
                header=header,
                encoding="utf-8",
            )
            next_idx += len(df_batch)
            batch_data = []
            header_written = True

        try:
            for i, item in enumerate(filtered_list):
                symbol = str(item["symbol"]).strip()

                # 请求行业信息
                url = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax"
                params = {"code": symbol}
                try:
                    res = requests.get(
                        url, params=params, proxies=self.proxy
                    ).text.lower()
                    json_object = json.loads(res)
                except Exception as e:
                    print(f"请求或解析失败 {symbol}: {e}")
                    continue

                if "status" in json_object and json_object["status"] != 0:
                    print(
                        f"Error fetching data for {symbol}: {json_object.get('message', 'unknown')}"
                    )
                    continue

                industry = None
                if (
                    json_object
                    and "jbzl" in json_object
                    and json_object["jbzl"]
                    and "sshy" in json_object["jbzl"]
                ):
                    industry = json_object["jbzl"]["sshy"]
                    if industry == "--":
                        industry = None

                if industry:
                    batch_data.append({"symbol": symbol, "industry": industry})
                    if len(batch_data) >= batch_size:
                        flush_batch()
                else:
                    print(f"未获取到行业信息: {symbol}")

                # 更新进度（基于索引）
                tool.progress_bar(total, len(processed_symbols) + etf_count + i + 1)

            # 循环结束，写入剩余数据
            flush_batch()

            # 最终去重（按 symbol 保留最后一条，并重置 idx）
            if file_path_industry.exists():
                df_final = pd.read_csv(file_path_industry, dtype=str)
                df_final.drop_duplicates(subset=["symbol"], keep="last", inplace=True)
                df_final.reset_index(drop=True, inplace=True)
                df_final["idx"] = df_final.index
                df_final.to_csv(file_path_industry, index=False, encoding="utf-8")
                print(f"最终去重后保存，共 {len(df_final)} 条记录")

        except KeyboardInterrupt:
            print("\n用户中断，正在保存已处理数据...")
            flush_batch()
            print("已保存当前进度，下次运行将从断点继续")
            raise

        print("行业信息获取完成")
