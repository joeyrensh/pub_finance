#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from finance import FINANCE_ROOT
from finance.utility.toolkit import ToolKit
import time
import random
import os
import glob
import pandas as pd
import akshare as ak

"""
东方财经的A股日K数据获取接口：

市场代码：
0: 深证/创业板/新三板/ SZ
1: 上证/科创板      SH

股票列表URL：
http://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery&pn=1&pz=20000&po=1&
np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:0&fields=f2,f3,f4,f5,f6,f7,f12,f15,f16,f17,f18&_=1636942751421

历史数据URL：
https://92.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery&
secid=1.600066&ut=&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101
&fqt=1&beg=20211101&end=20211115&smplmt=755&lmt=1000000&_=1636942751421

历史数据返回字段列表：
date,open,close,high,low,volume,turnover,amplitude,chg,change,换手率

"""


class AKCNHistoryDataCrawler:
    def __init__(self):
        self.proxy = None

    def _get_tickers_from_local_csv(self):
        """
        从 FINANCE_ROOT / cnstockinfo/ 目录下自动读取最新日期的 CSV 文件提取 ticker 列表，
        根据带有 SZ/SH/ETF 前缀的 symbol 进行分类归集。
        """
        dir_path = FINANCE_ROOT / "cnstockinfo"
        if not os.path.exists(dir_path):
            # 兼容相对路径容错
            dir_path = "cnstockinfo"
            if not os.path.exists(dir_path):
                raise FileNotFoundError(f"目录不存在: {dir_path}")

        csv_files = glob.glob(os.path.join(str(dir_path), "stock_*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"在 {dir_path} 下未找到任何 stock_*.csv 文件")

        # 按照文件名排序选取最新日期的文件
        latest_file = sorted(csv_files)[-1]
        print(f"读取本地最新股票列表文件: {latest_file}")

        df_info = pd.read_csv(latest_file, dtype=str)
        
        # 兼容不同 CSV 文件的列名标识
        symbol_col = next((c for c in df_info.columns if c.lower() in ['symbol', '代码']), None)
        name_col = next((c for c in df_info.columns if c.lower() in ['name', '名称']), None)
        
        if not symbol_col or not name_col:
            raise KeyError(f"文件 {latest_file} 中缺少 symbol 或 name 标识列")

        sh_list = []
        sz_list = []
        etf_list = []

        for _, row in df_info.iterrows():
            sym_raw = str(row[symbol_col]).strip().upper()
            name = str(row[name_col]).strip()

            if sym_raw.startswith("SH"):
                # ak.stock_zh_a_daily 需要小写前缀 sh600000 格式
                raw_num = sym_raw[2:]
                sh_list.append({"symbol_ak": f"sh{raw_num}", "symbol_out": f"SH{raw_num}", "name": name})
            elif sym_raw.startswith("SZ"):
                # ak.stock_zh_a_daily 需要小写前缀 sz000001 格式
                raw_num = sym_raw[2:]
                sz_list.append({"symbol_ak": f"sz{raw_num}", "symbol_out": f"SZ{raw_num}", "name": name})
            elif sym_raw.startswith("ETF"):
                # ak.fund_etf_hist_em 需要纯数字 510300 格式
                raw_num = sym_raw[3:]
                etf_list.append({"symbol_ak": raw_num, "symbol_out": f"ETF{raw_num}", "name": name})

        return sh_list, sz_list, etf_list

    """
    akshare获取A股历史数据
    """

    def get_cn_stock_history_ak(self, start_date, end_date, file_path, source_type="file"):
        """
        获取A股历史数据
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param file_path: 输出文件路径
        :param source_type: "file" 表示从 cnstockinfo/ 目录下最新 CSV 读取列表；
                            "akshare" 表示从东财网络接口获取列表
        """
        batch_size = 50  # 每处理 50 只股票向 CSV 落盘一次，清理内存
        is_first_write = True  # 控制全局仅第一次写入时保存 Header

        # 判断数据源类型
        if source_type == "file":
            sh_tickers, sz_tickers, etf_tickers = self._get_tickers_from_local_csv()
        else:
            sh_tickers, sz_tickers, etf_tickers = [], [], []

        # ==================== 1. 获取A股历史数据, 上海市场 ====================
        if source_type == "akshare":
            stock_sh_a_spot_em = ak.stock_sh_a_spot_em()
            pd_stock_sh = stock_sh_a_spot_em[
                [
                    "代码",
                    "名称",
                    "今开",
                    "最新价",
                    "最高",
                    "最低",
                    "成交量",
                    "总市值",
                    "市盈率-动态",
                ]
            ].copy()
            pd_stock_sh = pd_stock_sh[pd_stock_sh["最新价"] > 0]
            for _, row in pd_stock_sh.reset_index(drop=True).iterrows():
                raw_num = str(row["代码"]).strip()
                sh_tickers.append({
                    "symbol_ak": "sh" + raw_num,
                    "symbol_out": "SH" + raw_num,
                    "name": row["名称"]
                })

        tool = ToolKit("历史数据下载")
        list_records = []
        sh_total = len(sh_tickers)

        for index, item_info in enumerate(sh_tickers):
            symbol_ak = item_info["symbol_ak"]
            symbol_out = item_info["symbol_out"]
            name = item_info["name"]
            try:
                stock_zh_a_daily_df = ak.stock_zh_a_daily(
                    symbol=symbol_ak,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )
                for i, r in stock_zh_a_daily_df.iterrows():
                    item = {
                        "symbol": symbol_out,
                        "name": name,
                        "open": r["open"],
                        "close": r["close"],
                        "high": r["high"],
                        "low": r["low"],
                        "volume": r["volume"],
                        "date": r["date"],
                    }
                    list_records.append(item)
            except Exception as e:
                print("获取历史数据失败：", symbol_ak, e)
                continue

            # 分批落盘
            if (index + 1) % batch_size == 0 or (index + 1) == sh_total:
                if list_records:
                    df = pd.DataFrame(list_records)
                    df.to_csv(
                        file_path,
                        mode="a",
                        index=True,
                        header=is_first_write,
                    )
                    is_first_write = False  # 首次写入后不再写入 Header
                    list_records.clear()  # 释放内存

            tool.progress_bar(sh_total, index)

        # ==================== 2. 获取A股历史数据, 深圳市场 ====================
        if source_type == "akshare":
            stock_sz_a_spot_em = ak.stock_sz_a_spot_em()
            pd_stock_sz = stock_sz_a_spot_em[
                [
                    "代码",
                    "名称",
                    "今开",
                    "最新价",
                    "最高",
                    "最低",
                    "成交量",
                    "总市值",
                    "市盈率-动态",
                ]
            ].copy()
            pd_stock_sz = pd_stock_sz[pd_stock_sz["最新价"] > 0]
            for _, row in pd_stock_sz.reset_index(drop=True).iterrows():
                raw_num = str(row["代码"]).strip()
                sz_tickers.append({
                    "symbol_ak": "sz" + raw_num,
                    "symbol_out": "SZ" + raw_num,
                    "name": row["名称"]
                })

        tool = ToolKit("历史数据下载")
        list_records = []
        sz_total = len(sz_tickers)

        for index, item_info in enumerate(sz_tickers):
            symbol_ak = item_info["symbol_ak"]
            symbol_out = item_info["symbol_out"]
            name = item_info["name"]
            try:
                stock_zh_a_daily_df = ak.stock_zh_a_daily(
                    symbol=symbol_ak,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )
                for i, r in stock_zh_a_daily_df.iterrows():
                    item = {
                        "symbol": symbol_out,
                        "name": name,
                        "open": r["open"],
                        "close": r["close"],
                        "high": r["high"],
                        "low": r["low"],
                        "volume": r["volume"],
                        "date": r["date"],
                    }
                    list_records.append(item)
            except Exception as e:
                print("获取历史数据失败：", symbol_ak, e)
                continue

            # 分批落盘
            if (index + 1) % batch_size == 0 or (index + 1) == sz_total:
                if list_records:
                    df = pd.DataFrame(list_records)
                    df.to_csv(
                        file_path,
                        mode="a",
                        index=True,
                        header=is_first_write,
                    )
                    is_first_write = False
                    list_records.clear()  # 释放内存

            tool.progress_bar(sz_total, index)

        # ==================== 3. ETF历史数据 ====================
        if source_type == "akshare":
            fund_etf_spot_em_df = ak.fund_etf_spot_em()
            pd_etf = fund_etf_spot_em_df[
                [
                    "代码",
                    "名称",
                    "开盘价",
                    "最新价",
                    "最高价",
                    "最低价",
                    "成交量",
                    "总市值",
                ]
            ].copy()
            pd_etf = pd_etf[pd_etf["最新价"] > 0]
            for _, row in pd_etf.reset_index(drop=True).iterrows():
                raw_num = str(row["代码"]).strip()
                etf_tickers.append({
                    "symbol_ak": raw_num,
                    "symbol_out": "ETF" + raw_num,
                    "name": row["名称"]
                })

        tool = ToolKit("历史数据下载")
        list_records = []
        etf_total = len(etf_tickers)

        for index, item_info in enumerate(etf_tickers):
            symbol_ak = item_info["symbol_ak"]
            symbol_out = item_info["symbol_out"]
            name = item_info["name"]
            try:
                time.sleep(1 + random.uniform(1, 3))
                fund_etf_hist_em_df = ak.fund_etf_hist_em(
                    symbol=symbol_ak,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )
                for i, r in fund_etf_hist_em_df.iterrows():
                    item = {
                        "symbol": symbol_out,
                        "name": name,
                        "open": r["开盘"],
                        "close": r["收盘"],
                        "high": r["最高"],
                        "low": r["最低"],
                        "volume": r["成交量"],
                        "date": r["日期"],
                    }
                    list_records.append(item)
            except Exception as e:
                print("获取ETF历史数据失败：", symbol_ak, e)
                continue

            # 分批落盘
            if (index + 1) % batch_size == 0 or (index + 1) == etf_total:
                if list_records:
                    df = pd.DataFrame(list_records)
                    df.to_csv(
                        file_path,
                        mode="a",
                        index=True,
                        header=is_first_write,
                    )
                    is_first_write = False
                    list_records.clear()  # 释放内存

            tool.progress_bar(etf_total, index)