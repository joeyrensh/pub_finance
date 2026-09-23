#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import concurrent.futures
import csv
import hashlib
import itertools
import json
import logging
import math
import os
import glob
from pathlib import Path
import random
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict
import tempfile
import shutil
import numpy as np

import pandas as pd
from curl_cffi import requests  # 替换为 curl_cffi
from fake_useragent import UserAgent

from finance import FINANCE_ROOT
from finance.utility.emcookie_generation import CookieGeneration
from finance.utility.fileinfo import FileInfo
from finance.utility.get_proxy import ProxyManager
from finance.utility.toolkit import ToolKit
import yfinance as yf
import threading

logger = logging.getLogger(__name__)


class EMWebCrawlerUti:
    DEFAULT_COOKIES: Dict[str, str] = {
        "qgqp_b_id": "64128e722243aac323ad9a57e33fe37f",
        "st_pvi": "44203626923623",
        "st_si": "04662034469518",
    }
    DEFAULT_PSI_SUFFIX = "-113200301321-6001712214"

    def __init__(self, use_proxy=True):  # 1. 默认 use_proxy=True 兼容原有调用
        """
        # 美股/A股日数据及历史数据爬虫（不复权版）
        :param use_proxy: 是否开启代理，默认 True。传入 False 则不使用代理直连。
        """
        self.__url_list = "http://push2.eastmoney.com/api/qt/clist/get"
        self.__url_history = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

        self.use_proxy = use_proxy
        self.pm = ProxyManager()
        self.pm_us = ProxyManager(proxy_type="overseas")

        # 专属 yfinance 的线程锁与当前有效代理（独立隔离）
        self.current_working_proxy_us = None
        self.proxy_lock_us = threading.Lock()
        # 2. 初始化时将 enable_proxy 传入
        self.cg = CookieGeneration()
        self.cg.generate_em_cookies()
        # 初始化proxy为空，避免调用Class时立即获取代理，改为在需要时再获取
        self.proxy = None

        self.headers = {
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        # 4. 初始化 Cookie 数据与会话状态
        self.cookie_path = FINANCE_ROOT / "utility" / "eastmoney_cookie.json"
        self._cookie_base = self.parse_cookie_string()
        # 使用线程安全的自增计数器替代非安全的 int += 1
        self._counter = itertools.count(start=1)

        self.pz = 100

    def parse_cookie_string(self) -> Dict[str, str]:
        """读取 JSON 格式的 cookie 文件（只在初始化时读取一次）"""
        if not self.cookie_path.exists():
            logger.warning(f"Cookie 文件不存在: {self.cookie_path}，使用默认兜底配置")
            return self.DEFAULT_COOKIES.copy()

        try:
            with open(self.cookie_path, "r", encoding="utf-8") as f:
                cookie_data = json.load(f)
                logger.info("已成功加载 JSON cookie 信息")
                return cookie_data
        except Exception as e:
            logger.error(f"读取 Cookie 文件异常: {e}，使用兜底配置", exc_info=True)
            return self.DEFAULT_COOKIES.copy()

    def get_dynamic_cookies(self) -> Dict[str, str]:
        """基于模板动态生成包含最新时间戳与计数的 Cookie 字典"""
        cookies = self._cookie_base.copy()

        # 1. 精准更新 st_psi 时间戳 (YYYYMMDDHHMMSSmmm)
        now = datetime.now()
        timestamp_str = now.strftime("%Y%m%d%H%M%S") + f"{now.microsecond // 1000:03d}"

        orig_psi = cookies.get("st_psi", "")
        suffix = (
            orig_psi[orig_psi.find("-") :]
            if "-" in orig_psi
            else self.DEFAULT_PSI_SUFFIX
        )
        cookies["st_psi"] = f"{timestamp_str}{suffix}"

        # 2. 线程安全地更新请求计数器
        cookies["st_sn"] = str(next(self._counter))

        # 3. 清理 delete 标识
        if cookies.get("st_asi") == "delete":
            cookies.pop("st_asi", None)

        return cookies

    @staticmethod
    def generate_ut_param() -> str:
        """生成唯一的 ut 追踪参数（标准 32 位 MD5 格式）"""
        import uuid

        return hashlib.md5(uuid.uuid4().bytes).hexdigest()

    def build_params(self, market, mkt_code, page_num):
        base_params = {
            "pn": f"{page_num}",
            "pz": self.pz,
            "po": "1",
            "np": "1",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
            "_": str(int(time.time() * 1000)),
        }

        if market == "us":
            base_params["fs"] = f"m:{mkt_code}"
        elif market == "cn":
            if mkt_code == "0":
                base_params["fs"] = "m:0 t:6,m:0 t:80"
            elif mkt_code == "1":
                base_params["fs"] = "m:1 t:2,m:1 t:23"
            elif mkt_code == "etf":
                base_params["fs"] = "b:MK0021,b:MK0022,b:MK0023,b:MK0024,b:MK0827"
                base_params["wbp2u"] = "|0|0|0|web"

        return base_params

    def get_etf_market_code(self, symbol):
        if symbol.startswith("5"):
            return "1"
        elif symbol.startswith("1"):
            return "0"
        else:
            return "0"

    def format_symbol(self, market, mkt_code, item):
        if market == "us":
            return item["f12"]
        elif market == "cn":
            if mkt_code == "etf":
                return f"ETF{item['f12']}"
            elif mkt_code == "0":
                return f"SZ{item['f12']}"
            elif mkt_code == "1":
                return f"SH{item['f12']}"
        return item["f12"]

    def get_total_pages(self, market, mkt_code):
        params = self.build_params(market, mkt_code, 1)
        if self.proxy is None:
            self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
        for _ in range(3):
            try:
                res = requests.get(
                    self.__url_list,
                    params=params,
                    proxies=self.proxy,
                    headers=self.headers,
                    cookies=self.get_dynamic_cookies(),
                    timeout=10,
                    impersonate="chrome120",  # 使用 curl_cffi 模拟 Chrome 120 的 TLS 指纹
                ).json()
                if res["data"]["total"] < 1000:
                    self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
                    continue
                break
            except Exception:
                print("请求失败，正在重试...", _)
                self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
                continue

        total_page_no = math.ceil(res["data"]["total"] / self.pz)
        print(f"市场: {mkt_code}, 总页数: {total_page_no}")
        return total_page_no

    def get_stock_list(self, market, trade_date, target_file=None, from_latest_file=True):
        """
        获取股票/基金列表及对应 mkt_code
        :param market: 市场标识 ("us" 或 "cn")
        :param trade_date: 交易日期标识
        :param target_file: 导出的目标文件路径（保留原逻辑）
        :param from_latest_file: 是否直接从本地最新的 stock_*.csv 文件中提取 unique symbol
        """
        if target_file is None:
            target_file = FINANCE_ROOT / f"{market}stockinfo" / f"stock_list_{trade_date}.csv"

        # =========================================================================
        # 分支 1：从本地最新日期的 stock_*.csv 文件直接构建 Symbol List
        # =========================================================================
        if from_latest_file:
            data_dir = FINANCE_ROOT / f"{market}stockinfo"
            
            # 调用 FileInfo 获取严格过滤且已按日期升序排序的文件列表
            file_info = FileInfo(trade_date=trade_date, market=market)
            files = file_info.get_file_list
            print("files: ", files)

            if not files:
                raise FileNotFoundError(f"未在目录 {data_dir} 下找到任何交易日期 <= {trade_date} 的合法 stock_*.csv 文件")

            # 获取最后一个（即最新日期）文件
            latest_file = files[-1]
            print(f"📌 [from_latest_file=True] 正在从最新数据文件提取 Symbol 列表: {latest_file.name}")

            df = pd.read_csv(latest_file, usecols=lambda c: c.lower() in ["symbol"])
            col_name = next(c for c in df.columns if c.lower() == "symbol")

            unique_symbols = (
                df[col_name]
                .dropna()
                .astype(str)
                .str.strip()
                .str.upper()
                .unique()
            )

            result = []
            if market == "us":
                # 美股后续走 yfinance 无需精确 mkt_code，默认初始化为 "0"
                for sym in sorted(unique_symbols):
                    result.append({"symbol": sym, "mkt_code": "0"})

            elif market == "cn":
                # A 股根据 Symbol 前缀解析 mkt_code
                for sym in sorted(unique_symbols):
                    actual_mkt_code = "0"
                    if sym.startswith("SH"):
                        actual_mkt_code = "1"
                    elif sym.startswith("SZ"):
                        actual_mkt_code = "0"
                    elif sym.startswith("ETF"):
                        raw_code = sym[3:]  # 去掉 ETF 前缀
                        actual_mkt_code = self.get_etf_market_code(raw_code)
                    
                    result.append({"symbol": sym, "mkt_code": actual_mkt_code})

            print(f"✅ 成功从 {latest_file.name} 提取并还原了 {len(result)} 个 Symbol 记录")
            return result

        # =========================================================================
        # 分支 2：原逻辑 —— 通过东财 API 接口在线抓取并维护缓存文件
        # =========================================================================
        cache_file = (
            FINANCE_ROOT / f"{market}stockinfo" / f"daily_stock_cache_{trade_date}.json"
        )
        cache_data = {}

        if os.path.exists(target_file) and not os.path.exists(cache_file):
            df_stock_list = pd.read_csv(
                target_file,
                usecols=["symbol", "mkt_code"],
                on_bad_lines="skip",
                engine="python",
                encoding="utf-8",
            )
            return df_stock_list.to_dict(orient="records")

        if self.proxy is None:
            self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
        if os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)
                    print(f"加载缓存文件: {cache_file}")
            except:
                print(f"缓存文件格式错误，将重新创建: {cache_file}")
                cache_data = {}
                if os.path.exists(target_file):
                    os.remove(target_file)
        else:
            print(f"无有效缓存，将创建新缓存: {cache_file}")
            if os.path.exists(target_file):
                os.remove(target_file)

        cookie_str = self.get_dynamic_cookies()

        if market == "us":
            mkt_codes = ["105", "106", "107"]
        elif market == "cn":
            mkt_codes = ["0", "1", "etf"]

        for m in mkt_codes:
            if m not in cache_data:
                cache_data[m] = []

            max_page = self.get_total_pages(market, m)
            tool = ToolKit(f"市场代码{m}，下载中...")

            for i in range(1, max_page + 1):
                tool.progress_bar(max_page, i)
                if i in cache_data[m]:
                    continue

                params = self.build_params(market, m, i)
                res = {}
                last_error_msg = ""

                # 1. 使用局部变量保存当前代理，隔离多线程间的代理抢占与竞态冲突
                current_proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)

                for attempt in range(1, 4):  # 尝试 3 次
                    try:
                        response = requests.get(
                            self.__url_list,
                            params=params,
                            proxies=current_proxy,  # 使用局部独立的代理变量
                            headers=self.headers,
                            cookies=self.get_dynamic_cookies(),
                            timeout=10,
                            impersonate="chrome120",
                        )

                        # 校验 HTTP 响应状态码
                        if response.status_code != 200:
                            last_error_msg = f"HTTP 状态码异常: {response.status_code}"
                            current_proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
                            continue

                        res = response.json()

                        # 校验业务层数据合法性
                        if (
                            not isinstance(res, dict)
                            or res.get("rc") != 0
                            or not res.get("data")
                            or not res["data"].get("diff")
                        ):
                            last_error_msg = f"业务数据异常或被拦截: {res}"
                            current_proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
                            continue

                        # 校验全部通过，成功跳出循环，不会触发后面的 else 块
                        break

                    except Exception as e:
                        last_error_msg = f"网络请求/解析异常: {str(e)}"
                        print(f"请求第 {attempt} 次失败，正在更换代理重试... 错误原因: {e}")
                        current_proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)

                else:
                    # 只有当 3 次重试全部失败（没有触发 break）时，才会走进这个 else 块
                    raise RuntimeError(
                        f"获取接口数据失败(已重试3次) | 对应页码: {i} | 最终报错原因: {last_error_msg} | 接口最后返回: {res}"
                    )

                page_data = []
                for item in res["data"]["diff"]:
                    if any(
                        item.get(key) == "-"
                        for key in [
                            "f12",
                            "f14",
                            "f17",
                            "f2",
                            "f15",
                            "f16",
                            "f5",
                            "f20",
                        ]
                    ):
                        continue

                    symbol_val = self.format_symbol(market, m, item)

                    if m == "etf":
                        actual_mkt_code = self.get_etf_market_code(item["f12"])
                    else:
                        actual_mkt_code = m

                    data_dict = {"symbol": symbol_val, "mkt_code": actual_mkt_code}
                    page_data.append(data_dict)

                if page_data:
                    file_exists = (
                        os.path.exists(target_file) and os.path.getsize(target_file) > 0
                    )
                    pd.DataFrame(page_data).to_csv(
                        target_file,
                        mode="a" if file_exists else "w",
                        index=False,
                        header=not file_exists,
                    )

                cache_data[m].append(i)
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2)

        if os.path.exists(cache_file):
            os.remove(cache_file)
            print(f"缓存文件 {cache_file} 已删除")

        df_stock_list = pd.read_csv(
            target_file,
            usecols=["symbol", "mkt_code"],
            on_bad_lines="skip",
            engine="python",
            encoding="utf-8",
        )
        return df_stock_list.to_dict(orient="records")

    def get_daily_gz_info(self, market, trade_date):
        # 此API不需要代理，直接请求即可
        self.proxy = None
        url = "https://quote.eastmoney.com/center/api/qqzq.js?"
        res = requests.get(url, proxies=self.proxy, impersonate="chrome120").text

        lines = res.strip().split("\n")
        data = []
        for line in lines:
            fields = line.split(",")
            data.append(fields)

        filtered_rows = []
        for row in data[2:]:
            if len(row) > 2:
                code = row[1]
                if (market == "us" and code == "US10Y_B") or (
                    market == "cn" and code == "CN10Y_B"
                ):
                    code = row[1]
                    name = row[2]
                    date = row[3]
                    new = row[5]
                    filtered_rows.append([code, name, date, new])

        # 获取输出文件路径与所在目录
        output_filepath = Path(FileInfo(trade_date, market).get_file_path_gz)
        output_dir = output_filepath.parent

        # 1. 写入当日最新数据
        with open(output_filepath, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["code", "name", "date", "new"])
            writer.writerows(filtered_rows)

        # 2. 清理历史 gz_*.csv 数据文件（保留最近 10%，清理早期 90%）
        self._clean_old_gz_files(output_dir)

    def _clean_old_gz_files(self, gz_dir: Path):
        """扫描 gz_dir 目录下的 gz_*.csv 文件，按日期排序并删除前 90% 的历史文件"""
        gz_files = sorted(glob.glob(os.path.join(gz_dir, "gz_*.csv")))
        total_files = len(gz_files)

        if total_files == 0:
            return

        # 计算待删除的文件数量 (清理前 90%)
        delete_count = int(total_files * 0.9)

        if delete_count > 0:
            files_to_delete = gz_files[:delete_count]
            print(f"🧹 触发历史文件清理: 共发现 {total_files} 个文件，将清理较早的 {delete_count} 个 (90%)...")
            
            for file_path in files_to_delete:
                try:
                    os.remove(file_path)
                    print(f"  🗑️ 已删除历史文件: {os.path.basename(file_path)}")
                except Exception as e:
                    print(f"  ⚠️ 删除文件失败 {os.path.basename(file_path)}: {e}")

    def get_daily_stock_info(self, market, trade_date):
        if self.proxy is None:
            self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
        cache_file = (
            FINANCE_ROOT / f"{market}stockinfo" / f"daily_stock_cache_{trade_date}.json"
        )
        cache_data = {}
        file_name_d = FileInfo(trade_date, market).get_file_path_latest

        if os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)
                    print(f"加载缓存文件: {cache_file}")
            except:
                print(f"缓存文件格式错误，将重新创建: {cache_file}")
                cache_data = {}
                if os.path.exists(file_name_d):
                    os.remove(file_name_d)
        else:
            print(f"无有效缓存，将创建新缓存: {cache_file}")
            if os.path.exists(file_name_d):
                os.remove(file_name_d)

        cookie_str = self.get_dynamic_cookies()

        if market == "us":
            mkt_codes = ["105", "106", "107"]
        elif market == "cn":
            mkt_codes = ["0", "1", "etf"]

        for m in mkt_codes:
            if m not in cache_data:
                cache_data[m] = []

            max_page = self.get_total_pages(market, m)
            tool = ToolKit(f"市场代码{m}，共{max_page}页")

            for i in range(1, max_page + 1):
                tool.progress_bar(max_page, i)
                if i in cache_data[m]:
                    continue

                params = self.build_params(market, m, i)
                res = {}
                for _ in range(3):
                    try:
                        res = requests.get(
                            self.__url_list,
                            params=params,
                            proxies=self.proxy,
                            headers=self.headers,
                            cookies=self.get_dynamic_cookies(),
                            timeout=10,
                            impersonate="chrome120",
                        ).json()
                        if (
                            res.get("rc") != 0
                            or not res.get("data")
                            or not res["data"].get("diff")
                        ):
                            self.proxy = self.pm.get_working_proxy(
                                enable_proxy=self.use_proxy
                            )
                            continue
                        break
                    except Exception:
                        print("请求失败，正在重试...", _)
                        self.proxy = self.pm.get_working_proxy(
                            enable_proxy=self.use_proxy
                        )
                        continue
                else:
                    raise RuntimeError(
                        f"获取接口数据失败，最后返回: {locals().get('res', None)}"
                    )

                page_data = []
                for item in res["data"]["diff"]:
                    if any(
                        item.get(key) == "-"
                        for key in [
                            "f12",
                            "f14",
                            "f17",
                            "f2",
                            "f15",
                            "f16",
                            "f5",
                            "f20",
                        ]
                    ):
                        continue

                    symbol_val = self.format_symbol(market, m, item)
                    data_dict = {
                        "symbol": symbol_val,
                        "name": item["f14"],
                        "open": item["f17"],
                        "close": item["f2"],
                        "high": item["f15"],
                        "low": item["f16"],
                        "volume": item["f5"],
                        "total_value": item["f20"],
                        "pe": item["f9"],
                    }
                    page_data.append(data_dict)

                if page_data:
                    df = pd.DataFrame(page_data)
                    date = datetime.strptime(trade_date, "%Y%m%d")
                    df["date"] = date

                    file_exists = (
                        os.path.exists(file_name_d) and os.path.getsize(file_name_d) > 0
                    )
                    df.to_csv(
                        file_name_d,
                        mode="a" if file_exists else "w",
                        index=True,
                        header=not file_exists,
                    )
                cache_data[m].append(i)
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2)

        if os.path.exists(cache_file):
            os.remove(cache_file)
            print(f"缓存文件 {cache_file} 已删除")

        self.get_daily_gz_info(market, trade_date)

    def get_his_stock_info(
        self, mkt_code, symbol, start_date, end_date, cache_path=None
    ):
        if self.proxy is None:
            self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)

        if str(mkt_code) in ["105", "106", "107"]:
            symbol_val = symbol
        elif str(mkt_code) in ["0", "1"]:
            symbol_val = re.sub(r"^(ETF|SZ|SH)", "", symbol)
        cookie_str = self.get_dynamic_cookies()

        params = {
            "secid": f"{mkt_code}.{symbol_val}",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56",
            "klt": "101",
            "fqt": "0",  # 修改重点: 从 "1"(前复权) 改为 "0"(不复权/真实历史价格)
            "beg": start_date,
            "end": end_date,
            "smplmt": "755",
            "lmt": "1000000",
        }
        res = {}
        for _ in range(3):
            try:
                res = requests.get(
                    self.__url_history,
                    params=params,
                    proxies=self.proxy,
                    headers=self.headers,
                    cookies=cookie_str,
                    timeout=10,
                    impersonate="chrome120",
                ).json()
                if res.get("rc") != 0 or not res.get("data"):
                    self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
                    continue
                break
            except Exception as e:  # 使用通用 Exception 兼容 curl_cffi 异常
                print("请求失败，正在重试...", _)
                self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
                continue
        else:
            raise RuntimeError(
                f"获取接口数据失败，最后返回: {locals().get('res', None)}"
            )

        klines = res.get("data", {}).get("klines", [])
        if not klines:
            if cache_path is not None:
                with open(cache_path, "a") as f:
                    f.write(f"{symbol}\n")
                return []

        name = res.get("data", {}).get("name", "")
        print("开始处理：", name)

        list = []
        for i in klines:
            if (
                i.split(",")[1] == "-"
                or i.split(",")[2] == "-"
                or i.split(",")[3] == "-"
                or i.split(",")[4] == "-"
                or i.split(",")[5] == "-"
            ):
                continue

            dict = {
                "symbol": symbol,
                "name": name,
                "open": i.split(",")[1],
                "close": i.split(",")[2],
                "high": i.split(",")[3],
                "low": i.split(",")[4],
                "volume": i.split(",")[5],
                "date": i.split(",")[0],
            }
            list.append(dict)
        return list

    def get_his_stock_info_list(
        self,
        market,
        start_date,
        end_date,
        file_path,
        empty_klines_cache_path=None,
        stock_list_cache_path=None,
    ):
        paths = {
            "us": {
                "empty_klines": FINANCE_ROOT
                / "usstockinfo"
                / "us_empty_klines_cache.csv",
                "stock_list": FINANCE_ROOT / "usstockinfo" / "us_stock_list_cache.csv",
            },
            "cn": {
                "empty_klines": FINANCE_ROOT
                / "cnstockinfo"
                / "cn_empty_klines_cache.csv",
                "stock_list": FINANCE_ROOT / "cnstockinfo" / "cn_stock_list_cache.csv",
            },
        }

        empty_klines_cache_path = (
            empty_klines_cache_path or paths[market]["empty_klines"]
        )
        stock_list_cache_path = stock_list_cache_path or paths[market]["stock_list"]
        tickinfo = self.get_stock_list(market, end_date, stock_list_cache_path)

        done_symbols = set()
        if os.path.exists(file_path):
            try:
                df_exist = pd.read_csv(
                    file_path,
                    usecols=["symbol"],
                    on_bad_lines="skip",
                    engine="python",
                    encoding="utf-8",
                )
                if not df_exist.empty and "symbol" in df_exist.columns:
                    done_symbols = set(df_exist["symbol"].astype(str).unique())
                else:
                    done_symbols = set()
            except pd.errors.EmptyDataError:
                print(f"文件 {file_path} 为空，无法解析")
                done_symbols = set()

        empty_klines_symbols = set()
        if os.path.exists(empty_klines_cache_path):
            with open(empty_klines_cache_path, "r") as f:
                empty_klines_symbols = set(line.strip() for line in f if line.strip())

        tickinfo = [
            item
            for item in tickinfo
            if item["symbol"] not in done_symbols
            and item["symbol"] not in empty_klines_symbols
        ]
        print(
            f"总共{len(tickinfo)}只股票，已完成{len(done_symbols)}，klines为空{len(empty_klines_symbols)}，待处理{len(tickinfo)}"
        )
        batch0 = len(done_symbols)
        batch_size = 10
        batch_count = 0
        tool = ToolKit("历史数据下载")

        with open(file_path, "a") as csvfile:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                for h in range(0, len(tickinfo), batch_size):
                    batch_count += 1
                    batch_list = []
                    futures = []
                    # 动态挑选抓取函数：美股使用 yfinance，A股沿用东财接口
                    fetch_func = (
                        self.get_us_his_stock_info_yf
                        if market == "us"
                        else self.get_his_stock_info
                    )

                    for t in range(1, batch_size + 1):
                        index = h + t - 1
                        if index < len(tickinfo):
                            # 根据市场构建参数组
                            if market == "us":
                                args = (
                                    tickinfo[index]["symbol"],
                                    start_date,
                                    end_date,
                                    empty_klines_cache_path,
                                )
                            else:
                                args = (
                                    tickinfo[index]["mkt_code"],
                                    tickinfo[index]["symbol"],
                                    start_date,
                                    end_date,
                                    empty_klines_cache_path,
                                )

                            future = executor.submit(fetch_func, *args)
                            futures.append(future)
                    for future in concurrent.futures.as_completed(futures):
                        list1 = future.result()
                        if list1 is not None:
                            batch_list.extend(list1)

                    tool.progress_bar(len(tickinfo), h)
                    if len(batch_list) > 0:
                        try:
                            columns = [
                                "symbol",
                                "name",
                                "open",
                                "close",
                                "high",
                                "low",
                                "volume",
                                "date",
                            ]
                            df = pd.DataFrame(batch_list)
                            df = df[columns]
                            df = df.dropna(how="all")
                            df.to_csv(
                                csvfile,
                                mode="a",
                                index=True,
                                header=(h == 0 and batch0 == 0),
                            )
                        except IOError:
                            pass

    def format_proxy_url(self, proxy_input):
        """格式化代理地址为标准 URL 字符串"""
        if not proxy_input:
            return None
        if isinstance(proxy_input, str):
            if not proxy_input.startswith(("http://", "https://", "socks5://")):
                return f"http://{proxy_input}"
            return proxy_input
        if isinstance(proxy_input, (list, tuple)) and len(proxy_input) >= 2:
            return f"http://{proxy_input[0]}:{proxy_input[1]}"
        return str(proxy_input)

    def get_us_his_stock_info_yf(
        self, symbol, start_date, end_date, cache_path=None, auto_adjust=False
    ):
        """基于 yfinance 获取美股历史日 K 数据

        :param auto_adjust: 是否自动调整（前复权/除权调整），默认 False (不复权)
        - 自动将 T_A 映射为 Yahoo 正确格式 T-PA (以及 T-A, T.A 备选)
        - 导出 CSV / 返回数据严格保持原始输入 symbol (如 T_A)
        """
        # 0. 防御校验：处理 NaN / float / None 等无效 symbol 输入
        if pd.isna(symbol) or not symbol:
            print(f"[Warning] 捕获到无效的 symbol 输入: {symbol}，已跳过。")
            return []
        
        # 强制转换为标准的干净字符串
        symbol = str(symbol).strip()
        if not symbol or symbol.lower() == "nan":
            return []        
        
        os.environ.setdefault("CURL_CA_BUNDLE", "")
        os.environ.setdefault("SSL_CERT_FILE", "")

        s_date = pd.to_datetime(start_date).strftime("%Y-%m-%d")
        e_date = pd.to_datetime(end_date).strftime("%Y-%m-%d")

        df_hist = pd.DataFrame()
        found_symbol_yf = None  # 记录在 yfinance 上实际匹配成功的符号
        matched_ticker = None   # 保存已成功获取数据的 ticker 句柄，避免重新实例化
        company_name = None #默认symbol为company name

        # 1. 构建 Yahoo Finance 的候选 Symbol 尝试顺序
        # 仅当包含 '_' 时构建多种替换规则；普通 symbol 只有其自身 1 个候选，避免无谓开销
        if "_" in symbol:
            yf_candidates = [
                symbol.replace("_", "-P"),  # 优先股语法（最常见）：T_A -> T-PA
                symbol.replace("_", "-"),   # 普通类股语法：T_A -> T-A
                symbol.replace("_", "."),   # 点号语法：T_A -> T.A
            ]
        else:
            yf_candidates = [symbol]

        for attempt in range(1, 4):
            # ================= 专属 yfinance 线程锁逻辑 =================
            proxy_dict = self.current_working_proxy_us

            if not proxy_dict:
                with self.proxy_lock_us:
                    if not self.current_working_proxy_us:
                        print(f"[US yfinance] 当前无可用海外代理，启动 pm_us 检索...")
                        new_proxy = self.pm_us.get_working_proxy(
                            enable_proxy=self.use_proxy
                        )
                        if not new_proxy:
                            new_proxy = self.pm_us.get_next_proxy()
                        self.current_working_proxy_us = new_proxy

                    proxy_dict = self.current_working_proxy_us
            # ============================================================

            raw_proxy = (
                (proxy_dict.get("socks5") or proxy_dict.get("https") or proxy_dict.get("http"))
                if isinstance(proxy_dict, dict) else None
            )
            proxy_str = self.format_proxy_url(raw_proxy) if raw_proxy else None

            if proxy_str:
                os.environ["HTTP_PROXY"] = proxy_str
                os.environ["HTTPS_PROXY"] = proxy_str
            else:
                os.environ.pop("HTTP_PROXY", None)
                os.environ.pop("HTTPS_PROXY", None)

            try:
                # 2. 依次按候选优先级进行拉取测试
                for candidate in yf_candidates:
                    ticker = yf.Ticker(candidate)
                    df_candidate = ticker.history(
                        start=s_date, end=e_date, interval="1d", auto_adjust=auto_adjust
                    )
                    if df_candidate is not None and not df_candidate.empty:
                        df_hist = df_candidate
                        found_symbol_yf = candidate  # 标记匹配成功的 Yahoo 符号（如 T-PA）
                        matched_ticker = ticker     # 直接保留成功的 ticker 对象
                        break

                # ==================== 4. 获取真实公司名称 (复用句柄 + 轻量读取) ====================
                company_name = symbol
                # 提升性能，跳过company name获取请求
                # if matched_ticker is not None:
                #     try:
                #         info = matched_ticker.info
                #         company_name = info.get("longName") or info.get("shortName") or symbol
                #     except Exception:
                #         company_name = symbol

                if df_hist is not None and not df_hist.empty:
                    break
                else:
                    # 所有候选都尝试完毕仍无数据，抛出异常进入 catch
                    raise Exception(f"404 Not Found for all candidates of {symbol}")

            except Exception as e:
                err_str = str(e)

                # 精准拦截 404 / 资源不存在：退出尝试，不重试，不杀代理
                if "404" in err_str or "Not Found" in err_str or "delisted" in err_str:
                    print(f"[{symbol}] 尝试候选名 {yf_candidates} 均未找到数据，确定为不存在/退市。")
                    df_hist = None
                    break

                # 真正的网络/超时问题才触发代理失效
                print(f"[{symbol}] yfinance 第 {attempt} 次失败，标记 pm_us 代理失效... 错误: {e}")
                with self.proxy_lock_us:
                    if self.current_working_proxy_us == proxy_dict:
                        self.current_working_proxy_us = None

            finally:
                os.environ.pop("HTTP_PROXY", None)
                os.environ.pop("HTTPS_PROXY", None)

        # 3. 未找到数据的落盘缓存
        if df_hist is None or df_hist.empty:
            if cache_path is not None:
                with open(cache_path, "a", encoding="utf-8") as f:
                    f.write(f"{symbol}\n")
            return []

        # ==================== 5. 高性能向量化数据格式化 ====================
        df_res = df_hist.reset_index().copy()

        # 校验必填列
        req_cols = ["Date", "Open", "Close", "High", "Low", "Volume"]
        missing_cols = [c for c in req_cols if c not in df_res.columns]
        if missing_cols:
            return []

        # 过滤关键数值缺失的数据行
        df_res = df_res.dropna(subset=["Open", "Close", "Volume"])
        if df_res.empty:
            return []

        # 向量化转换列与数据格式
        df_res["symbol"] = symbol
        df_res["name"] = company_name
        df_res["date"] = pd.to_datetime(df_res["Date"]).dt.strftime("%Y-%m-%d")
        df_res["open"] = df_res["Open"].astype(float).round(4).astype(str)
        df_res["close"] = df_res["Close"].astype(float).round(4).astype(str)
        df_res["high"] = df_res["High"].astype(float).round(4).astype(str)
        df_res["low"] = df_res["Low"].astype(float).round(4).astype(str)
        df_res["volume"] = df_res["Volume"].astype(int).astype(str)

        out_cols = ["symbol", "name", "open", "close", "high", "low", "volume", "date"]
        return df_res[out_cols].to_dict("records")

    def restore_yfinance_raw_csv(
        self,
        trade_date: str = None,
        market: str = None,
        history_file_path: str = None,
        chunksize: int = 200000
    ):
        """
        根据固定的复权因子文件 (symbol, date, dividend, split_ratio)，
        通过流式 Batch 批次处理还原历史 K 线 CSV 中的拆股数据，无多余临时文件并原位覆盖。
        """
        # 1. 确定复权因子文件与历史 K 线文件路径
        if hasattr(self, 'file_actions_history') and self.file_actions_history:
            actions_file = self.file_actions_history
        else:
            file_info = FileInfo(trade_date, market)
            actions_file = file_info.get_file_path_actions_history

        target_history_file = history_file_path or file_info.get_file_path_history

        if not os.path.exists(actions_file):
            raise FileNotFoundError(f"未找到复权因子文件: {actions_file}")
        if not os.path.exists(target_history_file):
            raise FileNotFoundError(f"未找到历史K线文件: {target_history_file}")

        # 2. 预载复权因子文件 (Schema: symbol, date, dividend, split_ratio)
        df_actions = pd.read_csv(actions_file, dtype={'symbol': str, 'date': str})
        df_actions['date'] = pd.to_datetime(df_actions['date']).dt.strftime('%Y-%m-%d')
        
        # 仅筛选存在实际拆股的记录
        splits_df = df_actions[
            (df_actions['split_ratio'].notnull()) & 
            (df_actions['split_ratio'] != 0) & 
            (df_actions['split_ratio'] != 1.0)
        ][['symbol', 'date', 'split_ratio']]
        
        # 构建 {symbol: {date_str: split_ratio}} 快速字典索引
        symbol_splits_map = {
            sym: group.set_index('date')['split_ratio'].to_dict()
            for sym, group in splits_df.groupby('symbol')
        }

        # 3. 在 /tmp 下仅建立 1 个最终输出临时文件
        temp_dir = tempfile.mkdtemp(dir="/tmp", prefix="yf_restore_")
        output_temp_file = os.path.join(temp_dir, "output_restored.csv")

        try:
            print(f"[Start] 开始流式批处理还原文件: {target_history_file}")

            leftover_df = None
            is_first_write = True
            all_cols = []
            symbol_col = None
            date_col = None

            # 内部函数：还原单个完整的 Symbol DataFrame 并追加写入目标临时 CSV
            def process_and_append_symbol(df_sym: pd.DataFrame):
                nonlocal is_first_write

                # 1. 确保将 Date 列解析为 DatetimeIndex/Series 类型
                date_series = pd.to_datetime(df_sym[date_col])
                
                # 2. 按日期正序重新排列
                sort_idx = date_series.argsort()
                df_sym = df_sym.iloc[sort_idx].reset_index(drop=True)
                date_series = date_series.iloc[sort_idx].reset_index(drop=True)

                current_symbol = str(df_sym[symbol_col].iloc[0])
                
                # 3. 格式化日期字符串（此时 date_series 必定为 datetime 格式，.dt 不会报错）
                date_str_series = date_series.dt.strftime('%Y-%m-%d')
                splits_dict = symbol_splits_map.get(current_symbol, {})

                # 4. 若存在拆股记录，则执行向量化还原
                if splits_dict:
                    splits = date_str_series.map(splits_dict).fillna(1.0).values
                    
                    # 倒序累乘未来发生的所有拆股倍数
                    split_factors_rev = np.cumprod(splits[::-1])[::-1]
                    cum_factors = np.ones(len(splits), dtype=float)
                    if len(splits) > 1:
                        # 向后平移 1 位：t 日乘数为 t+1 日至最新日拆股因子之积
                        cum_factors[:-1] = split_factors_rev[1:]

                    # 放大价格
                    for p_col in ['Open', 'High', 'Low', 'Close', 'open', 'high', 'low', 'close']:
                        if p_col in df_sym.columns:
                            df_sym.loc[:, p_col] = df_sym[p_col] * cum_factors

                    # 缩小成交量
                    for v_col in ['Volume', 'volume']:
                        if v_col in df_sym.columns:
                            vol_series = df_sym[v_col] / cum_factors
                            df_sym.loc[:, v_col] = vol_series.round().astype('int64')

                # 5. 统一日期格式写入
                df_sym.loc[:, date_col] = date_str_series

                # 6. 追加写入临时 CSV
                df_sym.to_csv(
                    output_temp_file,
                    mode='a',
                    index=False,
                    header=is_first_write,
                    columns=all_cols
                )
                is_first_write = False

            # -------------------------------------------------------------
            # 单线程流式 Chunk 分批读取与粘合
            # -------------------------------------------------------------
            for chunk in pd.read_csv(target_history_file, chunksize=chunksize, low_memory=False):
                if not all_cols:
                    all_cols = chunk.columns.tolist()
                    symbol_col = next(c for c in all_cols if c.lower() == 'symbol')
                    date_col = next(c for c in all_cols if c.lower() == 'date')

                # 若上一个 chunk 末尾有未处理完的 Symbol，拼接在当前 chunk 头部
                if leftover_df is not None:
                    chunk = pd.concat([leftover_df, chunk], ignore_index=True)
                    leftover_df = None

                # 保持原顺序获取 chunk 内出现的 Symbol 列表
                unique_symbols = chunk[symbol_col].unique()

                # 除了最后一个 Symbol 外，前面的 Symbol 在当前 Chunk 中必已完整包含
                for sym in unique_symbols[:-1]:
                    sym_df = chunk[chunk[symbol_col] == sym].copy()  # 加上 .copy()
                    process_and_append_symbol(sym_df)

                # 最后一个 Symbol 可能在下一个 Chunk 中还有后续行，暂存至 leftover_df
                last_sym = unique_symbols[-1]
                leftover_df = chunk[chunk[symbol_col] == last_sym].copy()

            # 处理全文件读取完毕后剩余的最后一个 Symbol
            if leftover_df is not None and len(leftover_df) > 0:
                process_and_append_symbol(leftover_df)

            # -------------------------------------------------------------
            # 原子覆盖原历史文件
            # -------------------------------------------------------------
            print(f"[Finish] 还原完毕，覆盖原文件: {target_history_file}")
            shutil.move(output_temp_file, target_history_file)
            print("拆股还原处理全部成功完成！")

        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)        