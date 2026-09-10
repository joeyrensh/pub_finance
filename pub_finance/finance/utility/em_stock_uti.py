#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import concurrent.futures
import csv
import hashlib
import json
import math
import os
import random
import re
import time
from datetime import datetime

import pandas as pd
from curl_cffi import requests  # 替换为 curl_cffi
from fake_useragent import UserAgent

from finance import FINANCE_ROOT
from finance.utility.emcookie_generation import CookieGeneration
from finance.utility.fileinfo import FileInfo
from finance.utility.get_proxy import ProxyManager
from finance.utility.toolkit import ToolKit


class EMWebCrawlerUti:
    def __init__(self, use_proxy=True):  # 1. 默认 use_proxy=True 兼容原有调用
        """
        # 美股/A股日数据及历史数据爬虫
        :param use_proxy: 是否开启代理，默认 True。传入 False 则不使用代理直连。
        """
        self.__url_list = "https://push2.eastmoney.com/api/qt/clist/get"
        self.__url_history = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

        self.use_proxy = use_proxy
        self.pm = ProxyManager()
        # 2. 初始化时将 enable_proxy 传入
        self.cg = CookieGeneration()
        self.cg.generate_em_cookies()
        self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)

        self.headers = {
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        # 4. 初始化 Cookie 数据与会话状态
        self._cookie_base = self.parse_cookie_string()
        self.visit_count = 1  # st_sn 请求递增计数器

        self.pz = 100

    def parse_cookie_string(self):
        """读取 JSON 格式的 cookie 文件（只在初始化时读取一次）"""
        cookie_file = FINANCE_ROOT / "utility" / "eastmoney_cookie.json"
        try:
            with open(cookie_file, "r", encoding="utf-8") as f:
                cookie_data = json.load(f)
                print("已成功加载 JSON cookie 信息")
                return cookie_data
        except Exception as e:
            print(f"读取 Cookie 文件异常: {e}，使用兜底配置")
            return {
                "qgqp_b_id": "64128e722243aac323ad9a57e33fe37f",
                "st_pvi": "44203626923623",
                "st_si": "04662034469518",
            }

    def get_dynamic_cookies(self):
        """基于模板动态生成并更新包含最新时间戳与计数的 Cookie 字典"""
        cookies = self._cookie_base.copy()

        # 更新 st_psi 的第一段时间戳 (YYYYMMDDHHMMSSmmm)
        now_str = time.strftime("%Y%m%d%H%M%S")
        ms_str = f"{int(time.time() * 1000) % 1000:03d}"

        orig_psi = cookies.get("st_psi", "")
        # 保留 JSON 中原本真实的会话后缀标识
        if "-" in orig_psi:
            suffix = orig_psi[orig_psi.find("-") :]
        else:
            suffix = "-113200301321-6001712214"

        cookies["st_psi"] = f"{now_str}{ms_str}{suffix}"

        # 更新请求计数器
        cookies["st_sn"] = str(self.visit_count)
        self.visit_count += 1

        # 清理 delete 标记
        if cookies.get("st_asi") == "delete":
            cookies.pop("st_asi", None)

        return cookies

    def generate_ut_param(self):
        def generate_china_ip():
            china_networks = [
                (58, random.randint(0, 255)),
                (59, random.randint(0, 255)),
                (60, random.randint(0, 255)),
                (61, random.randint(0, 255)),
                (106, random.randint(0, 255)),
                (110, random.randint(0, 255)),
                (111, random.randint(0, 255)),
                (112, random.randint(0, 255)),
                (113, random.randint(0, 255)),
                (114, random.randint(0, 255)),
                (115, random.randint(0, 255)),
                (116, random.randint(0, 255)),
                (117, random.randint(0, 255)),
                (118, random.randint(0, 255)),
                (119, random.randint(0, 255)),
                (120, random.randint(0, 255)),
                (121, random.randint(0, 255)),
                (122, random.randint(0, 255)),
                (123, random.randint(0, 255)),
                (124, random.randint(0, 255)),
                (125, random.randint(0, 255)),
                (171, random.randint(0, 255)),
                (175, random.randint(0, 255)),
                (180, random.randint(0, 255)),
                (182, random.randint(0, 255)),
                (183, random.randint(0, 255)),
                (202, random.randint(0, 255)),
                (210, random.randint(0, 255)),
                (211, random.randint(0, 255)),
                (218, random.randint(0, 255)),
                (219, random.randint(0, 255)),
                (220, random.randint(0, 255)),
                (221, random.randint(0, 255)),
                (222, random.randint(0, 255)),
                (223, random.randint(0, 255)),
            ]
            network = random.choice(china_networks)
            ip_parts = [
                network[0],
                network[1],
                random.randint(1, 254),
                random.randint(1, 254),
            ]
            return ".".join(map(str, ip_parts))

        random_ip = generate_china_ip()
        timestamp = int(time.time() * 1000)
        random_num = random.randint(1000000000, 9999999999)

        base_str = f"{timestamp}{random_num}{random_ip}"
        ut_hash = hashlib.md5(base_str.encode()).hexdigest()
        return ut_hash

    def build_params(self, market, mkt_code, page_num):
        base_params = {
            "pn": f"{page_num}",
            "pz": self.pz,
            "po": "1",
            "np": "1",
            # "ut": self.generate_ut_param(),
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

    def get_stock_list(self, market, trade_date, target_file=None):
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
        # 原逻辑把 self.proxy 清空为 None，现改用 get_working_proxy
        self.proxy = self.pm.get_working_proxy(enable_proxy=self.use_proxy)
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

        output_filename = FileInfo(trade_date, market).get_file_path_gz
        with open(output_filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["code", "name", "date", "new"])
            writer.writerows(filtered_rows)

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
            "ut:": self.generate_ut_param(),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56",
            "klt": "101",
            "fqt": "1",
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
                    for t in range(1, batch_size + 1):
                        index = h + t - 1
                        if index < len(tickinfo):
                            future = executor.submit(
                                self.get_his_stock_info,
                                tickinfo[index]["mkt_code"],
                                tickinfo[index]["symbol"],
                                start_date,
                                end_date,
                                empty_klines_cache_path,
                            )
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
