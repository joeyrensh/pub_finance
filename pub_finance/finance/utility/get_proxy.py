import datetime
import hashlib
import json
import math
import os
import random
import time
from pathlib import Path
import pandas as pd
from curl_cffi import requests

from finance import FINANCE_ROOT
import logging
import itertools
from typing import Any, Dict

logger = logging.getLogger(__name__)


class ProxyManager:
    DEFAULT_COOKIES: Dict[str, str] = {
        "qgqp_b_id": "64128e722243aac323ad9a57e33fe37f",
        "st_pvi": "44203626923623",
        "st_si": "04662034469518",
    }
    DEFAULT_PSI_SUFFIX = "-113200301321-6001712214"

    def __init__(self, proxy_file_path=FINANCE_ROOT / "utility/proxy.txt"):
        self.proxy_file_path = proxy_file_path
        self.proxies_list = []
        self.current_proxy_index = 0
        self.load_proxies()

        # 1. 切换为安全的 HTTPS 协议
        self.__url_list = "http://push2.eastmoney.com/api/qt/clist/get"

        # 2. 精简 Headers，只保留业务必需的 Referer/Accept，不手写 User-Agent 以免与 impersonate 冲突
        self.headers = {
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        # 3. 内存缓存 Cookie 基础模板与请求计数器
        self.cookie_path = FINANCE_ROOT / "utility/eastmoney_cookie.json"
        self._cookie_base = self.parse_cookie_string()
        # 使用线程安全的自增计数器替代非安全的 int += 1
        self._counter = itertools.count(start=1)

        # 4. 东财通用固定 ut Token
        self.COMMON_UT = "fa5fd1943c7b386f172d6893dbfba10b"

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
        now = datetime.datetime.now()
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
        # 前端 ut 参数实质上是一个 32 位唯一标识符，无需复杂模拟 IP，使用 uuid4 更加 Pythonic 且高效
        import uuid

        return hashlib.md5(uuid.uuid4().bytes).hexdigest()

    def load_proxies(self):
        """从文件加载代理列表，忽略以#开头的行"""
        if os.path.exists(self.proxy_file_path):
            with open(self.proxy_file_path, "r", encoding="utf-8") as f:
                # 过滤掉以#开头的行和空行
                self.proxies_list = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                ]
            print(f"已加载 {len(self.proxies_list)} 个代理")
            # 可选：打印被忽略的注释行数量
            if len(self.proxies_list) > 0:
                print(f"代理列表示例：{self.proxies_list[:3]}...")  # 显示前 3 个代理
        else:
            print(f"代理文件 {self.proxy_file_path} 不存在")
            self.proxies_list = []

    def get_next_proxy(self):
        """获取下一个代理"""
        if not self.proxies_list:
            return None

        proxy_str = self.proxies_list[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(
            self.proxies_list
        )

        # 转换为 curl_cffi / requests 需要的格式
        proxy_dict = {
            "http": f"http://{proxy_str}",
            "https": f"http://{proxy_str}",
        }

        return proxy_dict

    def validate_proxy(self, proxy_dict):
        """使用指定代理获取总页数"""
        params = {
            "pn": "1",
            "pz": "100",
            "po": "1",
            "np": "1",
            "ut": self.COMMON_UT,  # 使用标准固定 ut Token
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": "m:0 t:6,m:0 t:80",
            "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
            "_": str(int(time.time() * 1000)),  # 防缓存时间戳
        }

        try:
            response = requests.get(
                self.__url_list,
                params=params,
                proxies=proxy_dict,
                headers=self.headers,
                cookies=self.get_dynamic_cookies(),
                timeout=5,
                impersonate="chrome120",
                verify=False,
            )

            # 1. 检查 HTTP 状态码
            if response.status_code != 200:
                return False, f"HTTP状态码异常: {response.status_code}"

            # 2. 检查返回内容是否为 HTML（拦截/劫持/错误页）
            text = response.text.strip()
            if text.startswith("<"):
                return False, f"返回了HTML而非JSON(可能是代理劫持或重定向): {text[:50]}"

            # 3. 安全解析 JSON
            res = response.json()
            total_page_no = math.ceil(res.get("data", {}).get("total", 0) / 100)

            return (
                (True, total_page_no) if total_page_no > 0 else (False, total_page_no)
            )

        except Exception as e:
            return False, f"请求/解析失败: {str(e)}"

    def test_proxy(self, proxy_dict, test_function):
        try:
            result, content = test_function(proxy_dict)
            if result:
                return True
            else:
                print(f"❌ 代理测试失败 [{proxy_dict['http']}]: {content}")
                return False
        except Exception as e:
            print(f"❌ 代理测试异常 [{proxy_dict['http']}]: {e}")
            return False

    def get_working_proxy(self, max_retries=3, enable_proxy=True):
        """获取一个可用的代理。

        参数 max_retries: 轮询整个代理列表的最大次数（默认 3 轮）。
        返回代理字典，如果所有代理均不可用则返回 None。
        """
        if not self.proxies_list or not enable_proxy:
            return None

        total = len(self.proxies_list)
        # 最多测试 max_retries * total 次（即完整遍历 max_retries 轮）
        total_tests = max_retries * total

        # 记录本次调用开始时的索引
        start_index = self.current_proxy_index
        tested = 0

        for _ in range(total_tests):
            proxy = self.get_next_proxy()
            tested += 1

            # 计算当前代理在列表中的索引（0‑based）
            current_idx = (start_index + tested - 1) % total
            # 计算当前是第几轮（从 1 开始）
            round_num = (tested - 1) // total + 1
            # 如果轮次超过 max_retries，提前终止（理论上不会发生）
            if round_num > max_retries:
                break

            print(
                f"测试进度：第{round_num}轮 {current_idx + 1}/{total} - 测试代理：{proxy}"
            )

            if self.test_proxy(proxy, self.validate_proxy):
                print(f"✅ 代理可用：{proxy}")
                return proxy

        print("所有代理测试失败（已达最大重试轮数）")
        return None

    def save_working_proxies_to_file(
        self, output_file=FINANCE_ROOT / "utility/working_proxies.txt"
    ):
        r"""测试所有代理并将有效代理保存到指定文件

        每次调用都会重新生成文件
        # 站大爷 IP 格式化命令：
        # 把 Port：改成 :
        # :%s/Port：/:/g
        # 把 IP 行和下一行端口合并
        # :%s/\(\d\+\.\d\+\.\d\+\.\d\+\)\n\n\?:\(\d\+\)/\1:\2/g
        # 删除其他无关行
        # :g!/^\d\+\.\d\+\.\d\+\.\d\+:/d
        """
        if not self.proxies_list:
            print("没有代理可供测试")
            return []

        # 重置索引，确保测试所有代理
        self.current_proxy_index = 0

        working_proxies = []
        total_proxies = len(self.proxies_list)

        print(f"开始测试 {total_proxies} 个代理...")

        for i, proxy_str in enumerate(self.proxies_list):
            proxy_dict = {
                "http": f"http://{proxy_str}",
                "https": f"http://{proxy_str}",
            }

            # 显示进度
            progress = (i + 1) / total_proxies * 100
            print(
                f"测试进度：{i + 1}/{total_proxies} ({progress:.0f}%) - 测试代理：{proxy_str}"
            )

            # 测试代理
            success = self.test_proxy(proxy_dict, self.validate_proxy)
            if success:
                working_proxies.append(proxy_str)
                print(f"✅ 代理可用：{proxy_str}")
            else:
                print(f"❌ 代理不可用：{proxy_str}")

        # 保存有效代理到文件
        if working_proxies:
            # 确保目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # 写入文件（覆盖模式）
            with open(output_file, "w", encoding="utf-8") as f:
                for proxy in working_proxies:
                    f.write(f"{proxy}\n")

            print(f"✅ 已保存 {len(working_proxies)} 个有效代理到：{output_file}")
            print(f"有效代理列表：{working_proxies}")
        else:
            print("❌ 没有找到任何有效代理")

    def sync_working_proxies_to_proxy_file(
        self,
        working_proxies_file=FINANCE_ROOT / "utility/working_proxies.txt",
        proxy_file=FINANCE_ROOT / "utility/proxy.txt",
    ):
        """将 working_proxies.txt 中的最新可用代理同步到 proxy.txt

        - 检查新代理是否已存在于 proxy.txt
        - 如果不存在，append 到文件首行
        - 不添加任何注释
        """
        # 读取 working_proxies.txt
        if not os.path.exists(working_proxies_file):
            print(f"工作代理文件 {working_proxies_file} 不存在")
            return

        with open(working_proxies_file, "r", encoding="utf-8") as f:
            working_proxies = [line.strip() for line in f if line.strip()]

        print(f"读取到 {len(working_proxies)} 个工作代理")

        # 读取现有 proxy.txt
        existing_proxies = []
        if os.path.exists(proxy_file):
            with open(proxy_file, "r", encoding="utf-8") as f:
                existing_proxies = [line.strip() for line in f if line.strip()]
            print(f"现有 proxy.txt 中有 {len(existing_proxies)} 个代理")

        # 找出需要添加的新代理（存在于 working_proxies 但不存在于 existing_proxies）
        new_proxies = [p for p in working_proxies if p not in existing_proxies]

        if not new_proxies:
            print("没有新代理需要添加")
            return

        print(f"发现 {len(new_proxies)} 个新代理需要添加")

        # 构建新的代理列表：新代理在前 + 原有代理
        updated_proxies = new_proxies + existing_proxies

        # 写入 proxy.txt（覆盖模式）
        with open(proxy_file, "w", encoding="utf-8") as f:
            for proxy in updated_proxies:
                f.write(f"{proxy}\n")

        print(f"✅ 已同步 {len(new_proxies)} 个新代理到 {proxy_file}")
        print(f"新代理列表：{new_proxies}")
