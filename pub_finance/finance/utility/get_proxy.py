import datetime
import hashlib
import json
import logging
import math
import os
import random
import time
import uuid
import itertools
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Literal

import pandas as pd
from curl_cffi import requests
from finance import FINANCE_ROOT

logger = logging.getLogger(__name__)

# 定义代理类型
ProxyType = Literal["cn", "overseas"]


class ProxyManager:
    DEFAULT_COOKIES: Dict[str, str] = {
        "qgqp_b_id": "64128e722243aac323ad9a57e33fe37f",
        "st_pvi": "44203626923623",
        "st_si": "04662034469518",
    }
    DEFAULT_PSI_SUFFIX = "-113200301321-6001712214"

    # 预设文件路径映射
    PROXY_PATH_MAP = {
        "cn": FINANCE_ROOT / "utility/proxy.txt",
        "overseas": FINANCE_ROOT / "proxy/overseas_proxies.txt",
    }

    def __init__(
        self,
        proxy_file_path: Optional[Path] = None,
        proxy_type: ProxyType = "cn",
    ):
        """初始化代理管理器

        :param proxy_file_path: 显式指定的代理文件路径。若为 None，则根据 proxy_type
        自动匹配。
        :param proxy_type: 代理类型，支持 "cn" (默认国内) 或 "overseas" (海外)。
        """
        self.proxy_type = proxy_type

        # 优先使用显式指定的路径，未指定则根据 proxy_type 寻找预设路径
        if proxy_file_path is not None:
            self.proxy_file_path = Path(proxy_file_path)
        else:
            self.proxy_file_path = self.PROXY_PATH_MAP.get(
                proxy_type, self.PROXY_PATH_MAP["cn"]
            )

        self.proxies_list = []
        self.current_proxy_index = 0
        self.load_proxies()

        # 1. 切换为安全的 HTTPS 协议
        self.__url_list = "http://push2.eastmoney.com/api/qt/clist/get"

        # 2. 精简 Headers
        self.headers = {
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        # 3. 内存缓存 Cookie 基础模板与请求计数器
        self.cookie_path = FINANCE_ROOT / "utility" / "eastmoney_cookie.json"
        self._cookie_base = self.parse_cookie_string()
        self._counter = itertools.count(start=1)

        # 4. 东财通用固定 ut Token
        self.COMMON_UT = "fa5fd1943c7b386f172d6893dbfba10b"

    @classmethod
    def create_overseas_manager(
        cls, proxy_file_path: Optional[Path] = None
    ) -> "ProxyManager":
        """快捷工厂方法：创建一个专门管理海外代理的 ProxyManager 实例"""
        return cls(proxy_file_path=proxy_file_path, proxy_type="overseas")

    def parse_cookie_string(self) -> Dict[str, str]:
        """读取 JSON 格式的 cookie 文件"""
        if not self.cookie_path.exists():
            logger.warning(
                f"Cookie 文件不存在: {self.cookie_path}，使用默认兜底配置"
            )
            return self.DEFAULT_COOKIES.copy()

        try:
            with open(self.cookie_path, "r", encoding="utf-8") as f:
                cookie_data = json.load(f)
                logger.info("已成功加载 JSON cookie 信息")
                return cookie_data
        except Exception as e:
            logger.error(
                f"读取 Cookie 文件异常: {e}，使用兜底配置", exc_info=True
            )
            return self.DEFAULT_COOKIES.copy()

    def get_dynamic_cookies(self) -> Dict[str, str]:
        """动态生成包含最新时间戳与计数的 Cookie 字典"""
        cookies = self._cookie_base.copy()
        now = datetime.datetime.now()
        timestamp_str = (
            now.strftime("%Y%m%d%H%M%S") + f"{now.microsecond // 1000:03d}"
        )

        orig_psi = cookies.get("st_psi", "")
        suffix = (
            orig_psi[orig_psi.find("-") :]
            if "-" in orig_psi
            else self.DEFAULT_PSI_SUFFIX
        )
        cookies["st_psi"] = f"{timestamp_str}{suffix}"
        cookies["st_sn"] = str(next(self._counter))

        if cookies.get("st_asi") == "delete":
            cookies.pop("st_asi", None)

        return cookies

    @staticmethod
    def generate_ut_param() -> str:
        """生成唯一的 ut 追踪参数（标准 32 位 MD5 格式）"""
        return hashlib.md5(uuid.uuid4().bytes).hexdigest()

    def load_proxies(self):
        """从文件加载代理列表"""
        if os.path.exists(self.proxy_file_path):
            with open(self.proxy_file_path, "r", encoding="utf-8") as f:
                self.proxies_list = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                ]
            print(
                f"[{self.proxy_type.upper()} 代理模式] 已加载 {len(self.proxies_list)} 个代理 ({self.proxy_file_path})"
            )
            if len(self.proxies_list) > 0:
                print(f"代理列表示例：{self.proxies_list[:3]}...")
        else:
            print(
                f"[{self.proxy_type.upper()} 代理模式] 代理文件 {self.proxy_file_path} 不存在"
            )
            self.proxies_list = []

    def get_next_proxy(self) -> Optional[Dict[str, str]]:
        """获取下一个代理"""
        if not self.proxies_list:
            return None

        proxy_str = self.proxies_list[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(
            self.proxies_list
        )

        return {
            "http": proxy_str,
            "https": proxy_str,
        }

    def validate_proxy(self, proxy_dict: Dict[str, str]) -> Tuple[bool, Any]:
        """国内代理默认校验函数（东财 API 校验）"""
        params = {
            "pn": "1",
            "pz": "100",
            "po": "1",
            "np": "1",
            "ut": self.COMMON_UT,
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": "m:0 t:6,m:0 t:80",
            "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
            "_": str(int(time.time() * 1000)),
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

            if response.status_code != 200:
                return False, f"HTTP状态码异常: {response.status_code}"

            text = response.text.strip()
            if text.startswith("<"):
                return (
                    False,
                    f"返回了HTML而非JSON(可能是代理劫持或重定向): {text[:50]}",
                )

            res = response.json()
            total_page_no = math.ceil(
                res.get("data", {}).get("total", 0) / 100
            )

            return (
                (True, total_page_no)
                if total_page_no > 0
                else (False, total_page_no)
            )

        except Exception as e:
            return False, f"请求/解析失败: {str(e)}"

    def validate_overseas_proxy_yfinance(
            self, proxy_dict: Dict[str, str], test_symbol: str = "MSFT"
        ) -> Tuple[bool, Any]:
        """专门针对海外代理的校验函数（具备 NoneType 防崩与深度校验）"""
        import yfinance as yf

        proxy_str = (
            proxy_dict.get("socks5")
            or proxy_dict.get("https")
            or proxy_dict.get("http")
        )

        if not proxy_str:
            return False, "代理字典中未找到有效的 URL (http/https/socks5)"
        os.environ.setdefault("CURL_CA_BUNDLE", "")
        os.environ.setdefault("SSL_CERT_FILE", "")    
        os.environ["HTTP_PROXY"] = proxy_str
        os.environ["HTTPS_PROXY"] = proxy_str

        try:
            ticker = yf.Ticker(test_symbol)

            # 使用 fast_info 代替 info，响应更快且更不易触发 yfinance 底层 JSON 提取 Bug
            fast_info = ticker.fast_info
            
            # 安全判断：提取最新价格或市值
            last_price = getattr(fast_info, "last_price", None)

            if last_price is not None and not math.isnan(last_price):
                return (
                    True,
                    f"yfinance 验证成功: [{test_symbol}] 当前最新价: {last_price:.2f}",
                )
            else:
                return (
                    False,
                    f"yfinance 响应成功但未拿到有效行情 (可能是代理被 Yahoo 隐式拦截)",
                )

        except TypeError as te:
            # 专门捕获 'NoneType' object is not subscriptable 等 yfinance 解析空数据的异常
            return False, f"Yahoo 返回数据为空/格式被拦截 (yfinance 解析失败: {te})"
        except Exception as e:
            return False, f"yfinance 请求异常: {str(e)}"
        finally:
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)

    def test_proxy(
        self, proxy_dict: Dict[str, str], test_function=None
    ) -> bool:
        """测试单个代理是否可用"""
        if test_function is None:
            # 根据 proxy_type 选择默认的验证函数
            test_function = (
                self.validate_overseas_proxy_yfinance
                if self.proxy_type == "overseas"
                else self.validate_proxy
            )

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

    def get_working_proxy(
        self, max_retries=3, enable_proxy=True, test_function=None
    ) -> Optional[Dict[str, str]]:
        """获取一个当前可用的代理字典"""
        if not self.proxies_list or not enable_proxy:
            return None

        total = len(self.proxies_list)
        total_tests = max_retries * total
        start_index = self.current_proxy_index
        tested = 0

        for _ in range(total_tests):
            proxy = self.get_next_proxy()
            tested += 1

            current_idx = (start_index + tested - 1) % total
            round_num = (tested - 1) // total + 1
            if round_num > max_retries:
                break

            print(
                f"[{self.proxy_type.upper()}测试] 第{round_num}轮 {current_idx + 1}/{total} - 测试代理：{proxy}"
            )

            if self.test_proxy(proxy, test_function=test_function):
                print(f"✅ 代理可用：{proxy}")
                return proxy

        print(
            f"[{self.proxy_type.upper()}] 所有代理测试失败（已达最大重试轮数）"
        )
        return None