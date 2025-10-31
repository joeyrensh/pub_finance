import json
import os
import requests
import math
from datetime import datetime
import pandas as pd


class ProxyManager:
    def __init__(self, proxy_file_path="./utility/proxy.txt"):
        self.proxy_file_path = proxy_file_path
        self.proxies_list = []
        self.current_proxy_index = 0
        self.load_proxies()
        self.__url_list = "http://push2.eastmoney.com/api/qt/clist/get"
        self.headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        self.cookie_str = self.parse_cookie_string()

    def parse_cookie_string(self):
        # 读取 JSON 格式的 cookie 文件
        with open("./utility/eastmoney_cookie.json", "r", encoding="utf-8") as f:
            cookie_data = json.load(f)
            print("已加载 JSON cookie 信息")
        # 直接返回解析后的字典
        return cookie_data

    def load_proxies(self):
        """从文件加载代理列表"""
        if os.path.exists(self.proxy_file_path):
            with open(self.proxy_file_path, "r", encoding="utf-8") as f:
                self.proxies_list = [line.strip() for line in f if line.strip()]
            print(f"已加载 {len(self.proxies_list)} 个代理")
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

        # 转换为requests需要的格式
        proxy_dict = {"http": f"http://{proxy_str}", "https": f"http://{proxy_str}"}

        return proxy_dict

    # 在您的类中添加以下方法
    def validate_proxy(self, proxy_dict):
        """使用指定代理获取总页数"""
        params = {
            "pn": "6",
            "pz": "100",
            "po": "1",
            "np": "1",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": "m:0",
            "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
        }
        try:
            res = requests.get(
                self.__url_list,
                params=params,
                proxies=proxy_dict,
                headers=self.headers,
                cookies=self.cookie_str,
                timeout=10,  # 添加超时设置
            ).json()

            total_page_no = math.ceil(res["data"]["total"] / self.pz)
            if total_page_no > 0:
                return True
            else:
                return False
        except Exception:
            return False

    def test_proxy(self, proxy_dict, test_function):
        """测试代理是否可用"""
        try:
            # 使用测试函数检查代理是否可用
            result = test_function(proxy_dict)
            if result:
                return True
            else:
                print(f"代理测试失败")
                return False
        except Exception as e:
            print(f"代理测试失败: {e}")
            return False

    def get_working_proxy(self, max_retries=10):
        """获取一个可用的代理"""
        if not self.proxies_list:
            return None

        for _ in range(max_retries):
            proxy = self.get_next_proxy()
            print(f"测试代理: {proxy}")

            success = self.test_proxy(proxy, self.validate_proxy)
            if success:
                print(f"代理可用: {proxy}")
                return proxy

        print("所有代理测试失败")
        return None
