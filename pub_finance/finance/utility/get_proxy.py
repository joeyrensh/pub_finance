import json
import os
import requests
import math
from datetime import datetime
import pandas as pd
import random
import hashlib
import time


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

    def randomize_cookie_string(
        self, cookie_dict, keys_to_randomize=None, key_lengths=None
    ):
        """
        增强版 cookie 字符串解析函数，支持为特定键生成随机值

        Args:
            cookie_str (str): cookie 字符串
            keys_to_randomize (list): 需要随机化的 cookie 键列表，默认为 ['nid', 'qgqp_b_id']
            key_lengths (dict): 特定键的随机值长度，例如 {'nid': 32, 'qgqp_b_id': 32}

        Returns:
            dict: 解析后的 cookie 字典，特定键已被随机化
        """

        def generate_random_hex(length=32):
            """生成指定长度的随机十六进制字符串"""
            return "".join(random.choices("0123456789abcdef", k=length))

        # 设置默认需要随机化的键
        keys_to_randomize = ["nid"]

        # 设置默认键长度
        key_lengths = {"nid": 32}

        # 对特定键生成随机值
        for key in keys_to_randomize:
            if key in cookie_dict:
                length = key_lengths.get(key, 32)  # 默认长度32
                cookie_dict[key] = generate_random_hex(length)
        # print(f"✅ 解析并随机化Cookie: {cookie_dict}")
        return cookie_dict

    def generate_ut_param(self):
        """生成基于中国地区随机IP的32位十六进制格式ut参数"""

        # 生成随机中国IP地址
        def generate_china_ip():
            # 中国IP地址的主要A类、B类网络号
            china_networks = [
                (58, random.randint(0, 255)),  # 58.x.x.x - 中国电信
                (59, random.randint(0, 255)),  # 59.x.x.x - 中国电信
                (60, random.randint(0, 255)),  # 60.x.x.x - 中国联通
                (61, random.randint(0, 255)),  # 61.x.x.x - 中国电信
                (106, random.randint(0, 255)),  # 106.x.x.x - 中国教育网
                (110, random.randint(0, 255)),  # 110.x.x.x - 中国电信
                (111, random.randint(0, 255)),  # 111.x.x.x - 中国联通
                (112, random.randint(0, 255)),  # 112.x.x.x - 中国移动
                (113, random.randint(0, 255)),  # 113.x.x.x - 中国电信
                (114, random.randint(0, 255)),  # 114.x.x.x - 中国电信
                (115, random.randint(0, 255)),  # 115.x.x.x - 中国电信
                (116, random.randint(0, 255)),  # 116.x.x.x - 中国移动
                (117, random.randint(0, 255)),  # 117.x.x.x - 中国移动
                (118, random.randint(0, 255)),  # 118.x.x.x - 中国电信
                (119, random.randint(0, 255)),  # 119.x.x.x - 中国电信
                (120, random.randint(0, 255)),  # 120.x.x.x - 中国联通
                (121, random.randint(0, 255)),  # 121.x.x.x - 中国联通
                (122, random.randint(0, 255)),  # 122.x.x.x - 中国电信
                (123, random.randint(0, 255)),  # 123.x.x.x - 中国联通
                (124, random.randint(0, 255)),  # 124.x.x.x - 中国联通
                (125, random.randint(0, 255)),  # 125.x.x.x - 中国电信
                (171, random.randint(0, 255)),  # 171.x.x.x - 中国电信
                (175, random.randint(0, 255)),  # 175.x.x.x - 中国电信
                (180, random.randint(0, 255)),  # 180.x.x.x - 中国移动
                (182, random.randint(0, 255)),  # 182.x.x.x - 中国电信
                (183, random.randint(0, 255)),  # 183.x.x.x - 中国电信
                (202, random.randint(0, 255)),  # 202.x.x.x - 中国教育和科研网
                (210, random.randint(0, 255)),  # 210.x.x.x - 中国教育和科研网
                (211, random.randint(0, 255)),  # 211.x.x.x - 中国教育和科研网
                (218, random.randint(0, 255)),  # 218.x.x.x - 中国联通
                (219, random.randint(0, 255)),  # 219.x.x.x - 中国联通
                (220, random.randint(0, 255)),  # 220.x.x.x - 中国电信
                (221, random.randint(0, 255)),  # 221.x.x.x - 中国联通
                (222, random.randint(0, 255)),  # 222.x.x.x - 中国电信
                (223, random.randint(0, 255)),  # 223.x.x.x - 中国移动
            ]

            network = random.choice(china_networks)
            ip_parts = [
                network[0],
                network[1],
                random.randint(1, 254),
                random.randint(1, 254),
            ]
            return ".".join(map(str, ip_parts))

        # 生成随机IP并用于ut参数
        random_ip = generate_china_ip()
        timestamp = int(time.time() * 1000)
        random_num = random.randint(1000000000, 9999999999)

        # 将IP地址加入基础字符串
        base_str = f"{timestamp}{random_num}{random_ip}"

        # 使用MD5生成32位十六进制字符串
        ut_hash = hashlib.md5(base_str.encode()).hexdigest()
        return ut_hash

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
                print(f"代理列表示例: {self.proxies_list[:3]}...")  # 显示前3个代理
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
            "pn": "1",
            "pz": "100",
            "po": "1",
            "np": "1",
            "ut": self.generate_ut_param(),
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": "m:0 t:6,m:0 t:80",
            "fields": "f2,f5,f9,f12,f14,f15,f16,f17,f20",
        }
        """
        his info test
        """
        # his_url = "http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.600649&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20251101&end=20251130&smplmt=755&lmt=1000000"
        # try:
        #     res = requests.get(
        #         his_url,
        #         proxies=proxy_dict,
        #         headers=self.headers,
        #         cookies=self.cookie_str,
        #         timeout=5,  # 添加超时设置
        #     ).json()
        #     klines = res.get("data", {}).get("klines", [])
        #     if klines:
        #         print(f"代理测试成功，获取到 {len(klines)} 条K线数据")
        #         return True, len(klines)
        #     else:
        #         return False, 0
        # except Exception as e:
        #     return False, e
        """
        daily info test
        """
        try:
            res = requests.get(
                self.__url_list,
                params=params,
                proxies=proxy_dict,
                headers=self.headers,
                cookies=self.cookie_str,
                timeout=5,  # 添加超时设置
            ).json()
            total_page_no = math.ceil(res["data"]["total"] / 100)
            if total_page_no > 0:
                return True, total_page_no
            else:
                return False, total_page_no
        except Exception as e:
            return False, e

    def test_proxy(self, proxy_dict, test_function):
        """测试代理是否可用"""
        try:
            # 使用测试函数检查代理是否可用
            result, content = test_function(proxy_dict)
            if result:
                return True
            else:
                print(f"❌ 代理测试失败: {proxy_dict}")
                return False
        except Exception as e:
            print(f"❌ 代理测试失败: {e}")
            return False

    def get_working_proxy(self, max_retries=3):
        """获取一个可用的代理"""
        if not self.proxies_list:
            return None
        target_retries = max(max_retries, len(self.proxies_list))

        for _ in range(target_retries):
            proxy = self.get_next_proxy()
            # 显示进度
            progress = _ / target_retries * 100
            print(
                f"测试进度: {_+1}/{target_retries} ({progress:.0f}%) - 测试代理: {proxy}"
            )

            success = self.test_proxy(proxy, self.validate_proxy)
            if success:
                print(f"✅ 代理可用: {proxy}")
                return proxy

        print("所有代理测试失败")
        return None

    def save_working_proxies_to_file(self, output_file="./utility/working_proxies.txt"):
        r"""
        测试所有代理并将有效代理保存到指定文件
        每次调用都会重新生成文件
        # 站大爷IP格式化命令：
        # IP和端口行格式化
        # 1:  %s/^\([0-9.]\+\)\s\+\(\d\+\).*$/\1:\2/
        # 2:  :%s/\v.*?(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\s+(\d+).*/\1.\2.\3.\4:\5/
        # 去除非IP和端口行
        # 1:  v/\v^\d+\.\d+\.\d+\.\d+:\d+$/d
        # 2:  :g!/.*:.*/d
        # 追加到proxy.txt尾部，添加一行空行，用于区分新增IP，运行如下命令
        # grep -vxFf proxy.txt new_proxy.txt >> proxy.txt
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
            proxy_dict = {"http": f"http://{proxy_str}", "https": f"http://{proxy_str}"}

            # 显示进度
            progress = (i + 1) / total_proxies * 100
            print(
                f"测试进度: {i+1}/{total_proxies} ({progress:.0f}%) - 测试代理: {proxy_str}"
            )

            # 测试代理
            success = self.test_proxy(proxy_dict, self.validate_proxy)
            if success:
                working_proxies.append(proxy_str)
                print(f"✅ 代理可用: {proxy_str}")
            else:
                print(f"❌ 代理不可用: {proxy_str}")

        # 保存有效代理到文件
        if working_proxies:
            # 确保目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # 写入文件（覆盖模式）
            with open(output_file, "w", encoding="utf-8") as f:
                for proxy in working_proxies:
                    f.write(f"{proxy}\n")

            print(f"✅ 已保存 {len(working_proxies)} 个有效代理到: {output_file}")
            print(f"有效代理列表: {working_proxies}")
        else:
            print("❌ 没有找到任何有效代理")
