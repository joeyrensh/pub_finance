from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import tempfile
import chromedriver_autoinstaller
from selenium.webdriver.chrome.service import Service
from finance.paths import FINANCE_ROOT


class CookieGeneration(object):
    def get_cookies_with_selenium(self, url, headless=True):
        """
        使用Selenium模拟浏览器访问并获取所有Cookie

        Args:
            url (str): 要访问的URL
            headless (bool): 是否使用无头模式（不显示浏览器界面）

        Returns:
            dict: 包含所有Cookie的字典

        安装相关package:
            sudo apt install chromium-chromedriver
            pip install selenium
            pip install fake-useragent
        """
        # 配置Chrome选项
        # chromedriver_autoinstaller.install()
        print("🔍 检查并安装chromedriver...")

        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/chromium-browser"

        # 为每次运行创建一个临时的用户数据目录
        user_data_dir = tempfile.mkdtemp()
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
        chrome_options.add_argument("--headless")  # 无头模式
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        print("🔧 配置Chrome选项完成")

        # from fake_useragent import UserAgent
        # 创建一个UserAgent对象
        # ua = UserAgent()
        # 生成一个随机User-Agent字符串
        # random_user_agent = ua.random
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        )
        # chrome_options.add_argument(f"--user-agent={random_user_agent}")

        # 启动浏览器
        service = Service(executable_path="/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=chrome_options)

        try:
            print("🚀 启动浏览器访问页面...")
            print(f"🌐 目标URL: {url}")

            # 访问页面
            driver.get(url)

            # 等待页面加载完成
            print("⏳ 等待页面加载...")
            wait = WebDriverWait(driver, 10)

            # 等待直到页面中有特定元素加载完成（例如表格或特定标签）
            try:
                # 尝试等待页面中的某个关键元素出现
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                print("✅ 页面关键元素加载完成")
            except Exception:
                # 如果找不到特定元素，至少等待几秒确保JavaScript执行完成
                print("⚠️ 未找到特定元素，等待基础页面加载")
                time.sleep(5)

            # 获取所有Cookie
            cookies = driver.get_cookies()

            # 将Cookie列表转换为字典格式
            cookies_dict = {}
            for cookie in cookies:
                cookies_dict[cookie["name"]] = cookie["value"]

            # 打印结果
            print("-" * 50)
            if cookies_dict:
                print("✅ 成功获取到Cookie!")
                print(f"📊 共获取到 {len(cookies_dict)} 个Cookie:")

                for name, value in cookies_dict.items():
                    print(f"  🍪 {name}: {value}")

                # 将Cookie保存为JSON文件（可选）
                with open(
                    FINANCE_ROOT / "utility/eastmoney_cookie.json",
                    "w",
                    encoding="utf-8",
                ) as f:
                    json.dump(cookies_dict, f, ensure_ascii=False, indent=2)
                print("💾 Cookie已保存到 eastmoney_cookie.json")
            else:
                print("❌ 未获取到任何Cookie")

            return cookies_dict

        except Exception as e:
            print(f"❌ 发生错误: {e}")
            return {}

        finally:
            # 关闭浏览器
            driver.quit()
            print("🔚 浏览器已关闭")

    def print_cookie_header(self, cookies_dict):
        """
        将Cookie字典转换为请求头格式并打印

        Args:
            cookies_dict (dict): Cookie字典
        """
        if cookies_dict:
            cookie_header = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
            # print("\n📋 Cookie请求头格式:")
            # print(cookie_header)
            return cookie_header
        return ""

    def generate_em_cookies(self):
        target_url = "https://quote.eastmoney.com/center/gridlist.html#hs_a_board"

        # 获取Cookie（设置为False可以显示浏览器界面）
        cookies = self.get_cookies_with_selenium(target_url, headless=True)

        # 打印可用于requests的Cookie头格式
        cookie_header = self.print_cookie_header(cookies)

        # 新增：将Cookie字符串直接导出到文本文件
        # if cookie_header:
        #     # 直接将Cookie字符串保存到文本文件
        #     with open(FINANCE_ROOT / "utility/cookie.txt", "w", encoding="utf-8") as f:
        #         f.write(cookie_header)
        #     print("✅ Cookie字符串已成功导出到 cookie.txt 文件")
