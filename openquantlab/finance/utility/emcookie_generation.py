import json
import time
from playwright.sync_api import sync_playwright
from finance import FINANCE_ROOT


class CookieGeneration(object):
    def get_cookies_with_playwright(self, url):
        print("🚀 启动轻量化 Playwright...")
        cookies_dict = {}

        with sync_playwright() as p:
            # 极限省内存参数配置
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-software-rasterizer",  # 禁用软件渲染器
                    "--disable-extensions",  # 禁用扩展
                    "--disable-background-networking",  # 禁用后台网络交互
                    "--disable-default-apps",
                    "--disable-sync",  # 禁用同步
                    "--disable-translate",  # 禁用翻译
                    "--hide-scrollbars",  # 隐藏滚动条减少绘制开销
                    "--metrics-recording-only",
                    "--mute-audio",  # 静音
                    "--no-first-run",
                    "--safebrowsing-disable-auto-update",
                    "--single-process",  # 核心：单进程模式（极大降低多进程 RAM 消耗）
                ],
            )

            context = browser.new_context(
                viewport={"width": 800, "height": 600},  # 降低分辨率，减少渲染内存占用
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            )

            # 阻止静态图片和 CSS 加载（极耗内存），仅保留 JS/HTML 用于生成 Cookie
            page = context.new_page()
            page.route(
                "**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,mp4}",
                lambda route: route.abort(),
            )

            # 注入隐藏痕迹逻辑
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            try:
                print(f"🌐 目标URL: {url}")
                # 仅等待 DOM 加载
                page.goto(url, wait_until="domcontentloaded", timeout=20000)

                time.sleep(3)  # 等待 JS 写入 Cookie

                # 提取 Cookie
                cookies = context.cookies()
                for c in cookies:
                    cookies_dict[c["name"]] = c["value"]

                if cookies_dict:
                    print(f"✅ 成功获取到 {len(cookies_dict)} 个 Cookie")
                    json_path = FINANCE_ROOT / "utility/eastmoney_cookie.json"
                    json_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(cookies_dict, f, ensure_ascii=False, indent=2)

                return cookies_dict

            except Exception as e:
                print(f"❌ 发生错误: {e}")
                return {}

            finally:
                browser.close()
                print("🔚 浏览器已关闭")

    def generate_em_cookies(self):
        target_url = "https://quote.eastmoney.com/center/gridlist.html#hs_a_board"
        return self.get_cookies_with_playwright(target_url)
