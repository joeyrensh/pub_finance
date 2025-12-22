from playwright.sync_api import sync_playwright


def dash_to_pdf(
    url: str,
    output_pdf: str = "dash_report.pdf",
    wait_selector: str = "#_dash-app-content",
):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        # 访问 Dash 页面
        page.goto(url, wait_until="networkidle")

        # 等待 Dash 核心节点渲染完成
        page.wait_for_selector(wait_selector, timeout=60_000)

        # 可选：额外等待图表动画完成
        page.wait_for_timeout(1000)

        # 导出 PDF
        page.pdf(
            path=output_pdf,
            format="A4",
            print_background=True,
            margin={
                "top": "20mm",
                "bottom": "20mm",
                "left": "15mm",
                "right": "15mm",
            },
        )

        browser.close()


dash_to_pdf("http://127.0.0.1:8050/dash-financial-report/cn-stock-performance")
