import os
import time
import requests
import json
import re
import pandas as pd
from datetime import datetime
from lxml import html
from playwright.async_api import async_playwright
import asyncio


def get_us_stock_list():
    current_timestamp = int(time.mktime(datetime.now().timetuple()))
    stock_list = []
    url = (
        "https://23.push2.eastmoney.com/api/qt/clist/get?cb=jQuery"
        "&pn={page}&pz=100000&po=1&np=1&ut=&fltt=2&invt=2&fid=f3&fs=m:{mkt_code}&fields=f2,f3,f4,f5,f6,f7,f12,f14,f15,f16,f17,f18,f20,f21&_={unix_time}"
    )
    for mkt_code in ["105", "106", "107"]:
        for page in range(1, 100):
            url_re = url.format(
                page=page, mkt_code=mkt_code, unix_time=current_timestamp
            )
            res = requests.get(url_re).text
            res_p = re.sub(r"\].*", "]", re.sub(r".*:\[", "[", res, 1), 1)
            try:
                json_object = json.loads(res_p)
            except ValueError:
                break
            for i in json_object:
                if any(
                    i[field] == "-"
                    for field in ["f12", "f17", "f2", "f15", "f16", "f5", "f20", "f21"]
                ):
                    continue
                stock_list.append({"symbol": i["f12"], "mkt_code": mkt_code})
    return stock_list


async def get_comp_info(page, symbol, url):
    try:
        print(url)
        await page.goto(url, wait_until="networkidle")
        page_content = await page.content()
        tree = html.fromstring(page_content)
        div = tree.xpath(
            '//body[@class="ame"]/div[@class="main_content"]/div[@class="m_box"]/div[@class="bd"]/table[@class="companyinfo-tab"]/tbody/tr[2]/td[2]'
        )
        if div:
            value = div[0].xpath("string(.)").split("：", 1)[1].replace(" ", "")
        else:
            value = "N/A"
        return {"symbol": symbol, "industry": value}
    finally:
        await page.close()  # 确保页面在使用完后关闭


async def fetch_data(browser, tick_list):
    tasks = []
    url_template = "https://stockpage.10jqka.com.cn/{symbol}/company/"
    for stock in tick_list:
        symbol = stock["symbol"].upper()
        url = url_template.format(symbol=symbol)
        page = await browser.new_page()
        tasks.append(get_comp_info(page, symbol, url))
    results = await asyncio.gather(*tasks)
    return results


async def main(file_path):
    if os.path.isfile(file_path):
        os.remove(file_path)
    tick_list = get_us_stock_list()
    batch_size = 10
    all_results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for idx in range(0, len(tick_list), batch_size):
            batch = tick_list[idx : idx + batch_size]
            results = await fetch_data(browser, batch)
            all_results.extend(results)
            df = pd.DataFrame(results)
            df.to_csv(file_path, mode="a", index=True, header=(idx == 0))
        # if all_results:
        #     df = pd.DataFrame(all_results)
        #     df.to_csv(file_path, mode="a", index=True, header=False)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main("./usstockinfo/test.csv"))
