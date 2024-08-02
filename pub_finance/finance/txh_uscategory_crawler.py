import os
import pandas as pd
from lxml import html
from playwright.async_api import async_playwright
import asyncio
import random
import time
from fake_useragent import UserAgent


async def get_house_main_page(page, url):
    try:
        await page.goto(url, wait_until="networkidle")
        page_content = await page.content()
        print("page_content:", page_content)
        tree = html.fromstring(page_content)
        divs = tree.xpath(
            '//div[@class="content"]/div[@class="leftContent"]/ul[@class="listContent"]/li[@class="clear xiaoquListItem"]'
        )
        if divs:
            for div in divs:
                dict = {}
                # 获取data-id属性的值
                data_id = div.get("data-id")
                # 查找<li>标签下的<img>标签，并获取alt属性的值
                img_tag = div.xpath('.//img[@class="lj-lazy"]')
                if img_tag:
                    alt_text = img_tag[0].get("alt")
                else:
                    continue
                sell_cnt_div = div.xpath(
                    './/div[@class="xiaoquListItemRight"]/div[@class="xiaoquListItemSellCount"]/a/span'
                )
                if sell_cnt_div:
                    sell_cnt = sell_cnt_div[0].xpath("string(.)")
                else:
                    sell_cnt = ""
                district_div = div.xpath(
                    './/div[@class="info"]/div[@class="positionInfo"]/a[@class="district"]'
                )
                if district_div:
                    district = district_div[0].xpath("string(.)")
                else:
                    district = ""

                dict = {
                    "data_id": data_id,
                    "al_text": alt_text,
                    "sell_cnt": sell_cnt,
                    "district": district,
                }
                list.append(dict)
        return list
    finally:
        await page.close()  # 确保页面在使用完后关闭


async def get_house_detail_page(page, url, item):
    try:
        print(url)
        await page.goto(url, wait_until="networkidle")
        page_content = await page.content()
        tree = html.fromstring(page_content)
        unit_price_div = tree.xpath(
            '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquPrice clear"]/div[@class="fl"]/span[@class="xiaoquUnitPrice"]'
        )
        if len(unit_price_div) > 0:
            unit_price = unit_price_div[0].xpath("string(.)")
        else:
            unit_price = ""
        deal_price_div = tree.xpath(
            '//div[@class="m-content"]/div[@class="box-l xiaoquMainContent"]/div[@class="frameDeal"]/div[@class="frameDealList"]/ol[@class="frameDealListItem"]/li[1]/div[@class="frameDealUnitPrice"]'
        )
        if len(deal_price_div) > 0:
            deal_price = deal_price_div[0].xpath("string(.)")
        else:
            deal_price = ""
        deal_date_div = tree.xpath(
            '//div[@class="m-content"]/div[@class="box-l xiaoquMainContent"]/div[@class="frameDeal"]/div[@class="frameDealList"]/ol[@class="frameDealListItem"]/li[1]/div[@class="frameDealDate"]'
        )
        if len(deal_date_div) > 0:
            deal_date = deal_date_div[0].xpath("string(.)")
        else:
            deal_date = ""
        lanlong_div = tree.xpath(
            '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemOneLine"]/div[@class="xiaoquInfoItem outerItem"][2]/span[@class="xiaoquInfoContent outer"]/span[@mendian]'
        )
        if len(lanlong_div) > 0:
            lanlong = lanlong_div[0].get("xiaoqu")
        else:
            lanlong = ""
        age_div = tree.xpath(
            '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][2]/div[@class="xiaoquInfoItem"][2]/span[@class="xiaoquInfoContent"]'
        )
        if len(age_div) > 0:
            age = age_div[0].xpath("string(.)")
        else:
            age = ""
        house_type_div = tree.xpath(
            '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][2]/div[@class="xiaoquInfoItem"][1]/span[@class="xiaoquInfoContent"]'
        )
        if len(house_type_div) > 0:
            house_type = house_type_div[0].xpath("string(.)")
        else:
            house_type = ""
        total_cnt_div = tree.xpath(
            '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][1]/div[@class="xiaoquInfoItem"][2]/span[@class="xiaoquInfoContent"]'
        )
        if len(total_cnt_div) > 0:
            total_cnt = total_cnt_div[0].xpath("string(.)")
        else:
            total_cnt = ""
        structure_div = tree.xpath(
            '//div[@class="xiaoquOverview"]/div[@class="xiaoquDescribe fr"]/div[@class="xiaoquInfo"]/div[@class="xiaoquInfoItemMulty"]/div[@class="xiaoquInfoItemCol"][1]/div[@class="xiaoquInfoItem"][1]/span[@class="xiaoquInfoContent"]'
        )
        if len(structure_div) > 0:
            structure = structure_div[0].xpath("string(.)")
        else:
            structure = ""

        dict = {
            "data_id": item["data_id"],
            "al_text": item["al_text"],
            "sell_cnt": item["sell_cnt"],
            "district": item["district"],
            "unit_price": unit_price,
            "deal_price": deal_price,
            "deal_date": deal_date,
            "lanlong": lanlong,
            "age": age,
            "total_cnt": total_cnt,
            "structure": structure,
            "house_type": house_type,
        }
        return dict
    finally:
        await page.close()  # 确保页面在使用完后关闭


async def fetch_house_list(browser, url, max_page, semaphore):
    tasks = []
    numbers = list(range(1, max_page))
    random.shuffle(numbers)
    for number in numbers:
        time.sleep(random.randint(1, 3))
        url_r = url.format(pgno=number)
        print("level1 url:", url_r)
        async with semaphore:
            page = await browser.new_page()
            tasks.append(get_house_main_page(page, url_r))
            results = await asyncio.gather(*tasks)
    return results


async def fetch_house_info(browser, url, houselist):
    tasks = []
    for item in houselist:
        url = url.format(data_id=item["data_id"])
        page = await browser.new_page()
        tasks.append(get_house_main_page(page, url))
    results = await asyncio.gather(*tasks)
    return results


async def main(file_path):
    if os.path.isfile(file_path):
        os.remove(file_path)
    districts = ["huangpu"]
    max_page = 5
    url = "https://sh.lianjia.com/xiaoqu/{district}/pg{{pgno}}cro21/"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        semaphore = asyncio.Semaphore(3)
        for district in districts:
            url_r = url.format(district=district)
            results = await fetch_house_list(browser, url_r, max_page, semaphore)
            df = pd.DataFrame(results)
            print(df)
        # if all_results:
        #     df = pd.DataFrame(all_results)
        #     df.to_csv(file_path, mode="a", index=True, header=False)
        await browser.close()


# async def main(file_path):
#     if os.path.isfile(file_path):
#         os.remove(file_path)
#     tick_list = get_us_stock_list()
#     batch_size = 10
#     all_results = []
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         for idx in range(0, len(tick_list), batch_size):
#             batch = tick_list[idx : idx + batch_size]
#             results = await fetch_data(browser, batch)
#             all_results.extend(results)
#             df = pd.DataFrame(results)
#             df.to_csv(file_path, mode="a", index=True, header=(idx == 0))
#         # if all_results:
#         #     df = pd.DataFrame(all_results)
#         #     df.to_csv(file_path, mode="a", index=True, header=False)
#         await browser.close()


if __name__ == "__main__":
    asyncio.run(main("./houseinfo/test.csv"))
