import akshare as ak
import pandas as pd
from uscrawler.ak_incre_crawler import AKUSWebCrawler

ak_daily_crawler = AKUSWebCrawler()
df_stock_daily = ak_daily_crawler.get_us_daily_stock_info_ak("20251020")
