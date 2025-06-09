import time
import random
import requests
import re
import json
import pandas as pd
from datetime import datetime
import os
from utility.fileinfo import FileInfo
import csv
import akshare as ak
import math
from utility.em_stock_uti import EMWebCrawlerUti
from cncrawler.eastmoney_tickercategory_crawler import EMCNTickerCategoryCrawler
from uscrawler.eastmoney_tickercategory_crawler import EMUsTickerCategoryCrawler

em = EMWebCrawlerUti()
# em.get_daily_stock_info("cn", "20250609")
start_date = "20250609"
end_date = "20250609"
file_path = "./usstockinfo/test.csv"
em.get_his_stock_info_list(
    "us",
    start_date,
    end_date,
    file_path,
)
