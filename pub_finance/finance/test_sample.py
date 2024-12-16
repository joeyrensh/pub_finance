import time
import requests
import re
import json
import pandas as pd
from datetime import datetime
import os
from utility.fileinfo import FileInfo

url = "https://quote.eastmoney.com/center/api/qqzq.js?"
res = requests.get(url).text


print(res)


# filtered_rows = []
# for row in res[1:]:
#     parts = row.split(",")
#     code = parts[1]
#     if code == "CN10Y_B":
#         filtered_rows.append(row)
# print(filtered_rows)
