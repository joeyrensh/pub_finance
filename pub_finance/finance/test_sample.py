import time
import requests
import re
import json
import pandas as pd
from datetime import datetime
import os
from utility.fileinfo import FileInfo
import csv

url = "https://quote.eastmoney.com/center/api/qqzq.js?"
res = requests.get(url).text

lines = res.strip().split("\n")
data = []
for line in lines:
    fields = line.split(",")
    data.append(fields)

header = data[1]
filtered_rows = []
for row in data[2:]:
    if len(row) > 2:  # 确保parts列表至少有两个元素
        code = row[1]
        if code == "CN10Y_B":
            code = row[1]
            name = row[2]
            date = row[3]
            new = row[5]

            # 将这些字段添加到一个新的列表中，准备写入CSV文件
            filtered_rows.append([code, name, date, new])


output_filename = FileInfo("20241216", "cn").get_file_path_gz
with open(output_filename, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    # 可选：写入表头
    writer.writerow(["code", "name", "date", "new"])
    # 写入数据行
    writer.writerows(filtered_rows)
