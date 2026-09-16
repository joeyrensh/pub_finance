from pathlib import Path
import sys
import time
import functools

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.toolkit import ToolKit
import time
import random
import os
import glob
import pandas as pd
import akshare as ak

stock_zh_a_daily_df = ak.stock_zh_a_daily(
    symbol="sh603181",
    start_date="2026-09-01",
    end_date="2026-09-16",
    adjust="",
)
print(stock_zh_a_daily_df)