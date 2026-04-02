from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.utility.get_proxy import ProxyManager

import yfinance as yf

ticker = yf.Ticker("AAPL")
info = ticker.info
print(info)
