import akshare as ak
import pandas as pd
import requests
import time
import random
import logging
from requests.exceptions import ProxyError, ConnectionError, Timeout, HTTPError
import csv
import os


info = ak.stock_zh_a_spot_em()
print(info)
