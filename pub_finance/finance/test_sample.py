#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import argparse
import functools
import gc
import multiprocessing
from multiprocessing import Queue
from pathlib import Path
import sys
import time
import traceback
import progressbar

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.cncrawler.ak_incre_crawler_em import AKCNWebCrawler
from finance.utility.backtrader_exec import BacktraderExec
from finance.utility.em_stock_uti_fqt import EMWebCrawlerUti
from finance.utility.stock_analysis_simplify import StockProposal
from finance.utility.toolkit import ToolKit

em = EMWebCrawlerUti()
em.get_daily_gz_info("cn", '20260923')