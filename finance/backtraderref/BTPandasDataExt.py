#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from os import close
import backtrader as bt
from utility.ToolKit import ToolKit
from datetime import datetime
import pandas as pd
from datetime import datetime, timedelta
from utility.MyEmail import MyEmail
import seaborn as sns
from utility.FileInfo import FileInfo


class BTPandasDataExt(bt.feeds.PandasData):
    # Add a 'pe' line to the inherited ones from the base class
    lines = ("market",)

    # openinterest in GenericCSVData has index 7 ... add 1
    # add the parameter to the parameters inherited from the base class
    params = (("market", -1),)
