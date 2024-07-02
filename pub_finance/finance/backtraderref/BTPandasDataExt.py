#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import backtrader as bt


class BTPandasDataExt(bt.feeds.PandasData):
    # Add a 'pe' line to the inherited ones from the base class
    lines = ("market",)

    # openinterest in GenericCSVData has index 7 ... add 1
    # add the parameter to the parameters inherited from the base class
    params = (("market", -1),)
