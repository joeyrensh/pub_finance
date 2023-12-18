#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from uscrawler.EMUsTickerCategoryCrawler import EMUsTickerCategoryCrawler
from cncrawler.EMCNTickerCategoryCrawler import EMCNTickerCategoryCrawler
from utility.ToolKit import ToolKit
from pathlib import Path
import os

class test:
    def test(self, c=None, **kwargs) -> int:
        self.a = kwargs.get('a', 0)
        self.b = kwargs.get('b', 0)
        print('a:', self.a, 'b:', self.b, 'c:' ,c)

# 主程序入口
if __name__ == "__main__":
    # t = test()
    # t.test(b=2, a=3, c=1)



    # Define file paths
    data_dir = Path("data")
    print(data_dir)
    current_path = os.getcwd()
    print("Current path:", current_path)

