#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import sys


class ToolKit(object):

    def __init__(self, val):
        self.val = val
        print('\n' + val + '开始处理...')

    # 执行进度显示
    def progress_bar(self, var, i):
        print("\r{}处理进度: {}%: ".format(self.val, int(i * 100 / var)), "▋" * (i * 100 // var), end="", flush=True)

