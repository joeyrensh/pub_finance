#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import threading


""" MyThread.py线程类 """


class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args
        self.result = None

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        """等待线程执行完毕"""
        threading.Thread.join(self)
        try:
            return self.result
        except Exception:
            return None
