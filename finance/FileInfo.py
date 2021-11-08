#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os
import re

""" 新浪财经对应的文件方法 """

class FileInfo:

    def __init__(self, trade_date):
        self.file_name_day = './usstockinfo/SinaTicker_' \
            + trade_date + '.csv'
        self.file_name_mi = './usstockinfo/SinaTicker5Min_' \
            + trade_date + '.csv'
        self.file_name_rt = './usstockinfo/SinaTickerRealTime_' \
            + trade_date + '.csv'
        self.file_name_sta = './usstrategy/UsStrategy_' + trade_date + '.csv'
        self.files_path = './usstockinfo/'
        self.trade_date = trade_date

    """ 返回新浪财经日数据文件路径 """
    @property
    def get_file_name_day(self):
        return self.file_name_day

    """ 返回新浪财经5分钟数据文件路径 """
    @property
    def get_file_name_mi(self):
        return self.file_name_mi

    """ 返回新浪财经实时数据文件路径 """
    @property
    def get_file_name_rt(self):
        return self.file_name_rt

    """ 返回新浪财经策略数据文件路径 """
    @property
    def get_file_name_sta(self):
        return self.file_name_sta

    """ 返回新浪财经数据文件路径，用来查找数据文件列表 """
    @property
    def get_files_path(self):
        return self.files_path

    """ 返回新浪财经日数据文件列表，返回List """
    @property
    def get_files_day_list(self):
        path_list = os.listdir(self.files_path)
        file_list = []
        for file in path_list:
            """ 返回小于等于当前交易日期的文件列表 """
            if re.search('SinaTicker_', file) and str(file)\
                    .replace('SinaTicker_', '')\
                    .replace('.csv', '') <= self.trade_date:
                file_list.append(self.files_path + file)
        file_list.sort()
        return file_list
