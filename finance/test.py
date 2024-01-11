from pyspark import StorageLevel
from utility.MyEmail import MyEmail
from utility.FileInfo import FileInfo
import pandas as pd
import seaborn as sns
import os
import sys
from numpy import size
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from matplotlib.pylab import mpl
import matplotlib.font_manager as fm
from utility.MyEmail import MyEmail
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from email.mime.text import MIMEText
import gc

file = FileInfo('20240111', 'cn')
file_name_day = file.get_file_path_latest
file_path_trade = file.get_file_path_trade
spark = SparkSession.builder.master(
    "local").appName("SparkTest").getOrCreate()
df1 = spark.read.csv(file_path_trade,
                     header=None, inferSchema=True)
df1.coalesce(2)
