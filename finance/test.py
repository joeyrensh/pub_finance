import base64
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
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from email.mime.text import MIMEText
import plotly.io as pio
import pandas as pd
import plotly.graph_objects as go
from utility.TickerInfo import TickerInfo


tk = TickerInfo('20240109', 'us')
print(tk.get_stock_list())
