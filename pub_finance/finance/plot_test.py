import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px
import pandas as pd


# 创建字典，键对应元组
my_dict = {"key1": ("value1",), "key2": ("value3", "value4")}

# 存取值
print(my_dict["key1"])  # 输出: ('value1', 'value2')
print(my_dict["key1"][0])  # 输出: 'value1'
print(my_dict["key1"][1])  # 输出: 'value2'
