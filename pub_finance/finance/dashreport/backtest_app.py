#!/usr/bin/env python3
"""回测应用入口 - 端口 8050"""
import sys
import os

# 设置路径
# sys.path.insert(0, "/home/ubuntu/pub_finance")
# os.chdir('/home/ubuntu/pub_finance/finance')

import dash
from dash import dcc, html
from dash.dependencies import Input, Output

# 创建应用
app = dash.Dash(
    __name__,
    title="回测分析",
    assets_folder="/home/ubuntu/pub_finance/finance/dashreport/assets",
    suppress_callback_exceptions=True,
)

# 导入页面布局
from finance.dashreport.pages.backtest_page import BacktestPage

# 创建页面实例并获取布局
backtest_page = BacktestPage(app)
app.layout = backtest_page.create_layout()

if __name__ == "__main__":
    print("=" * 60)
    print("回测分析服务器启动成功！")
    print("访问地址：http://0.0.0.0:8050")
    print("=" * 60)
    app.run(host="0.0.0.0", port=8050, debug=False)
