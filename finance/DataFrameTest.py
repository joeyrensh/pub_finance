#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
from utility.FileInfo import FileInfo
from tabulate import tabulate
from utility.ToolKit import ToolKit
from utility.MyEmail import MyEmail
from datetime import datetime, timedelta
import pandas as pd
import seaborn as sns
from backtraderref.BTStrategy import BTStrategy
import backtrader as bt
from utility.TickerInfo import TickerInfo
from backtraderref.BTPandasDataExt import BTPandasDataExt
import numpy as np
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import sys
from utility.StockProposal import StockProposal

# 主程序入口
if __name__ == "__main__":
    _mail_user = os.environ.get("email_addr")
    _mail_password = os.environ.get("email_key")

    send_from = _mail_user
    to = _mail_user
    subject = 'this is a test'
    message = 'this is body text'
    msg = MIMEMultipart('mixed')
    msg.attach(MIMEText(message, "html"))
    image = MIMEImage(open('TRdraw.png', 'rb').read())
    msg.attach(image)
    msg["Subject"] = subject
    msg["To"] = to
    msg["From"] = send_from

    smtp_server = smtplib.SMTP_SSL("smtp.163.com", 465)
    smtp_server.ehlo()
    smtp_server.login(_mail_user, _mail_password)
    smtp_server.sendmail(send_from, to, msg.as_string())
    smtp_server.close()
    print("Email sent successfully!")
    """ 发送邮件 """
    # StockProposal("us", '20220927').send_btstrategy_by_email()  
    # print(os.path.abspath(sys.argv[0]))