#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import parseaddr, formataddr
from email.header import Header
import os
import base64
import csv


class MyEmail(object):
    """string_to_encode = xxx
    encoded_string = base64.b64encode(string_to_encode.encode("utf-8")).decode("utf-8")
    """

    # _mail_user = base64.b64decode(os.environ.get("email_addr")).decode("utf-8")
    # _mail_password = base64.b64decode(os.environ.get("email_key")).decode("utf-8")

    def __init__(self):

        # 打开文件
        file_path = "./mail.conf"
        with open(file_path, "r", newline="") as file:
            # 创建 CSV 读取器
            reader = csv.reader(file)
            mail_user_list = list()
            mail_pwd = str()
            # 逐行读取文件
            for i, row in enumerate(reader):
                # 处理每一行的列数据
                if i == 0:
                    if len(row) >= 2:
                        mail_user_list.append(row[0])
                        mail_pwd = row[1]
                else:
                    mail_user_list.append(row[0])
        self._mail_password = base64.b64decode(mail_pwd).decode("utf-8")
        self.send_from = mail_user_list[0]
        self.to = mail_user_list
        self.server = "smtp.qq.com"  # smtp.163.com smtp.qq.com
        self.port = 465
        self.msg = MIMEMultipart("related")
        self.msg["MIME-Version"] = "1.1"
        self.msg["To"] = self.send_from
        self.msg["From"] = self.format_addr(
            "Quantitative trading <%s>" % self.send_from
        )

    def send_email(self, subject, message):
        try:
            self.subject = subject
            self.msg["Subject"] = subject
            self.msg.attach(MIMEText(message, "html"))
            smtp_server = smtplib.SMTP_SSL(self.server, self.port)
            smtp_server.ehlo()
            smtp_server.login(self.send_from, self._mail_password)
            smtp_server.sendmail(
                self.send_from,
                self.to,
                self.msg.as_string(),
            )
            smtp_server.close()
            print("Email sent successfully!")
        except Exception as ex:
            print("Something went wrong….", ex)

    def send_email_embedded_image(self, subject, message, image_path):
        try:
            self.subject = subject
            self.msg["Subject"] = subject
            self.msg.attach(MIMEText(message, "html"))
            for i, image_path_s in enumerate(image_path):
                try:
                    image = MIMEImage(open(image_path_s, "rb").read())
                    image.add_header("Content-ID", f"<image{i}>")
                    self.msg.attach(image)
                except IOError:
                    print(
                        f"Unable to open image file {image_path_s}. Please check the path and try again."
                    )
            smtp_server = smtplib.SMTP_SSL(self.server, self.port)
            smtp_server.ehlo()
            smtp_server.login(self.send_from, self._mail_password)
            smtp_server.sendmail(
                self.send_from,
                self.to,
                self.msg.as_string(),
            )
            smtp_server.close()
            print("Email sent successfully!")
        except Exception as ex:
            print("Something went wrong….", ex)

    def format_addr(self, s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, "utf-8").encode(), addr))
