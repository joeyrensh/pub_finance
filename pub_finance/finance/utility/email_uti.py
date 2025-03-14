#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import parseaddr, formataddr
from email.header import Header
import base64
import csv
import os
from email.mime.base import MIMEBase


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
                    # 根据文件扩展名确定MIME类型
                    ext = image_path_s.split(".")[-1].lower()
                    with open(image_path_s, "rb") as f:
                        file_data = f.read()

                    if ext == "svg":
                        # 处理SVG矢量图
                        img = MIMEImage(
                            file_data,
                            _subtype="svg+xml",
                            # _encoder=encoders.encode_7or8bit,
                        )
                        img.add_header("Content-Type", "image/svg+xml")
                    else:
                        # 处理常规图片格式
                        img = MIMEImage(file_data)

                    # 统一添加资源标识
                    img.add_header("Content-ID", f"<image{i}>")
                    img.add_header(
                        "Content-Disposition",
                        "inline",
                        filename=os.path.basename(image_path_s),
                    )
                    self.msg.attach(img)

                except IOError:
                    print(f"无法打开图片文件 {image_path_s}，请检查路径")
                except Exception as e:
                    print(f"处理图片时发生意外错误：{str(e)}")
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
