#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import parseaddr, formataddr
from email.header import Header
import os


class MyEmail(object):

    _mail_user = os.environ.get("email_addr")
    _mail_password = os.environ.get("email_key")

    def __init__(self):
        self.send_from = self._mail_user
        self.to = self._mail_user
        self.msg = MIMEMultipart("mixed")
        self.msg["To"] = self.to
        self.msg["From"] = self.format_addr(
            "Quantitative trading <%s>" % self._mail_user
        )

    def send_email(self, subject, message):

        try:
            self.subject = subject
            self.msg["Subject"] = subject
            self.msg.attach(MIMEText(message, "html"))
            smtp_server = smtplib.SMTP_SSL("smtp.163.com", 465)
            smtp_server.ehlo()
            smtp_server.login(self._mail_user, self._mail_password)
            smtp_server.sendmail(
                self.send_from,
                [
                    self.to,
                    "24491350@qq.com",
                    "45124142@qq.com",
                    "77538801@qq.com",
                    "854043506@qq.com",
                ],
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
            for image_path_s in image_path:
                image = MIMEImage(open(image_path_s, "rb").read())
                self.msg.attach(image)
            smtp_server = smtplib.SMTP_SSL("smtp.163.com", 465)
            smtp_server.ehlo()
            smtp_server.login(self._mail_user, self._mail_password)
            smtp_server.sendmail(
                self.send_from,
                [
                    self.to,
                    "24491350@qq.com",
                    "45124142@qq.com",
                    "77538801@qq.com",
                    "854043506@qq.com",
                ],
                self.msg.as_string(),
            )
            smtp_server.close()
            print("Email sent successfully!")
        except Exception as ex:
            print("Something went wrong….", ex)

    def format_addr(self, s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, "utf-8").encode(), addr))
