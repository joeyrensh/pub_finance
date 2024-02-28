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


class MyEmail(object):
    """string_to_encode = xxx
    encoded_string = base64.b64encode(string_to_encode.encode("utf-8")).decode("utf-8")
    """

    _mail_user = base64.b64decode(os.environ.get("email_addr")).decode("utf-8")
    _mail_password = base64.b64decode(os.environ.get("email_key")).decode("utf-8")

    def __init__(self):
        self.send_from = self._mail_user
        self.to = self._mail_user
        self.server = "smtp.qq.com"  # smtp.163.com smtp.qq.com
        self.port = 465
        self.msg = MIMEMultipart("related")
        self.msg["MIME-Version"] = "1.1"
        self.msg["To"] = self.to
        self.msg["From"] = self.format_addr(
            "Quantitative trading <%s>" % self._mail_user
        )

    def send_email(self, subject, message):
        try:
            self.subject = subject
            self.msg["Subject"] = subject
            self.msg.attach(MIMEText(message, "html"))
            smtp_server = smtplib.SMTP_SSL(self.server, self.port)
            smtp_server.ehlo()
            smtp_server.login(self._mail_user, self._mail_password)
            print(self.msg.as_string())
            smtp_server.sendmail(
                self.send_from,
                [self.to],
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
            smtp_server.login(self._mail_user, self._mail_password)
            smtp_server.sendmail(
                self.send_from,
                [self.to, 
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
