#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import smtplib
from email.mime.text import MIMEText
import os


class MyEmail(object):

    _mail_user = os.environ.get('email_addr')
    _mail_password = os.environ.get('email_key')

    def __init__(self, subject, message):
        self.send_from = self._mail_user
        self.to = self._mail_user
        self.subject = subject
        self.msg = MIMEText(message, "html")
        self.msg['Subject'] = subject
        self.msg['To'] = self.to
        self.msg['From'] = self.send_from

    def send_email(self):

        try:
            smtp_server = smtplib.SMTP_SSL('smtp.163.com', 465)
            smtp_server.ehlo()
            smtp_server.login(self._mail_user, self._mail_password)
            smtp_server.sendmail(self.send_from, self.to, self.msg.as_string())
            smtp_server.close()
            print("Email sent successfully!")
        except Exception as ex:
            print("Something went wrong….", ex)
