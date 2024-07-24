#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import requests
import json


""" 发送钉钉信息 """


class DingDing(object):
    """钉钉webhook地址，自定义标签模式"""

    # dd_ak = os.environ.get("dd_ak")
    # __url = "https://oapi.dingtalk.com/robot/send?access_token=" + dd_ak

    def __init__(self):
        self.api_url = self.__url

    def send_markdown(self, content, title):
        data = {
            "msgtype": "markdown",
            "markdown": {
                "text": content,
                "title": title,
            },
        }
        return self._send_request(data)

    def _send_request(self, data):
        headers = {"Content-Type": "application/json; charset=utf-8"}
        data = json.dumps(data)
        result = requests.post(self.api_url, data=data, headers=headers)
        return result.text
