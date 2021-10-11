import requests
import json


# 发送钉钉信息
class DingDing(object):

    # 钉钉webhook地址，自定义标签模式
    __url = 'https://oapi.dingtalk.com/robot/send?access_token=' \
            '831aeb38519de912882b1a972023e21355efdbe95c36153af206d37b050ea347'

    def __init__(self):
        self.api_url = self.__url

    def send_markdown(self, content, title):
        data = {
            "msgtype": "markdown",
            "markdown": {
                "text": content,
                "title": title,
            }
        }
        return self._send_request(data)

    def _send_request(self, data):
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        data = json.dumps(data)
        result = requests.post(self.api_url, data=data, headers=headers)
        return result.text

