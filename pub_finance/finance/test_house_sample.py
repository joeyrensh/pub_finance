import requests

proxies = {
    "http": "socks5h://fc4239f5.w1o0q7h0v5-scnode-hk03.bzlxzl.com:57003",
    "https": "socks5h://fc4239f5.w1o0q7h0v5-scnode-hk03.bzlxzl.com:57003",
}

response = requests.get("https://sh.lianjia.com/", proxies=proxies)
print(response.text)
