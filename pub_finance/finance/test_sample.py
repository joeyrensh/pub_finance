import requests
from bs4 import BeautifulSoup
import re
from time import sleep
import random


def get_yahoo_industry(symbol, proxy_list=None, retries=1):
    url = f"https://finance.yahoo.com/quote/{symbol}/profile/"

    # 2024-05-15最新Cookie（需定期更新）
    cookies = {
        "tbla_id": "3c175ed2-34ab-4c8f-a325-3a4d2493aea8-tucte5d69bd",
        "gpp": "DBAA",
        "gpp_sid": "-1",
        "OTH": "v=2&s=0&d=eyJraWQiOiIwIiwiYWxnIjoiUlMyNTYifQ.eyJjdSI6eyJndWlkIjoiUkRSVDNSUTQzSjRBWjc3WEMzUEFSS1M1WlEiLCJwZXJzaXN0ZW50Ijp0cnVlLCJzaWQiOiJHZjZuOXpHcHhlRmIifX0.tbUnq84fo0p4vQMmcSDxqoBdnTvpsZpmOGECYsV-vSTuQvrUPC0McbZpc58ehxpfb3nEEZJBj5TDtUS6TeF5oAyk3eMjasVlcb9Ssm7G89HZodl1rRsluffY83jDvd3tCRt5saostoa4XT20BrDgGDL1JcrMvj5qI9oR24f05r4",
        "OTHD": "g=E424E7596F41F6D5ACA263908A29CA59739416616AFAD1853C143B9223681AC3&s=6B9A0A6999D565A122C65EC605BAD29DD99CCB78B047B4853CA5B349DC775EA1&b=bid-b2ba7c5jlg6p9&j=sg&bd=400bcaaa019a7540083aa03397359e03&gk=x1a9z-qJCjBBcc&sk=x1a9z-qJCjBBcc&bk=x1a9z-qJCjBBcc&iv=B4568D32EE0009CCEBD6A77EF67F0722&v=1&u=0",
        "F": "d=kE1mccU9vAlhicgE0Tv.GIYH3lDU9CfLtT92i5..e9ROxiQbp.icoA4-",
        "T": "af=JnRzPTE3NDExNDA4MTcmcHM9TmlzS1U1a0cxYnU1MkZyaG9yM3MwZy0t&d=bnMBeWFob28BZwFSRFJUM1JRNDNKNEFaNzdYQzNQQVJLUzVaUQFhYwFBS0lRS21kTgFhbAFqb2V5cmhmMTk4NEBnbWFpbC5jb20Bc2MBbWJyX3JlZ2lzdHJhdGlvbgFmcwFUT2NSX0Fsbng3TlIBenoBUk43eG5CQTdFAWEBUUFFAWxhdAFSTjd4bkIBbnUBMA--&kt=EAAjl5WNacDvVVc6ZVxFXDR1w--~I&ku=FAAn4lDEB3X1abO5dsIEC8OOoeRzZNlSCmKvFsnirhLlGqVyTr.bq3hitL8VdA9w5W.eoKSzL1zYtYYXmpU83QjSBbpEtmzNgV37EkoDCoUlVk84gEMG425NXwYdZ.4w.wgfGGxyzFTA5ndx7ejBnpxboAcnrTREROpTnH0dXgCSuU-~E",
        "PH": "l=en-SG",
        "Y": "v=1&n=4f27qh2a1itfg&l=sfbn1h9ewf8cx32pi676dgjpm33jix20vbalkbf8/o&p=n2kvvsg00000000&r=1eo&intl=sg",
        "cmp": "t=1741140818&j=0&u=1---",
        "GUC": "AQEACAJnyPtn70IdyARD&s=AQAAABMJa4nP&g=Z8ezgw",
        "A1": "d=AQABBCkbWGcCEJazEkRDCmVtKOPhVmFHLbEFEgEACAL7yGfvZ69E8HgB_eMBAAcIKRtYZ2FHLbEID3Ct-4X1GAvY6f58JQhbGgkBBwoBhg&S=AQAAAoZvmK8rYstGheaSjfpbkcQ",
        "A3": "d=AQABBCkbWGcCEJazEkRDCmVtKOPhVmFHLbEFEgEACAL7yGfvZ69E8HgB_eMBAAcIKRtYZ2FHLbEID3Ct-4X1GAvY6f58JQhbGgkBBwoBhg&S=AQAAAoZvmK8rYstGheaSjfpbkcQ",
        "A1S": "d=AQABBCkbWGcCEJazEkRDCmVtKOPhVmFHLbEFEgEACAL7yGfvZ69E8HgB_eMBAAcIKRtYZ2FHLbEID3Ct-4X1GAvY6f58JQhbGgkBBwoBhg&S=AQAAAoZvmK8rYstGheaSjfpbkcQ",
        "axids": "gam=y-X8rQWS5G2uKD_3DZUHxZHt4YD08P2QdyrCN_SblQ7_vKw5VKew---A&dv360=eS1Yb0RJblZWRTJ1R1hVd2RQdF83SUI3OFVfZ3M1X0dyeTdfSnZHTE4wUHNRT1B0SVh5QkxzdUtOcjlMdXRIODdTX25DNX5B&ydsp=y-EeVhz5NE2uKTTPM6G.PEMfF5NE6ALUp_7AVTuxXGpDSLh82KpTxFI3a4FAeSgAb.xRIo~A&tbla=y-W9SU4dBG2uJcuiq_2t9s7k6diECRcQiG_x27iqbffe9E3FW4Cg---A",
        "PRF": "theme%3Dauto%26t%3DBABA%252BLAZR%252BNIO%252BSE%252BFUTU%252BXPEV%252BBEKE%252BMSFT%252BTSLA%252BPDD%26ft%3DthemeSwitchEducation",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # 设置代理列表（示例）
    if proxy_list is None:
        proxy_list = [
            "http://proxy1.example.com:8080",
            "socks5://user:pass@proxy2.example.com:1080",
            "http://proxy3.example.com:3128",
        ]

    # 随机打乱代理顺序
    random.shuffle(proxy_list)

    for proxy in proxy_list:
        for attempt in range(retries):
            try:
                # 配置代理字典
                proxies = {"http": proxy, "https": proxy}

                with requests.Session() as session:
                    # 设置会话级代理
                    session.proxies = proxies

                    # 第一次握手请求（激活Cookie）
                    session.get(
                        "https://finance.yahoo.com",
                        headers=headers,
                        cookies=cookies,
                        timeout=10,
                    )

                    # 正式请求
                    response = session.get(
                        url,
                        headers=headers,
                        cookies=cookies,
                        timeout=15,  # 适当延长超时
                    )

                    # 反爬检测
                    if "cookieChallenge" in response.url:
                        print(f"代理 {proxy} 触发验证，尝试下一个...")
                        break  # 跳出重试循环

                    # 解析页面
                    soup = BeautifulSoup(response.text, "html.parser")

                    # 使用CSS选择器精准定位
                    industry_div = soup.select_one("div.company-stats")
                    if not industry_div:
                        continue  # 可能页面结构变化，直接重试

                    industry_label = industry_div.find(
                        "dt", string=re.compile(r"Industry:")
                    )
                    if industry_label:
                        industry = industry_label.find_next("a").text.strip()
                        return f"✅ 成功通过代理 {proxy} 获取结果: {industry}"
                    else:
                        print(
                            f"代理 {proxy} 页面结构异常，重试 {attempt + 1}/{retries}"
                        )
                        sleep(2)  # 添加间隔
                        continue

            except (
                requests.exceptions.ProxyError,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.SSLError,
            ) as e:
                print(f"代理 {proxy} 第 {attempt + 1} 次失败: {str(e)}")
                sleep(2)
                continue
            except Exception as e:
                print(f"代理 {proxy} 出现意外错误: {str(e)}")
                break  # 非代理相关错误直接跳出

    return "❌ 所有代理尝试失败，请检查网络或更新代理列表"


# 测试调用（示例代理列表）
print(
    get_yahoo_industry(
        "BABA",
        proxy_list=[
            "http://76.169.129.241:8080",
            "http://45.58.147.26:3128",
        ],
    )
)
