import requests
import akshare as ak
import random
import time
import csv

# 配置参数
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Referer": "https://xueqiu.com/",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}
COOKIES = {
    "cookiesu": "411732239656284",
    "HMACCOUNT": "829A850BB48E0510",
    "device_id": "c085ab5c8bae12e5d453006c3a533e17",
    "xq_is_login": "1",
    "u": "1061408583",
    "s": "c511qy2xxv",
    "Hm_lvt_1db88642e346389874251b5a1eded6e3": "1740983346",
    "xq_a_token": "723ca5dab61362ba68dd70dd6eea45d9f8cd054f",
    "xqat": "723ca5dab61362ba68dd70dd6eea45d9f8cd054f",
    "xq_r_token": "56ec6c8363a66a07a79e9b26c7325752eda84755",
    "xq_id_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjEwNjE0MDg1ODMsImlzcyI6InVjIiwiZXhwIjoxNzQzNTc1MzYyLCJjdG0iOjE3NDA5ODMzNjI1OTQsImNpZCI6ImQ5ZDBuNEFadXAifQ.Bdry9i2kFOeYw7AhRD5mJMSuCtbF1fufledQbLQLu6cHzI505mToKQYmgZECi2B280NOg4PzF0l0widnGiagiBU0a6wt2X7Bhb1WgMn0lW1uiwFWIxwZJf51Aw5XQWUeHK4HFkm_joq77cpkivRfpoZtHzh8v3vErwGfJjm_AOCLacPKZLFqRuK92bI96m68rEiQqLNa71MAq4oZAm74jhKrFH5g_2JdPc29rJozEPmPna-icwphRDrHiJuwENpysHD6M_QYaKjOIu9hw8Bsiw4tYq8NWcP7eEolPeRPiacgjnoYnhgQq_a-UbKY6q1cvtyfP0-Gr7Z1eHC_EyVaKg",
    "Hm_lpvt_1db88642e346389874251b5a1eded6e3": "1740996710",
}

BASE_URL = "https://stock.xueqiu.com/v5/stock/f10/us/industry.json"
OUTPUT_FILE = "./usstockinfo/industry_xueqiu.csv"


def create_session():
    """创建带持久化会话的请求对象"""
    session = requests.Session()

    # 添加自定义请求头
    session.headers.update(HEADERS)

    # 添加固定Cookie
    session.cookies.update(COOKIES)

    # 添加随机延迟中间件
    session.hooks["response"] = [
        lambda r, *args, **kwargs: time.sleep(random.uniform(1.5, 3.5))
    ]

    return session


def get_industry(session, symbol):
    """获取行业分类（带异常重试）"""
    try:
        params = {"symbol": symbol}
        response = session.get(
            BASE_URL,
            params=params,
            timeout=15,
        )

        # 验证响应状态
        if response.status_code != 200:
            print(f"异常状态码 {response.status_code}，尝试重试...")
            return "Error"

        data = response.json()

        # 提取首个行业名称
        industry = data.get("data", {}).get("industry")
        if industry and len(industry) > 0:
            industry_name = industry[3]["ind_name"]
            return industry_name
        return "NA"

    except Exception as e:
        print(f"请求异常: {str(e)}")
        return "Error"


# ====== 主程序 ======
def main(SYMBOLS):
    # 初始化会话
    session = create_session()

    # 预检测Cookie有效性
    test_response = session.get("https://xueqiu.com/setting/user", timeout=10)
    if "profile" not in test_response.text:
        print("Cookie已失效，需要更新认证信息！")
        return

    # 开始采集数据
    global_index = 1  # 全局索引计数器
    batch_buffer = []  # 批量写入缓冲区

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["idx", "symbol", "industry"])  # 写入表头

        for idx, symbol in enumerate(SYMBOLS, 1):
            retry_count = 0
            industry = "Error"

            while retry_count < 3:
                print(f"正在处理 [{idx}/{len(SYMBOLS)}] {symbol}...")
                industry = get_industry(session, symbol)

                if industry != "Error":
                    print(f"成功获取：{industry}")
                    break
                else:
                    retry_count += 1
                    print(f"第{retry_count}次重试...")
                    time.sleep(2**retry_count)

            # 将数据存入缓冲区
            if industry not in ("Error", "NA"):
                batch_buffer.append([global_index, symbol, industry])
                global_index += 1

            # 每10条记录写入一次
            if len(batch_buffer) == 10:
                writer.writerows(batch_buffer)
                f.flush()
                batch_buffer.clear()  # 清空缓冲区

            # 随机延迟
            time.sleep(random.uniform(1, 2))

        # 写入剩余记录
        if batch_buffer:
            writer.writerows(batch_buffer)
            f.flush()

    print(f"数据已保存至 {OUTPUT_FILE}")


if __name__ == "__main__":
    stock_us_spot_em_df = ak.stock_us_spot_em()
    symbol_list = list(
        set(stock_us_spot_em_df["代码"].apply(lambda x: x.split(".")[1]))
    )
    main(symbol_list)
