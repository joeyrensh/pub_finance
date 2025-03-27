import yfinance as yf
import requests
import logging
import time

# 配置日志记录
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

proxy = "http://190.26.208.230:999"

with requests.Session() as session:
    session.proxies = {"http": proxy, "https": proxy}
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "})
    start_time = time.time()

    # 获取行业信息
    ticker = yf.Ticker("TCBK", session=session)
    info = ticker.info
    industry = info.get("industry", None)

    logger.info("✅ 成功获取行业信息")
    print(industry)
    logger.info(f"耗时: {time.time() - start_time:.2f}s")
