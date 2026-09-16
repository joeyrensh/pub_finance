from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.utility.em_stock_uti_fqt import EMWebCrawlerUti
from finance.cncrawler.ak_history_crawler_fqt import AKCNHistoryDataCrawler
from finance import FINANCE_ROOT

# 历史数据起始时间，结束时间
# 文件名称定义
start_date = "20250101"
end_date = "20260915"
market = "us"
file_path = FINANCE_ROOT / f"{market}stockinfo" / "new_stock_data_fqt.csv"
em = EMWebCrawlerUti(use_proxy=True)
em.get_his_stock_info_list(
    market,
    start_date,
    end_date,
    file_path,
)

em.restore_yfinance_raw_csv(trade_date=end_date, market=market, history_file_path=file_path)

# 使用AKshare下载A股历史数据
# ak_his = AKCNHistoryDataCrawler()
# ak_his.get_cn_stock_history_ak(start_date, end_date, file_path)
