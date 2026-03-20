from finance.utility.em_stock_uti import EMWebCrawlerUti
from finance.cncrawler.ak_history_crawler import AKCNHistoryDataCrawler
from finance.paths import FINANCE_ROOT

# 历史数据起始时间，结束时间
# 文件名称定义
start_date = "20240101"
end_date = "20260115"
file_path = FINANCE_ROOT / "usstockinfo" / "new_stock_data.csv"
em = EMWebCrawlerUti()
em.get_his_stock_info_list(
    "us",
    start_date,
    end_date,
    file_path,
)

# 使用AKshare下载A股历史数据
# ak_his = AKCNHistoryDataCrawler()
# ak_his.get_cn_stock_history_ak(start_date, end_date, file_path)
