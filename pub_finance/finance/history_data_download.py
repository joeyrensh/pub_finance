from utility.em_stock_uti import EMWebCrawlerUti
from cncrawler.ak_history_crawler import AKCNHistoryDataCrawler

# 历史数据起始时间，结束时间
# 文件名称定义
em = EMWebCrawlerUti()
start_date = "20250930"
end_date = "20251007"
file_path = "./usstockinfo/stock_20251007.csv"
em.get_his_stock_info_list(
    "us",
    start_date,
    end_date,
    file_path,
)

# 使用AKshare下载A股历史数据
# ak_his = AKCNHistoryDataCrawler()
# ak_his.get_cn_stock_history_ak(start_date, end_date, file_path)
