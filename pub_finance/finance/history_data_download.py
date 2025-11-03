from utility.em_stock_uti import EMWebCrawlerUti
from cncrawler.ak_history_crawler import AKCNHistoryDataCrawler

# 历史数据起始时间，结束时间
# 文件名称定义
start_date = "20251101"
end_date = "20251103"
file_path = "./cnstockinfo/test.csv"
em = EMWebCrawlerUti()
em.get_his_stock_info_list(
    "cn",
    start_date,
    end_date,
    file_path,
)

# 使用AKshare下载A股历史数据
# ak_his = AKCNHistoryDataCrawler()
# ak_his.get_cn_stock_history_ak(start_date, end_date, file_path)
