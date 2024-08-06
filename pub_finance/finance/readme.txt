PYSPARK_PYTHON = /root/miniconda3/bin/python
PYSPARK_DRIVER_PYTHON = /root/miniconda3/bin/python
0 8 * * * cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/Main_category.py > /root/pub_finance/finance/category.log 2>&1
0 9 * * * cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/Main.py > /root/pub_finance/finance/us.log 2>&1
00 16 * * * cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/Main_cn.py > /root/pub_finance/finance/cn.log 2>&1
20 16 * * * cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/Main_cnetf.py > /root/pub_finance/finance/etf.log 2>&1
0 0 * * 2 cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/HouseInfoCrawler.py > /root/pub_finance/finance/house.log 2>&1

