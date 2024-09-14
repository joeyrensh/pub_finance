PYSPARK_PYTHON = /root/miniconda3/bin/python
PYSPARK_DRIVER_PYTHON = /root/miniconda3/bin/python
0 6 * * * cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/globalcategory_main.py > /root/pub_finance/finance/category.log 2>&1
35 7 * * * cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/usstock_main.py > /root/pub_finance/finance/us.log 2>&1
00 16 * * * cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/cnstock_main.py > /root/pub_finance/finance/cn.log 2>&1
20 16 * * * cd /root/pub_finance/finance ; /root/miniconda3/bin/python /root/pub_finance/finance/cnetf_main.py > /root/pub_finance/finance/etf.log 2>&1