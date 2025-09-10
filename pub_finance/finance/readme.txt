PYSPARK_PYTHON = /home/ubuntu/miniconda3/bin/python
PYSPARK_DRIVER_PYTHON = /home/ubuntu/miniconda3/bin/python
#00 6 * * 1 cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python /home/ubuntu/pub_finance/finance/globalcategory_main.py > /home/ubuntu/pub_finance/finance/category.log 2>&1
00 7 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/usstock_main.py > /home/ubuntu/pub_finance/finance/us.log 2>&1
20 7 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/usspecialstock_performance.py > /home/ubuntu/pub_finance/finance/us_special.log 2>&1
30 15 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/cnstock_main.py > /home/ubuntu/pub_finance/finance/cn.log 2>&1
50 15 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/cnetf_main.py > /home/ubuntu/pub_finance/finance/etf.log 2>&1
