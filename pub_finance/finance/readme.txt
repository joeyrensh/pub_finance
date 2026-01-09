PYSPARK_PYTHON = /home/ubuntu/miniconda3/bin/python
PYSPARK_DRIVER_PYTHON = /home/ubuntu/miniconda3/bin/python
00 7 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/usstock_main.py > /home/ubuntu/pub_finance/finance/us.log 2>&1
30 15 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/cnstock_main.py > /home/ubuntu/pub_finance/finance/cn.log 2>&1