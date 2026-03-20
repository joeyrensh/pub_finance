#!/bin/bash
cd /home/ubuntu/pub_finance/finance
source /home/ubuntu/miniconda3/etc/profile.d/conda.sh
conda activate base
python dashreport/backtest_app.py
