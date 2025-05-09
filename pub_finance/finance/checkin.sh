\cp -rf ./pub_finance/finance ./git-repo/pub_finance/pub_finance/
rm -rf ./git-repo/pub_finance/pub_finance/finance/*.log
rm -rf ./git-repo/pub_finance/pub_finance/finance/nohup.out
rm -rf ./git-repo/pub_finance/pub_finance/finance/dashreport/nohup.out
rm -rf ./git-repo/pub_finance/pub_finance/finance/usstockinfo/*
rm -rf ./git-repo/pub_finance/pub_finance/finance/cnstockinfo/*
rm -rf ./git-repo/pub_finance/pub_finance/finance/ta-lib-0.4.0-src.tar.gz
rm -rf ./git-repo/pub_finance/pub_finance/finance/ta-lib
rm -rf ./git-repo/pub_finance/pub_finance/finance/houseinfo/*.png
rm -rf ./git-repo/pub_finance/pub_finance/finance/*.png
rm -rf ./git-repo/pub_finance/pub_finance/finance/houseinfo/*.csv
rm -rf ./git-repo/pub_finance/pub_finance/finance/dashreport/assets/images/*.png
rm -rf ./git-repo/pub_finance/pub_finance/finance/dashreport/assets/images/*.svg
rm -rf ./git-repo/pub_finance/pub_finance/finance/data/*.csv
rm -rf ./git-repo/pub_finance/pub_finance/finance/.vscode
rm -rf ./git-repo/pub_finance/pub_finance/finance/backtraderref/__pycache__
rm -rf ./git-repo/pub_finance/pub_finance/finance/cncrawler/__pycache__
rm -rf ./git-repo/pub_finance/pub_finance/finance/dashreport/__pycache__
rm -rf ./git-repo/pub_finance/pub_finance/finance/dashreport/pages/__pycache__
rm -rf ./git-repo/pub_finance/pub_finance/finance/uscrawler/__pycache__
rm -rf ./git-repo/pub_finance/pub_finance/finance/usstrategy/__pycache__
rm -rf ./git-repo/pub_finance/pub_finance/finance/utility/__pycache__
echo -e "mail_name,mail_password\nmail_name1" > ./git-repo/pub_finance/pub_finance/finance/mail.conf
\cp -rf ./pub_finance/finance/houseinfo/proxies.csv ./git-repo/pub_finance/pub_finance/finance/houseinfo/
\cp -rf ./pub_finance/finance/usstockinfo/marketclosed.config ./git-repo/pub_finance/pub_finance/finance/usstockinfo/
\cp -rf ./pub_finance/finance/usstockinfo/industry_yfinance.csv ./git-repo/pub_finance/pub_finance/finance/usstockinfo/
\cp -rf ./pub_finance/finance/usstockinfo/industry_yfinance_mapping.csv ./git-repo/pub_finance/pub_finance/finance/usstockinfo/
\cp -rf ./pub_finance/finance/cnstockinfo/marketclosed.config ./git-repo/pub_finance/pub_finance/finance/cnstockinfo/
\cp -rf ./pub_finance/finance/cnstockinfo/industry.csv ./git-repo/pub_finance/pub_finance/finance/cnstockinfo/
cd ./git-repo/pub_finance/pub_finance/
git add *
git commit -a -m "update"
git push
