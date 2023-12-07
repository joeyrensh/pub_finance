\cp -rf finance ./git-repo/pub_finance
rm -f ./git-repo/pub_finance/finance/execute_us.log
rm -f ./git-repo/pub_finance/finance/execute_cn.log
rm -rf ./git-repo/pub_finance/finance/usstockinfo/*
rm -rf ./git-repo/pub_finance/finance/cnstockinfo/*
rm -rf ./git-repo/pub_finance/finance/ta-lib-0.4.0-src.tar.gz
rm -rf ./git-repo/pub_finance/finance/ta-lib
\cp -rf ./finance/usstockinfo/USStockMarketClosed.config ./git-repo/pub_finance/finance/usstockinfo/
\cp -rf ./finance/cnstockinfo/CNStockMarketClosed.config ./git-repo/pub_finance/finance/cnstockinfo/
cd ./git-repo/pub_finance
git add finance
git commit -m "update"
git push
