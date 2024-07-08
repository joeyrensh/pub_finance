\cp -rf finance ./git-repo/pub_finance/
rm -f ./git-repo/pub_finance/finance/us.log
rm -f ./git-repo/pub_finance/finance/cn.log
rm -f ./git-repo/pub_finance/finance/cnetf.log
rm -f ./git-repo/pub_finance/finance/category.log
rm -rf ./git-repo/pub_finance/finance/usstockinfo/*
rm -rf ./git-repo/pub_finance/finance/cnstockinfo/*
rm -rf ./git-repo/pub_finance/finance/ta-lib-0.4.0-src.tar.gz
rm -rf ./git-repo/pub_finance/finance/ta-lib
echo -e "mail_name,mail_password\nmail_name1" > ./git-repo/pub_finance/finance/mail.conf
\cp -rf ./finance/usstockinfo/USStockMarketClosed.config ./git-repo/pub_finance/finance/usstockinfo/
\cp -rf ./finance/cnstockinfo/CNStockMarketClosed.config ./git-repo/pub_finance/finance/cnstockinfo/
cd ./git-repo/pub_finance/
git add *
git commit -a -m "update"
git push
