\cp -rf ./pub_finance/finance ./git-repo/pub_finance/pub_finance/
rm -rf ./git-repo/pub_finance/pub_finance/finance/*.log
rm -rf ./git-repo/pub_finance/pub_finance/finance/nohup.out
rm -rf ./git-repo/pub_finance/pub_finance/finance/usstockinfo/*
rm -rf ./git-repo/pub_finance/pub_finance/finance/cnstockinfo/*
rm -rf ./git-repo/pub_finance/pub_finance/finance/ta-lib-0.4.0-src.tar.gz
rm -rf ./git-repo/pub_finance/pub_finance/finance/ta-lib
rm -rf ./git-repo/pub_finance/pub_finance/finance/houseinfo/*.png
rm -rf ./git-repo/pub_finance/pub_finance/finance/*.png
rm -rf ./git-repo/pub_finance/pub_finance/finance/houseinfo/*.csv
rm -rf ./git-repo/pub_finance/pub_finance/finance/images/*
echo -e "mail_name,mail_password\nmail_name1" > ./git-repo/pub_finance/pub_finance/finance/mail.conf
\cp -rf ./pub_finance/finance/houseinfo/proxies.csv ./git-repo/pub_finance/pub_finance/finance/houseinfo/
\cp -rf ./pub_finance/finance/usstockinfo/marketclosed.config ./git-repo/pub_finance/pub_finance/finance/usstockinfo/
\cp -rf ./pub_finance/finance/cnstockinfo/marketclosed.config ./git-repo/pub_finance/pub_finance/finance/cnstockinfo/
cd ./git-repo/pub_finance/pub_finance/
git add *
git commit -a -m "update"
git push
