\cp -rf ./pub_finance/finance ./git-repo/pub_finance/pub_finance/
echo -e "mail_name,mail_password\nmail_name1" > ./git-repo/pub_finance/pub_finance/finance/mail.conf
cd ./git-repo/pub_finance/pub_finance/
git add *
git commit -a -m "update"
git push
