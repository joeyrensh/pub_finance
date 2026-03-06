#!/bin/bash

SRC_DIR="./pub_finance"
DST_DIR="./git-repo/pub_finance/pub_finance"

# 清空目标
rm -rf "$DST_DIR"/*
mkdir -p "$DST_DIR"

# 复制 finance 下除了 cnstockinfo/usstockinfo 的内容
mkdir -p "$DST_DIR/finance"
for item in "$SRC_DIR/finance"/*; do
    name=$(basename "$item")
    if [ "$name" != "cnstockinfo" ] && [ "$name" != "usstockinfo" ]; then
        cp -r "$item" "$DST_DIR/finance/"
    fi
done

# 处理 cnstockinfo 和 usstockinfo
for special in cnstockinfo usstockinfo; do
    src="$SRC_DIR/finance/$special"
    dst="$DST_DIR/finance/$special"
    if [ -d "$src" ]; then
        mkdir -p "$dst"
        # 只复制非 stock_*.csv 和 gz_*.csv 文件
        find "$src" -maxdepth 1 -type f ! -name 'stock_*.csv' ! -name 'gz_*.csv' -exec cp {} "$dst/" \;
        # 复制子目录（如果有）
        find "$src" -mindepth 1 -type d -exec mkdir -p "$dst"/{} \;
    fi
done
echo -e "mail_name,mail_password\nmail_name1" > ./git-repo/pub_finance/pub_finance/finance/mail.conf
cd ./git-repo/pub_finance/pub_finance/
git add *
git commit -a -m "update"
git push
