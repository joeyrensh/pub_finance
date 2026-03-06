#!/bin/bash
OUTPUT_FILE="/home/ubuntu/pub_finance/finance/utility/new_proxy.txt"
BACKUP_FILE="/home/ubuntu/pub_finance/finance/utility/new_proxy.txt.bak"
TESTED_FILE="/tmp/proxy_tested_$(date +%Y%m%d).txt"
PROXY_LIST="/tmp/proxy_list_$(date +%Y%m%d).txt"
OLD_PROXY_FILE="/tmp/old_proxies_$(date +%Y%m%d).txt"

# 东方财富 API
test_url="https://92.push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.600066&ut=&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20211101&end=20211115&smplmt=755&lmt=1000000"

# 请求头配置
user_agents=(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
random_ua=${user_agents[$RANDOM % ${#user_agents[@]}]}

# Cookie 配置
cookies=(
  "EMFID=; st_si=; st_asi=; qgqp-auth-id="
  "st_asi=; EMFID=; st_si=; qgqp-auth-id="
  "qgqp-auth-id=; st_si=; EMFID="
  ""
)
random_cookie=${cookies[$RANDOM % ${#cookies[@]}]}

echo "========================================"
echo "每日代理 IP 测试（复测 + 累计）"
echo "开始：$(date "+%Y-%m-%d %H:%M:%S")"
echo "目标：200 个可用代理"
echo "========================================"

rm -f "$TESTED_FILE" "$PROXY_LIST" "$OLD_PROXY_FILE"
touch "$OUTPUT_FILE"

# 1. 备份并读取旧代理
if [ -s "$OUTPUT_FILE" ]; then
  cp "$OUTPUT_FILE" "$BACKUP_FILE"
  cp "$OUTPUT_FILE" "$OLD_PROXY_FILE"
  old_count=$(wc -l < "$OLD_PROXY_FILE")
  echo "发现 $old_count 个旧代理，开始复测..."
else
  old_count=0
  echo "无旧代理，跳过复测"
fi

# 2. 复测旧代理
old_valid=0
if [ -s "$OLD_PROXY_FILE" ]; then
  echo ""
  echo "=== 复测旧代理 ==="
  while read proxy; do
    result=$(timeout 3 curl -s -x http://$proxy --connect-timeout 2 \
      -A "$random_ua" \
      -H "Accept: application/json, text/plain, */*" \
      -H "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8" \
      -H "Referer: https://quote.eastmoney.com/" \
      -b "$random_cookie" \
      "$test_url" 2>/dev/null | grep -o "\"klines\"" | head -1)
    
    if [ -n "$result" ]; then
      echo "✅ [旧] $proxy"
      echo "$proxy" >> "$TESTED_FILE"
      old_valid=$((old_valid + 1))
    else
      echo "❌ [旧] $proxy"
    fi
  done < "$OLD_PROXY_FILE"
  echo "旧代理复测完成：$old_valid/$old_count 可用"
fi

# 3. 获取新代理
echo ""
echo "=== 获取新代理列表 ==="
curl -s --max-time 15 "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt" 2>/dev/null | head -200 >> "$PROXY_LIST"
curl -s --max-time 15 "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt" 2>/dev/null | head -200 >> "$PROXY_LIST"
curl -s --max-time 15 "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt" 2>/dev/null | head -200 >> "$PROXY_LIST"
curl -s --max-time 15 "https://raw.githubusercontent.com/proxy4p/proxy-list/master/proxy.txt" 2>/dev/null | head -200 >> "$PROXY_LIST"
curl -s --max-time 15 "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all" 2>/dev/null | head -300 >> "$PROXY_LIST"
curl -s --max-time 15 "https://proxylist.geonode.com/api/proxy-list?limit=200&protocols=http" 2>/dev/null | grep -oE "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+" >> "$PROXY_LIST"

# 去重（排除已测试的旧代理）
sort -u "$PROXY_LIST" -o "$PROXY_LIST"
if [ -s "$TESTED_FILE" ]; then
  grep -v -F -f "$TESTED_FILE" "$PROXY_LIST" > "${PROXY_LIST}.tmp"
  mv "${PROXY_LIST}.tmp" "$PROXY_LIST"
fi

total_proxies=$(wc -l < "$PROXY_LIST")
echo "获取到 $total_proxies 个新代理"

# 4. 测试新代理
if [ $total_proxies -gt 0 ]; then
  echo ""
  echo "=== 测试新代理 ==="
  count=0
  total=0
  target=$((200 - old_valid))
  
  while read proxy; do
    total=$((total + 1))
    
    if [ $count -ge $target ]; then
      echo "已达到目标数量"
      break
    fi
    
    result=$(timeout 3 curl -s -x http://$proxy --connect-timeout 2 \
      -A "$random_ua" \
      -H "Accept: application/json, text/plain, */*" \
      -H "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8" \
      -H "Referer: https://quote.eastmoney.com/" \
      -b "$random_cookie" \
      "$test_url" 2>/dev/null | grep -o "\"klines\"" | head -1)
    
    if [ -n "$result" ]; then
      echo "✅ [新] [$total/$total_proxies] $proxy"
      echo "$proxy" >> "$TESTED_FILE"
      count=$((count + 1))
    else
      echo "❌ [新] [$total/$total_proxies] $proxy"
    fi
    
    if [ $((total % 50)) -eq 0 ]; then
      echo "进度：$total/$total_proxies, 新增：$count/$target"
    fi
  done < "$PROXY_LIST"
  new_valid=$count
else
  new_valid=0
fi

# 5. 保存结果
total_valid=$((old_valid + new_valid))
sort -u "$TESTED_FILE" -o "$OUTPUT_FILE"

echo ""
echo "========================================"
echo "测试完成"
echo "结束：$(date "+%Y-%m-%d %H:%M:%S")"
echo "旧代理：$old_valid/$old_count 可用"
echo "新代理：$new_valid/$total_proxies 可用"
echo "总计：$total_valid 个可用代理"
echo "输出：$OUTPUT_FILE"
echo "备份：$BACKUP_FILE"
echo "========================================"

if [ -s "$OUTPUT_FILE" ]; then
  echo ""
  echo "✅ 可用代理列表 ($total_valid 个):"
  cat "$OUTPUT_FILE"
  echo ""
  echo "代理已保存到：$OUTPUT_FILE"
else
  echo "❌ 未找到可用代理"
fi
