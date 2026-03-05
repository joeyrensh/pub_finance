#!/bin/bash
US_LOG="/home/ubuntu/pub_finance/finance/us.log"
CN_LOG="/home/ubuntu/pub_finance/finance/cn.log"
us_lines=$(wc -l < "$US_LOG" 2>/dev/null || echo 0)
cn_lines=$(wc -l < "$CN_LOG" 2>/dev/null || echo 0)
us_period=$(grep -oP "current period: \K\d+" "$US_LOG" 2>/dev/null | tail -1)
cn_period=$(grep -oP "current period: \K\d+" "$CN_LOG" 2>/dev/null | tail -1)
us_date=$(grep -oP "current date \K[0-9-]+" "$US_LOG" 2>/dev/null | tail -1)
cn_date=$(grep -oP "current date \K[0-9-]+" "$CN_LOG" 2>/dev/null | tail -1)
us_value=$(grep -oP "Final Portfolio Value: \K[\d.]+" "$US_LOG" 2>/dev/null | tail -1)
cn_value=$(grep -oP "Final Portfolio Value: \K[\d.]+" "$CN_LOG" 2>/dev/null | tail -1)
us_done=$(grep -q "doing task: 100%" "$US_LOG" 2>/dev/null && echo "完成" || echo "进行中")
cn_done=$(grep -q "doing task: 100%" "$CN_LOG" 2>/dev/null && echo "完成" || echo "进行中")
echo "=== 策略日志检查 ==="
echo "时间: $(date)"; echo ""
echo "US: $us_done | 日期:$us_date | 周期:$us_period | 市值:\$$us_value | 行数:$us_lines"
echo "CN: $cn_done | 日期:$cn_date | 周期:$cn_period | 市值:¥$cn_value | 行数:$cn_lines"
