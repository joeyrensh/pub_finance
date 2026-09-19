awk -F',' '
NR>1 {
    # 提取字段
    sym = $1; dt = $2; div = $3; split = $4;
    
    # 将 YYYY-MM-DD 转化为 Unix 时间戳 (秒)
    split(dt, a, "-");
    curr_ts = mktime(a[1]" "a[2]" "a[3]" 00 00 00");

    # 检查是否与上一条记录构成脏数据 (同 symbol、3天以内、div和split相等)
    if (sym == last_sym) {
        day_diff = (curr_ts - last_ts) / 86400;
        if (day_diff > 0 && day_diff <= 3 && div == last_div && split == last_split) {
            print "发现可能脏数据 -> 股票:", sym, "| 前一条:", last_dt " (Div:" last_div ", Split:" last_split ") | 当前条:", dt " (Div:" div ", Split:" split ")";
            count++;
        }
    }

    # 更新上一条记录的状态
    last_sym = sym; last_dt = dt; last_ts = curr_ts; last_div = div; last_split = split;
}
END {
    print "--------------------------------------------------";
    print "检测完成，共发现", count+0, "组潜在的相邻冗余脏数据。";
}' us_stock_actions_history.csv
