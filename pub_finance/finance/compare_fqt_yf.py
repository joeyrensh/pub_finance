import os
import tempfile
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import time
import functools

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.utility.tickerinfo import TickerInfo
from finance.utility.em_stock_uti_fqt import EMWebCrawlerUti
from finance import FINANCE_ROOT
# ----------------------------------------------------------------------
# CONFIG & PATHS
# ----------------------------------------------------------------------
ACTIONS_FILE = FINANCE_ROOT/"usstockinfo/us_stock_actions_history.csv"
START_DATE = "20250101"
END_DATE = "20260916"
YF_START = "2025-01-01"
YF_END = "2026-09-16"

# 1. 自动筛选覆盖多种复权场景的代表性股票 (10~20只)
def select_sample_tickers(actions_path, min_count=10, max_count=20):
    print("🔍 [Step 1] 读取复权因子文件，筛选多元测试股票样本...")
    if not os.path.exists(actions_path):
        raise FileNotFoundError(f"复权因子文件不存在: {actions_path}")

    df_act = pd.read_csv(actions_path)
    df_act['dividend'] = pd.to_numeric(df_act.get('dividend', 0), errors='coerce').fillna(0)
    df_act['split_ratio'] = pd.to_numeric(df_act.get('split_ratio', 1), errors='coerce').fillna(1)

    # 过滤 2025 年以来的公司变动
    df_act['date_dt'] = pd.to_datetime(df_act['date'].astype(str), errors='coerce')
    df_act_recent = df_act[df_act['date_dt'] >= "2025-01-01"]

    has_div = df_act_recent[df_act_recent['dividend'] > 0]
    has_split = df_act_recent[df_act_recent['split_ratio'] > 1.0]      # 拆股
    has_reverse = df_act_recent[(df_act_recent['split_ratio'] < 1.0) & (df_act_recent['split_ratio'] > 0)] # 合并

    # 交叉筛选分类样本
    div_symbols = set(has_div['symbol'].unique())
    split_symbols = set(has_split['symbol'].unique())
    reverse_symbols = set(has_reverse['symbol'].unique())

    both_div_split = list(div_symbols.intersection(split_symbols))
    pure_split = list(split_symbols - div_symbols)
    pure_reverse = list(reverse_symbols)
    pure_div = list(div_symbols - split_symbols - reverse_symbols)

    selected = []
    # 尽可能挑选丰富多样的样本组合
    selected.extend(both_div_split[:4])    # 分红+拆股组合
    selected.extend(pure_split[:4])         # 纯拆股
    selected.extend(pure_reverse[:4])       # 纯合并
    selected.extend(pure_div[:6])           # 纯分红

    # 去重并截取 10 ~ 20 只
    selected = list(dict.fromkeys(selected))
    if len(selected) < min_count:
        # 如果 2025 后样本不足，从全量历史中补充
        all_symbols = df_act['symbol'].unique().tolist()
        selected.extend([s for s in all_symbols if s not in selected])
        selected = selected[:max_count]
    elif len(selected) > max_count:
        selected = selected[:max_count]

    print(f"✅ 成功选择 {len(selected)} 只测试股票: {selected}")
    return selected


# 2. 调用 Yahoo Finance 拉取前复权 K 线并保存
def fetch_and_save_yf_fqt(ticker_info, symbols, start_date, end_date):
    print(f"\n📥 [Step 2] 正在调用 get_us_his_stock_info_yf 获取 Yahoo 前复权数据...")
    all_records = []
    for sym in symbols:
        try:
            records = ticker_info.get_us_his_stock_info_yf(
                symbol=sym,
                start_date=start_date,
                end_date=end_date,
                auto_adjust=True  # 启用 Yahoo 前复权
            )
            if records:
                all_records.extend(records)
                print(f"  - {sym}: 成功获取 {len(records)} 条日 K 数据")
            else:
                print(f"  - ⚠️ {sym}: 获取数据为空")
        except Exception as e:
            print(f"  - ❌ {sym}: 获取失败，原因: {e}")

    if not all_records:
        raise RuntimeError("未能成功从 yfinance 获取到任何有效的前复权 K 线数据！")

    df_yf = pd.DataFrame(all_records)
    
    # 存入临时 CSV 供后续核验与对齐
    temp_dir = tempfile.gettempdir()
    temp_csv_path = os.path.join(temp_dir, "yf_fqt_temp_compare.csv")
    df_yf.to_csv(temp_csv_path, index=False)
    print(f"💾 Yahoo 前复权数据已暂存至: {temp_csv_path}")

    return df_yf, temp_csv_path


# 3. 对比自定义前复权算法与 yfinance 前复权数据
def compare_kline_data(df_my_fqt, df_yf, actions_path=None):
    print("\n⚖️ [Step 3] 开始核对【自研前复权算法】与【yfinance 官方前复权】数值差异...")

    df_my = df_my_fqt.copy()
    df_yf = df_yf.copy()

    # 1. 规范列名与类型
    cols_to_check = ['open', 'high', 'low', 'close', 'volume']
    for df in [df_my, df_yf]:
        df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        for col in cols_to_check:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 2. 按 (symbol, date) 进行关联对齐
    merged = pd.merge(
        df_my, 
        df_yf, 
        on=['symbol', 'date'], 
        suffixes=('_my', '_yf'), 
        how='inner'
    )

    if merged.empty:
        print("❌ 错误：两组数据在 (symbol, date) 维度未能成功匹配到交集！")
        return

    # 3. 读取复权事件记录 (用于关联除权除息日)
    actions_map = {}
    if actions_path and os.path.exists(actions_path):
        try:
            # 读取 CSV 并确保字符串列没有隐式空格
            df_act = pd.read_csv(actions_path, dtype=str)
            df_act['symbol'] = df_act['symbol'].astype(str).str.strip().str.upper()
            df_act['date'] = pd.to_datetime(df_act['date'].astype(str).str.strip()).dt.strftime('%Y-%m-%d')
            
            # 关键防御 1：显式转换为 float 类型，失败的填入默认值
            df_act['dividend'] = pd.to_numeric(df_act.get('dividend'), errors='coerce').fillna(0.0)
            df_act['split_ratio'] = pd.to_numeric(df_act.get('split_ratio'), errors='coerce').fillna(1.0)
            
            # 关键防御 2：精准判断事件（支持小数拆股 1.025 / 1.01，以及 0.0 现金分红但有拆股的情况）
            has_dividend = df_act['dividend'] > 1e-6
            has_split = ~np.isclose(df_act['split_ratio'], 1.0, atol=1e-5) # 不等于 1.0 (容忍浮点数微小误差)
            
            df_act_valid = df_act[has_dividend | has_split].sort_values('date')

            for sym, group in df_act_valid.groupby('symbol'):
                actions_map[sym] = group['date'].tolist()
                
            print(f"✅ 成功加载 Actions 复权事件：涵盖 {len(actions_map)} 只股票的除权除息记录。")
        except Exception as e:
            print(f"⚠️ 读取复权因子事件文件失败: {e}")

    # 4. 向量化计算各个指标的偏差 (消除迭代循环，大幅提升效率)
    diff_masks = {}
    for col in ['open', 'high', 'low', 'close']:
        abs_diff = np.abs(merged[f"{col}_my"] - merged[f"{col}_yf"])
        rel_diff = abs_diff / (merged[f"{col}_yf"] + 1e-6)
        diff_masks[col] = (abs_diff > 0.02) & (rel_diff > 0.005)

    # 成交量偏差校验
    vol_abs_diff = np.abs(merged['volume_my'] - merged['volume_yf'])
    vol_rel_diff = vol_abs_diff / (merged['volume_yf'] + 1e-6)
    diff_masks['volume'] = (vol_abs_diff > 100) & (vol_rel_diff > 0.01)

    # 汇总任何存在偏差的行
    merged['has_diff'] = False
    for col, mask in diff_masks.items():
        merged[f"diff_{col}"] = mask
        merged['has_diff'] |= mask

    # 5. 按股票维度分析与统计
    all_symbols = sorted(merged['symbol'].unique().tolist())
    total_symbols_count = len(all_symbols)
    
    diff_symbols_df = merged[merged['has_diff']]
    diff_symbols = sorted(diff_symbols_df['symbol'].unique().tolist())
    no_diff_symbols = sorted(list(set(all_symbols) - set(diff_symbols)))
    
    # 6. 构建以股票为核心的差异汇总表
    symbol_reports = []
    for sym in diff_symbols:
        sym_df = merged[merged['symbol'] == sym].sort_values('date')
        sym_diff_df = sym_df[sym_df['has_diff']]

        # 获取事件发生日列表
        act_dates = actions_map.get(sym, [])

        # 区分复权事件发生前/发生后的差异日期范围
        if act_dates:
            min_event_d = act_dates[0]    # 最早事件日
            max_event_d = act_dates[-1]   # 最近事件日
            
            pre_event_diffs = sym_diff_df[sym_diff_df['date'] < min_event_d]['date'].tolist()
            post_event_diffs = sym_diff_df[sym_diff_df['date'] >= min_event_d]['date'].tolist()
            
            pre_range = f"{pre_event_diffs[0]} ~ {pre_event_diffs[-1]}" if pre_event_diffs else "无"
            post_range = f"{post_event_diffs[0]} ~ {post_event_diffs[-1]}" if post_event_diffs else "无"
            
            # 格式化事件日：如果事件过多，截断显示
            if len(act_dates) <= 3:
                event_str = ", ".join(act_dates)
            else:
                event_str = f"{act_dates[0]} ~ {act_dates[-1]} (共{len(act_dates)}次)"
        else:
            diff_dates = sym_diff_df['date'].tolist()
            event_str = "未在Actions中找到"
            pre_range = f"{diff_dates[0]} ~ {diff_dates[-1]}" if diff_dates else "无"
            post_range = "N/A"

        # 各指标异常天数统计
        sym_report = {
            "Symbol": sym,
            "复权事件日": event_str,
            "事件前差异区间": pre_range,
            "事件后差异区间": post_range,
            "Open偏差数": int(sym_df['diff_open'].sum()),
            "High偏差数": int(sym_df['diff_high'].sum()),
            "Low偏差数": int(sym_df['diff_low'].sum()),
            "Close偏差数": int(sym_df['diff_close'].sum()),
            "Volume偏差数": int(sym_df['diff_volume'].sum()),
            "总对比天数": len(sym_df)
        }
        symbol_reports.append(sym_report)

    # 7. 打印全新格式化的校准对比报告
    print("\n" + "="*80)
    print("                 📈 前复权算法校准对比报告                 ")
    print("="*80)
    print(f"📊 对比总数据量 : {len(merged)} 条日 K 线记录")
    print(f"📌 评估股票总数 : {total_symbols_count} 只")
    print(f"✅ 完全一致股票 : {len(no_diff_symbols)} 只 ({', '.join(no_diff_symbols) if no_diff_symbols else '无'})")
    print(f"⚠️ 存在差异股票 : {len(diff_symbols)} 只 ({', '.join(diff_symbols) if diff_symbols else '无'})")
    print("-" * 80)

    if not diff_symbols:
        print("🎉【校验完美通过】：自研前复权算法计算结果与 yfinance 差异率低于 0.5%！")
    else:
        df_summary = pd.DataFrame(symbol_reports)
        print("📌 差异股票事件 & O/H/L/C/V 偏差统计表：\n")
        print(df_summary.to_string(index=False))

        print("\n🔍 典型偏差样例分析 (抽样前 5 处显著偏差)：")
        sample_diffs = []
        for sym in diff_symbols[:3]:
            sub_df = merged[(merged['symbol'] == sym) & merged['has_diff']].head(2)
            for _, r in sub_df.iterrows():
                # 寻找第一个不匹配的指标名
                diff_col = next(c for c in cols_to_check if r[f"diff_{c}"])
                my_val = r[f"{diff_col}_my"]
                yf_val = r[f"{diff_col}_yf"]
                abs_d = abs(my_val - yf_val)
                rel_d = abs_d / (yf_val + 1e-6) * 100
                sample_diffs.append({
                    "Symbol": sym,
                    "Date": r['date'],
                    "异常指标": diff_col,
                    "自研值": round(my_val, 4),
                    "YF官方值": round(yf_val, 4),
                    "绝对误差": round(abs_d, 4),
                    "相对误差": f"{round(rel_d, 2)}%"
                })
        print(pd.DataFrame(sample_diffs).to_string(index=False))

        print("\n💡 排查建议指南：")
        print("1. 若【事件前存在差异】而【事件后完全一致】：说明除权除息日之后数据未受影响，重点排查复权因子的【累乘/累加方向】或【拆股比例分子分母颠倒】。")
        print("2. 若【仅 Volume 存在偏差】：说明价格复权正确，但成交量未按照拆股比例做反向缩放/扩股调整（注意：现金分红通常不触发 Volume 复权调整）。")
        print("3. 若【仅 Close/Open 存在微小偏差】：可能由 Yahoo Finance 现金分红扣税(Net Dividend)或浮点舍入精度导致。")
    print("="*80)

# ----------------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------------
def main():
    ticker_info = TickerInfo(trade_date=END_DATE, market='us')
    em = EMWebCrawlerUti()

    # 1. 筛选股票样本
    sample_symbols = select_sample_tickers(ACTIONS_FILE, min_count=10, max_count=20)

    # 2. 从 yfinance 抓取官方前复权 K 线
    df_yf, _ = fetch_and_save_yf_fqt(
        ticker_info=em,
        symbols=sample_symbols,
        start_date=YF_START,
        end_date=YF_END
    )

    print("\n⚙️ [Step 4] 调用 get_history_data_fqt 执行自研算法前复权还原...")
    
    # 重新获取数据
    df_my_fqt = ticker_info.get_history_data_fqt()
    
    if df_my_fqt is None or df_my_fqt.empty:
        print("❌ 错误: ticker_info.get_history_data_fqt() 返回为空，请检查本地不复权数据文件是否存在！")
        return
    
    # 仅保留本次测试的股票范围
    if 'symbol' in df_my_fqt.columns:
        df_my_fqt = df_my_fqt[df_my_fqt['symbol'].isin(sample_symbols)]

    # 4. 评估差异
    compare_kline_data(df_my_fqt, df_yf)


if __name__ == "__main__":
    main()