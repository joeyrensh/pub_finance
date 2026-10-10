import os
import json
import tempfile
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance.utility.tickerinfo import TickerInfo
from finance.utility.em_stock_uti_fqt import EMWebCrawlerUti
from finance import FINANCE_ROOT

# ----------------------------------------------------------------------
# CONFIG & PATHS
# ----------------------------------------------------------------------
ACTIONS_FILE = FINANCE_ROOT / "usstockinfo/us_stock_actions_history.csv"
START_DATE = "20250101"
END_DATE = "20260916"
YF_START = "2025-01-01"
YF_END = "2026-09-16"

# 临时文件缓存路径
TEMP_DIR = tempfile.gettempdir()
CACHE_SYMBOLS_PATH = os.path.join(TEMP_DIR, "yf_compare_sample_symbols.json")
CACHE_YF_CSV_PATH = os.path.join(TEMP_DIR, "yf_compare_fqt_data.csv")


# 1. 自动筛选覆盖多种复权场景的代表性股票 (10~20只)
def select_sample_tickers(actions_path, min_count=10, max_count=20):
    print("[Step 1] 读取复权因子文件，筛选多元测试股票样本...")
    if not os.path.exists(actions_path):
        raise FileNotFoundError(f"复权因子文件不存在: {actions_path}")

    df_act = pd.read_csv(actions_path)
    df_act['dividend'] = pd.to_numeric(df_act.get('dividend', 0), errors='coerce').fillna(0.0)
    df_act['split_ratio'] = pd.to_numeric(df_act.get('split_ratio', 1), errors='coerce').fillna(1.0)

    # 过滤 2025 年以来的公司变动
    df_act['date_dt'] = pd.to_datetime(df_act['date'].astype(str), errors='coerce')
    df_act_recent = df_act[df_act['date_dt'] >= "2025-01-01"]

    has_div = df_act_recent[df_act_recent['dividend'] > 0]
    has_split = df_act_recent[df_act_recent['split_ratio'] > 1.0]      # 拆股
    has_reverse = df_act_recent[(df_act_recent['split_ratio'] < 1.0) & (df_act_recent['split_ratio'] > 0)] # 合并

    div_symbols = set(has_div['symbol'].unique())
    split_symbols = set(has_split['symbol'].unique())
    reverse_symbols = set(has_reverse['symbol'].unique())

    both_div_split = list(div_symbols.intersection(split_symbols))
    pure_split = list(split_symbols - div_symbols)
    pure_reverse = list(reverse_symbols)
    pure_div = list(div_symbols - split_symbols - reverse_symbols)

    selected = []
    selected.extend(both_div_split[:4])    # 分红+拆股组合
    selected.extend(pure_split[:4])         # 纯拆股
    selected.extend(pure_reverse[:4])       # 纯合并
    selected.extend(pure_div[:6])           # 纯分红

    selected = list(dict.fromkeys(selected))
    if len(selected) < min_count:
        all_symbols = df_act['symbol'].unique().tolist()
        selected.extend([s for s in all_symbols if s not in selected])
        selected = selected[:max_count]
    elif len(selected) > max_count:
        selected = selected[:max_count]

    print(f"成功选择 {len(selected)} 只测试股票: {selected}")
    return selected


# 2. 调用 Yahoo Finance 拉取前复权 K 线并保存
def fetch_and_save_yf_fqt(ticker_info, symbols, start_date, end_date):
    print(f"\n[Step 2] 正在调用 get_us_his_stock_info_yf 获取 Yahoo 前复权数据...")
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
                print(f"  - 警告: {sym} 获取数据为空")
        except Exception as e:
            print(f"  - 错误: {sym} 获取失败，原因: {e}")

    if not all_records:
        raise RuntimeError("未能成功从 yfinance 获取到任何有效的前复权 K 线数据！")

    df_yf = pd.DataFrame(all_records)
    
    # 写入缓存文件
    df_yf.to_csv(CACHE_YF_CSV_PATH, index=False)
    print(f"Yahoo 前复权数据已缓存至: {CACHE_YF_CSV_PATH}")

    return df_yf


# 3. 对比自定义前复权算法与 yfinance 前复权数据
def compare_kline_data(df_my_fqt, df_yf, df_my_raw=None, actions_path=None):
    print("\n[Step 3] 开始核对【自研前复权算法】与【yfinance 官方前复权】数值差异...")

    df_my = df_my_fqt.copy()
    df_yf = df_yf.copy()

    # 1. 规范列名与类型
    cols_to_check = ['open', 'high', 'low', 'close', 'volume']
    for df in [df_my, df_yf]:
        df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        for col in cols_to_check:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 若传入了未复权原始数据，统一格式处理
    if df_my_raw is not None:
        df_my_raw = df_my_raw.copy()
        df_my_raw['symbol'] = df_my_raw['symbol'].astype(str).str.strip().str.upper()
        df_my_raw['date'] = pd.to_datetime(df_my_raw['date']).dt.strftime('%Y-%m-%d')
        for col in cols_to_check:
            df_my_raw[col] = pd.to_numeric(df_my_raw[col], errors='coerce')
        df_my = pd.merge(df_my, df_my_raw, on=['symbol', 'date'], suffixes=('', '_raw'), how='left')

    # 2. 按 (symbol, date) 进行关联对齐
    merged = pd.merge(
        df_my, 
        df_yf, 
        on=['symbol', 'date'], 
        suffixes=('_my', '_yf'), 
        how='inner'
    )

    if merged.empty:
        print("错误：两组数据在 (symbol, date) 维度未能成功匹配到交集！")
        return

    # 3. 读取复权事件记录 (精准修复 Actions 映射)
    actions_map = {}
    actions_detail_map = {}
    if actions_path and os.path.exists(actions_path):
        try:
            df_act = pd.read_csv(actions_path)
            df_act['symbol'] = df_act['symbol'].astype(str).str.strip().str.upper()
            df_act['date'] = pd.to_datetime(df_act['date'].astype(str).str.strip()).dt.strftime('%Y-%m-%d')
            
            df_act['dividend'] = pd.to_numeric(df_act['dividend'], errors='coerce').fillna(0.0)
            df_act['split_ratio'] = pd.to_numeric(df_act['split_ratio'], errors='coerce').fillna(1.0)
            
            valid_mask = (df_act['dividend'] > 1e-6) | (~np.isclose(df_act['split_ratio'], 1.0, atol=1e-5))
            df_act_valid = df_act[valid_mask].sort_values('date')

            for sym, group in df_act_valid.groupby('symbol'):
                actions_map[sym] = group['date'].tolist()
                actions_detail_map[sym] = group.set_index('date').to_dict('index')
                
            print(f"成功加载 Actions 复权事件：涵盖 {len(actions_map)} 只股票的除权除息记录。")
        except Exception as e:
            print(f"警告: 读取复权因子事件文件失败: {e}")

    # 4. 向量化计算各个指标的偏差
    diff_masks = {}
    for col in ['open', 'high', 'low', 'close']:
        abs_diff = np.abs(merged[f"{col}_my"] - merged[f"{col}_yf"])
        rel_diff = abs_diff / (merged[f"{col}_yf"] + 1e-6)
        diff_masks[col] = (abs_diff > 0.02) & (rel_diff > 0.002)

    # 成交量偏差校验
    vol_abs_diff = np.abs(merged['volume_my'] - merged['volume_yf'])
    vol_rel_diff = vol_abs_diff / (merged['volume_yf'] + 1e-6)
    diff_masks['volume'] = (vol_abs_diff > 100) & (vol_rel_diff > 0.01)

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

        act_dates = actions_map.get(sym, [])

        if act_dates:
            min_event_d = act_dates[0]
            max_event_d = act_dates[-1]
            
            pre_event_diffs = sym_diff_df[sym_diff_df['date'] < min_event_d]['date'].tolist()
            post_event_diffs = sym_diff_df[sym_diff_df['date'] >= min_event_d]['date'].tolist()
            
            pre_range = f"{pre_event_diffs[0]} ~ {pre_event_diffs[-1]}" if pre_event_diffs else "无"
            post_range = f"{post_event_diffs[0]} ~ {post_event_diffs[-1]}" if post_event_diffs else "无"
            
            if len(act_dates) <= 3:
                event_str = ", ".join(act_dates)
            else:
                event_str = f"{act_dates[0]} ~ {act_dates[-1]} (共{len(act_dates)}次)"
        else:
            diff_dates = sym_diff_df['date'].tolist()
            event_str = "未在Actions中找到"
            pre_range = f"{diff_dates[0]} ~ {diff_dates[-1]}" if diff_dates else "无"
            post_range = "N/A"

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

    # 7. 打印对比报告
    print("\n" + "="*80)
    print("                 前复权算法校准对比报告                 ")
    print("="*80)
    print(f"对比总数据量 : {len(merged)} 条日 K 线记录")
    print(f"评估股票总数 : {total_symbols_count} 只")
    print(f"完全一致股票 : {len(no_diff_symbols)} 只 ({', '.join(no_diff_symbols) if no_diff_symbols else '无'})")
    print(f"存在差异股票 : {len(diff_symbols)} 只 ({', '.join(diff_symbols) if diff_symbols else '无'})")
    print("-" * 80)

    if not diff_symbols:
        print("【校验完美通过】：自研前复权算法计算结果与 yfinance 差异率低于 0.2%！")
    else:
        df_summary = pd.DataFrame(symbol_reports)
        print("差异股票事件 & O/H/L/C/V 偏差统计表：\n")
        print(df_summary.to_string(index=False))

        # ----------------------------------------------------------------------
        # 8. 增强版：深度故障诊断样本日志输出
        # ----------------------------------------------------------------------
        print("\n" + "="*80)
        print("🔍 [深度诊断] 典型偏差样本全维度剖析 (含原始数据与复权因子)")
        print("="*80)
        
        # 抽取有代表性的偏差股票 (取前 4 只差异股)
        for sym in diff_symbols[:4]:
            sym_df = merged[(merged['symbol'] == sym) & merged['has_diff']].sort_values('date')
            sample_rows = sym_df.head(2) # 每只股票抽 2 条记录
            
            print(f"\n📌 股票: {sym}")
            # 打印该股票关联的所有除权事件
            act_details = actions_detail_map.get(sym, {})
            if act_details:
                act_str_list = [f"[{d}] 分红:{v['dividend']}, 拆股比:{v['split_ratio']}" for d, v in act_details.items()]
                print(f"   └─ 历史复权事件记录: {'; '.join(act_str_list)}")
            else:
                print("   └─ 历史复权事件记录: 无事件")

            for _, r in sample_rows.iterrows():
                # 找出所有存在偏差的指标列
                diff_cols = [c for c in cols_to_check if r[f"diff_{c}"]]
                
                # 计算各种辅助因子
                raw_close = r.get('close_raw', np.nan)
                my_close = r['close_my']
                yf_close = r['close_yf']
                
                raw_vol = r.get('volume_raw', np.nan)
                my_vol = r['volume_my']
                yf_vol = r['volume_yf']

                # 计算缩放比例/因子
                my_price_factor = round(my_close / raw_close, 6) if pd.notna(raw_close) and raw_close > 0 else "N/A"
                yf_price_factor = round(yf_close / raw_close, 6) if pd.notna(raw_close) and raw_close > 0 else "N/A"
                
                my_vol_factor = round(my_vol / raw_vol, 6) if pd.notna(raw_vol) and raw_vol > 0 else "N/A"
                yf_vol_factor = round(yf_vol / raw_vol, 6) if pd.notna(raw_vol) and raw_vol > 0 else "N/A"

                print(f"\n  [日期: {r['date']}] 异常指标: {', '.join(diff_cols)}")
                print(f"  ├─ 价格 (Close): 原始未复权={raw_close} | 自研前复权={my_close} (缩放比:{my_price_factor}) | YF官方={yf_close} (缩放比:{yf_price_factor})")
                print(f"  ├─ 成交量(Vol): 原始未复权={raw_vol} | 自研前复权={my_vol} (缩放比:{my_vol_factor}) | YF官方={yf_vol} (缩放比:{yf_vol_factor})")
                
                # 打印具体的指标误差明细
                for dc in diff_cols:
                    m_v, y_v = r[f"{dc}_my"], r[f"{dc}_yf"]
                    abs_err = abs(m_v - y_v)
                    rel_err = abs_err / (y_v + 1e-6) * 100
                    print(f"  └─ 明细 [{dc.upper()}]: 自研={m_v} vs YF={y_v} | 绝对误差={abs_err:.4f} | 相对误差={rel_err:.2f}%")

    print("\n" + "="*80)


def main(force: bool = False):
    """
    主运行入口
    :param force: 是否强制重新生成抽样股票与刷新 YF 缓存 (True/False)
    """
    em = EMWebCrawlerUti()

    # 检查缓存是否存在
    has_symbols_cache = os.path.exists(CACHE_SYMBOLS_PATH)
    has_yf_csv_cache = os.path.exists(CACHE_YF_CSV_PATH)

    if not force and has_symbols_cache and has_yf_csv_cache:
        print(f"[缓存命中] 成功读取 /tmp 缓存文件，跳过采样与 YF 接口调用。")
        
        # 1. 直接读取股票列表缓存
        with open(CACHE_SYMBOLS_PATH, "r", encoding="utf-8") as f:
            sample_symbols = json.load(f)
        print(f"  └─ 已采纳股票列表 ({len(sample_symbols)}只): {sample_symbols}")

        # 2. 直接读取 YF 数据缓存
        df_yf = pd.read_csv(CACHE_YF_CSV_PATH)
        print(f"  └─ 已采纳 Yahoo 前复权数据 ({len(df_yf)}条记录): {CACHE_YF_CSV_PATH}")

    else:
        print(f"[缓存未命中或启用 force=True] 开始全流程抓取并建立缓存...")
        
        # 1. 筛选股票样本并写入缓存
        sample_symbols = select_sample_tickers(ACTIONS_FILE, min_count=10, max_count=20)
        with open(CACHE_SYMBOLS_PATH, "w", encoding="utf-8") as f:
            json.dump(sample_symbols, f, ensure_ascii=False, indent=2)
        print(f"  └─ 股票列表已缓存至: {CACHE_SYMBOLS_PATH}")

        # 2. 从 yfinance 抓取官方前复权 K 线 (内部会自动写入 CACHE_YF_CSV_PATH)
        df_yf = fetch_and_save_yf_fqt(
            ticker_info=em,
            symbols=sample_symbols,
            start_date=YF_START,
            end_date=YF_END
        )

    print("\n[Step 4] 调用 get_history_data_fqt 执行自研算法前复权还原...")
    
    # 3. 动态实例化 TickerInfo，获取自研前复权数据与未复权原始数据
    ticker_info = TickerInfo(trade_date=END_DATE, market='us')
    df_my_fqt = ticker_info.get_history_data_fqt()
    df_my_raw = ticker_info.get_history_data()
    
    if df_my_fqt is None or df_my_fqt.empty:
        print("错误: ticker_info.get_history_data_fqt() 返回为空，请检查本地数据文件是否存在！")
        return
    
    # 过滤测试样本股票
    if 'symbol' in df_my_fqt.columns:
        df_my_fqt = df_my_fqt[df_my_fqt['symbol'].isin(sample_symbols)]
    if df_my_raw is not None and not df_my_raw.empty and 'symbol' in df_my_raw.columns:
        df_my_raw = df_my_raw[df_my_raw['symbol'].isin(sample_symbols)]

    # 4. 评估差异，将 ACTIONS_FILE 和未复权原始数据传入对比函数
    compare_kline_data(df_my_fqt, df_yf, df_my_raw=df_my_raw, actions_path=ACTIONS_FILE)


if __name__ == "__main__":
    # 默认 force=False，重复运行时秒级完成；需要重新采样并更新数据时设为 main(force=True)
    main(force=False)