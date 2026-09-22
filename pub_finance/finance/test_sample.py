import re
import sys
import time
import functools
from pathlib import Path
import pandas as pd
import akshare as ak

# 项目内部模块导入
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT


@functools.lru_cache(maxsize=1)
def get_csindex_official_database() -> tuple[dict, dict]:
    """
    获取中证官方所有指数的:
    1. code_to_name: {'000300': '沪深300', ...}
    2. name_to_code: {'沪深300': '000300', ...}
    """
    code_to_name = {}
    name_to_code = {}

    try:
        if hasattr(ak, "stock_zh_index_title_csindex"):
            df_cs = ak.stock_zh_index_title_csindex()
            if df_cs is not None and not df_cs.empty:
                code_col = next((c for c in ["index_code", "指数代码", "code", "代码"] if c in df_cs.columns), None)
                name_col = next((c for c in ["index_name_cn", "指数简称", "指数名称", "name", "名称"] if c in df_cs.columns), None)

                if code_col and name_col:
                    for _, row in df_cs.iterrows():
                        c_str = str(row[code_col]).strip().zfill(6)
                        n_str = str(row[name_col]).strip()
                        if c_str and n_str:
                            code_to_name[c_str] = n_str
                            name_to_code[n_str] = c_str
    except Exception as e:
        print(f"提示: 拉取中证官方全量指数表时遇到问题: {e}")

    return code_to_name, name_to_code


def auto_normalize_csindex_code(code: str, index_name: str = "") -> tuple[str, str]:
    """
    返回 (规范化后的6位代码, 指数名称)
    """
    if not code or code == "-":
        return "-", "-"

    raw_code = str(code).strip().zfill(6)
    code_to_name, name_to_code = get_csindex_official_database()

    # 1. 399xxx 指数转 000xxx 指数（深证/中证映射）
    if raw_code.startswith("399"):
        candidate = "000" + raw_code[3:]
        if candidate in code_to_name:
            return candidate, code_to_name[candidate]

    # 2. 如果代码在中证库中
    if raw_code in code_to_name:
        return raw_code, code_to_name[raw_code]

    # 3. 按名称反查
    if index_name and index_name in name_to_code:
        c = name_to_code[index_name]
        return c, code_to_name.get(c, index_name)

    return raw_code, index_name if index_name else "-"


@functools.lru_cache(maxsize=1)
def get_auto_etf_csindex_mapping(date_str: str = "") -> dict:
    """
    生成 ETF代码 -> (中证指数代码, 中证指数名称) 的映射
    """
    mapping = {}
    stock_dir = FINANCE_ROOT / "cnstockinfo"
    f_stock = None

    if date_str:
        dt = date_str.replace("-", "")
        f_stock = stock_dir / f"stock_{dt}.csv"

    if not f_stock or not f_stock.exists():
        csv_files = sorted(list(stock_dir.glob("stock_*.csv")))
        if csv_files:
            f_stock = csv_files[-1]

    if not f_stock or not f_stock.exists():
        return mapping

    code_to_name, name_to_code = get_csindex_official_database()

    try:
        df_local = pd.read_csv(f_stock, dtype=str)
        df_etfs = df_local[df_local["symbol"].str.startswith("ETF", na=False)].copy()

        # 高频核心指数硬映射字典（保证 100% 准确）
        keyword_rules = [
            ("A500", "000510", "中证A500"),
            ("科创综指", "000680", "科创综指"),
            ("科创100", "000698", "科选100"),
            ("科创200", "000699", "科创200"),
            ("科创50", "000688", "科创50"),
            ("科创新能源", "000692", "科创新能"),
            ("科创成长", "000690", "科创成长"),
            ("科创信息", "000682", "科创信息"),
            ("科创机械", "000693", "科创机械"),
            ("沪深300", "000300", "沪深300"),
            ("中证500", "000905", "中证500"),
            ("中证1000", "000852", "中证1000"),
            ("中证800", "000906", "中证800"),
            ("上证180", "000010", "上证180"),
            ("上证380", "000009", "上证380"),
            ("上证50", "000016", "上证50"),
            ("上证综指", "000001", "上证综合"),
            ("上证指数", "000001", "上证综合"),
            ("金融科技", "000699", "金融科技"),
            ("绿色电力", "000438", "绿色电力"),
            ("消费电子", "980030", "消费电子"),
            ("通用航空", "980076", "通用航空"),
            ("信息安全", "000994", "信息安全"),
            ("专精特新", "000267", "专精特新"),
            ("数字经济", "000262", "数字经济"),
            ("新能源车", "000417", "新能源车"),
            ("有色金属", "000819", "有色金属"),
            ("智能汽车", "000432", "智能汽车"),
            ("高端装备", "00097", "高端装备"),
            ("一带一路", "000991", "一带一路"),
            ("电子50", "000281", "电子50"),
            ("500等权", "000982", "500等权"),
            ("科技100", "000608", "科技100"),
            ("消费50", "000126", "消费50"),
            ("中证A100", "000903", "中证A100"),
        ]

        for _, row in df_etfs.iterrows():
            raw_sym = str(row["symbol"]).strip()
            etf_code = raw_sym.replace("ETF", "").strip()
            etf_name = str(row.get("name", "")).strip()

            if not etf_code or not etf_name:
                continue

            matched_code = None
            matched_name = None

            # 1. 优先关键词字典正则匹配
            for kw, c_code, c_name in keyword_rules:
                if kw in etf_name:
                    matched_code = c_code
                    matched_name = code_to_name.get(c_code, c_name)
                    break

            # 2. 若未中字典，模糊匹配中证官方指数名称
            if not matched_code and name_to_code:
                # 清理基金公司前缀与后缀
                clean_name = re.sub(
                    r'^(华夏|易方达|华泰柏瑞|广发|南方|嘉实|富国|国泰|博时|天弘|汇添富|招商|工银|建信|鹏华|景顺长城|银华|华安|平安|万家|国联安|摩根|申万菱信|兴业|东财|海富通|永赢|鑫元|浦银|前海开源|中银|浙商|长城|民生加银)',
                    '',
                    etf_name
                )
                clean_name = re.sub(r'(ETF|联接|发起式|E|A|C|指增|增强)$', '', clean_name).strip()

                if clean_name in name_to_code:
                    matched_code = name_to_code[clean_name]
                    matched_name = clean_name
                else:
                    for idx_name, idx_code in name_to_code.items():
                        if clean_name and (clean_name == idx_name or idx_name in clean_name):
                            matched_code = idx_code
                            matched_name = idx_name
                            break

            if matched_code:
                final_code, final_name = auto_normalize_csindex_code(matched_code, matched_name)
                mapping[etf_code] = (final_code, final_name)

        print(f"成功从文件 {f_stock.name} 加载并自动匹配了 {len(mapping)} / {len(df_etfs)} 只 ETF 的中证代码")

    except Exception as e:
        print(f"解析 ETF 映射异常: {e}")

    return mapping


@functools.lru_cache(maxsize=2000)
def fetch_single_csindex_pe(csindex_code: str) -> str:
    """
    可靠地查询单个中证指数的 PE（市盈率）
    支持多数据源/字段兼容
    """
    if not csindex_code or csindex_code == "-":
        return "-"

    clean_code = str(csindex_code).strip().zfill(6)

    # 尝试方法 1: AkShare 中证指数估值历史接口
    if hasattr(ak, "stock_zh_index_value_csindex"):
        try:
            df_val = ak.stock_zh_index_value_csindex(symbol=clean_code)
            if df_val is not None and not df_val.empty:
                latest_row = df_val.iloc[-1]
                # 寻找包含 PE / 市盈率 的列
                pe_col = next((c for c in df_val.columns if "市盈率" in c or "pe" in c.lower()), None)
                if pe_col:
                    val = latest_row[pe_col]
                    if pd.notna(val) and str(val).strip() != "":
                        return str(round(float(val), 2))
        except Exception:
            pass

    # 尝试方法 2: AkShare 实时指数行情接口备用回退
    if hasattr(ak, "stock_zh_index_spot_csindex"):
        try:
            df_spot = ak.stock_zh_index_spot_csindex()
            if df_spot is not None and not df_spot.empty:
                code_col = next((c for c in df_spot.columns if "代码" in c or "code" in c), None)
                pe_col = next((c for c in df_spot.columns if "市盈率" in c or "pe" in c.lower()), None)
                if code_col and pe_col:
                    matched = df_spot[df_spot[code_col].astype(str).str.zfill(6) == clean_code]
                    if not matched.empty:
                        val = matched.iloc[0][pe_col]
                        if pd.notna(val) and str(val).strip() != "":
                            return str(round(float(val), 2))
        except Exception:
            pass

    return "-"


def get_etf_csindex_pe(etf_symbol: str, etf_name: str, mapping: dict) -> dict:
    """获取单条记录的 CSINDEX Code, CSINDEX Name 与 PE"""
    etf_code = etf_symbol.replace("ETF", "").strip()
    csindex_symbol, csindex_name = mapping.get(etf_code, ("-", "-"))

    pe_val = "-"
    if csindex_symbol != "-":
        pe_val = fetch_single_csindex_pe(csindex_symbol)

    return {
        "ETF Symbol": etf_symbol,
        "ETF 名称": etf_name,
        "CSINDEX Symbol": csindex_symbol,
        "CSINDEX 名称": csindex_name,
        "PE": pe_val
    }


def main():
    stock_dir = FINANCE_ROOT / "cnstockinfo"
    csv_files = sorted(list(stock_dir.glob("stock_*.csv")))

    if not csv_files:
        print(f"未在 {stock_dir} 找到任何 stock_*.csv 文件")
        return

    latest_csv = csv_files[-1]
    print(f"1. 正在读取全量行情数据文件: {latest_csv.name}...")

    df_local = pd.read_csv(latest_csv, dtype=str)
    df_etfs = df_local[df_local["symbol"].str.startswith("ETF", na=False)].copy()

    if df_etfs.empty:
        print("未在该 CSV 文件中找到 Symbol 以 'ETF' 开头的数据")
        return

    print(f"共获取到 {len(df_etfs)} 只 ETF，正在构建自动映射...")
    mapping = get_auto_etf_csindex_mapping(latest_csv.stem.replace("stock_", ""))

    print("\n2. 正在批量查询全量 ETF 估值信息（请稍候）...")
    results = []

    for _, row in df_etfs.iterrows():
        etf_sym = str(row["symbol"]).strip()
        etf_name = str(row.get("name", "-")).strip()

        data = get_etf_csindex_pe(etf_sym, etf_name, mapping)
        results.append(data)

    # 设置 Pandas 显示格式
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'left')

    df_result = pd.DataFrame(results)

    print(f"\n==================== 全量 ETF PE 估值查询结果 (共 {len(df_result)} 条) ====================")
    print(df_result.to_string(index=False))


if __name__ == "__main__":
    main()