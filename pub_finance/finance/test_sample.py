import os
import time
import pandas as pd
import requests


def fetch_eastmoney_ex_symbols(start_date: str, end_date: str) -> set:
    """从东财 API 稳定性最高的 RPT_SHAREBONUS_DET 拉取指定区间的全量除权代码"""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://data.eastmoney.com/",
    }

    filter_str = (
        f"(EX_DIVIDEND_DATE>='{start_date}')(EX_DIVIDEND_DATE<='{end_date}')"
    )

    page_size = 500
    page_number = 1
    raw_ex_symbols = set()

    while True:
        params = {
            "sortColumns": "EX_DIVIDEND_DATE",
            "sortTypes": "-1",
            "pageSize": str(page_size),
            "pageNumber": str(page_number),
            "reportName": "RPT_SHAREBONUS_DET",
            "columns": "SECURITY_CODE,EX_DIVIDEND_DATE",
            "filter": filter_str,
        }

        try:
            res = requests.get(
                url, params=params, headers=headers, timeout=10
            ).json()
            if not res.get("success") or not res.get("result"):
                break

            result = res["result"]
            data_list = result.get("data") or []

            for item in data_list:
                code = str(item.get("SECURITY_CODE", "")).zfill(6)
                if code and code != "000000":
                    raw_ex_symbols.add(code)

            total_pages = result.get("pages") or 1
            if page_number >= total_pages or not data_list:
                break
            page_number += 1

        except Exception as e:
            print(f"[ETL Warning] 拉取第 {page_number} 页除权事件失败: {e}")
            break

    return raw_ex_symbols


def get_local_universe(data_dir: str) -> set:
    """获取本地数据库/目录中已存在的所有标的代码 (包含股票 + ETF)

    例如读取 /root/pub_finance/data/raw/ 下的所有 parquet 或 csv 文件名
    """
    if not os.path.exists(data_dir):
        # 兜底测试示例标的池
        return {
            "600519",
            "000001",
            "510300",
            "159915",
            "588000",
        }

    # 扫描本地 parquet/csv 文件名获取 Universe
    files = os.listdir(data_dir)
    universe = {
        f.split(".")[0].zfill(6)
        for f in files
        if f.endswith((".parquet", ".csv"))
    }
    return universe


def run_weekly_factor_update_pipeline(
    start_date: str, end_date: str, data_dir: str
):
    """周度复权因子更新管道主入口"""
    print(f"=== 开始执行周度复权因子更新 [{start_date} ~ {end_date}] ===")

    # 1. 获取全网本周触发除权的标的列表
    all_ex_symbols = fetch_eastmoney_ex_symbols(start_date, end_date)
    print(f"1. 市场全量触发除权事件的代码数: {len(all_ex_symbols)}")

    # 2. 获取本地存储的监控标的池
    local_universe = get_local_universe(data_dir)
    print(f"2. 本地已监控的标的总数 (Universe): {len(local_universe)}")

    # 3. 求交集：精准锁定本地需要重算因子的标的
    affected_targets = list(all_ex_symbols.intersection(local_universe))
    print(
        f"3. 精准锁定本周需更新因子的本地标的: {len(affected_targets)} 只"
    )

    if affected_targets:
        print(f"受影响标的清单: {affected_targets}")

    # 4. 执行因子更新逻辑
    for symbol in affected_targets:
        # TODO: 读取 symbol 的不复权价格 -> 重新计算 Cumulative Factor -> 覆盖写回 factor.parquet
        pass

    print("=== 周度复权因子更新完成 ===\n")
    return affected_targets


# 测试运行
if __name__ == "__main__":
    # 测试当前周
    run_weekly_factor_update_pipeline(
        start_date="2026-09-14",
        end_date="2026-09-20",
        data_dir="/root/pub_finance/data/raw",  # 替换为你本地的 raw 数据目录
    )