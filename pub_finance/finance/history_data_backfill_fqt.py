#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import csv
import glob
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Dict, List, Set

import pandas as pd
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import FINANCE_ROOT
from finance.utility.em_stock_uti_fqt import EMWebCrawlerUti


class StockDataUpdater:
    def __init__(
        self, data_dir: Path, update_cols: List[str], key_cols: List[str] = ["symbol", "date"], batch_size: int = 10000
    ):
        """
        指定特定股票进行历史数据回刷，解决不复权/复权纠偏问题
        :param data_dir: 数据文件目录
        :param update_cols: 需要更新的列名列表
        :param key_cols: 关键列（用于匹配数据行），默认 ['symbol', 'date']
        :param batch_size: 批次处理大小，默认 10000 行
        """
        self.data_dir = Path(data_dir)
        self.update_cols = update_cols
        self.key_cols = key_cols
        self.batch_size = batch_size
        
        # 排除 .bk 备份文件与 _new.csv 文件
        raw_files = sorted(glob.glob(os.path.join(self.data_dir, "stock_*.csv")))
        self.all_files = [f for f in raw_files if not f.endswith(".bk")]

        # 验证关键列和更新列不重叠
        if set(key_cols) & set(update_cols):
            raise ValueError("关键列和更新列不能重叠")

    def get_latest_stock_file(self) -> Path:
        """获取 data_dir 目录下修改时间最新的 stock_*.csv 文件（排除 _new 和 .bk）"""
        files = [
            Path(f) for f in self.all_files 
            if not f.endswith("_new.csv") and not f.endswith(".bk")
        ]
        if not files:
            raise FileNotFoundError(f"未在目录 {self.data_dir} 下找到任何 stock_*.csv 文件")
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        print(f"📌 自动从最新数据文件提取 Symbol 列表: {latest_file.name}")
        return latest_file

    def get_symbols_from_latest_file(self) -> List[str]:
        """从最新的股票数据文件中提取去重后的 Symbol 列表"""
        latest_file = self.get_latest_stock_file()
        df = pd.read_csv(latest_file, usecols=lambda c: c.lower() in ["symbol"])
        col_name = next(c for c in df.columns if c.lower() == "symbol")
        symbols = (
            df[col_name]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .unique()
            .tolist()
        )
        print(f"✅ 成功从 {latest_file.name} 中提取到 {len(symbols)} 个 Symbol")
        return sorted(symbols)

    def fetch_yfinance_data(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        new_data_path: Path,
        max_retries: int = 3,
        symbol_batch_size: int = 100,  # 每抓取 100 只股票追加写入一次 CSV
    ):
        """使用 yfinance 方法按批次（Batch）拉取不复权历史数据，支持分批低内存检查与断点续抓"""
        em = EMWebCrawlerUti()

        fetched_symbols: Set[str] = set()
        
        if new_data_path.exists() and new_data_path.stat().st_size > 0:
            print(f"🔍 检测到已存在缓存文件 {new_data_path.name}，正在分批扫描已抓取的 Symbol...")
            try:
                for chunk in pd.read_csv(
                    new_data_path,
                    usecols=lambda c: c.lower() in ["symbol"],
                    chunksize=50000,
                    dtype=str,
                ):
                    col_name = next(c for c in chunk.columns if c.lower() == "symbol")
                    unique_in_chunk = (
                        chunk[col_name]
                        .dropna()
                        .str.strip()
                        .str.upper()
                        .unique()
                    )
                    fetched_symbols.update(unique_in_chunk)

                print(f"✅ 成功提取到已完成的 {len(fetched_symbols)} 个 Symbol，将自动启用断点续抓。")
            except Exception as e:
                print(f"⚠️ 解析缓存文件失败 ({e})，将重置临时文件并重新抓取。")
                new_data_path.unlink()
                fetched_symbols.clear()

        is_first_write = not (new_data_path.exists() and new_data_path.stat().st_size > 0)
        remaining_symbols = [s for s in symbols if s.upper() not in fetched_symbols]
        skipped_count = len(symbols) - len(remaining_symbols)

        if skipped_count > 0:
            print(f"⏭️ 自动跳过已抓取的 {skipped_count} 个 Symbol，剩余 {len(remaining_symbols)} 个 Symbol 待抓取。")

        if not remaining_symbols:
            print(f"🎉 所有 {len(symbols)} 个 Symbol 均已抓取完毕，跳过 API 请求流程。")
            return

        print(f"🚀 开始通过 yfinance 分批抓取历史数据 ({start_date} ~ {end_date})...")

        batch_buffer = []
        total_fetched_count = 0
        total_remaining = len(remaining_symbols)

        for idx, symbol in enumerate(remaining_symbols, 1):
            symbol_data = None
            for retry in range(max_retries):
                try:
                    symbol_data = em.get_us_his_stock_info_yf(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        cache_path=None,
                    )

                    if symbol_data:
                        batch_buffer.extend(symbol_data)
                        print(f"[{idx}/{total_remaining}] Symbol: {symbol} | 成功获取 {len(symbol_data)} 条数据")
                        break

                    print(f"  ⚠️ [Symbol: {symbol}] 数据返回为空，进行第 {retry + 1} 次重试...")
                except Exception as e:
                    print(f"  ❌ [Symbol: {symbol}] 请求异常: {e}，进行第 {retry + 1} 次重试...")

            if len(batch_buffer) >= symbol_batch_size or idx == total_remaining:
                if batch_buffer:
                    df_batch = pd.DataFrame(batch_buffer)
                    
                    required_cols = set(self.key_cols + self.update_cols)
                    missing_cols = required_cols - set(df_batch.columns)
                    if missing_cols:
                        raise ValueError(f"抓取的数据缺少以下必要字段: {missing_cols}")

                    df_batch.to_csv(
                        new_data_path,
                        mode="a",
                        index=False,
                        header=is_first_write,
                        encoding="utf-8"
                    )
                    
                    total_fetched_count += len(batch_buffer)
                    is_first_write = False
                    batch_buffer.clear()
                    print(f"💾 [Progress] 已累积追加保存 {total_fetched_count} 条记录至 {new_data_path.name}")

        if not new_data_path.exists() or new_data_path.stat().st_size == 0:
            raise RuntimeError("未抓取到任何有效数据，终止后续更新流程。")

        print(f"🎉 数据抓取落盘完成，文件位置: {new_data_path}")

    def load_new_data(self, new_data_path: Path) -> Dict:
        """加载新爬取的股票数据并构建快速查找字典"""
        new_df = pd.read_csv(new_data_path, dtype={col: str for col in self.key_cols})

        for col in self.update_cols:
            if col in new_df.columns:
                new_df[col] = pd.to_numeric(new_df[col], errors="coerce")

        new_df = new_df.drop_duplicates(subset=self.key_cols, keep="last")
        return new_df.set_index(self.key_cols)[self.update_cols].to_dict("index")

    def process_files(self, new_data_dict: Dict):
        """处理所有历史数据文件，逐个替换更新并生成 _new.csv"""
        for file_path in tqdm(self.all_files, desc="回刷历史 CSV 文件中"):
            if file_path.endswith("_new.csv") or file_path.endswith(".bk"):
                continue

            base_name = os.path.basename(file_path)
            new_file_path = os.path.join(
                self.data_dir, base_name.replace(".csv", "_new.csv")
            )

            self._process_single_file(file_path, new_file_path, new_data_dict)

    def _process_single_file(self, input_path: str, output_path: str, new_data_dict: Dict):
        # 精确读取原始文件的 Header 字符串列表
        with open(input_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                raw_header = next(reader)
            except StopIteration:
                print(f"文件 {input_path} 为空，跳过。")
                return

        existing_keys = set()
        file_dates = set()

        # 扫描原有文件 Key 集合
        for chunk in self._read_csv_in_chunks(input_path):
            if chunk is None or chunk.empty:
                continue
            keys = set(zip(chunk[self.key_cols[0]].astype(str), chunk[self.key_cols[1]].astype(str)))
            existing_keys.update(keys)
            dates_in_chunk = set(chunk[self.key_cols[1]].dropna().astype(str).unique())
            file_dates.update(dates_in_chunk)

        if not file_dates:
            print(f"文件 {input_path} 无有效日期，跳过")
            return

        filtered_data = {
            (symbol, date_str): values
            for (symbol, date_str), values in new_data_dict.items()
            if date_str in file_dates
        }

        if not filtered_data:
            print(f"文件 {os.path.basename(input_path)} 无匹配的新数据日期")
            return

        update_dict = {}
        append_dict = {}
        for key, values in filtered_data.items():
            if key in existing_keys:
                update_dict[key] = values
            else:
                append_dict[key] = values

        print(
            f"文件 {os.path.basename(input_path)}: 待更新 {len(update_dict)} 行, 待追加 {len(append_dict)} 行"
        )

        temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", newline="", encoding="utf-8")
        temp_path = temp_file.name
        try:
            is_first_chunk = True
            for chunk in self._read_csv_in_chunks(input_path):
                if chunk is None or chunk.empty:
                    continue
                updated_chunk = self._update_chunk_with_dict(chunk, update_dict)
                updated_chunk.to_csv(
                    temp_path,
                    mode="a",
                    index=False,
                    header=is_first_chunk,
                    encoding="utf-8"
                )
                is_first_chunk = False

            if append_dict:
                data_columns = [c for c in raw_header if c != ""]
                new_rows_df = self._build_new_rows_df(append_dict, data_columns)
                new_rows_df.to_csv(
                    temp_path,
                    mode="a",
                    index=False,
                    header=is_first_chunk,
                    encoding="utf-8"
                )

            temp_file.close()
            self._sort_and_save(temp_path, output_path, raw_header)

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _read_csv_in_chunks(self, file_path: str):
        """低内存分块读取 CSV 文件，自动跳过第一列无名索引"""
        return pd.read_csv(
            file_path,
            chunksize=self.batch_size,
            index_col=0 if self._has_unnamed_index(file_path) else None,
            dtype={col: str for col in self.key_cols},
        )

    def _has_unnamed_index(self, file_path: str) -> bool:
        """检查 CSV 文件首列是否为无名索引列"""
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
                return len(header) > 0 and header[0] == ""
            except StopIteration:
                return False

    def _update_chunk_with_dict(self, chunk_df: pd.DataFrame, update_dict: Dict) -> pd.DataFrame:
        if not update_dict:
            return chunk_df
        
        temp_keys = zip(
            chunk_df[self.key_cols[0]].astype(str),
            chunk_df[self.key_cols[1]].astype(str)
        )
        
        for idx, key in zip(chunk_df.index, temp_keys):
            if key in update_dict:
                vals = update_dict[key]
                for col, new_val in vals.items():
                    if col in chunk_df.columns:
                        chunk_df.at[idx, col] = new_val

        return chunk_df

    def _build_new_rows_df(self, append_dict: Dict, columns: List[str]) -> pd.DataFrame:
        rows = []
        for (symbol, date), values in append_dict.items():
            row = {col: None for col in columns}
            row[self.key_cols[0]] = symbol
            row[self.key_cols[1]] = date
            for col, val in values.items():
                if col in row:
                    row[col] = val
            rows.append(row)
        return pd.DataFrame(rows)[columns]

    def _sort_and_save(self, temp_path: str, output_path: str, raw_header: List[str]):
        """全量加载临时文件，按 key_cols 排序，并完美重建原始 Header 与递增索引列"""
        full_df = pd.read_csv(temp_path, dtype={col: str for col in self.key_cols})

        if self.key_cols[0] in full_df.columns and self.key_cols[1] in full_df.columns:
            full_df.sort_values(by=self.key_cols, ascending=[True, True], inplace=True)

        full_df.reset_index(drop=True, inplace=True)
        has_unnamed_first_col = len(raw_header) > 0 and raw_header[0] == ""

        if has_unnamed_first_col:
            full_df.to_csv(output_path, index=True, index_label="", encoding="utf-8")
        else:
            full_df.to_csv(output_path, index=False, encoding="utf-8")

    def replace_old_files_with_new(self):
        """备份原始 stock_xxx.csv 为 stock_xxx.csv.bk，并将 stock_xxx_new.csv 覆写回原文件"""
        new_files = glob.glob(os.path.join(self.data_dir, "stock_*_new.csv"))
        
        if not new_files:
            print("⚠️ 未找到任何待更新的 _new.csv 文件。")
            return

        for new_file in new_files:
            old_file = new_file.replace("_new.csv", ".csv")
            bk_file = f"{old_file}.bk"

            # 1. 备份原文件
            if os.path.exists(old_file):
                shutil.copy2(old_file, bk_file)
                print(f"📦 已备份原文件: {os.path.basename(old_file)} -> {os.path.basename(bk_file)}")

            # 2. 用 _new.csv 覆盖原文件
            os.replace(new_file, old_file)

        print("所有新生成的文件更名成功，原文件已成功备份为 .bk 并完成替换覆盖！")


if __name__ == "__main__":
    MARKET = "us"
    DATA_DIR = FINANCE_ROOT / f"{MARKET}stockinfo"
    UPDATE_COLS = ["open", "close", "high", "low", "volume"]
    NEW_DATA_PATH = DATA_DIR / "new_stock_data.csv"
    START_DATE = "20260915"
    END_DATE = "20260916"

    updater = StockDataUpdater(data_dir=DATA_DIR, update_cols=UPDATE_COLS, batch_size=10000)

    symbol_list = updater.get_symbols_from_latest_file()

    updater.fetch_yfinance_data(
        symbols=symbol_list,
        start_date=START_DATE,
        end_date=END_DATE,
        new_data_path=NEW_DATA_PATH,
    )

    try:
        new_data_dict = updater.load_new_data(NEW_DATA_PATH)
        print(f"📖 成功加载了 {len(new_data_dict)} 条待回刷的差异记录")
    except Exception as e:
        print(f"❌ 加载新数据失败: {e}")
        sys.exit(1)

    updater.process_files(new_data_dict)
    updater.replace_old_files_with_new()

    print("✨ 全部历史数据回刷工作完美结束！")