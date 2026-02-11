import pandas as pd
import os
import glob
import csv
import tempfile
from tqdm import tqdm  # 进度条库，可选安装
import akshare as ak  # 爬取股票数据的库，可选安装
from utility.em_stock_uti import EMWebCrawlerUti


class StockDataUpdater:
    def __init__(
        self, data_dir, update_cols, key_cols=["symbol", "date"], batch_size=10000
    ):
        """
        指定特定股票进行历史数据回刷，解决前复权的问题
        :param data_dir: 数据文件目录
        :param update_cols: 需要更新的列名列表
        :param key_cols: 关键列（用于匹配数据行），默认['symbol', 'date']
        :param batch_size: 批次处理大小，默认10000行
        """
        self.data_dir = data_dir
        self.update_cols = update_cols
        self.key_cols = key_cols
        self.batch_size = batch_size
        self.all_files = glob.glob(os.path.join(data_dir, "stock_*.csv"))

        # 验证关键列和更新列不重叠
        if set(key_cols) & set(update_cols):
            raise ValueError("关键列和更新列不能重叠")

    def load_new_data(self, new_data_path):
        """加载新爬取的股票数据"""
        new_df = pd.read_csv(new_data_path)

        # 验证新数据包含必要的列
        required_cols = set(self.key_cols + self.update_cols)
        if not required_cols.issubset(set(new_df.columns)):
            missing = required_cols - set(new_df.columns)
            raise ValueError(f"新数据缺少必要的列: {missing}")

        # 创建查找字典 {(symbol, date): {col: value}}
        return new_df.set_index(self.key_cols)[self.update_cols].to_dict("index")

    def process_files(self, new_data_dict):
        """处理所有文件，逐个替换并生成新文件"""
        for file_path in tqdm(self.all_files, desc="处理文件中"):
            # 生成新文件名
            base_name = os.path.basename(file_path)
            new_file_path = os.path.join(
                self.data_dir, base_name.replace(".csv", "_new.csv")
            )

            # 处理单个文件
            self._process_single_file(file_path, new_file_path, new_data_dict)

    def _process_single_file(self, input_path, output_path, new_data_dict):
        """处理单个文件，分批读取、更新、排序和保存"""
        # 创建临时文件用于存储更新后的数据
        temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv")
        temp_path = temp_file.name

        try:
            # 读取文件头
            with open(input_path, "r") as f:
                try:
                    header = next(csv.reader(f))
                except Exception:
                    print(f"文件 {input_path} 为空，已跳过。")
                    return

            # 写入临时文件头
            temp_writer = csv.DictWriter(temp_file, fieldnames=header)
            temp_writer.writeheader()

            # 分批读取输入文件
            processed_rows = 0
            for chunk in self._read_csv_in_chunks(input_path):
                # 更新当前批次
                updated_chunk = self._update_chunk(chunk, new_data_dict)

                # 修正列名：如果 header[0] == ""，则将 'Unnamed: 0' 列重命名为 ''
                if header[0] == "" and "Unnamed: 0" in updated_chunk.columns:
                    updated_chunk = updated_chunk.rename(columns={"Unnamed: 0": ""})

                # 写入更新后的批次到临时文件
                for _, row in updated_chunk.iterrows():
                    temp_writer.writerow(row.to_dict())

                processed_rows += len(chunk)
                print(f"已处理 {processed_rows} 行...", end="\r")

            # 关闭临时文件
            temp_file.close()

            # 读取临时文件，排序并重置索引
            self._sort_and_save(temp_path, output_path, header)

            print(f"\n文件 {os.path.basename(input_path)} 处理完成")

        finally:
            # 确保删除临时文件
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _read_csv_in_chunks(self, file_path):
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                print(f"文件 {file_path} 为空，已跳过。")
                return
        # 如果首列为空字符串，说明有 idx 列
        if header[0] == "":
            # pandas 会自动命名为 'Unnamed: 0'
            return pd.read_csv(
                file_path,
                chunksize=self.batch_size,
                header=0,
                dtype={col: str for col in self.key_cols},
            )
        else:
            return pd.read_csv(
                file_path,
                chunksize=self.batch_size,
                header=0,
                names=header,
                dtype={col: str for col in self.key_cols},
            )

    def _update_chunk(self, chunk_df, new_data_dict):
        """更新数据块中的行（不新增行）"""
        # 创建临时键列
        chunk_df["temp_key"] = chunk_df.apply(
            lambda row: (str(row[self.key_cols[0]]), str(row[self.key_cols[1]])), axis=1
        )

        # 标记需要更新的行
        chunk_df["needs_update"] = chunk_df["temp_key"].isin(new_data_dict.keys())

        # 更新标记的行
        for idx, row in chunk_df.iterrows():
            if row["needs_update"]:
                key = row["temp_key"]
                for col in self.update_cols:
                    # 只更新存在的列
                    if col in chunk_df.columns:
                        chunk_df.at[idx, col] = new_data_dict[key].get(col, row[col])

        # 清理临时列
        chunk_df.drop(
            columns=["temp_key", "needs_update"], inplace=True, errors="ignore"
        )

        return chunk_df

    def _sort_and_save(self, temp_path, output_path, header):
        """读取临时文件，排序并保存最终结果"""
        # 读取整个临时文件
        full_df = pd.read_csv(temp_path)

        # 按symbol和date排序
        if self.key_cols[0] in full_df.columns and self.key_cols[1] in full_df.columns:
            full_df.sort_values(by=self.key_cols, inplace=True)

        # 只有当没有 idx 列时才重置索引
        if not (header[0] == "" or header[0].lower() == "unnamed: 0"):
            full_df.reset_index(drop=True, inplace=True)

        # 保证列顺序与header一致
        full_df = full_df[[col for col in header if col in full_df.columns]]

        # 写出时手动写header，保证idx列无header
        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            # 让 index 作为首列写出
            for idx, row in full_df.iterrows():
                writer.writerow([idx] + list(row))

    def _ensure_header_order(self, file_path, original_header):
        df = pd.read_csv(file_path)
        # 保持原 header 顺序
        ordered_columns = [col for col in original_header if col in df.columns]
        df = df[ordered_columns]
        # 写出时保留空表头
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(original_header)
            for row in df.itertuples(index=False, name=None):
                writer.writerow(row)

    def replace_old_files_with_new(self):
        """
        将所有 stock_xxx_new.csv 文件重命名为 stock_xxx.csv，覆盖原文件
        """
        new_files = glob.glob(os.path.join(self.data_dir, "stock_*_new.csv"))
        for new_file in new_files:
            old_file = new_file.replace("_new.csv", ".csv")
            os.replace(new_file, old_file)
        print("所有 *_new.csv 文件已重命名为 *.csv")

    def get_latest_updated_data(
        self, symbol_list, start_date, end_date, NEW_DATA_PATH, market
    ):
        em = EMWebCrawlerUti()
        list_s = []
        list = []
        for h in range(0, len(symbol_list)):
            mkt_code = symbol_list[h]["mkt_code"]
            symbol = symbol_list[h]["symbol"]
            list_s = em.get_his_stock_info(
                mkt_code, symbol, start_date, end_date, cache_path=None
            )
            list.extend(list_s)
        df = pd.DataFrame(list)
        df.to_csv(
            NEW_DATA_PATH,
            mode="w",
            index=True,
            header=True,
        )


# 使用示例
if __name__ == "__main__":
    # 配置参数
    DATA_DIR = "./usstockinfo"  # 数据文件目录
    UPDATE_COLS = ["open", "close", "high", "low", "volume"]  # 需要更新的列
    NEW_DATA_PATH = "./usstockinfo/new_stock_data.csv"  # 新爬取的数据文件
    BATCH_SIZE = 10000  # 每批处理的行数
    """每股列表，需要重新匹配market code"""
    # 美股如下
    # https://92.push2his.eastmoney.com/api/qt/stock/kline/get?secid=106.BABA&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20210101&end=20500101&smplmt=755&lmt=1000000
    symbol_list = [{"symbol": "NOW", "mkt_code": 106}]  # 示例股票代码列表
    # A股如下 - SZ:0 / SH:1
    # 例如：symbol_list = [{"symbol": "000001", "mkt_code": 0}, {"symbol": "600000", "mkt_code": 1}]

    # # 创建更新器
    updater = StockDataUpdater(DATA_DIR, UPDATE_COLS, batch_size=BATCH_SIZE)
    updater.get_latest_updated_data(
        symbol_list, "20240101", "20260210", NEW_DATA_PATH, market="us"
    )

    # 加载新数据到字典
    try:
        new_data_dict = updater.load_new_data(NEW_DATA_PATH)
        print(f"加载了 {len(new_data_dict)} 条新数据记录")
    except Exception as e:
        print(f"加载新数据失败: {e}")
        exit(1)

    # 处理所有文件
    updater.process_files(new_data_dict)
    # 全部无异常后，重命名
    updater.replace_old_files_with_new()

    print("所有文件处理完成！新文件已保存为 *_new.csv 格式")
