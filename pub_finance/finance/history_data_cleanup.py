import os
import pandas as pd
import glob
import tempfile
from datetime import datetime
from finance.paths import FINANCE_ROOT


def clean_stock_data_pandas_batch(
    stock_files, delete_before_date="2024-01-01", chunk_size=10000
):
    """
    使用pandas分批处理大文件的数据清理程序

    参数:
    delete_before_date: 删除早于此日期的数据，格式为'YYYY-MM-DD'
    chunk_size: 每次处理的数据块大小，默认10000行
    """
    try:
        cutoff_date = datetime.strptime(delete_before_date, "%Y-%m-%d")
    except ValueError:
        print("错误: 日期格式不正确，请使用'YYYY-MM-DD'格式")
        return

    if not stock_files:
        print("未找到任何stock_xxx.csv文件")
        return

    print(f"找到 {len(stock_files)} 个stock文件，分块大小: {chunk_size} 行")

    processed_files = 0
    deleted_files = 0
    cleaned_files = 0

    for file_path in stock_files:
        temp_file = None
        try:
            print(f"\n正在处理文件: {file_path}")

            # 首先检查文件是否为空
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                print(f"文件 {file_path} 为空，删除文件")
                os.remove(file_path)
                deleted_files += 1
                continue

            # 创建临时文件
            temp_file = tempfile.NamedTemporaryFile(
                mode="w",
                newline="",
                encoding="utf-8",
                delete=False,
                suffix=".csv",
                dir=os.path.dirname(file_path),
            )
            temp_path = temp_file.name
            temp_file.close()  # 关闭文件，pandas会重新打开

            # 使用pandas分块读取和处理
            total_rows = 0
            kept_rows = 0
            first_chunk = True

            # 分块读取CSV文件
            chunk_iterator = pd.read_csv(file_path, chunksize=chunk_size)

            for i, chunk in enumerate(chunk_iterator):
                total_rows += len(chunk)

                # 确保date列是datetime类型
                try:
                    chunk["date"] = pd.to_datetime(chunk["date"])
                except Exception as e:
                    print(f"警告: 在处理块 {i + 1} 时日期转换出错: {str(e)}")
                    # 如果日期转换失败，保留整个块
                    chunk_clean = chunk
                else:
                    # 筛选出需要保留的数据（日期不早于指定日期）
                    mask = chunk["date"] >= cutoff_date
                    chunk_clean = chunk[mask]

                kept_rows += len(chunk_clean)

                # 写入处理后的数据块
                if first_chunk:
                    # 第一个块写入表头
                    chunk_clean.to_csv(temp_path, mode="w", index=False)
                    first_chunk = False
                else:
                    # 后续块追加，不写入表头
                    chunk_clean.to_csv(temp_path, mode="a", header=False, index=False)

                print(f"  - 已处理 {total_rows} 行，保留 {kept_rows} 行")

            # 判断处理结果
            if kept_rows == 0:
                print(f"文件 {file_path}: 所有 {total_rows} 条记录都被删除，删除文件")
                os.remove(file_path)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                deleted_files += 1
            elif kept_rows < total_rows:
                print(
                    f"文件 {file_path}: 从 {total_rows} 条记录清理到 {kept_rows} 条记录"
                )
                # 用临时文件替换原文件
                os.replace(temp_path, file_path)
                cleaned_files += 1
            else:
                print(f"文件 {file_path}: 所有 {total_rows} 条记录均无需清理")
                # 删除临时文件，保留原文件
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                processed_files += 1

        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {str(e)}")
            # 清理临时文件（如果存在）
            if temp_file and os.path.exists(temp_file.name):
                os.remove(temp_file.name)

    # 输出总结
    print("\n处理完成!")
    print(f"处理文件总数: {len(stock_files)}")
    print(f"完全删除的文件: {deleted_files}")
    print(f"部分清理的文件: {cleaned_files}")
    print(f"无需更改的文件: {processed_files}")


def preview_clean_effect_pandas(
    stock_files, delete_before_date="2024-01-01", sample_size=5000
):
    """
    预览清理效果，使用pandas进行高效抽样

    参数:
    delete_before_date: 删除早于此日期的数据
    sample_size: 抽样大小，默认5000行
    """
    try:
        cutoff_date = datetime.strptime(delete_before_date, "%Y-%m-%d")
    except ValueError:
        print("错误: 日期格式不正确，请使用'YYYY-MM-DD'格式")
        return

    if not stock_files:
        print("未找到任何stock_xxx.csv文件")
        return

    print(f"找到 {len(stock_files)} 个stock文件")
    print(f"预览删除早于 {delete_before_date} 的数据效果:\n")

    for file_path in stock_files:
        try:
            # 获取文件大小以估算总行数
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                print(f"🗑️ {file_path}: 空文件 (将被删除)")
                continue

            # 使用pandas读取前几行进行预览
            df_sample = pd.read_csv(file_path, nrows=sample_size)

            if df_sample.empty:
                print(f"🗑️ {file_path}: 无数据 (将被删除)")
                continue

            # 确保date列是datetime类型
            try:
                df_sample["date"] = pd.to_datetime(df_sample["date"])
            except Exception as e:
                print(f"❌ {file_path}: 日期格式错误 - {str(e)}")
                continue

            # 统计符合条件的数据
            mask = df_sample["date"] >= cutoff_date
            kept_count = mask.sum()
            total_count = len(df_sample)

            # 估算总行数
            if total_count > 0:
                estimated_total = int(
                    file_size / (os.path.getsize(file_path) / total_count)
                )
            else:
                estimated_total = 0

            if kept_count == 0:
                print(
                    f"🗑️ {file_path}: 所有约 {estimated_total} 条记录都将被删除 (文件将被删除)"
                )
            elif kept_count < total_count:
                deleted_count = total_count - kept_count
                # 估算总删除数
                estimated_deleted = int(deleted_count * (estimated_total / total_count))
                estimated_kept = estimated_total - estimated_deleted
                print(
                    f"🧹 {file_path}: 预计将删除约 {estimated_deleted} 条记录，保留约 {estimated_kept} 条记录"
                )
            else:
                print(f"✅ {file_path}: 所有约 {estimated_total} 条记录均需要保留")

        except Exception as e:
            print(f"❌ {file_path}: 读取错误 - {str(e)}")


def get_file_stats(stock_files):
    """
    获取所有stock文件的统计信息
    """
    if not stock_files:
        print("未找到任何stock_xxx.csv文件")
        return

    print(f"找到 {len(stock_files)} 个stock文件")
    print("\n文件统计信息:")
    print("-" * 60)

    total_size = 0
    total_rows = 0

    for file_path in stock_files:
        try:
            file_size = os.path.getsize(file_path)
            total_size += file_size

            # 使用pandas读取文件获取行数
            df = pd.read_csv(file_path, nrows=1)  # 只读取一行来检查结构
            row_count = sum(1 for line in open(file_path)) - 1  # 减去表头

            total_rows += row_count

            print(
                f"{os.path.basename(file_path):<20} | 大小: {file_size / 1024 / 1024:.2f} MB | 行数: {row_count:>8}"
            )

        except Exception as e:
            print(f"{os.path.basename(file_path):<20} | 读取错误: {str(e)}")

    print("-" * 60)
    print(
        f"{'总计':<20} | 大小: {total_size / 1024 / 1024:.2f} MB | 行数: {total_rows:>8}"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="清理股票数据文件")
    parser.add_argument(
        "--date",
        type=str,
        default="2024-01-01",
        help="删除早于此日期的数据，格式: YYYY-MM-DD",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=10000, help="分块处理的大小，默认10000行"
    )
    parser.add_argument(
        "--preview", action="store_true", help="仅预览清理效果，不执行实际清理"
    )
    parser.add_argument("--stats", action="store_true", help="显示文件统计信息")

    args = parser.parse_args()
    file_pattern = "stock_*.csv"
    data_dir = FINANCE_ROOT / "usstockinfo"
    stock_files = glob.glob(os.path.join(data_dir, file_pattern))

    # 显示文件统计信息
    if args.stats:
        get_file_stats(stock_files)
        exit(0)

    # 预览清理效果
    if args.preview:
        print("=== 预览清理效果 ===")
        preview_clean_effect_pandas(stock_files, args.date)
        exit(0)

    # 执行实际清理
    print("=== 执行实际清理 ===")
    print(f"将删除早于 {args.date} 的数据")
    print(f"分块大小: {args.chunk_size} 行")

    user_input = input("确认执行清理操作? (y/N): ")
    if user_input.lower() == "y":
        clean_stock_data_pandas_batch(stock_files, args.date, args.chunk_size)
    else:
        print("取消清理操作")
