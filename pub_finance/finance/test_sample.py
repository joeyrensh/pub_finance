import pandas as pd

input_file = "./cnstockinfo/stock_20260915.csv"
output_file = "./cnstockinfo/stock_20260915_new.csv"

# 每次处理 100,000 行，可根据内存大小自由调节
chunk_size = 100000

# 标识是否是第一次写入表头
is_first_chunk = True

print(f"开始处理文件 {input_file} ...")

# 分块读取 CSV
for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size)):
    # 1. 对 volume 字段除以 100 转换为手
    chunk["volume"] = chunk["volume"] / 100.0

    # 2. 追加写入新文件
    # mode='a' 表示追加，index=False 防止 pandas 额外生成新的索引列
    chunk.to_csv(
        output_file,
        mode="a",
        index=False,
        header=is_first_chunk,  # 只有第一块写入时保留表头
    )

    # 第一块写入完毕后，将标志位置为 False，后续块不再写入表头
    is_first_chunk = False

    print(f"已完成第 {i + 1} 块处理 ({len(chunk)} 行)")

print(f"处理完成！生成新文件：{output_file}")