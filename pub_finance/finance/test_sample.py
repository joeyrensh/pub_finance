import pandas as pd

df = pd.Series([-8.6, -8.6, -6.0, -3.1, -1.7])
print(df.quantile(0.8))  # 输出 -2.82


def quantile_real_value(series, q):
    # 去除NaN并排序
    s = series.dropna().sort_values()
    if len(s) == 0:
        return None
    # 计算实际位置（向上取整，保证取到真实值）
    idx = int(np.ceil(q * len(s))) - 1
    idx = min(max(idx, 0), len(s) - 1)
    return s.iloc[idx]


import numpy as np

df["epr_threshold_80_real"] = df.groupby("IND")["EPR"].transform(
    lambda x: quantile_real_value(x, 0.8)
)
