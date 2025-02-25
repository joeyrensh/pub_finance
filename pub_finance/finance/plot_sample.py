from plotly.colors import sample_colorscale
import plotly.express as px
import numpy as np

# 使用 Peach_r 色阶生成 10 种颜色
hex_colors = sample_colorscale(
    px.colors.sequential.Electric,  # 原始色阶
    samplepoints=np.linspace(0, 1, 10),  # 生成 10 个等间距点
    colortype="rgb",  # 输出为十六进制
)

# 创建饼图
fig = px.pie(
    values=[10] * 10,  # 假设有 10 个数据点
    names=[f"Item {i + 1}" for i in range(10)],
    color_discrete_sequence=hex_colors,  # 直接指定颜色序列
)

# 更新样式
fig.update_traces(
    textinfo="label+value+percent",
    textfont=dict(size=12, family="Arial"),
    marker=dict(line=dict(color="white", width=2)),
    opacity=0.8,
)

fig.show()
