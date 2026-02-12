import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import ast
from finance.dashreport.utils import Header, make_dash_format_table
import pandas as pd
import pathlib
import os
import empyrical as ep
from datetime import timedelta


class ChartBuilder:
    def __init__(self):
        self.theme_config = {
            "light": {
                "positive_int": "#d60a22",  # 红色 - 正数
                "negative_int": "#037b66",  # 绿色 - 负数
                "text_color": "#000000",  # 黑色
                "grid": "rgba(0, 0, 0, 0.2)",  # 网格线
                "background": "rgba(255, 255, 255, 0)",  # 透明背景
                "legend_bg": "rgba(246, 248, 249, 0.8)",
                "strategy_colors": [
                    "#0c6552",
                    "#0d876d",
                    "#00a380",
                    "#00b89a",
                    "#ffa700",
                    "#d50b3e",
                    "#a90a3f",
                    "#7a0925",
                ],
                "long": "#e01c3a",
                "short": "#0d876d",
                "pnl_colors": [
                    "#0c6552",
                    "#0d876d",
                    "#00a380",
                    "#ffa700",
                    "#d50b3e",
                ],
                "border": "rgba(200, 200, 200, 0.8)",
                "outside_text": "#777777",
                "cumret": "#e01c3a",
                "drawdown": "#0d876d",
                "drawdown_fill": "rgba(13,135,109,0.3)",
                "table_header": "rgba(245,245,245,0)",
                "table_cell": "rgba(0,0,0,0)",
                "hover_bg": "#ffffff",
                "hover_text": "#ffffff",
                "hover_border": "#cccccc",
            },
            "dark": {
                "positive_int": "#ff6b6b",  # 亮红色 - 正数
                "negative_int": "#00c4a6",  # 亮绿色 - 负数
                "text_color": "#ffffff",  # 白色
                "grid": "rgba(255, 255, 255, 0.2)",  # 网格线
                "background": "rgba(0, 0, 0, 0)",  # 透明背景
                "legend_bg": "rgba(20, 20, 20, 0.6)",
                "strategy_colors": [
                    "#0d7b67",
                    "#0e987f",
                    "#01b08f",
                    "#00c4a6",
                    "#ffa700",
                    "#e90c4a",
                    "#cf1745",
                    "#b6183d",
                ],
                "long": "#ff4d6d",
                "short": "#2ec4a6",
                "pnl_colors": [
                    "#0d7b67",
                    "#0e987f",
                    "#01b08f",
                    "#ffa700",
                    "#e90c4a",
                ],
                "border": "rgba(255, 255, 255, 0.4)",
                "outside_text": "#aaaaaa",
                "cumret": "#ff6b6b",
                "drawdown": "#6bcfb5",
                "drawdown_fill": "rgba(107,207,181,0.3)",
                "table_header": "rgba(64,64,64,0)",
                "table_cell": "rgba(0,0,0,0)",
                "hover_bg": "#1a1a1a",
                "hover_text": "#ffffff",
                "hover_border": "#666666",
            },
        }
        self.font_family = (
            '-apple-system, BlinkMacSystemFont, "PingFang SC", '
            '"Helvetica Neue", Arial, sans-serif'
        )
        self.base_fig_width = 1440

    def _get_scale(self, client_width, min_scale=0.65, max_scale=1.05):
        """计算缩放比例"""
        if client_width < 550:
            scale = client_width / self.base_fig_width
        else:
            scale = 1.0
        return max(min_scale, min(scale, max_scale))

    def _get_font_sizes(self, client_width, base_font=12, min_scale=0.9, max_scale=1.0):
        """获取字体大小"""
        scale = self._get_scale(client_width, min_scale, max_scale)
        font_size = int(base_font * scale)
        return scale, font_size

    def calendar_heatmap(
        self,
        page,
        df,
        theme="light",  # 主题参数
        client_width=1440,
    ):
        """
        Parameters:
        -----------
        df : pandas.DataFrame
            包含日历热图数据的DataFrame

        Returns:
        --------
        plotly.graph_objects.Figure
            交互式图表对象
        """
        scale, base_font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.0
        )

        # 获取当前主题配置
        config = self.theme_config.get(theme, self.theme_config["light"])
        hover_config = self.theme_config["light"]
        # 休市颜色使用中性色 + 透明度
        holiday_color = config["text_color"].replace("#", "")
        if len(holiday_color) == 6:  # 如果是hex颜色
            holiday_color = f"rgba({int(holiday_color[0:2], 16)}, {int(holiday_color[2:4], 16)}, {int(holiday_color[4:6], 16)}, 0.5)"
        else:
            holiday_color = config["text_color"]
        # 转换数据类型
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # 处理industry_top3列
        if "industry_top3" in df.columns:

            def parse_industry_string(x):
                if pd.isna(x) or x == "":
                    return []
                return [item.strip() for item in str(x).split(",") if item.strip()]

            df["industry_top3_parsed"] = df["industry_top3"].apply(
                parse_industry_string
            )
        else:
            df["industry_top3_parsed"] = [[]] * len(df)

        # 筛选周一到周五的数据
        df = df[df["day_of_week"] <= 4].copy()
        df = df.sort_values(by="date").reset_index(drop=True)

        # 计算每周的起始日期
        df["week_start"] = df["date"] - pd.to_timedelta(df["day_of_week"], unit="d")

        # 确定每周的顺序
        unique_weeks = (
            df["week_start"].drop_duplicates().sort_values().reset_index(drop=True)
        )
        week_mapping = {date: i for i, date in enumerate(unique_weeks)}
        df["week_order"] = df["week_start"].map(week_mapping)

        # 获取数据中的最新日期
        if len(df) > 0:
            latest_date = df["date"].max()
            weekday = latest_date.weekday()
            trading_days = 21 + weekday
            filtered_dates = pd.date_range(
                end=latest_date, periods=trading_days, freq="B"
            )
        else:
            filtered_dates = pd.DatetimeIndex([])

        # 计算字体大小
        s = df["s_pnl"].dropna()

        # 正收益
        pos_vals = s[s > 0]
        if len(pos_vals) >= 0:
            pos_median = np.quantile(pos_vals, 0.5)
            pos_max = pos_vals.max()
        else:
            pos_median = 0
            pos_max = 0

        # 负收益（取绝对值）
        neg_vals = -s[s < 0]  # 变成正数
        if len(neg_vals) > 0:
            neg_median = np.quantile(neg_vals, 0.5)
            neg_max = neg_vals.max()
        else:
            neg_median = 0
            neg_max = 0

        max_size_increase = 10

        def compute_font_size(s_pnl, base_font_size):
            # 正收益
            if s_pnl >= 0 and pos_max > pos_median:
                ratio = (s_pnl - pos_median) / (pos_max - pos_median)
                ratio = min(max(ratio, 0), 1)
                ratio_real = (s_pnl - 0) / (pos_max - 0)
                if s_pnl <= pos_median:
                    return base_font_size, ratio_real

                return base_font_size + ratio * max_size_increase, ratio_real

            # 负收益
            if s_pnl < 0 and neg_max > neg_median:
                abs_v = abs(s_pnl)
                ratio = (abs_v - neg_median) / (neg_max - neg_median)
                ratio = min(max(ratio, 0), 1)
                ratio_real = (s_pnl - 0) / (neg_max - 0)
                if abs_v <= neg_median:
                    return base_font_size, ratio_real

                return base_font_size + ratio * max_size_increase, ratio_real

            # 0 或异常情况
            return base_font_size, ratio_real

        # 创建图表
        fig = go.Figure()

        # 添加热力图（透明背景）
        fig.add_trace(
            go.Heatmap(
                x=df["day_of_week"],
                y=df["week_order"],
                z=df["s_pnl"],
                xgap=3,
                ygap=3,
                colorscale=[[0, "rgba(0, 0, 0, 0)"], [1, "rgba(0, 0, 0, 0)"]],
                showscale=False,
                hoverinfo="text",
                hovertext=df.apply(
                    lambda row: ChartBuilder._create_hover_text(row), axis=1
                ),
                hovertemplate="%{hovertext}<extra></extra>",
            )
        )

        def truncate_text_by_display_width(text, max_display_width=16):
            """
            按显示宽度截断文本
            规则：1个汉字 = 2个字符宽度，1个英文字符 = 1个字符宽度
            max_display_width=16 对应：8个汉字 或 16个英文字符
            """

            text = str(text).strip()
            if not text:
                return text

            total_width = 0
            result_chars = []

            for char in text:
                # 计算字符宽度
                if "\u4e00" <= char <= "\u9fff":  # 中文字符
                    char_width = 2
                else:  # 英文字符、数字、标点等
                    char_width = 1

                # 检查是否超过最大宽度
                if total_width + char_width > max_display_width:
                    break

                result_chars.append(char)
                total_width += char_width

            result = "".join(result_chars)

            # 如果截断了，添加省略号
            if len(result) < len(text):
                result += ".."

            return result

        # ===============================
        # 1️⃣ 统计出现频率最高的 3 个行业
        # ===============================
        from collections import Counter

        industry_counter = Counter()

        for _, row in df.iterrows():
            items = row["industry_top3_parsed"]
            if not items:
                continue
            for item in items:
                if item:
                    industry_counter[str(item).strip()] += 1

        # 定义排名映射：原始排名 → 显示值
        rank_mapping = {1: 2, 2: 1.5, 3: 1}

        # ===============================
        # 2️⃣ 为每个数据点添加 annotation
        # ===============================
        for i, row in df.iterrows():
            day_of_week = row["day_of_week"]
            week_order = row["week_order"]
            date_str = row["date"].strftime("%Y-%m-%d")
            year, month, day = date_str.split("-")
            industry_items = row["industry_top3_parsed"]
            col3_value = row["s_pnl"]

            # -------- 字体大小 --------
            dynamic_font_size, ratio = compute_font_size(col3_value, base_font_size + 4)
            dynamic_font_size = int(dynamic_font_size)
            # 格式化 ratio 为百分比，保留 1 位小数（例如 "10.1%"）
            try:
                formatted_ratio = f"{float(ratio) * 100:.1f}%"
            except Exception:
                formatted_ratio = str(ratio)

            # -------- 颜色 --------
            if col3_value > 0:
                dynamic_text_color = config["positive_int"]
                hover_bg = hover_config["positive_int"]
            elif col3_value < 0:
                dynamic_text_color = config["negative_int"]
                hover_bg = hover_config["negative_int"]
            else:
                dynamic_text_color = config["text_color"]
                hover_bg = config["text_color"]
                dynamic_font_size = base_font_size

            industry_items = [
                str(item).strip() if item else "" for item in industry_items
            ]

            # dynamic_font_size = base_font_size + 6

            # ===============================
            # 上方行业（第二大行业）
            # ===============================
            if len(industry_items) >= 2:
                industry_text_top = industry_items[1]

                rank_text_top = next(
                    (
                        i + 1
                        for i, (ind, _) in enumerate(industry_counter.most_common(3))
                        if ind == industry_text_top
                    ),
                    0,
                )

                if len(industry_text_top) > 6:
                    industry_text_top = truncate_text_by_display_width(
                        industry_text_top, 12
                    )

                if rank_text_top > 0:
                    industry_text_top = f"<b>{industry_text_top}</b>"

                font_size_top = (
                    rank_mapping.get(rank_text_top, 0)
                    if page.startswith("cn")
                    else rank_mapping.get(rank_text_top, 0) * 0.2
                )

                fig.add_annotation(
                    x=day_of_week + 0.1,
                    y=week_order,
                    text=industry_text_top,
                    showarrow=False,
                    font=dict(
                        family=self.font_family,
                        # size=(
                        #     base_font_size
                        #     if not page.startswith("cn")
                        #     else base_font_size + 2
                        # ),
                        size=(base_font_size + font_size_top),
                        color=config["text_color"],
                    ),
                    align="center",
                    xanchor="center",
                    yanchor="top",
                    opacity=0.9,
                )

            # ===============================
            # 下方行业（第一大行业）
            # ===============================
            if len(industry_items) >= 1:
                industry_text_bottom = industry_items[0]

                rank_text_bottom = next(
                    (
                        i + 1
                        for i, (ind, _) in enumerate(industry_counter.most_common(3))
                        if ind == industry_text_bottom
                    ),
                    0,
                )

                if len(industry_text_bottom) > 6:
                    industry_text_bottom = truncate_text_by_display_width(
                        industry_text_bottom, 12
                    )

                # ⭐ Top3 行业加粗
                if rank_text_bottom > 0:
                    industry_text_bottom = f"<b>{industry_text_bottom}</b>"

                font_size_bottom = (
                    rank_mapping.get(rank_text_bottom, 0)
                    if page.startswith("cn")
                    else rank_mapping.get(rank_text_bottom, 0) * 0.2
                )

                fig.add_annotation(
                    x=day_of_week + 0.1,
                    y=week_order,
                    text=industry_text_bottom,
                    showarrow=False,
                    font=dict(
                        family=self.font_family,
                        # size=base_font_size,
                        size=(base_font_size + font_size_bottom),
                        color=config["text_color"],
                    ),
                    align="center",
                    xanchor="center",
                    yanchor="bottom",
                    opacity=0.9,
                )

            # ===============================
            # 日期（月 / 日）
            # ===============================
            fig.add_annotation(
                x=day_of_week - 0.52,
                y=week_order,
                text=month,
                showarrow=False,
                align="left",
                xanchor="left",
                yanchor="middle",
                yshift=int(dynamic_font_size * scale * 0.48),
                font=dict(
                    family=self.font_family,
                    size=dynamic_font_size,
                    color=dynamic_text_color,
                ),
                opacity=0.9,
            )

            fig.add_annotation(
                x=day_of_week - 0.52,
                y=week_order,
                text=day,
                showarrow=False,
                align="left",
                xanchor="left",
                yanchor="middle",
                yshift=-int(dynamic_font_size * scale * 0.48),
                font=dict(
                    family=self.font_family,
                    size=dynamic_font_size,
                    color=dynamic_text_color,
                ),
                opacity=0.9,
            )

        # 设置图表布局
        fig.update_layout(
            xaxis=dict(
                tickmode="array",
                tickvals=[0, 1, 2, 3, 4, 5, 6],
                ticktext=[
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                    "Sunday",
                ],
                showgrid=False,
                zeroline=False,
                showticklabels=True,
                dtick=1,
                tickfont=dict(
                    family=self.font_family,
                    size=base_font_size,
                    # weight="bold",
                    color=config["text_color"],  # 使用中性色
                ),
            ),
            yaxis=dict(
                showgrid=False,
                zeroline=False,
                showticklabels=False,
                autorange="reversed",
            ),
            plot_bgcolor=config["background"],
            paper_bgcolor=config["background"],
            margin=dict(l=0, r=0, t=0, b=0, pad=0),
            autosize=True,  # 修改点：设为False以使用固定尺寸
            dragmode=False,
            hoverlabel=dict(
                bgcolor=hover_bg,
                bordercolor=config["hover_border"],
                font_color=config["hover_text"],
                font_size=base_font_size,
            ),
        )

        # 在每周之间添加横向分隔线
        unique_weeks_list = sorted(df["week_order"].unique()) if len(df) > 0 else []

        for week in unique_weeks_list[1:]:
            fig.add_hline(
                y=week - 0.5,
                line=dict(color=config["grid"], width=1),
                opacity=0.6,
                layer="below",
            )

        for day in range(1, 5):
            fig.add_vline(
                x=day - 0.5,
                line=dict(color=config["grid"], width=1),
                opacity=0.6,
                layer="below",
            )

        # 缺失日期处理逻辑
        if len(df) > 0 and len(filtered_dates) > 0:
            existing_dates = set(df["date"])
            missing_dates = set(filtered_dates) - existing_dates
            missing_dates = [date for date in missing_dates if date.weekday() < 5]

            # 为每个缺失日期添加 annotation
            for missing_date in missing_dates:
                day_of_week = missing_date.dayofweek
                week_start = missing_date - pd.to_timedelta(day_of_week, unit="d")

                if week_start in week_mapping:
                    week_order = week_mapping[week_start]

                    # 添加 annotation
                    fig.add_annotation(
                        x=day_of_week,
                        y=week_order,
                        text="休市",
                        showarrow=False,
                        font=dict(
                            family=self.font_family,
                            size=base_font_size,
                            color=config["text_color"],  # 使用带透明度的中性色
                        ),
                        align="center",
                        xanchor="center",
                        yanchor="middle",
                    )

        return fig

    @staticmethod
    def _create_hover_text(row):
        """创建悬停文本"""
        date_str = row["date"].strftime("%Y-%m-%d")
        day_name = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ][int(row["day_of_week"])]
        value = row["s_pnl"]
        industry_items = row.get("industry_top3_parsed", [])
        inds = [str(item) for item in industry_items[:3]] if industry_items else []
        # 补齐到 3 个条目，避免索引错误
        while len(inds) < 3:
            inds.append("")

        # hover_lines = (
        #     f"<b>{date_str}</b><br>"
        #     f"<b>   ├</b> {inds[0]}<br>"
        #     f"<b>   ├</b> {inds[1]}<br>"
        #     f"<b>   ├</b>{inds[2]}"
        #     "<extra></extra>"
        # )
        hover_lines = (
            f"<b>{date_str}</b><br>"
            f"<b>   ├</b>{inds[0]},{inds[1]},{inds[2]}<br>"
            "<extra></extra>"
        )

        return hover_lines

    def strategy_chart(
        self,
        page,
        df,
        theme="light",
        client_width=1440,
    ):
        cfg = self.theme_config.get(theme, self.theme_config["light"])
        text_color = cfg["text_color"]
        grid_color = cfg["grid"]
        legend_bg = cfg["legend_bg"]
        strategy_colors = cfg["strategy_colors"]

        scale, base_font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )
        scale, title_font_size = self._get_font_sizes(
            client_width, base_font=14, min_scale=0.9, max_scale=1.05
        )

        df["pnl_pos"] = df["pnl"].clip(lower=0)
        df["pnl_neg"] = df["pnl"].clip(upper=0)

        df_group = df.groupby("date")["pnl_pos"].sum().reset_index()

        max_pnl = df_group["pnl_pos"].max()
        min_pnl = df_group["pnl_pos"].min()

        threshold = df["success_rate"].quantile(0.1)
        min_success_rate = df.loc[
            df["success_rate"] >= threshold,
            "success_rate",
        ].min()

        safe_rate = max(min_success_rate, 0.2)
        max_range = min(2 * max_pnl, max_pnl * 2 / safe_rate)

        df_group2 = df.groupby("date")["pnl_neg"].sum().reset_index()
        min_range = df_group2["pnl_neg"].min()

        # =========================
        # 策略顺序 & 分组
        # =========================
        strategy_order = [
            "多头排列",
            "均线金叉",
            "均线收敛",
            "突破年线",
            "突破半年线",
            "连续上涨",
            "成交量放大",
            "红三兵",
        ]

        groups = dict(list(df.groupby("strategy")))

        fig = go.Figure()

        # =========================
        # 7. 线型（width × scale）
        # =========================
        line_styles = [
            {"dash": "solid", "width": 2.2 * scale},
            {"dash": "solid", "width": 2.2 * scale},
            {"dash": "dashdot", "width": 2.0 * scale},
            {"dash": "dash", "width": 1.8 * scale},
            {"dash": "6,3", "width": 1.6 * scale},
            {"dash": "5,5", "width": 1.4 * scale},
            {"dash": "4,6", "width": 1.2 * scale},
            {"dash": "2,8", "width": 1.0 * scale},
        ]

        for i, strategy in enumerate(strategy_order):
            if strategy not in groups:
                continue

            data = groups[strategy]
            color = strategy_colors[i]

            fig.add_trace(
                go.Scatter(
                    x=data["date"],
                    y=data["ema_success_rate"],
                    mode="lines",
                    name=strategy,
                    line=dict(
                        width=line_styles[i]["width"],
                        dash=line_styles[i]["dash"],
                        shape="spline",
                        color=color,
                    ),
                    yaxis="y",
                    hovertemplate=(
                        # "<b>日期</b>: %{x|%Y-%m-%d}<br>"  # 修改这里：添加日期格式化
                        "<b>成功率</b>: " + strategy + " %{y:.2%}<br>"
                        "<extra></extra>"
                    ),
                    legendgroup=strategy,
                )
            )
            data_pos = data[data["pnl"] >= 0]
            fig.add_trace(
                go.Bar(
                    x=data_pos["date"],
                    y=data_pos["pnl_pos"],
                    name=strategy,
                    marker=dict(
                        color=color,
                        line=dict(color=color),
                    ),
                    yaxis="y2",
                    showlegend=False,
                    hovertemplate=(
                        # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                        "<b>收益</b>: " + strategy + " %{y:,.0f}<br>"
                        "<extra></extra>"
                    ),
                    legendgroup=strategy,
                    # base=0,
                )
            )
            data_neg = data[data["pnl"] < 0]
            fig.add_trace(
                go.Bar(
                    x=data_neg["date"],
                    y=data_neg["pnl_neg"],
                    name=strategy,
                    marker=dict(
                        color=color,
                        line=dict(color=color),
                    ),
                    yaxis="y2",
                    showlegend=False,
                    hovertemplate=(
                        # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                        "<b>收益</b>: " + strategy + " %{y:,.0f}<br>"
                        "<extra></extra>"
                    ),
                    legendgroup=strategy,
                    # base=0,
                )
            )

        fig.update_layout(
            xaxis=dict(
                mirror=True,
                ticks="outside",
                tickfont=dict(
                    family=self.font_family,
                    size=base_font_size,
                    color=text_color,
                ),
                showline=False,
                zeroline=False,
                gridcolor=grid_color,
                tickmode="linear",
                dtick="M1",
                tickformat="%Y-%m",
                hoverformat="%Y-%m-%d",
                # domain=[0, 1],
                # automargin=True,
            ),
            yaxis=dict(
                side="left",
                mirror=True,
                ticks="inside",
                tickfont=dict(
                    family=self.font_family,
                    size=base_font_size,
                    color=text_color,
                ),
                showline=False,
                zeroline=False,
                gridcolor=grid_color,
                dtick=0.1,
                fixedrange=True,
                range=[0, 1],
            ),
            yaxis2=dict(
                side="right",
                overlaying="y",
                showgrid=False,
                tickfont=dict(
                    family=self.font_family,
                    size=base_font_size,
                    color=text_color,
                ),
                showline=False,
                zeroline=False,
                range=[min_range, max_range],
                tickformat="~s",
                # anchor="free",
                # position=0.94,
                showticklabels=False,
            ),
            legend=dict(
                orientation="v",
                x=-0.05,
                y=1,
                xanchor="left",
                yanchor="top",
                font=dict(
                    family=self.font_family,
                    size=base_font_size,
                    color=text_color,
                ),
                bgcolor=legend_bg,
                borderwidth=0,
                itemsizing="trace",
                tracegroupgap=0,
                # itemsizing="constant",
            ),
            barmode="relative",
            bargap=0.2,
            bargroupgap=0.2,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=True,
            dragmode=False,
            hovermode="x",
            hoverlabel=dict(font_size=base_font_size),
        )
        return fig

    def trade_info_chart(
        self,
        page,
        df,
        theme="light",
        client_width=1440,
    ):

        cfg = self.theme_config.get(theme, self.theme_config["light"])
        text_color = cfg["text_color"]
        grid_color = cfg["grid"]

        scale, font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )
        scale, title_font_size = self._get_font_sizes(
            client_width, base_font=14, min_scale=0.9, max_scale=1.05
        )

        # =========================
        # 3. Figure & traces（原逻辑）
        # =========================
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["buy_date"],
                y=df["total_cnt"],
                mode="lines+markers",
                name="Total",
                line=dict(color=cfg["long"], width=2 * scale),
                yaxis="y",
                hovertemplate=(
                    # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                    "<b>总数</b>: %{y}<br>"
                    "<extra></extra>"
                ),
            )
        )

        fig.add_trace(
            go.Bar(
                x=df["buy_date"],
                y=df["buy_cnt"],
                name="Long",
                marker_color=cfg["long"],
                marker_line_color=cfg["long"],
                yaxis="y",
                hovertemplate=(
                    # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                    "<b>买入数量</b>: %{y}<br>"
                    "<extra></extra>"
                ),
            )
        )

        fig.add_trace(
            go.Bar(
                x=df["buy_date"],
                y=df["sell_cnt"],
                name="Short",
                marker_color=cfg["short"],
                marker_line_color=cfg["short"],
                yaxis="y",
                hovertemplate=(
                    # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                    "<b>卖出数量</b>: %{y}<br>"
                    "<extra></extra>"
                ),
            )
        )

        fig.update_layout(
            title=dict(
                text="Last 180 days trade info",
                y=0.9,
                x=0.5,
                font=dict(
                    size=title_font_size,
                    color=text_color,
                    family=self.font_family,
                ),
            ),
            xaxis=dict(
                mirror=True,
                ticks="outside",
                tickfont=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                showline=False,
                zeroline=False,
                gridcolor=grid_color,
                tickmode="linear",
                dtick="M1",
                tickformat="%Y-%m",
                hoverformat="%Y-%m-%d",
            ),
            yaxis=dict(
                side="left",
                mirror=True,
                ticks="outside",
                tickfont=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                showline=False,
                gridcolor=grid_color,
                ticklabelposition="outside",
                tickangle=0,
                zeroline=False,
            ),
            legend=dict(
                orientation="v",
                x=-0.05,
                y=1,
                xanchor="left",
                yanchor="top",
                font=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                bgcolor=cfg["legend_bg"],
                borderwidth=0,
                tracegroupgap=0,
            ),
            barmode="stack",
            bargap=0.2,
            bargroupgap=0.2,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=True,
            dragmode=False,
            hovermode="x",
            hoverlabel=dict(font_size=font_size),
        )

        xmin = pd.to_datetime(df["buy_date"].min())
        xmax = pd.to_datetime(df["buy_date"].max())

        fig.update_xaxes(
            range=[
                xmin - timedelta(days=0.5),
                xmax + timedelta(days=0.5),
            ]
        )

        return fig

    def industry_pnl_trend(
        self,
        page,
        df,
        theme="light",
        client_width=1440,
    ):

        cfg = self.theme_config.get(theme, self.theme_config["light"])
        text_color = cfg["text_color"]

        scale, font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )
        scale, title_font_size = self._get_font_sizes(
            client_width, base_font=14, min_scale=0.9, max_scale=1.05
        )

        # =========================
        # 3. chart
        # =========================
        fig = px.line(
            df,
            x="buy_date",
            y="pnl",
            color="industry",
            line_group="industry",
            color_discrete_sequence=cfg["pnl_colors"],
        )
        fig.update_traces(
            line=dict(width=2 * scale),
            hovertemplate=(
                # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                "<b>收益</b>: %{fullData.name} %{y}<br>"
                "<extra></extra>"
            ),
        )

        fig.update_xaxes(
            mirror=True,
            ticks="inside",
            ticklabelposition="inside",
            tickfont=dict(
                size=font_size,
                color=text_color,
                family=self.font_family,
            ),
            title=dict(
                text=None,
                font=dict(
                    size=title_font_size,
                    color=text_color,
                    family=self.font_family,
                ),
            ),
            showline=False,
            linecolor=cfg["grid"],
            zeroline=False,
            gridcolor=cfg["grid"],
            tickmode="linear",
            dtick="M1",
            tickformat="%Y-%m",
            hoverformat="%Y-%m-%d",
        )

        fig.update_yaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(
                size=font_size,
                color=text_color,
                family=self.font_family,
            ),
            title=dict(
                text=None,
                font=dict(
                    size=title_font_size,
                    color=text_color,
                    family=self.font_family,
                ),
            ),
            showline=False,
            linecolor=cfg["grid"],
            zeroline=False,
            gridcolor=cfg["grid"],
            ticklabelposition="outside",
            tickangle=0,
            autorange=True,
        )

        fig.update_layout(
            title=dict(
                text="Last 180 days top5 pnl",
                x=0.5,
                y=0.9,
                font=dict(
                    size=title_font_size,
                    color=text_color,
                    family=self.font_family,
                ),
            ),
            legend_title_text=None,
            legend=dict(
                orientation="v",
                x=-0.05,
                xanchor="left",
                y=1,
                yanchor="top",
                font=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                bgcolor=cfg["legend_bg"],
                borderwidth=0,
                tracegroupgap=0,
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=True,
            dragmode=False,
            hovermode="x",
            hoverlabel=dict(font_size=font_size),
        )

        return fig

    def industry_position_treemap(
        self,
        page,
        df,
        theme="light",
        client_width=1440,
    ):

        cfg = self.theme_config.get(theme, self.theme_config["light"])
        text_color = cfg["text_color"]
        scale, font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.0
        )

        labels_wrapped_industry = df["industry"]
        values = df["cnt"]
        hex_colors = ["rgba(0,0,0,0)" for _ in range(20)]

        # =========================
        # Treemap
        # =========================
        fig = go.Figure(
            go.Treemap(
                labels=labels_wrapped_industry,
                parents=[""] * len(df),
                values=values,
                texttemplate="%{label}<br>%{percentParent:.0%}",
                insidetextfont=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                textposition="middle center",
                marker=dict(
                    colors=hex_colors,
                    line=dict(
                        color=cfg["border"],
                        width=1,
                    ),
                    showscale=False,
                    pad=dict(t=0, b=0, l=0, r=0),
                ),
                opacity=1,
                tiling=dict(
                    squarifyratio=1.2,
                    pad=10,
                ),
            )
        )

        # =========================
        # layout（theme 对齐）
        # =========================
        fig.update_layout(
            title=dict(text=None),
            showlegend=False,
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            treemapcolorway=hex_colors,
        )

        return fig

    def industry_profit_treemap(
        self,
        page,
        df,
        theme="light",
        client_width=1440,
    ):

        cfg = self.theme_config.get(theme, self.theme_config["light"])
        text_color = cfg["text_color"]
        scale, font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.0
        )

        labels = df["industry"]
        values = df["pl"]
        hex_colors = ["rgba(0,0,0,0)" for _ in range(20)]

        # =========================
        # Treemap
        # =========================
        fig = go.Figure(
            go.Treemap(
                labels=labels,
                parents=[""] * len(df),
                values=values,
                texttemplate="%{label}<br>%{percentParent:.0%}",
                insidetextfont=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                outsidetextfont=dict(
                    color=cfg["outside_text"],
                    family=self.font_family,
                ),
                textposition="middle center",
                marker=dict(
                    colors=hex_colors,
                    line=dict(
                        color=cfg["border"],
                        width=1,
                    ),
                    showscale=False,
                    pad=dict(t=0, b=0, l=0, r=0),
                ),
                opacity=1,
                tiling=dict(
                    squarifyratio=1.2,
                    pad=10,
                ),
            )
        )

        # =========================
        # layout
        # =========================
        fig.update_layout(
            title=dict(text=None),
            showlegend=False,
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            treemapcolorway=hex_colors,
        )

        return fig

    def annual_return(self, page, pnl: pd.Series, theme="light", client_width=1440):
        """
        生成年度收益图表
        """
        # =========================
        # 0. 基本校验
        # =========================
        if not isinstance(pnl, pd.Series):
            raise TypeError("pnl must be a pd.Series")

        if not isinstance(pnl.index, pd.DatetimeIndex):
            raise TypeError("pnl index must be DatetimeIndex")

        pnl = pnl.sort_index().dropna()

        # =========================
        # 1. theme 配置
        # =========================
        cfg = self.theme_config.get(theme, self.theme_config["light"])
        hover_config = self.theme_config["light"]
        text_color = cfg["text_color"]
        scale, base_font = self._get_font_sizes(
            client_width, base_font=16, min_scale=0.65, max_scale=1.05
        )
        scale, table_font = self._get_font_sizes(
            client_width, base_font=16, min_scale=0.65, max_scale=1.05
        )
        # =========================
        # 3. 收益 / 回撤计算
        # =========================
        cumulative = (1 + pnl).cumprod()
        peak = cumulative.cummax()
        drawdown = (cumulative - peak) / peak

        # =========================
        # 4. 年度统计
        # =========================
        years = sorted(pnl.index.year.unique())[-2:]
        stats = []

        for year in years:
            year_data = pnl[pnl.index.year == year]
            if len(year_data) >= 5:
                stats.append(
                    {
                        "YEAR": str(year),
                        "ANN.R": ep.annual_return(year_data),
                        "CUM.R": (1 + year_data).prod() - 1,
                        "MX.DD": ep.max_drawdown(year_data),
                        "D.RISK": np.percentile(year_data, 5),
                    }
                )

        if not stats:
            raise ValueError("Not enough data to compute annual statistics")

        perf_raw = pd.DataFrame(stats).set_index("YEAR")

        # 格式化百分比
        perf_fmt = perf_raw.copy()
        for col in perf_fmt.columns:
            perf_fmt[col] = perf_fmt[col].apply(lambda x: f"{x*100:.2f}%")

        # =========================
        # 5. 表格数据
        # =========================
        table_labels = ["ANN.R", "CUM.R", "MX.DD", "D.RISK"]

        table_data = {" ": table_labels}
        for year in perf_fmt.index:
            table_data[year] = perf_fmt.loc[
                year, ["ANN.R", "CUM.R", "MX.DD", "D.RISK"]
            ].tolist()

        table_df = pd.DataFrame(table_data)

        # =========================
        # 6. 表格颜色
        # =========================
        last_column = table_df.columns[-1]

        font_colors = []
        for col in table_df.columns:
            column_colors = []
            for i, val in enumerate(table_df[col]):
                if col == last_column and i >= 0 and val and val != "":
                    try:
                        num_val = float(val.replace("%", ""))
                        color = (
                            cfg["positive_int"] if num_val >= 0 else cfg["negative_int"]
                        )
                    except:
                        color = text_color
                else:
                    color = text_color
                column_colors.append(color)
            font_colors.append(column_colors)

        cell_colors = [cfg["table_cell"]] * len(table_df.columns)

        # =========================
        # 7. 创建图表
        # =========================
        TABLE_WIDTH_RATIO = 0.35
        CHART_WIDTH_RATIO = 0.65
        HORIZONTAL_SPACING = 0.005

        fig = go.Figure()

        # =========================
        # 8. 计算Y轴范围
        # =========================
        cum_min = cumulative.min()
        cum_max = cumulative.max()
        cum_range = [cum_min * 0.95, cum_max * 1.05]

        dd_min = drawdown.min()
        dd_max = 0
        dd_range = [dd_min * 1.05, 0.0]

        # =========================
        # 10. 添加回撤曲线
        # =========================
        fig.add_trace(
            go.Scatter(
                x=drawdown.index,
                y=drawdown,
                mode="lines",
                fill="tozeroy",
                name="Drawdown",
                line=dict(color=cfg["drawdown"], width=1 * scale),
                fillcolor=cfg["drawdown_fill"],
                hovertemplate=(
                    # "<b>Date</b>: %{x|%Y-%m-%d}<br>"
                    "<b>Drawdown</b>: %{y:.2%}<br>"
                    "<extra></extra>"
                ),
                hoverlabel=dict(
                    bgcolor=hover_config["drawdown"],
                ),
                yaxis="y",
            )
        )

        # =========================
        # 9. 添加累计收益曲线
        # =========================
        fig.add_trace(
            go.Scatter(
                x=cumulative.index,
                y=cumulative,
                mode="lines",
                name="Cum. Return",
                line=dict(color=cfg["cumret"], width=2 * scale),
                hovertemplate=(
                    # "<b>Date</b>: %{x|%Y-%m-%d}<br>"
                    "<b>Cum. Return</b>: %{y:.4f}<br>"
                    "<extra></extra>"
                ),
                hoverlabel=dict(
                    bgcolor=hover_config["cumret"],
                ),
                yaxis="y2",
            )
        )

        # =========================
        # 11. 关键点标注函数
        # =========================
        def get_label_position(
            index,
            data_series,
            is_near_right_threshold=0.2,
            avoid_indices=None,
            avoid_days=10,
        ):
            """
            返回文本标注位置，默认根据是否靠近右侧选择左右。
            当传入 avoid_indices（timestamp 列表）且有靠近的索引时，
            会尝试翻转锚点以避免与这些索引的注释重叠。
            """
            try:
                right_threshold_idx = data_series.index[
                    -int(len(data_series) * is_near_right_threshold)
                ]
            except Exception:
                right_threshold_idx = data_series.index[0]

            # 默认位于右侧附近时放在左上，否则右上
            pos = "top left" if index > right_threshold_idx else "top right"

            # 若提供了需要避让的索引，则在时间上接近时仅翻转上下位置，保持左右不变（简单、稳健）
            if avoid_indices:
                for ai in avoid_indices:
                    try:
                        delta_days = abs(
                            (pd.to_datetime(ai) - pd.to_datetime(index)).days
                        )
                    except Exception:
                        continue
                    if delta_days <= avoid_days:
                        pos = (
                            pos.replace("top", "bottom")
                            if pos.startswith("top")
                            else pos.replace("bottom", "top")
                        )
                        break
            return pos

        def get_compact_label(text, value, client_width):
            if client_width >= 550:
                return f"{text}: {value:.2%}"
            else:
                if "30D" in text:
                    return f"30D: {value:.1%}"
                elif "120D" in text:
                    return f"120D: {value:.1%}"
                else:
                    return f"{text[:3]}: {value:.1%}"

        # =========================
        # 12. 关键点标注
        # =========================
        # 最大累计收益
        if len(cumulative) > 0:
            cum_max_idx = cumulative.idxmax()
            cum_max_val = cumulative.max()
            last_x = cumulative.index[-1]
            last_y = cumulative.iloc[-1]

            fig.add_trace(
                go.Scatter(
                    x=[last_x],
                    y=[last_y],
                    mode="markers+text",
                    marker=dict(symbol="circle", size=8 * scale, color=cfg["cumret"]),
                    showlegend=False,
                    hovertemplate=(
                        f"<b>Latest Cum. Return</b>: {last_y:.4f}<br>"
                        f"<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor=hover_config["cumret"],
                    ),
                    yaxis="y2",
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=[cum_max_idx],
                    y=[cum_max_val],
                    mode="markers+text",
                    marker=dict(symbol="circle", size=8 * scale, color=cfg["cumret"]),
                    text=[f"Max: {cum_max_val:.2f}"],
                    textposition=get_label_position(cum_max_idx, cumulative),
                    textfont=dict(
                        size=base_font, color=text_color, family=self.font_family
                    ),
                    showlegend=False,
                    hovertemplate=(
                        f"<b>Max Cum. Return</b>: {cum_max_val:.4f}<br>"
                        f"<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor=hover_config["cumret"],
                    ),
                    yaxis="y2",
                )
            )

        # 在处理回撤注释前准备需要规避的索引列表（例如累计最大值与最新日期）
        avoid_list = []
        avoid_list.append(cum_max_idx)
        avoid_list.append(cumulative.index[-1])

        # 最大回撤
        if len(drawdown) > 0:
            max_dd_idx = drawdown.idxmin()
            max_dd_val = drawdown.min()

            fig.add_trace(
                go.Scatter(
                    x=[max_dd_idx],
                    y=[max_dd_val],
                    mode="markers+text",
                    marker=dict(symbol="circle", size=8 * scale, color=cfg["drawdown"]),
                    text=[get_compact_label("Max DD", max_dd_val, client_width)],
                    textposition=get_label_position(
                        max_dd_idx, drawdown, avoid_indices=avoid_list
                    ),
                    textfont=dict(size=base_font, color=text_color),
                    showlegend=False,
                    hovertemplate=(
                        f"<b>Max Drawdown</b>: {max_dd_val:.2%}<br>" f"<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor=hover_config["drawdown"],
                    ),
                    yaxis="y",
                )
            )
            avoid_list.append(max_dd_idx)

            # 30D最大回撤
            if len(drawdown) >= 30:
                w_30 = drawdown.iloc[-30:]
                idx_30 = w_30.idxmin()
                val_30 = w_30.loc[idx_30]

                if idx_30 != max_dd_idx:
                    fig.add_trace(
                        go.Scatter(
                            x=[idx_30],
                            y=[val_30],
                            mode="markers+text",
                            marker=dict(
                                symbol="diamond", size=6 * scale, color=cfg["drawdown"]
                            ),
                            text=[get_compact_label("30D DD", val_30, client_width)],
                            textposition=get_label_position(
                                idx_30,
                                drawdown,
                                is_near_right_threshold=0.2,
                                avoid_indices=avoid_list,
                            ),
                            textfont=dict(size=base_font, color=text_color),
                            showlegend=False,
                            hovertemplate=(
                                f"<b>30D Max Drawdown</b>: {val_30:.2%}<br>"
                                f"<extra></extra>"
                            ),
                            hoverlabel=dict(
                                bgcolor=hover_config["drawdown"],
                            ),
                            yaxis="y",
                        )
                    )
                avoid_list.append(idx_30)

            # 120D最大回撤
            if len(drawdown) >= 120:
                w_120 = drawdown.iloc[-120:]
                idx_120 = w_120.idxmin()
                val_120 = w_120.loc[idx_120]

                if idx_120 != max_dd_idx and (len(drawdown) < 30 or idx_120 != idx_30):
                    fig.add_trace(
                        go.Scatter(
                            x=[idx_120],
                            y=[val_120],
                            mode="markers+text",
                            marker=dict(
                                symbol="diamond", size=6 * scale, color=cfg["drawdown"]
                            ),
                            text=[get_compact_label("120D DD", val_120, client_width)],
                            textposition=get_label_position(
                                idx_120,
                                drawdown,
                                is_near_right_threshold=0.4,
                                avoid_indices=avoid_list,
                            ),
                            textfont=dict(size=base_font, color=text_color),
                            showlegend=False,
                            hovertemplate=(
                                f"<b>120D Max Drawdown</b>: {val_120:.2%}<br>"
                                f"<extra></extra>"
                            ),
                            hoverlabel=dict(
                                bgcolor=hover_config["drawdown"],
                            ),
                            yaxis="y",
                        )
                    )

        # =========================
        # 13. 布局设置
        # =========================
        # 计算图表区域（左侧）
        chart_domain_left = 0.0
        chart_domain_right = 1 - TABLE_WIDTH_RATIO - HORIZONTAL_SPACING

        # 计算表格区域（右侧）
        table_domain_left = chart_domain_right + HORIZONTAL_SPACING
        table_domain_right = 0.99

        chart_width = chart_domain_right - chart_domain_left
        legend_absolute_x = chart_domain_left + (0.04 * chart_width)

        # =========================
        # 14. 添加右侧表格
        # =========================
        header_height = int(table_font * 7) * scale
        cell_height = int(table_font * 6.8) * scale

        TABLE_Y_BOTTOM = 0.0
        TABLE_Y_TOP = 1.0
        cell_values = []
        for i, col in enumerate(table_df.columns):
            if i == 0:
                cell_values.append([f"<b>{v}</b>" for v in table_df[col]])
            else:
                cell_values.append(table_df[col])

        fig.add_trace(
            go.Table(
                domain=dict(
                    x=[table_domain_left, table_domain_right],  # 表格在右侧
                    y=[TABLE_Y_BOTTOM, TABLE_Y_TOP],
                ),
                header=dict(
                    values=list(table_df.columns),
                    fill_color=cfg["table_header"],
                    line=dict(color=cfg["border"], width=1),
                    font=dict(
                        size=table_font,
                        color=text_color,
                        family=self.font_family,
                        weight="bold",
                    ),
                    align=["center"] * len(table_df.columns),
                    height=header_height,
                ),
                cells=dict(
                    values=cell_values,
                    fill_color=cell_colors,
                    line=dict(color=cfg["border"], width=1),
                    font=dict(
                        size=table_font,
                        color=font_colors,
                        family=self.font_family,
                    ),
                    align=["center"] * len(table_df.columns),
                    height=cell_height,
                ),
                columnwidth=[1.0] + [1.0] * (len(table_df.columns) - 1),
            )
        )

        # =========================
        # 15. 更新图表布局
        # =========================
        # 从pnl的索引中获取最小和最大日期
        data_start_date = pnl.index.min()  # 最小日期
        data_end_date = pnl.index.max()  # 最大日期
        data_end_date_limited = data_end_date + pd.Timedelta(days=15)

        fig.update_layout(
            autosize=True,
            dragmode=False,
            width=None,
            height=None,
            margin=dict(l=0, r=0, t=0, b=0),
            font=dict(size=base_font, color=text_color, family=self.font_family),
            legend=dict(
                x=legend_absolute_x,
                y=0.98,
                xanchor="left",
                yanchor="top",
                bgcolor="rgba(0,0,0,0)",
                borderwidth=0,
                font=dict(size=base_font, family=self.font_family),
                itemsizing="trace",
                tracegroupgap=0,
                entrywidth=8,
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            hovermode="x",
            hoverlabel=dict(
                # bgcolor=cfg["hover_bg"],
                # bgcolor=hover_config["cumret"],
                font_size=base_font,
                font_family=self.font_family,
                # font_color=cfg["hover_text"],
                # bordercolor=cfg["hover_border"],
            ),
            bargap=0,
            bargroupgap=0,
            boxgap=0,
            boxgroupgap=0,
            # Y轴设置
            yaxis=dict(
                title="",
                side="right",
                position=chart_domain_right,
                showgrid=False,
                tickfont=dict(
                    size=base_font, color=text_color, family=self.font_family
                ),
                tickformat=".0%",
                range=dd_range,
                showticklabels=True,
                automargin=False,
                ticklabelposition="inside",
                showline=False,
                linewidth=1,
                linecolor=cfg["border"],
                zeroline=False,
                anchor="x",
            ),
            # Y2轴设置
            yaxis2=dict(
                title="",
                side="left",
                overlaying="y",
                position=chart_domain_left,
                showgrid=True,
                gridcolor=cfg["grid"],
                tickfont=dict(
                    size=base_font, color=text_color, family=self.font_family
                ),
                tickformat=".2f",
                range=cum_range,
                showticklabels=True,
                automargin=False,
                ticklabelposition="inside",
                showline=False,
                linewidth=1,
                linecolor=cfg["border"],
                zeroline=False,
            ),
            # X轴设置
            xaxis=dict(
                gridcolor=cfg["grid"],
                tickfont=dict(
                    size=base_font, color=text_color, family=self.font_family
                ),
                domain=[chart_domain_left, chart_domain_right],  # 图表区域
                rangeslider=dict(visible=False),
                showline=False,
                linewidth=1,
                linecolor=cfg["border"],
                mirror=True,
                anchor="y",
                tickmode="linear",
                dtick="M5",
                tickformat="%Y-%m",
                hoverformat="%Y-%m-%d",
                showgrid=True,
                position=0.0,
                range=[data_start_date, data_end_date_limited],  # 使用计算的范围
            ),
        )

        return fig
