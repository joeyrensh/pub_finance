import plotly.express as px
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import pandas as pd
import empyrical as ep
from datetime import timedelta
from typing import Any, Literal
from plotly.subplots import make_subplots
import re


class ChartBuilder:
    def __init__(self):
        self.theme_config = {
            "light": {
                "positive-int-color": "#ff4444",  # 红色 - 正数
                "negative-int-color": "#0d876d",  # 绿色 - 负数
                "text-color": "#333333",  # 黑色
                "gridcolor": "#D1D5DB",  # 网格线
                "background-color": "rgba(255, 255, 255, 0)",  # 透明背景
                "legend-bg-color": "rgba(246, 248, 249, 0.2)",
                "strategy-colors": [
                    "#FF4444",
                    "#FF4444",
                    "#0d876d",
                    "#0d876d",
                    "#0d876d",
                    "#64748B",
                    "#64748B",
                    "#64748B",
                ],
                "upgrade-marker-color": "#64748B",
                "total-cnt-color": "#64748B",
                "long": "#ff4444",
                "short": "#0d876d",
                "pnl-colors": [
                    "#0d876d",
                    "#FF4444",
                    "#64748B",
                    "#64748B",
                    "#64748B",
                ],
                "border-color": "#D1D5DB",
                "outside-text-color": "#777777",
                "cumret-line-color": "#ff4444",
                "drawdown-line-color": "#0d876d",
                "table-header-color": "rgba(245,245,245,0)",
                "table-cell-color": "rgba(0,0,0,0)",
                "hover-bg-color": "#ffffff",
                "hover-text-color": "#333333",
                "hover-border-color": "#cccccc",
            },
            "dark": {
                "positive-int-color": "#ff4444",  # 亮红色 - 正数
                "negative-int-color": "#00875A",  # 亮绿色 - 负数
                "text-color": "rgba(255, 255, 255, 0.9)",  # 白色
                "gridcolor": "#2D3748",  # 网格线
                "background-color": "rgba(0, 0, 0, 0)",  # 透明背景
                "legend-bg-color": "rgba(123, 50, 116, 0.02)",
                "strategy-colors": [
                    "#FF4444",
                    "#FF4444",
                    "#00875A",
                    "#00875A",
                    "#00875A",
                    "#64748B",
                    "#64748B",
                    "#64748B",
                ],
                "upgrade-marker-color": "#64748B",
                "total-cnt-color": "#64748B",
                "long": "#ff4444",
                "short": "#00875A",
                "pnl-colors": [
                    "#00875A",
                    "#FF4444",
                    "#64748B",
                    "#64748B",
                    "#64748B",
                ],
                "border-color": "#2D3748",
                "outside-text-color": "#64748B",
                "cumret-line-color": "#ff4444",
                "drawdown-line-color": "#00875A",
                "table-header-color": "rgba(64,64,64,0)",
                "table-cell-color": "rgba(0,0,0,0)",
                "hover-bg-color": "#1a1a1a",
                "hover-text-color": "rgba(255, 255, 255, 0.9)",
                "hover-border-color": "#666666",
            },
        }
        self.font_family = '"SF Pro Text", "PingFang SC", "Helvetica Neue", sans-serif'

    def _get_scale(
        self,
        base_fig_width=1440,
        client_width=None,
        min_scale=0.6,
        max_scale=1.05,
        mid_min_width=550,  # 中间区间的下限宽度
        mid_min_scale=0.85,  # 屏幕在 550px 时，期望的温和缩放比例 (不至于太小)
    ):
        """计算响应式 Dashboard 字体/线宽缩放比例 (支持平缓线性插值)"""
        # 兜底防御：若未获取到 client_width，默认返回 1.0
        if client_width is None or client_width <= 0:
            return 1.0

        # 1. 大屏区间 (>= 1440px)：保持满额 1.0 缩放
        if client_width >= base_fig_width:
            scale = 1.0

        # 2. 中屏插值区间 (550px ~ 1440px)：平缓过渡，避免过大或过小
        elif client_width >= mid_min_width:
            progress = (client_width - mid_min_width) / (base_fig_width - mid_min_width)
            scale = mid_min_scale + progress * (1.0 - mid_min_scale)

        # 3. 小屏/移动端区间 (< 550px)：按比例缩放，依靠 min_scale 保底
        else:
            # 在极小屏下按比例递减
            scale = client_width / base_fig_width

        # 4. 最终边界值约束
        return max(min_scale, min(scale, max_scale))

    def _get_font_sizes(
        self, client_width, base_font=12, min_scale=0.9, max_scale=1.05
    ):
        """获取字体大小"""
        scale = self._get_scale(1440, client_width, min_scale, max_scale)
        font_size = int(base_font * scale)
        return scale, font_size

    def _get_alpha_color(self, color_str: str, alpha: float = 0.5) -> str:
        """兼容 Hex (#0d876d) 和 rgba(...) 格式，并统一覆盖为新的 alpha 透明度。

        :param color_str: "#0d876d" 或 "rgba(13,135,109,0.3)" 或 "rgb(13,135,109)"
        :param alpha: 透明度 (0.0 到 1.0)
        """
        if not color_str:
            return f"rgba(150, 150, 150, {alpha})"

        color_str = color_str.strip()

        # 1. 匹配 rgba(...) 或 rgb(...) 格式
        rgb_match = re.match(
            r"^rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)", color_str, re.IGNORECASE
        )
        if rgb_match:
            r, g, b = rgb_match.groups()
            return f"rgba({r}, {g}, {b}, {alpha})"

        # 2. 匹配 #RRGGBB 或 #RGB 格式
        if color_str.startswith("#"):
            hex_str = color_str.lstrip("#")
            if len(hex_str) == 3:  # 缩写格式如 #f00 -> #ff0000
                hex_str = "".join([c * 2 for c in hex_str])
            if len(hex_str) == 6:
                r = int(hex_str[0:2], 16)
                g = int(hex_str[2:4], 16)
                b = int(hex_str[4:6], 16)
                return f"rgba({r}, {g}, {b}, {alpha})"

        # 3. 兜底处理 (若配置传的是特殊值)
        return color_str

    @staticmethod
    def _truncate_text_by_display_width(text, max_display_width=16):
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

    def calendar_heatmap(
        self,
        page,
        df,
        theme="light",
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
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )

        # 获取当前主题配置
        config = self.theme_config.get(theme, self.theme_config["light"])
        hover_config = self.theme_config.get(theme, self.theme_config["light"])
        # 休市颜色使用中性色 + 透明度
        holiday_color = config["text-color"].replace("#", "")
        if len(holiday_color) == 6:  # 如果是hex颜色
            holiday_color = f"rgba({int(holiday_color[0:2], 16)}, {int(holiday_color[2:4], 16)}, {int(holiday_color[4:6], 16)}, 0.5)"
        else:
            holiday_color = config["text-color"]
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

        # ===== 构建包含所有可能周的 week_mapping =====
        # 获取数据中的最新日期（用于计算交易日范围）
        if len(df) > 0:
            latest_date = df["date"].max()
            weekday = latest_date.weekday()
            trading_days = 21 + weekday
            filtered_dates = pd.date_range(
                end=latest_date, periods=trading_days, freq="B"
            )
        else:
            filtered_dates = pd.DatetimeIndex([])

        # 基于交易日范围生成所有可能的周起始
        if len(filtered_dates) > 0:
            all_week_starts = (
                pd.Series(filtered_dates)
                .apply(lambda d: d - pd.to_timedelta(d.weekday(), unit="d"))
                .unique()
            )
            all_week_starts = sorted(all_week_starts)
            week_mapping = {date: i for i, date in enumerate(all_week_starts)}
            total_weeks = len(week_mapping)
        else:
            # 如果没有交易日数据，回退到原始方式（仅基于df）
            unique_weeks = (
                df["week_start"].drop_duplicates().sort_values().reset_index(drop=True)
            )
            week_mapping = {date: i for i, date in enumerate(unique_weeks)}
            total_weeks = len(week_mapping)
        # ===== 结束 =====

        # 为数据中的每行分配 week_order
        df["week_order"] = df["week_start"].map(week_mapping)

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
            if s_pnl >= 0:
                if pos_max > pos_median and pos_max != 0:
                    ratio = max(
                        0, min(1, (s_pnl - pos_median) / (pos_max - pos_median))
                    )
                    ratio_real = s_pnl / pos_max
                    if s_pnl > pos_median:
                        return base_font_size + ratio * max_size_increase, ratio_real
                    return base_font_size, ratio_real
            elif s_pnl < 0:
                if neg_max > neg_median and neg_max != 0:
                    ratio = max(
                        0, min(1, (-s_pnl - neg_median) / (neg_max - neg_median))
                    )
                    ratio_real = s_pnl / neg_max
                    if -s_pnl > neg_median:
                        return base_font_size + ratio * max_size_increase, ratio_real
                    return base_font_size, ratio_real
            return base_font_size, 0

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

        # 生成带并列排名的列表（按计数值排序，相同计数排名相同）
        def get_ranked_industries(counter, top_n=3):
            # 按计数降序排序
            sorted_items = sorted(counter.items(), key=lambda x: x[1], reverse=True)
            ranked = []
            i = 0
            rank = 1
            while i < len(sorted_items) and len(ranked) < top_n:
                count = sorted_items[i][1]
                # 找出所有相同计数的行业
                same_count = []
                j = i
                while j < len(sorted_items) and sorted_items[j][1] == count:
                    same_count.append(sorted_items[j][0])
                    j += 1
                # 如果当前排名还在 top_n 范围内，添加这批行业（可能超出 top_n，但只取前 top_n 个行业）
                for industry in same_count:
                    if len(ranked) < top_n:
                        ranked.append((industry, rank))
                i = j
                rank += 1
            return ranked

        ranked_industries = get_ranked_industries(industry_counter, top_n=3)

        # 第三步：构建行业名称 → 排名字典（用于快速查询）
        industry_rank_map = {ind: r for ind, r in ranked_industries}

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
                dynamic_text_color = config["positive-int-color"]
                hover_bg = hover_config["positive-int-color"]
            elif col3_value < 0:
                dynamic_text_color = config["negative-int-color"]
                hover_bg = hover_config["negative-int-color"]
            else:
                dynamic_text_color = config["text-color"]
                hover_bg = config["text-color"]
                dynamic_font_size = base_font_size

            industry_items = [
                str(item).strip() if item else "" for item in industry_items
            ]

            if len(industry_items) >= 1:
                industry_text_top = industry_items[0]

                rank_text_top = industry_rank_map.get(industry_text_top, 0)

                if len(industry_text_top) > 6:
                    industry_text_top = self._truncate_text_by_display_width(
                        industry_text_top, 12
                    )

                # rank_symbols = {1: "①", 2: "②", 3: "③"}
                # rank_symbols = {1: "¹", 2: "²", 3: "³"}
                rank_symbols = {1: "#1", 2: "#2", 3: "#3"}

                if rank_text_top in rank_symbols:
                    industry_text_top = (
                        f"<span style='font-size:120%; font-weight:bold;'>{rank_symbols[rank_text_top]}</span>"
                        f" {industry_text_top}"
                    )

                font_size_top = (
                    rank_mapping.get(rank_text_top, 0)
                    if page.startswith("cn")
                    else rank_mapping.get(rank_text_top, 0) * 0.2
                )

                fig.add_annotation(
                    x=day_of_week,
                    y=week_order,
                    text=industry_text_top,
                    showarrow=False,
                    font=dict(
                        family=self.font_family,
                        # size=(base_font_size + font_size_top),
                        size=base_font_size,
                        color=config["text-color"],
                    ),
                    align="center",
                    xanchor="center",
                    yanchor="bottom",
                    opacity=0.9,
                )

            # # ===============================
            # # 下方行业（第一大行业）
            # # ===============================
            # if len(industry_items) >= 1:
            #     industry_text_bottom = industry_items[0]

            #     rank_text_bottom = next(
            #         (
            #             i + 1
            #             for i, (ind, _) in enumerate(industry_counter.most_common(3))
            #             if ind == industry_text_bottom
            #         ),
            #         0,
            #     )

            #     if len(industry_text_bottom) > 6:
            #         industry_text_bottom = self._truncate_text_by_display_width(
            #             industry_text_bottom, 12
            #         )

            #     # ⭐ Top3 行业加粗
            #     if rank_text_bottom in rank_symbols:
            #         industry_text_bottom = (
            #             f"{rank_symbols[rank_text_bottom]} {industry_text_bottom}"
            #         )

            #     font_size_bottom = (
            #         rank_mapping.get(rank_text_bottom, 0)
            #         if page.startswith("cn")
            #         else rank_mapping.get(rank_text_bottom, 0) * 0.2
            #     )

            #     fig.add_annotation(
            #         x=day_of_week + 0.1,
            #         y=week_order,
            #         text=industry_text_bottom,
            #         showarrow=False,
            #         font=dict(
            #             family=self.font_family,
            #             # size=(base_font_size + font_size_bottom),
            #             size=base_font_size,
            #             color=config["text-color"],
            #         ),
            #         align="center",
            #         xanchor="center",
            #         yanchor="bottom",
            #         opacity=0.9,
            #     )

            # ===============================
            # 日期（月 / 日）
            # ===============================
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=month + day,
                showarrow=False,
                align="center",
                xanchor="center",
                yanchor="top",
                yshift=int(dynamic_font_size * scale * 0.48),
                font=dict(
                    family=self.font_family,
                    size=dynamic_font_size,
                    color=dynamic_text_color,
                ),
                opacity=0.9,
            )

        # 设置图表布局
        if total_weeks > 0:
            yaxis_range = [total_weeks - 0.5, -0.5]
        else:
            yaxis_range = None

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
                    color=config["text-color"],
                ),
            ),
            yaxis=dict(
                showgrid=False,
                zeroline=False,
                showticklabels=False,
                autorange=False,
                range=yaxis_range,
            ),
            plot_bgcolor=config["background-color"],
            paper_bgcolor=config["background-color"],
            margin=dict(l=0, r=0, t=0, b=0, pad=0),
            autosize=True,
            dragmode=False,
            hoverlabel=dict(
                bgcolor=hover_bg,
                font_size=base_font_size,
                # font_color=config["hover-text-color"],
            ),
        )

        # ===== 简化的分割线绘制 =====
        # 横向分割线：每周之间
        for week in range(1, total_weeks):
            fig.add_hline(
                y=week - 0.5,
                line=dict(
                    color=config["gridcolor"],
                    width=1,
                ),
                opacity=0.6,
                layer="below",
            )

        # 纵向分割线：每列（工作日）之间，画在相邻工作日之间
        # 工作日为0=周一,1=周二,2=周三,3=周四,4=周五，画在1-2,2-3,3-4,4-5之间
        for day in range(1, 5):
            fig.add_vline(
                x=day - 0.5,
                line=dict(
                    color=config["gridcolor"],
                    width=1,
                ),
                opacity=0.6,
                layer="below",
            )
        # ===== 结束 =====

        # 缺失日期处理
        if len(df) > 0 and len(filtered_dates) > 0:
            existing_dates = set(df["date"])
            missing_dates = set(filtered_dates) - existing_dates
            missing_dates = [date for date in missing_dates if date.weekday() < 5]

            for missing_date in missing_dates:
                day_of_week = missing_date.dayofweek
                week_start = missing_date - pd.to_timedelta(day_of_week, unit="d")
                week_order = week_mapping[week_start]

                fig.add_annotation(
                    x=day_of_week,
                    y=week_order,
                    text="休市",
                    showarrow=False,
                    font=dict(
                        family=self.font_family,
                        size=base_font_size,
                        color=config["text-color"],
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
            f"<b>行业: </b>{inds[0]},{inds[1]},{inds[2]}<br>"
            "<extra></extra>"
        )

        return hover_lines

    # =========================================================
    # 1. 边缘防截断：智能文本位置计算（保持 r=0 贴边）
    # =========================================================
    @staticmethod
    def _get_smart_textposition(x_val, xmin, xmax, default_pos="top center"):
        """
        如果数据点落在时间轴最右侧 10% 区域，文本强行改用 left 方向（向左延伸），避免右侧被截断
        如果数据点落在时间轴最左侧 10% 区域，文本强行改用 right 方向（向右延伸）
        """
        if pd.isna(x_val) or pd.isna(xmin) or pd.isna(xmax):
            return default_pos

        total_days = (xmax - xmin).days
        if total_days <= 0:
            return default_pos

        current_offset = (x_val - xmin).days
        ratio = current_offset / total_days

        # 最右侧 10% 区域：强行左靠 (top left / bottom left / middle left)
        if ratio >= 0.90:
            if "top" in default_pos:
                return "top left"
            elif "bottom" in default_pos:
                return "bottom left"
            return "middle left"
        # 最左侧 10% 区域：强行右靠
        elif ratio <= 0.10:
            if "top" in default_pos:
                return "top right"
            elif "bottom" in default_pos:
                return "bottom right"
            return "middle right"

        return default_pos

    @staticmethod
    def _filter_overlapping_maxima(
        max_points,
        date_threshold_days=15,
        val_threshold_pct=0.10,
        val_threshold_abs=0.05,
    ):
        if not max_points:
            return set()

        # 按绝对值从大到小排序
        sorted_points = sorted(max_points, key=lambda p: abs(p["y"]), reverse=True)
        kept_points = []

        for pt in sorted_points:
            has_conflict = False
            for kept in kept_points:
                # 1. 计算日期天数差
                day_diff = abs((pt["x"] - kept["x"]).days)

                # 2. 计算数值绝对差与相对差
                abs_diff = abs(pt["y"] - kept["y"])
                base_val = max(abs(kept["y"]), abs(pt["y"]))
                rel_diff = abs_diff / base_val if base_val != 0 else 0

                # -------------------------------------------------------------
                # 碰撞判定逻辑重构：
                # 如果是百分比/比例数据（y 绝对值小于等于 1.5），强制使用绝对差 val_threshold_abs 判定
                # -------------------------------------------------------------
                if abs(pt["y"]) <= 1.5:  # 判定为百分比/比例数据
                    is_val_conflict = abs_diff <= val_threshold_abs
                else:  # 普通大数值（如 PnL 金额、股票数量）
                    is_val_conflict = rel_diff <= val_threshold_pct
                    if val_threshold_abs is not None:
                        is_val_conflict = is_val_conflict or (
                            abs_diff <= val_threshold_abs
                        )

                # 当【天数相近】且【视觉数值接近】时，判定为碰撞，淘汰较小者
                if day_diff <= date_threshold_days and is_val_conflict:
                    has_conflict = True
                    break

            if not has_conflict:
                kept_points.append(pt)

        return {p["strat"] for p in kept_points}

    def strategy_chart(
        self,
        page,
        df,
        theme="light",
        client_width=1440,
        bar_metric="future_pnl",  # 可选: 'pnl', 'cnt', 'avg', 'future_pnl', None
        trend_metric="success_rate",  # 可选: 'success_rate', 'symbol_ratio'
        future_pnl_col="ret_5d_future_avg",  # 当 bar_metric 为 'future_pnl' 时使用
        success_rate_col="success_rate_5d",  # 当 trend_metric 为 'success_rate' 时使用
        use_ema=True,  # 是否对 trend_metric 使用 EMA 平滑曲线
        show_max_annotation=True,  # 是否开启最大值标注
        collision_date_days=15,  # 碰撞判断：相近天数阈值
        collision_val_pct=0.05,  # 碰撞判断：数值差距百分比阈值
    ):
        cfg = self.theme_config.get(theme, self.theme_config["light"])
        text_color = cfg["text-color"]
        grid_color = cfg["gridcolor"]
        legend_bg = cfg["legend-bg-color"]
        strategy_colors = cfg["strategy-colors"]
        scale, base_font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )
        unified_scale, unified_font_size = self._get_font_sizes(
            client_width, base_font=16, min_scale=0.65, max_scale=1.05
        )

        df = df.copy()
        if "date" in df.columns and "strategy" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values(["strategy", "date"]).reset_index(drop=True)
            if (
                bar_metric == "future_pnl"
                and trend_metric == "success_rate"
                and not df.empty
            ):
                last_date = df["date"].max()
                df = df[df["date"] < last_date].reset_index(drop=True)

        show_bar = bar_metric is not None

        # =========================
        # 1. 数据预处理 (Bar 逻辑)
        # =========================
        bar_hover_fmt = None
        is_ratio_metric = (
            True
            if future_pnl_col
            in [
                "ret_5d_future_sum",
                "ret_5d_future_avg",
                "ret_1d_future_sum",
                "ret_1d_future_avg",
            ]
            else False
        )

        if show_bar:
            if bar_metric == "pnl":
                df["bar_pos"] = df["pnl"].clip(lower=0)
                df["bar_neg"] = df["pnl"].clip(upper=0)
                bar_hover_fmt = "<b>持仓PnL</b>: %{y:,.2f}"
            elif bar_metric == "cnt":
                df["bar_pos"] = df["cnt"]
                df["bar_neg"] = 0
                bar_hover_fmt = "<b>股票数量</b>: %{y:d}"
            elif bar_metric == "avg":
                df["avg"] = (df["pnl"] / df["cnt"].replace(0, np.nan)).fillna(0)
                df["bar_pos"] = df["avg"].clip(lower=0)
                df["bar_neg"] = df["avg"].clip(upper=0)
                bar_hover_fmt = "<b>单票平均PnL</b>: %{y:,.2f}"
            elif bar_metric == "future_pnl":
                if future_pnl_col not in df.columns:
                    raise ValueError(
                        f"future_pnl_col '{future_pnl_col}' does not exist in dataframe."
                    )
                df["bar_pos"] = df[future_pnl_col].clip(lower=0)
                df["bar_neg"] = df[future_pnl_col].clip(upper=0)

                col_lower = future_pnl_col.lower()
                day_prefix = (
                    "1日" if "1d" in col_lower else ("5日" if "5d" in col_lower else "")
                )

                if is_ratio_metric:
                    metric_label = (
                        f"{day_prefix}预期收益率" if day_prefix else "预期收益率"
                    )
                    bar_hover_fmt = f"<b>{metric_label}</b>: %{{y:+.2%}}"
                else:
                    metric_label = f"{day_prefix}预期PnL" if day_prefix else "预期PnL"
                    bar_hover_fmt = f"<b>{metric_label}</b>: %{{y:,.2f}}"

            df_pos_sum = df.groupby("date")["bar_pos"].sum()
            df_neg_sum = df.groupby("date")["bar_neg"].sum()

            if not df_pos_sum.empty and not df_neg_sum.empty:
                # 1. 过滤正值：只保留 <= 95% 分位数的数据（剔除最高的 5% 极端值），然后取最大值
                pos_cutoff = df_pos_sum.quantile(0.9)
                max_pos = df_pos_sum[df_pos_sum <= pos_cutoff].max()

                # 2. 过滤负值：只保留 >= 5% 分位数的数据（剔除最深的 5% 极端值），然后取最小值
                neg_cutoff = df_neg_sum.quantile(0.1)
                min_neg = df_neg_sum[df_neg_sum >= neg_cutoff].min()
            else:
                max_pos, min_neg = 0, 0
            # =========================================================
            # Y 轴 Range 计算逻辑 (加入 10% 缓冲区与 Ratio 精确控制)
            # =========================================================
            if bar_metric == "cnt":
                max_range = max_pos * 1.1 if max_pos > 0 else 10
                min_range = 0
            else:
                if is_ratio_metric:
                    if max_pos > 0:
                        max_range = min(1.0, max_pos * 1.10)
                    else:
                        max_range = 0.01  # 全负数时，给上方留一点空间，防止标注被裁

                    if min_neg < 0:
                        min_range = max(-1.0, min_neg * 1.10)
                    else:
                        min_range = 0.0  # 全正数时，0轴直接贴底
                else:
                    # 普通金额 (PnL, Avg) 类型
                    min_floor = 1.0
                    safe_max_pos = max(max_pos, min_floor)
                    safe_abs_neg = max(abs(min_neg), min_floor)

                    max_range = safe_max_pos * 1.10
                    min_range = -safe_abs_neg * 1.10
        else:
            min_range, max_range = 0, 1

        # =========================
        # 2. 数据预处理 (Trend 逻辑)
        # =========================
        if trend_metric == "success_rate":
            target_col = (
                success_rate_col
                if success_rate_col in df.columns
                else "success_rate_5d"
            )
            rate_label = "成功率(5日)" if "5d" in target_col else "成功率(1日)"
            hover_fmt = f"<b>{rate_label}</b>: %{{y:.2%}}"
        elif trend_metric == "symbol_ratio":
            target_col = "symbol_ratio"
            hover_fmt = "<b>股票占比</b>: %{y:.2%}"
        else:
            raise ValueError("trend_metric must be 'success_rate' or 'symbol_ratio'")

        # 1. 优先生成实际用于绘图的 trend_val 列
        if use_ema:
            span = 20 if isinstance(use_ema, bool) else use_ema
            smooth_series = df.groupby("strategy")[target_col].transform(
                lambda x: x.ewm(span=span, adjust=False).mean()
            )
            df["trend_val"] = smooth_series.where(df[target_col].notna())
        else:
            df["trend_val"] = df[target_col]

        # 2. 根据实际上图的 trend_val 评估 Y 轴 Range
        if trend_metric == "success_rate":
            # 过滤掉 dropna 和可能的异常 0 值（仅针对正常波动区间评估）
            valid_rates = df["trend_val"].dropna()

            if not valid_rates.empty:
                raw_min = valid_rates.min()
                raw_max = valid_rates.max()

                # 基于平滑后的真实起伏，设置 10%~15% 的紧致缓冲区
                if raw_max >= 1.0:
                    y1_max = 1.1
                else:
                    y1_max = min(1.0, raw_max * 1.1)

                y1_min = max(0.0, raw_min * 0.9)
                y1_dtick = (y1_max - y1_min) / 3
            else:
                y1_min, y1_max, y1_dtick = 0.0, 1.1, 1 / 3

            y1_range = [y1_min, y1_max]
            y1_autorange = False
        else:  # symbol_ratio
            y1_range, y1_dtick, y1_autorange = None, None, True

        # =========================
        # 3. 策略分组与 Core 策略计算
        # =========================
        strategy_order = [
            "多头排列",
            "突破年线",
            "均线金叉",
            "均线收敛",
            "突破半年线",
            "成交量放大",
            "红三兵",
            "连续上涨",
        ]

        group_long = ["多头排列", "突破年线"]
        group_mid = ["均线金叉", "均线收敛", "突破半年线"]
        group_short = ["成交量放大", "红三兵", "连续上涨"]

        core_strategies = []
        compare_col = "trend_val" if "trend_val" in df.columns else target_col

        if not df.empty and compare_col in df.columns:
            max_date = df["date"].max()
            cutoff_date = max_date - pd.Timedelta(days=1)

            df_recent = df[
                (df["date"] >= cutoff_date) & (df[compare_col].notna())
            ].copy()

            if not df_recent.empty:
                df_latest_in_window = (
                    df_recent.sort_values("date")
                    .groupby("strategy")
                    .last()
                    .reset_index()
                )
            else:
                df_latest_in_window = pd.DataFrame(columns=["strategy", compare_col])

            strat_score_map = {}
            for strat in strategy_order:
                match = df_latest_in_window[df_latest_in_window["strategy"] == strat]
                if not match.empty:
                    val = match.iloc[0][compare_col]
                    try:
                        strat_score_map[strat] = float(val) if pd.notna(val) else 0.0
                    except (ValueError, TypeError):
                        strat_score_map[strat] = 0.0
                else:
                    strat_score_map[strat] = 0.0

            def get_best_strat_in_group(group_list):
                sorted_group = sorted(
                    group_list, key=lambda s: strat_score_map.get(s, 0.0), reverse=True
                )
                return sorted_group[0]

            best_long = get_best_strat_in_group(group_long)
            best_mid = get_best_strat_in_group(group_mid)
            best_short = get_best_strat_in_group(group_short)

            core_strategies = [best_long, best_mid, best_short]

        if not core_strategies:
            core_strategies = [group_long[0], group_mid[0], group_short[0]]

        line_styles = [
            {"dash": "solid", "width": 1.5},
            {"dash": "solid", "width": 1.5},
            {"dash": "solid", "width": 1.5},
            {"dash": "solid", "width": 1.5},
            {"dash": "solid", "width": 1.5},
            {"dash": "dash", "width": 1.5},
            {"dash": "dash", "width": 1.5},
            {"dash": "dash", "width": 1.5},
        ]

        strategy_cfg = {
            strat: {
                "color": strategy_colors[idx],
                "style": line_styles[idx],
                "rank": idx,
            }
            for idx, strat in enumerate(strategy_order)
        }

        valid_strategies = [s for s in strategy_order if s in df["strategy"].values]
        df_sorted = df[df["strategy"].isin(valid_strategies)].copy()

        xmin = pd.to_datetime(df["date"].min())
        xmax = pd.to_datetime(df["date"].max())

        # =========================================================
        # 4. 预先计算碰撞筛选（分别处理 Trend 和 Bar）
        # =========================================================
        allowed_trend_annotation_strats = set()
        allowed_bar_annotation_strats = set()

        if show_max_annotation:
            # A. 收集所有 Valid 策略的 Trend Line Max 点候选
            trend_candidates = []
            for strat in valid_strategies:
                s_data = df_sorted[df_sorted["strategy"] == strat].dropna(
                    subset=["trend_val"]
                )
                if not s_data.empty:
                    m_row = s_data.loc[s_data["trend_val"].idxmax()]
                    trend_candidates.append(
                        {"strat": strat, "x": m_row["date"], "y": m_row["trend_val"]}
                    )

            allowed_trend_annotation_strats = self._filter_overlapping_maxima(
                trend_candidates,
                date_threshold_days=collision_date_days,
                val_threshold_pct=collision_val_pct,
                val_threshold_abs=0.08,
            )

            # B. 收集所有 Valid 策略的 Bar 极值点候选（支持正数最大 & 负数最小）
            if show_bar:
                bar_candidates = []
                for strat in valid_strategies:
                    s_data = df_sorted[df_sorted["strategy"] == strat]
                    if not s_data.empty:
                        pos_data = s_data[s_data["bar_pos"] > 0]
                        if not pos_data.empty:
                            m_row = pos_data.loc[pos_data["bar_pos"].idxmax()]
                            bar_candidates.append(
                                {
                                    "strat": strat,
                                    "x": m_row["date"],
                                    "y": m_row["bar_pos"],
                                }
                            )
                        else:
                            neg_data = s_data[s_data["bar_neg"] < 0]
                            if not neg_data.empty:
                                m_row = neg_data.loc[neg_data["bar_neg"].idxmin()]
                                bar_candidates.append(
                                    {
                                        "strat": strat,
                                        "x": m_row["date"],
                                        "y": m_row["bar_neg"],
                                    }
                                )

                allowed_bar_annotation_strats = self._filter_overlapping_maxima(
                    bar_candidates,
                    date_threshold_days=collision_date_days,
                    val_threshold_pct=collision_val_pct,
                    val_threshold_abs=0.03,
                )

        fig = go.Figure()

        # =========================================================
        # 5. Bar Trace & 碰撞防护后的 Bar Max 标注
        # =========================================================
        if show_bar:
            for strat in valid_strategies:
                data = df_sorted[df_sorted["strategy"] == strat]
                color = strategy_cfg[strat]["color"]
                is_core = strat in core_strategies
                visibility = True if is_core else "legendonly"

                if bar_metric in ["pnl", "avg", "future_pnl"]:
                    pos = data[data["bar_pos"] > 0]
                    neg = data[data["bar_neg"] < 0]

                    if not pos.empty:
                        fig.add_trace(
                            go.Bar(
                                x=pos["date"],
                                y=pos["bar_pos"],
                                xaxis="x2",
                                yaxis="y2",
                                marker=dict(
                                    color=color,
                                    line=dict(color=color, width=0.8 * scale),
                                ),
                                showlegend=False,
                                legendgroup=strat,
                                hovertemplate=f"{strat} {bar_hover_fmt}<extra></extra>",
                                visible=visibility,
                                cliponaxis=False,
                            )
                        )
                    if not neg.empty:
                        fig.add_trace(
                            go.Bar(
                                x=neg["date"],
                                y=neg["bar_neg"],
                                xaxis="x2",
                                yaxis="y2",
                                marker=dict(
                                    color=color,
                                    line=dict(color=color, width=0.8 * scale),
                                ),
                                showlegend=False,
                                legendgroup=strat,
                                hovertemplate=f"{strat} {bar_hover_fmt}<extra></extra>",
                                visible=visibility,
                                cliponaxis=False,
                            )
                        )
                else:  # cnt
                    fig.add_trace(
                        go.Bar(
                            x=data["date"],
                            y=data["bar_pos"],
                            xaxis="x2",
                            yaxis="y2",
                            marker=dict(
                                color=color, line=dict(color=color, width=0.8 * scale)
                            ),
                            showlegend=False,
                            legendgroup=strat,
                            hovertemplate=f"{strat} {bar_hover_fmt}<extra></extra>",
                            visible=visibility,
                            cliponaxis=False,
                        )
                    )

                # --- Bar 标注绘制逻辑（经过碰撞过滤 + 最右侧 top left 避让）---
                if strat in allowed_bar_annotation_strats:
                    pos_data = data[data["bar_pos"] > 0]
                    if not pos_data.empty:
                        max_bar_row = pos_data.loc[pos_data["bar_pos"].idxmax()]
                        max_x = max_bar_row["date"]
                        max_y = max_bar_row["bar_pos"]
                        default_pos = "top center"
                    else:
                        neg_data = data[data["bar_neg"] < 0]
                        if not neg_data.empty:
                            max_bar_row = neg_data.loc[neg_data["bar_neg"].idxmin()]
                            max_x = max_bar_row["date"]
                            max_y = max_bar_row["bar_neg"]
                            default_pos = "bottom center"
                        else:
                            continue

                    # 算出的防截断方位（最右侧 10% 自动变 top left / bottom left）
                    smart_pos = self._get_smart_textposition(
                        max_x, xmin, xmax, default_pos=default_pos
                    )
                    val_str = f"{max_y:+.1%}" if is_ratio_metric else f"{max_y:,.0f}"

                    fig.add_trace(
                        go.Scatter(
                            x=[max_x],
                            y=[max_y],
                            xaxis="x2",
                            yaxis="y2",
                            mode="text",
                            text=[f"{val_str}"],
                            textposition=smart_pos,
                            textfont=dict(
                                size=int(base_font_size),
                                color=color,
                                family=self.font_family,
                            ),
                            showlegend=False,
                            legendgroup=strat,
                            hoverinfo="skip",
                            visible=visibility,
                            cliponaxis=False,
                        )
                    )

        # =========================================================
        # 6. Trend Line & 碰撞防护后的 Trend Max 标注
        # =========================================================
        for strat in valid_strategies:
            data = df_sorted[df_sorted["strategy"] == strat]
            cfg_item = strategy_cfg[strat]
            is_core = strat in core_strategies
            visibility = True if is_core or not show_bar else "legendonly"

            fig.add_trace(
                go.Scatter(
                    x=data["date"],
                    y=data["trend_val"],
                    mode="lines",
                    name=strat,
                    connectgaps=False,
                    visible=visibility,
                    line=dict(
                        width=cfg_item["style"]["width"],
                        dash=cfg_item["style"]["dash"],
                        color=cfg_item["color"],
                    ),
                    xaxis="x",
                    yaxis="y1",
                    hovertemplate=f"{strat} {hover_fmt}<extra></extra>",
                    legendgroup=strat,
                    legendrank=cfg_item["rank"],
                    cliponaxis=False,
                )
            )

            # --- Trend 标注绘制逻辑（经过碰撞过滤 + 最右侧 top left 避让）---
            if strat in allowed_trend_annotation_strats:
                valid_trend_data = data.dropna(subset=["trend_val"])
                if not valid_trend_data.empty:
                    max_trend_row = valid_trend_data.loc[
                        valid_trend_data["trend_val"].idxmax()
                    ]
                    max_x = max_trend_row["date"]
                    max_y = max_trend_row["trend_val"]

                    max_val_str = (
                        f"{max_y:.1%}"
                        if trend_metric == "success_rate" or "ratio" in trend_metric
                        else f"{max_y:.2f}"
                    )

                    # 算出的防截断方位（最右侧 10% 自动变 top left）
                    smart_pos = self._get_smart_textposition(
                        max_x, xmin, xmax, default_pos="top center"
                    )

                    fig.add_trace(
                        go.Scatter(
                            x=[max_x],
                            y=[max_y],
                            xaxis="x",
                            yaxis="y",
                            mode="markers+text",
                            marker=dict(
                                symbol="circle",
                                size=6 * unified_scale,
                                color=cfg_item["color"],
                            ),
                            text=[f"{max_val_str}"],
                            textposition=smart_pos,
                            textfont=dict(
                                size=int(base_font_size),
                                color=cfg_item["color"],
                                family=self.font_family,
                            ),
                            showlegend=False,
                            legendgroup=strat,
                            hoverinfo="skip",
                            visible=visibility,
                        )
                    )

        # =========================================================
        # 7. Layout 配置（维持严格贴边 r=0）
        # =========================================================
        y1_domain = [0.35, 1.0] if show_bar else [0.0, 1.0]
        y2_domain = [0.0, 0.35]

        yaxis_config = dict(
            domain=y1_domain,
            side="left",
            mirror=False,
            ticklabelposition="inside",
            showticklabels=False,
            ticks="",
            tickfont=dict(
                family=self.font_family,
                size=base_font_size,
                color=text_color,
            ),
            showline=False,
            zeroline=False,
            gridcolor=grid_color,
            gridwidth=0.5,
            automargin=False,
        )

        if y1_autorange:
            yaxis_config["autorange"] = True
        elif y1_range is not None:
            yaxis_config["autorange"] = False
            yaxis_config["range"] = y1_range

        if y1_dtick is not None:
            yaxis_config["tickmode"] = "linear"
            yaxis_config["dtick"] = y1_dtick

        y2_tickfmt = "%" if is_ratio_metric else "~s"
        # 1. 针对比例/收益率类型的 Bar 图设置合理的 dtick
        y2_dtick = None
        if show_bar and is_ratio_metric:
            span = max_range - min_range
            y2_dtick = span / 3 if span > 0 else 0.05

        yaxis2_config = dict(
            domain=y2_domain,
            side="right",
            showgrid=True,
            gridcolor=grid_color,
            gridwidth=0.5,
            tickfont=dict(
                family=self.font_family,
                size=base_font_size,
                color=text_color,
            ),
            showline=False,
            zeroline=True,
            zerolinecolor=grid_color,
            zerolinewidth=0.5,
            range=[min_range, max_range],
            tickformat=y2_tickfmt,
            layer="below traces",
            ticklabelposition="inside",
            showticklabels=False,
            ticks="",
        )

        common_xaxis_args = dict(
            mirror=False,
            automargin=False,
            tickangle=0,
            showline=False,
            zeroline=False,
            domain=[0.0, 1.0],
            rangeslider=dict(visible=False),
            linecolor=grid_color,
            linewidth=1,
            gridcolor=grid_color,
            gridwidth=0.5,
            tickmode="linear",
            dtick="M1",
            tickformat="%Y-%m",
            hoverformat="%Y-%m-%d",
            range=[
                xmin - timedelta(days=0.5),
                xmax + timedelta(days=0.5),
            ],
        )
        if y2_dtick is not None:
            yaxis2_config["dtick"] = y2_dtick

        fig.update_layout(
            xaxis=dict(
                **common_xaxis_args,
                anchor="y",
                showticklabels=False,
            ),
            xaxis2=dict(
                **common_xaxis_args,
                anchor="y2",
                side="bottom",
                showticklabels=True,
                matches="x",
                tickfont=dict(
                    family=self.font_family,
                    size=base_font_size,
                    color=text_color,
                ),
            ),
            yaxis=yaxis_config,
            yaxis2=yaxis2_config,
            legend=dict(
                orientation="v",
                x=0,
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
            ),
            barmode="relative",
            bargap=0.4,
            bargroupgap=0.2,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=5, l=0, r=0),
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
        text_color = cfg["text-color"]
        grid_color = cfg["gridcolor"]

        scale, font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )
        unified_scale, unified_font_size = self._get_font_sizes(
            client_width, base_font=16, min_scale=0.65, max_scale=1.05
        )

        # =========================
        # 3. Figure & traces（原逻辑）
        # =========================
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["buy_date"],
                y=df["total_cnt"],
                mode="lines",
                name="Total",
                # line=dict(color=cfg["total-cnt-color"], width=3 * unified_scale),
                line=dict(color=cfg["total-cnt-color"], width=1.5),
                yaxis="y",
                hovertemplate=(
                    # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                    "<b>总数</b>: %{y}<br><extra></extra>"
                ),
            )
        )

        fig.add_trace(
            go.Bar(
                x=df["buy_date"],
                y=df["buy_cnt"],
                name="Long",
                marker=dict(
                    color=cfg["long"], line=dict(color=cfg["long"], width=0.8 * scale)
                ),
                yaxis="y",
                hovertemplate=(
                    # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                    "<b>买入数量</b>: %{y}<br><extra></extra>"
                ),
            )
        )

        fig.add_trace(
            go.Bar(
                x=df["buy_date"],
                y=df["sell_cnt"],
                name="Short",
                marker=dict(
                    color=cfg["short"], line=dict(color=cfg["short"], width=0.8 * scale)
                ),
                yaxis="y",
                hovertemplate=(
                    # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                    "<b>卖出数量</b>: %{y}<br><extra></extra>"
                ),
            )
        )

        xmin = pd.to_datetime(df["buy_date"].min())
        xmax = pd.to_datetime(df["buy_date"].max())
        ymax = df["total_cnt"].max()

        fig.update_layout(
            # title=dict(
            #     # text="Last 180 days trade info",
            #     text=r"""
            #         $\mathrm{成交}_{180d} = \sum_{i=1}^{N} \mathrm{买入}_i -
            #         \sum_{j=1}^{M} \mathrm{卖出}_j$
            #         """,
            #     y=0.9,
            #     x=0.5,
            #     font=dict(
            #         size=title_font_size,
            #         color=text_color,
            #         family=self.font_family,
            #     ),
            # ),
            title="",
            xaxis=dict(
                mirror=False,
                automargin=False,
                tickangle=0,
                tickfont=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                showline=False,
                zeroline=False,
                linecolor=grid_color,
                linewidth=1,
                gridcolor=grid_color,
                gridwidth=0.5,
                tickmode="linear",
                dtick="M1",
                tickformat="%Y-%m",
                hoverformat="%Y-%m-%d",
                range=[
                    xmin - timedelta(days=0.5),
                    xmax + timedelta(days=0.5),
                ],
            ),
            yaxis=dict(
                title=None,
                side="left",
                mirror=False,
                showticklabels=False,  # 关闭刻度标签
                ticks="",  # 关闭刻度线
                tickfont=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                showline=False,
                gridcolor=grid_color,
                gridwidth=0.5,
                ticklabelposition="inside",
                tickangle=0,
                zeroline=False,
                autorange=True,
                dtick=ymax / 3,
            ),
            legend=dict(
                orientation="v",
                x=0,
                y=1,
                xanchor="left",
                yanchor="top",
                font=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                bgcolor=cfg["legend-bg-color"],
                borderwidth=0,
                tracegroupgap=0,
            ),
            barmode="stack",
            bargap=0.4,
            bargroupgap=0.2,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=5, l=0, r=0),
            autosize=True,
            dragmode=False,
            hovermode="x",
            hoverlabel=dict(font_size=font_size),
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
        text_color = cfg["text-color"]

        scale, font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )
        unified_scale, unified_font_size = self._get_font_sizes(
            client_width, base_font=16, min_scale=0.65, max_scale=1.05
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
            color_discrete_sequence=cfg["pnl-colors"],
        )
        fig.update_traces(
            # line=dict(width=3 * unified_scale),
            line=dict(width=1.5),
            hovertemplate=(
                # "<b>日期</b>: %{x|%Y-%m-%d}<br>"
                "<b>收益</b>: %{fullData.name} %{y}<br><extra></extra>"
            ),
        )

        xmin = pd.to_datetime(df["buy_date"].min())
        xmax = pd.to_datetime(df["buy_date"].max())
        ymax = df["pnl"].max()

        fig.update_xaxes(
            title=None,
            mirror=False,
            automargin=False,
            tickangle=0,
            tickfont=dict(
                size=font_size,
                color=text_color,
                family=self.font_family,
            ),
            showline=False,
            linecolor=cfg["gridcolor"],
            linewidth=1,
            zeroline=False,
            gridcolor=cfg["gridcolor"],
            gridwidth=0.5,
            tickmode="linear",
            dtick="M1",
            tickformat="%Y-%m",
            hoverformat="%Y-%m-%d",
            range=[
                xmin - timedelta(days=0.5),
                xmax + timedelta(days=0.5),
            ],
        )

        fig.update_yaxes(
            title=None,
            mirror=False,
            showticklabels=False,  # 关闭刻度标签
            ticks="",  # 关闭刻度线
            tickfont=dict(
                size=font_size,
                color=text_color,
                family=self.font_family,
            ),
            showline=False,
            linecolor=cfg["gridcolor"],
            linewidth=1,
            zeroline=False,
            gridcolor=cfg["gridcolor"],
            gridwidth=0.5,
            ticklabelposition="inside",
            tickangle=0,
            autorange=True,
            dtick=ymax / 3,
        )

        fig.update_layout(
            # title=dict(
            #     # text="Last 180 days top5 pnl",
            #     text=r"""
            #         $\mathrm{Top5\ 盈亏}_{180d} = \sum_{i=1}^{5} \mathrm{PnL}_i
            #         \ (\text{倒排序})$
            #         """,
            #     x=0.5,
            #     y=0.9,
            #     font=dict(
            #         size=title_font_size,
            #         color=text_color,
            #         family=self.font_family,
            #     ),
            # ),
            title="",
            legend_title_text=None,
            legend=dict(
                orientation="v",
                x=0,
                xanchor="left",
                y=1,
                yanchor="top",
                font=dict(
                    size=font_size,
                    color=text_color,
                    family=self.font_family,
                ),
                bgcolor=cfg["legend-bg-color"],
                borderwidth=0,
                tracegroupgap=0,
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=5, l=0, r=0),
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
        text_color = cfg["text-color"]
        scale, font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
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
                        color=cfg["border-color"],
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
        text_color = cfg["text-color"]
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
                    color=cfg["outside-text-color"],
                    family=self.font_family,
                ),
                textposition="middle center",
                marker=dict(
                    colors=hex_colors,
                    line=dict(
                        color=cfg["border-color"],
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

    def _get_responsive_config(self, client_width):
        """
        基于设备断点 (Breakpoints) 的离散配置系统
        摒弃连续浮点数计算，彻底解决 Plotly 表格与 K 线图缝隙脱节的问题
        """
        w = client_width if (client_width and client_width > 0) else 1440

        # 1. 大屏 / Mac Safari 端 (>= 1200px) -> 1.2rem 视觉体系
        if w >= 1200:
            return {
                "scale": 1.0,
                "main_font": 16,
                "table_font": 16,
                "header_h": 37,
                "cell_h": 38,
                "gap_ratio": 0.05,  # 上下缝隙死死锁定为 3% 画布高度
                "table_ratio_cap": 0.27,  # Table 占画布比例上限
            }

        # 2. 中屏 / 笔记本 & iPad 横屏 (768px ~ 1199px) -> 1.1rem 视觉体系
        elif w >= 768:
            return {
                "scale": 0.9,
                "main_font": 14,
                "table_font": 14,
                "header_h": 38,
                "cell_h": 39,
                "gap_ratio": 0.06,
                "table_ratio_cap": 0.27,
            }

        # 3. 平板竖屏 (550px ~ 767px) -> 1.0rem 视觉体系
        elif w >= 550:
            return {
                "scale": 0.8,
                "main_font": 13,
                "table_font": 13,
                "header_h": 28,
                "cell_h": 29,
                "gap_ratio": 0.06,
                "table_ratio_cap": 0.27,
            }

        # 4. 小屏 / 手机端 (< 550px) -> 0.9rem 极简体系
        else:
            return {
                "scale": 0.65,
                "main_font": 10,
                "table_font": 10,
                "header_h": 21,
                "cell_h": 22,
                "gap_ratio": 0.06,
                "table_ratio_cap": 0.27,
            }

    def annual_return(self, page, pnl: pd.Series, theme="light", client_width=1440):
        """
        生成年度收益图表（上：图，下：表格）
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
        hover_config = self.theme_config.get(theme, self.theme_config["light"])
        text_color = cfg["text-color"]
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
            perf_fmt[col] = perf_fmt[col].apply(lambda x: f"{x * 100:.2f}%")

        # =========================
        # 5. 表格数据（转置：年份为行，指标为列，并按年份降序排列）
        # =========================
        df_transposed = perf_fmt.reset_index().rename(columns={"index": "YEAR"})
        df_transposed = df_transposed.sort_values("YEAR", ascending=False).reset_index(
            drop=True
        )

        table_columns = ["YEAR"] + list(perf_fmt.columns)
        table_data = df_transposed.values.tolist()
        table_df = pd.DataFrame(table_data, columns=table_columns)

        # =========================
        # 6. 表格颜色（仅最新年份行（第一行）的指标根据正负着色）
        # =========================
        latest_year = table_df.iloc[0]["YEAR"]  # 最新年份

        font_colors = []
        for i, row in table_df.iterrows():
            row_colors = []
            for j, col in enumerate(table_columns):
                if j == 0:
                    row_colors.append(text_color)
                else:
                    val_str = row[col]
                    if val_str and val_str != "":
                        try:
                            num_val = float(val_str.replace("%", ""))
                            if row["YEAR"] == latest_year:
                                color = (
                                    cfg["positive-int-color"]
                                    if num_val >= 0
                                    else cfg["negative-int-color"]
                                )
                            else:
                                color = text_color
                        except:
                            color = text_color
                    else:
                        color = text_color
                    row_colors.append(color)
            font_colors.append(row_colors)

        # 转换为列优先（与 cell_values 的列顺序一致）
        font_colors_by_col = [
            [font_colors[i][j] for i in range(len(table_df))]
            for j in range(len(table_columns))
        ]
        font_weights_by_col = ["normal"] * len(table_columns)
        font_sizes_by_col = [table_font + 3] * len(table_columns)

        # 准备表格单元格数据（列优先）
        cell_values = [table_df[col].tolist() for col in table_columns]

        # 表头颜色（原 header 部分）
        header_values = table_columns

        # =========================
        # 7. 创建图表
        # =========================

        fig = go.Figure()

        # =========================
        # 8. 计算Y轴范围
        # =========================
        cum_min = cumulative.min()
        cum_max = cumulative.max()
        cum_range = [cum_min, cum_max]
        cum_q1 = cum_min + (cum_max - cum_min) * 0.33
        cum_q2 = cum_min + (cum_max - cum_min) * 0.66

        dd_min = drawdown.min()
        dd_max = 0
        dd_range = [dd_min, 0.0]
        dd_q1 = dd_min + (dd_max - dd_min) * 0.33
        dd_q2 = dd_min + (dd_max - dd_min) * 0.66

        # =========================
        # 10. 添加回撤曲线
        # =========================
        fig.add_trace(
            go.Scatter(
                x=drawdown.index,
                y=drawdown,
                mode="lines",
                fill="tozeroy",
                fillgradient=dict(
                    type="vertical",
                    colorscale=[
                        (0.0, "rgba(255, 255, 255, 0.0)"),
                        (
                            1.0,
                            self._get_alpha_color(
                                cfg.get("drawdown-line-color"), alpha=0.2
                            ),
                        ),
                    ],
                ),
                name="Drawdown",
                legendgroup="drawdown",
                visible="legendonly",
                line=dict(color=cfg["drawdown-line-color"], width=1),
                hovertemplate=("<b>Drawdown</b>: %{y:.2%}<br><extra></extra>"),
                hoverlabel=dict(
                    bgcolor=hover_config["drawdown-line-color"],
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
                fill="tozeroy",
                fillgradient=dict(
                    type="vertical",
                    colorscale=[
                        (0.0, "rgba(255, 255, 255, 0.0)"),
                        (
                            1.0,
                            self._get_alpha_color(
                                cfg.get("cumret-line-color"), alpha=0.2
                            ),
                        ),
                    ],
                ),
                name="Cum. Return",
                line=dict(color=cfg["cumret-line-color"], width=1.5),
                hovertemplate=("<b>Cum. Return</b>: %{y:.4f}<br><extra></extra>"),
                hoverlabel=dict(
                    bgcolor=hover_config["cumret-line-color"],
                ),
                yaxis="y2",
            )
        )

        # =========================
        # 11. 关键点标注函数（保持原样，未改动）
        # =========================
        def get_label_position(
            index: Any,
            data_series: pd.Series,
            current_value: Any,
            is_near_right_threshold: float = 0.2,
            avoid_indices: list | None = None,
            avoid_days: int = 20,
            avoid_y_threshold: float = 0.02,
            avoid_overflow: bool = True,
            avoid_scale_map: dict | None = None,
            data_series_map: dict | None = None,
        ) -> Literal["top left", "top right", "bottom left", "bottom right"]:
            try:
                right_threshold_idx = data_series.index[
                    -int(len(data_series) * is_near_right_threshold)
                ]
            except Exception:
                right_threshold_idx = data_series.index[0]

            pos = "top left" if index > right_threshold_idx else "top right"

            if avoid_indices:
                for ai in avoid_indices:
                    try:
                        delta_days = abs(
                            (pd.to_datetime(ai) - pd.to_datetime(index)).days
                        )
                        if data_series_map and ai in data_series_map:
                            series = data_series_map[ai]
                        else:
                            series = data_series
                        other_value = series.loc[ai]
                        if avoid_scale_map and ai in avoid_scale_map:
                            other_value = other_value / avoid_scale_map[ai]
                        delta_y = abs(abs(current_value) - abs(other_value))
                    except Exception:
                        continue
                    if delta_days <= avoid_days or delta_y <= avoid_y_threshold:
                        pos = (
                            pos.replace("top", "bottom")
                            if pos.startswith("top")
                            else pos.replace("bottom", "top")
                        )
                        break
            data_min = data_series.min()
            data_max = data_series.max()
            # 如果当前值距离底部小于总范围的 2%（可调整阈值），且 pos 包含 "bottom"
            if (current_value - data_min) <= 0.05 * (
                data_max - data_min
            ) and avoid_overflow:
                if "bottom" in pos:
                    pos = pos.replace("bottom", "top")
            if (data_max - current_value) <= 0.05 * (
                data_max - data_min
            ) and avoid_overflow:
                if "top" in pos:
                    pos = pos.replace("top", "bottom")
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
        # 12. 关键点标注（完全保持原样）
        # =========================
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
                    marker=dict(
                        symbol="circle", size=8 * scale, color=cfg["cumret-line-color"]
                    ),
                    showlegend=False,
                    hovertemplate=f"<b>Latest Cum. Return</b>: {last_y:.4f}<br><extra></extra>",
                    hoverlabel=dict(bgcolor=hover_config["cumret-line-color"]),
                    yaxis="y2",
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=[
                        cumulative.index[
                            max(
                                int(len(cumulative) * (0.25 if scale >= 1 else 0.45)),
                                cumulative.index.get_loc(cumulative.idxmax()),
                            )
                        ]
                    ],
                    y=[cum_max_val],
                    mode="markers+text",
                    marker=dict(
                        symbol="circle", size=8 * scale, color=cfg["cumret-line-color"]
                    ),
                    text=[f"Max: {cum_max_val:.2f}"],
                    textposition=get_label_position(
                        cum_max_idx, cumulative, cum_max_val, avoid_overflow=False
                    ),
                    textfont=dict(
                        size=base_font, color=text_color, family=self.font_family
                    ),
                    cliponaxis=False,
                    showlegend=False,
                    hovertemplate=f"<b>Max Cum. Return</b>: {cum_max_val:.4f}<br><extra></extra>",
                    hoverlabel=dict(bgcolor=hover_config["cumret-line-color"]),
                    yaxis="y2",
                )
            )

        avoid_list = []
        avoid_list.append(cum_max_idx)
        avoid_list.append(cumulative.index[-1])
        avoid_scale_map = {cum_max_idx: 100, cumulative.index[-1]: 100}
        data_series_map = {cum_max_idx: cumulative, cumulative.index[-1]: cumulative}

        if len(drawdown) > 0:
            max_dd_idx = drawdown.idxmin()
            max_dd_val = drawdown.min()

            fig.add_trace(
                go.Scatter(
                    x=[max_dd_idx],
                    y=[max_dd_val],
                    mode="markers+text",
                    visible="legendonly",
                    legendgroup="drawdown",
                    marker=dict(
                        symbol="circle",
                        size=8 * scale,
                        color=cfg["drawdown-line-color"],
                    ),
                    text=[get_compact_label("Max DD", max_dd_val, client_width)],
                    textposition=get_label_position(
                        max_dd_idx,
                        drawdown,
                        max_dd_val,
                        is_near_right_threshold=0,
                        avoid_days=0,
                        avoid_indices=avoid_list,
                        avoid_scale_map=avoid_scale_map,
                        data_series_map=data_series_map,
                    ),
                    textfont=dict(size=base_font, color=text_color),
                    cliponaxis=False,
                    showlegend=False,
                    hovertemplate=f"<b>Max Drawdown</b>: {max_dd_val:.2%}<br><extra></extra>",
                    hoverlabel=dict(bgcolor=hover_config["drawdown-line-color"]),
                    yaxis="y",
                )
            )
            avoid_list.append(max_dd_idx)

            if len(drawdown) >= 30:
                w_30 = drawdown.iloc[-30:]
                idx_30 = w_30.idxmin()
                val_30 = w_30.loc[idx_30]
                if abs((idx_30 - max_dd_idx).days) > 10:
                    fig.add_trace(
                        go.Scatter(
                            x=[idx_30],
                            y=[val_30],
                            mode="markers+text",
                            visible="legendonly",
                            legendgroup="drawdown",
                            marker=dict(
                                symbol="diamond",
                                size=6 * scale,
                                color=cfg["drawdown-line-color"],
                            ),
                            text=[get_compact_label("30D DD", val_30, client_width)],
                            textposition=get_label_position(
                                idx_30,
                                drawdown,
                                val_30,
                                is_near_right_threshold=0.4,
                                avoid_days=0,
                                avoid_indices=avoid_list,
                                avoid_scale_map=avoid_scale_map,
                                data_series_map=data_series_map,
                            ),
                            textfont=dict(size=base_font, color=text_color),
                            showlegend=False,
                            hovertemplate=f"<b>30D Max Drawdown</b>: {val_30:.2%}<br><extra></extra>",
                            hoverlabel=dict(
                                bgcolor=hover_config["drawdown-line-color"]
                            ),
                            yaxis="y",
                        )
                    )
                avoid_list.append(idx_30)

            if len(drawdown) >= 120:
                w_120 = drawdown.iloc[-120:]
                idx_120 = w_120.idxmin()
                val_120 = w_120.loc[idx_120]
                if (
                    abs((idx_120 - max_dd_idx).days) > 10
                    and abs((idx_120 - idx_30).days) > 10
                ):
                    fig.add_trace(
                        go.Scatter(
                            x=[idx_120],
                            y=[val_120],
                            mode="markers+text",
                            visible="legendonly",
                            legendgroup="drawdown",
                            marker=dict(
                                symbol="diamond",
                                size=6 * scale,
                                color=cfg["drawdown-line-color"],
                            ),
                            text=[get_compact_label("120D DD", val_120, client_width)],
                            textposition=get_label_position(
                                idx_120,
                                drawdown,
                                val_120,
                                is_near_right_threshold=0.4,
                                avoid_indices=avoid_list,
                                avoid_scale_map=avoid_scale_map,
                                data_series_map=data_series_map,
                            ),
                            textfont=dict(size=base_font, color=text_color),
                            showlegend=False,
                            hovertemplate=f"<b>120D Max Drawdown</b>: {val_120:.2%}<br><extra></extra>",
                            hoverlabel=dict(
                                bgcolor=hover_config["drawdown-line-color"]
                            ),
                            yaxis="y",
                        )
                    )

        # =========================
        # 13. 布局设置（上下结构：图占上部，表占下部，中间留间距）
        # =========================
        # CHART_DOMAIN_Y = [0.34, 1.0]
        # TABLE_DOMAIN_Y = [0.0, 0.27]

        # 获取当前屏幕断点的布局尺寸配置
        layout_cfg = self._get_responsive_config(client_width)

        # 计算 Table 绝对像素高度及画布占比
        num_rows = len(cell_values[0]) if cell_values and len(cell_values) > 0 else 0
        table_pixel_height = (
            layout_cfg["header_h"] + (layout_cfg["cell_h"] * num_rows) + 15
        )

        # 基于 1.5 比例计算基准画布高度
        base_canvas_height = (client_width if client_width else 1440) / (
            1.6 if (client_width or 1440) <= 550 else 2.2
        )
        raw_table_ratio = table_pixel_height / base_canvas_height

        # 约束 Table 在画布上的 y 轴分配比例
        table_y_ratio = min(layout_cfg["table_ratio_cap"], max(0.5, raw_table_ratio))

        # 无缝垂直 Domain 计算
        TABLE_DOMAIN_Y = [0.0, round(table_y_ratio, 3)]
        CHART_DOMAIN_Y = [round(table_y_ratio + layout_cfg["gap_ratio"], 3), 1.0]

        # 添加 Table (将尺寸用 layout_cfg，色彩用 theme_cfg)
        fig.add_trace(
            go.Table(
                domain=dict(x=[0.0, 1.0], y=TABLE_DOMAIN_Y),
                header=dict(
                    values=header_values,
                    fill_color=cfg["table-header-color"],
                    line=dict(color=cfg["border-color"], width=0.5),
                    font=dict(
                        size=layout_cfg["table_font"],
                        color=cfg["text-color"],
                        family=self.font_family,
                    ),
                    align=["left"] * len(header_values),
                    height=layout_cfg["header_h"],
                ),
                cells=dict(
                    values=cell_values,
                    fill_color=cfg["table-cell-color"],
                    line=dict(color=cfg["border-color"], width=0.5),
                    font=dict(
                        size=font_sizes_by_col,
                        color=font_colors_by_col,
                        family=self.font_family,
                        weight=font_weights_by_col,
                    ),
                    align=["left"] * len(header_values),
                    height=layout_cfg["cell_h"],
                ),
            )
        )

        legend_absolute_x = 0

        # 更新图表布局
        fig.update_layout(
            dragmode=False,
            autosize=True,
            margin=dict(l=0, r=1, t=0, b=0),
            font=dict(size=base_font, color=text_color, family=self.font_family),
            legend=dict(
                x=legend_absolute_x,
                y=1,
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
            hoverlabel=dict(font_size=base_font, font_family=self.font_family),
            bargap=0,
            bargroupgap=0,
            boxgap=0,
            boxgroupgap=0,
            # Y轴（右）设置
            yaxis=dict(
                title="",
                side="right",
                showgrid=False,
                tickfont=dict(
                    size=base_font, color=text_color, family=self.font_family
                ),
                range=dd_range,
                tickmode="array",
                tickvals=[dd_q1, dd_q2],
                ticktext=[f"{dd_q1:.0%}", f"{dd_q2:.0%}"],
                showticklabels=True,
                automargin=False,
                ticklabelposition="inside",
                showline=False,
                linewidth=1,
                linecolor=cfg["border-color"],
                zeroline=False,
                domain=CHART_DOMAIN_Y,  # 图表垂直区域
            ),
            # Y2轴（左）设置
            yaxis2=dict(
                title="",
                side="left",
                overlaying="y",
                showgrid=True,
                gridcolor=cfg["gridcolor"],
                gridwidth=0.5,
                tickfont=dict(
                    size=base_font, color=text_color, family=self.font_family
                ),
                tickmode="array",
                tickvals=[cum_q1, cum_q2],
                ticktext=[f"{cum_q1:.2f}", f"{cum_q2:.2f}"],
                range=cum_range,
                showticklabels=True,
                automargin=False,
                ticklabelposition="inside",
                showline=False,
                linewidth=1,
                linecolor=cfg["border-color"],
                zeroline=False,
                domain=CHART_DOMAIN_Y,  # 与yaxis共享相同垂直区域
            ),
            # X轴设置
            xaxis=dict(
                gridcolor=cfg["gridcolor"],
                tickfont=dict(
                    size=base_font, color=text_color, family=self.font_family
                ),
                domain=[0.0, 1.0],  # 图表横向占满
                rangeslider=dict(visible=False),
                showline=False,
                linewidth=1,
                linecolor=cfg["border-color"],
                mirror=False,
                anchor="y",
                tickformat="%Y-%m",
                hoverformat="%Y-%m-%d",
                showgrid=True,
                gridwidth=0.5,
                position=0.0,
                side="bottom",
                range=[
                    pnl.index.min() - timedelta(days=0.5),
                    pnl.index.max() + timedelta(days=0.5),
                ],
                nticks=6,
            ),
        )

        return fig

    def kl_fig(
        self,
        his,
        trades,
        pos_detail=None,
        symbol=None,
        theme="light",
        client_width=1440,
    ):
        """
        绘制K线图（含成交量和买卖点）

        Args:
            his: pd.DataFrame, 历史行情，必须包含 ['datetime','open','high','low','close','volume']
            trades: list[dict], 交易记录，每条 dict 应已过滤为当前股票，至少包含 {'date','price','type','strategy'}
            pos_detail: pd.DataFrame, 持仓明细（已过滤为当前股票）
            symbol: str, 股票代码（仅用于图例名称）
            theme: str, 'light' 或 'dark'
            client_width: int, 客户端宽度

        Returns:
            plotly.graph_objects.Figure
        """
        df = his.copy()
        if df.empty:
            return go.Figure()

        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")

        # 主题配置
        cfg = self.theme_config.get(theme, self.theme_config["light"])
        scale, font_size = self._get_font_sizes(
            client_width, base_font=16, min_scale=0.65, max_scale=1.05
        )
        min_price = df["low"].min() * 0.97
        max_price = df["high"].max() * 1.03

        # 子图
        fig = make_subplots(
            rows=2,
            cols=1,
            row_heights=[0.8, 0.2],
            vertical_spacing=0,
            shared_xaxes=True,
        )

        # 图例名称：股票代码 + 名称（取最后一次买入时的名称）
        name = next((t["name"] for t in reversed(trades) if t.get("name")), None)
        industry = next(
            (t["industry"] for t in reversed(trades) if t.get("industry")), None
        )
        # legend_name = f"{symbol}#{name}" if name else symbol
        legend_name = (
            f"<b>{symbol}</b> ({name} / <i>{industry}</i>)" if name else symbol
        )
        trades = [
            t for t in trades if pd.to_datetime(t["date"]) >= df["datetime"].min()
        ]
        pos_detail = [
            t for t in pos_detail if pd.to_datetime(t["date"]) >= df["datetime"].min()
        ]

        # K线
        fig.add_trace(
            go.Candlestick(
                x=df["datetime"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name=legend_name,
                increasing=dict(line_color=cfg["long"], fillcolor=cfg["long"]),
                decreasing=dict(line_color=cfg["short"], fillcolor=cfg["short"]),
                line=dict(width=0.8 * scale),
                showlegend=True,
                hovertemplate=(
                    "<b>%{x|%Y-%m-%d}</b><br>开盘: %{open:.2f}<br>最高: %{high:.2f}<br>"
                    "最低: %{low:.2f}<br>收盘: %{close:.2f}<extra></extra>"
                ),
                hoverlabel=dict(font_size=font_size, font_family=self.font_family),
            ),
            row=1,
            col=1,
        )

        # 成交量（颜色与K线涨跌一致）
        colors = [
            cfg["long"] if df["close"].iloc[i] >= df["open"].iloc[i] else cfg["short"]
            for i in range(len(df))
        ]
        fig.add_trace(
            go.Bar(
                x=df["datetime"],
                y=df["volume"],
                name="成交量",
                marker=dict(color=colors, line=dict(color=colors, width=0.8 * scale)),
                opacity=1.0,
                showlegend=True,
            ),
            row=2,
            col=1,
        )

        # ----- 1. 提前计算全局价格跨度，锁定恒定的像素级偏移量 -----
        y_min = df["low"].min()
        y_max = df["high"].max()
        price_range = y_max - y_min if y_max != y_min else 1.0

        # 设定固定偏移步长（例如：悬浮高度恒定为全局视图高度的 2.5%）
        offset_base = price_range * 0.025
        offset_upgrade = price_range * 0.045  # 策略升级点错开更高高度

        # ----- 2. 买卖点标记（改为专业箭头/几何 Marker） -----
        for t in trades:
            try:
                td = pd.to_datetime(t["date"])
                tp = t["price"]
                tt = t["type"]
                ts = t["strategy"]

                # 获取当根 Bar 的 High
                price_row = df[df["datetime"] == td]
                bar_high = price_row["high"].iloc[0] if not price_row.empty else tp

                is_buy = tt == "买入"
                color = cfg["long"] if is_buy else cfg["short"]

                # 专业金融样式：买入用向上箭头/三角形，卖出用向下箭头/三角形
                marker_symbol = "triangle-up" if is_buy else "triangle-down"

                # 悬浮点 = K线最高价 + 固定价格步长
                suspension_price = bar_high + offset_base
                stem_length = suspension_price - bar_high

                fig.add_trace(
                    go.Scatter(
                        x=[td],
                        y=[suspension_price],
                        mode="markers+text",  # 组合模式：同时显示 Marker 图形和节点文字
                        cliponaxis=False,
                        text="B" if is_buy else "S",  # 标记内部或边缘的标注
                        textposition="top center",  # 文字在 Marker 上方居中
                        textfont=dict(size=int(10 * scale), color=color, weight="bold"),
                        marker=dict(
                            symbol=marker_symbol,
                            size=int(8 * scale),  # 专业 Marker 大小
                            color=color,
                        ),
                        error_y=dict(
                            type="data",
                            array=[0],
                            arrayminus=[stem_length],  # 下拉连线连接至 Bar High
                            symmetric=False,
                            width=0,
                            color=cfg.get("gridcolor"),
                            thickness=1,
                        ),
                        showlegend=False,
                        hovertemplate=(
                            f"<b>{tt}</b><br>价格：{tp:.2f}<br>策略：{ts}<br>日期：%{{x|%Y-%m-%d}}<extra></extra>"
                        ),
                        hoverlabel=dict(
                            font_size=font_size,
                            font_family=self.font_family,
                            bgcolor=color,
                        ),
                    ),
                    row=1,
                    col=1,
                )
            except:
                pass

        # ----- 策略升级点计算 -----
        STRATEGY_LEVELS = {
            # 长线策略 (级别 1)
            "多头排列": "1",
            "突破年线": "1",
            # 趋势策略 (级别 2)
            "均线金叉": "2",
            "突破半年线": "2",
            "均线收敛": "2",
            # 短线策略 (级别 3)
            "成交量放大": "3",
            "红三兵": "3",
            "连续上涨": "3",
        }
        upgrade_points = []
        if pos_detail:
            df_pos = pd.DataFrame(pos_detail)
            df_pos["date"] = pd.to_datetime(df_pos["date"], errors="coerce")
            df_pos = df_pos.dropna(subset=["date"])
            df_pos = df_pos.sort_values("date").reset_index(drop=True)

            if not df_pos.empty:
                buy_trades = [
                    t
                    for t in trades
                    if t.get("type") == "买入"
                    and pd.to_datetime(t["date"]) >= df["datetime"].min()
                ]
                buy_trades.sort(key=lambda x: pd.to_datetime(x["date"]))
                buy_dates = [pd.to_datetime(b["date"]) for b in buy_trades]
                buy_strategies = [b.get("strategy", "") for b in buy_trades]

                for idx, buy_date in enumerate(buy_dates):
                    buy_strategy = buy_strategies[idx]
                    next_buy_date = (
                        buy_dates[idx + 1] if idx + 1 < len(buy_dates) else None
                    )
                    candidates = df_pos[df_pos["date"] > buy_date]
                    if next_buy_date:
                        candidates = candidates[candidates["date"] < next_buy_date]
                    candidates = candidates.sort_values("date")
                    if candidates.empty:
                        continue
                    current_strategy = buy_strategy
                    for _, rec in candidates.iterrows():
                        if rec["strategy"] != current_strategy:
                            strat_name = rec["strategy"]
                            # --- 获取策略等级映射 (未在字典中的兜底为 str(strat_name)) ---
                            level = STRATEGY_LEVELS.get(strat_name, strat_name)

                            upgrade_points.append(
                                {
                                    "date": rec["date"],
                                    "strategy": strat_name,
                                    "label": str(
                                        level
                                    ),  # 存储转换后的 1 / 2 / 3 文本标签
                                }
                            )
                            current_strategy = strat_name

        # ----- 3. 策略升级标记 (改为专业 Marker) -----
        for up in upgrade_points:
            td = up["date"]
            price_row = df[df["datetime"] == td]
            if price_row.empty:
                continue

            bar_high = price_row["high"].iloc[0]

            suspension_price = bar_high + offset_upgrade
            stem_length = suspension_price - bar_high

            fig.add_trace(
                go.Scatter(
                    x=[td],
                    y=[suspension_price],
                    mode="markers+text",
                    cliponaxis=False,
                    text=up["label"],  # <-- 这里替换为映射好的 1/2/3 字符串
                    textposition="top center",
                    textfont=dict(
                        size=int(8 * scale),
                        color=cfg["upgrade-marker-color"],
                    ),
                    marker=dict(
                        symbol="triangle-up",
                        size=int(8 * scale),
                        color=cfg["upgrade-marker-color"],
                    ),
                    error_y=dict(
                        type="data",
                        array=[0],
                        arrayminus=[stem_length],
                        symmetric=False,
                        width=0,
                        color=cfg.get("gridcolor"),
                        thickness=1,
                    ),
                    showlegend=False,
                    hovertemplate=(
                        f"<b>策略更新 (L{up['label']})</b><br>日期：%{{x|%Y-%m-%d}}<br>策略：{up['strategy']}<extra></extra>"
                    ),
                    hoverlabel=dict(
                        font_size=font_size,
                        font_family=self.font_family,
                        bgcolor=cfg["upgrade-marker-color"],
                    ),
                ),
                row=1,
                col=1,
            )
        """
        斜向阻力和支撑通道线
        """
        # def _add_support_resistance_channel(
        #     fig,
        #     df,
        #     support_resistance_period=100,
        #     force_parallel=True,
        #     pivot_window=5,
        #     row=1,
        #     col=1,
        # ):
        #     """绘制真正贴合波峰波谷的技术分析通道（基于凸包/最外侧包络几何算法）"""
        #     if (
        #         support_resistance_period <= 0
        #         or len(df) < support_resistance_period
        #         or df.empty
        #     ):
        #         return

        #     recent = df.iloc[-support_resistance_period:].copy().reset_index(drop=True)
        #     if len(recent) < pivot_window * 2 + 1:
        #         return

        #     # 1. 建立时间轴 t (天数)
        #     base_date = recent["datetime"].min()
        #     recent["t"] = (recent["datetime"] - base_date).dt.total_seconds() / 86400.0

        #     # 2. 识别基于 Close 的波峰与波谷（替换原先的 high 和 low）
        #     recent["is_pivot_low"] = (
        #         recent["close"]
        #         == recent["close"].rolling(2 * pivot_window + 1, center=True).min()
        #     )
        #     recent["is_pivot_high"] = (
        #         recent["close"]
        #         == recent["close"].rolling(2 * pivot_window + 1, center=True).max()
        #     )

        #     # 提取波谷和波峰点阵 (t, close)
        #     p_lows = recent[recent["is_pivot_low"]][["t", "close"]].values
        #     p_highs = recent[recent["is_pivot_high"]][["t", "close"]].values

        #     # 保底逻辑同步改为 close
        #     if len(p_lows) < 2:
        #         p_lows = recent.sort_values("close").head(4)[["t", "close"]].values
        #     if len(p_highs) < 2:
        #         p_highs = (
        #             recent.sort_values("close", ascending=False)
        #             .head(4)[["t", "close"]]
        #             .values
        #         )

        #     # 3. 寻找包络支撑线（寻找一条斜率，使得它穿过某个波谷，且所有波谷都在其上方或紧贴）
        #     def find_envelope_line(points, is_upper=False):
        #         """寻找最佳外包络切线 (Upper 为阻力线，Lower 为支撑线)"""
        #         best_slope = 0
        #         best_intercept = 0
        #         min_error = float("inf")

        #         # 遍历任意两点构成的候选直线，寻找能包住所有点且误差最小的线
        #         n = len(points)
        #         for i in range(n):
        #             for j in range(i + 1, n):
        #                 t1, y1 = points[i]
        #                 t2, y2 = points[j]
        #                 if t1 == t2:
        #                     continue

        #                 slope = (y2 - y1) / (t2 - t1)
        #                 intercept = y1 - slope * t1

        #                 # 计算所有点到这条线的垂直距离
        #                 diffs = points[:, 1] - (slope * points[:, 0] + intercept)

        #                 # 支撑线：所有点应该在线上方 (diffs >= -eps)
        #                 # 阻力线：所有点应该在线下方 (diffs <= eps)
        #                 if is_upper:
        #                     penalty = np.sum(diffs[diffs > 1e-4]) * 100
        #                     fitting_loss = np.sum(np.abs(diffs))
        #                 else:
        #                     penalty = np.sum(np.abs(diffs[diffs < -1e-4])) * 100
        #                     fitting_loss = np.sum(np.abs(diffs))

        #                 total_loss = fitting_loss + penalty
        #                 if total_loss < min_error:
        #                     min_error = total_loss
        #                     best_slope = slope
        #                     best_intercept = intercept

        #         return best_slope, best_intercept

        #     # 分别计算阻力线和支撑线的斜率截距
        #     slope_high, intercept_high = find_envelope_line(p_highs, is_upper=True)
        #     slope_low, intercept_low = find_envelope_line(p_lows, is_upper=False)

        #     if force_parallel:
        #         # 平行通道：统一采用波峰波谷斜率的平均值，并向外推至刚好切中极值点
        #         avg_slope = (slope_high + slope_low) / 2

        #         # 重新调整阻力线截距（使其刚好切住最高波峰）
        #         intercept_high = np.max(p_highs[:, 1] - avg_slope * p_highs[:, 0])
        #         # 重新调整支撑线截距（使其刚好切住最低波谷）
        #         intercept_low = np.min(p_lows[:, 1] - avg_slope * p_lows[:, 0])

        #         slope_high = avg_slope
        #         slope_low = avg_slope
        #     else:
        #         # 【新增抗发散约束】：禁止“喇叭口”模式（上轨向上 + 下轨向下）
        #         if slope_high > slope_low:
        #             # 当发生发散时，强制将负斜率的下轨/上轨纠正为平行，采用较贴合整体走势的斜率
        #             # 优先采用绝对值更小（更平稳）的斜率，防止任意一条线倾角过大飞天/崩塌
        #             m_slope = (
        #                 slope_high if abs(slope_high) < abs(slope_low) else slope_low
        #             )
        #             # 如果平稳斜率太陡，限制最大斜率
        #             slope_high = m_slope
        #             slope_low = m_slope

        #             # 重新定位截距
        #             intercept_high = np.max(p_highs[:, 1] - slope_high * p_highs[:, 0])
        #             intercept_low = np.min(p_lows[:, 1] - slope_low * p_lows[:, 0])

        #     # 5. 画线
        #     x0_dt, x1_dt = recent["datetime"].iloc[0], recent["datetime"].iloc[-1]
        #     t0, t1 = recent["t"].iloc[0], recent["t"].iloc[-1]

        #     # 支撑线 (Bottom)
        #     y0_low = slope_low * t0 + intercept_low
        #     y1_low = slope_low * t1 + intercept_low
        #     fig.add_shape(
        #         type="line",
        #         x0=x0_dt,
        #         y0=y0_low,
        #         x1=x1_dt,
        #         y1=y1_low,
        #         line=dict(color=cfg.get("long"), width=0.6, dash="solid"),
        #         row=row,
        #         col=col,
        #     )

        #     # 阻力线 (Top)
        #     y0_high = slope_high * t0 + intercept_high
        #     y1_high = slope_high * t1 + intercept_high
        #     fig.add_shape(
        #         type="line",
        #         x0=x0_dt,
        #         y0=y0_high,
        #         x1=x1_dt,
        #         y1=y1_high,
        #         line=dict(color=cfg.get("short"), width=0.6, dash="solid"),
        #         row=row,
        #         col=col,
        #     )

        # _add_support_resistance_channel(fig, df, force_parallel=False)

        """
        水平阻力和支撑线
        """

        def add_horizontal_sr_levels(
            fig,
            df,
            period=120,
            pivot_window=5,
            max_lines=2,
            min_dist_pct=0.05,  # 新增：两条支撑/阻力线之间的最小允许间距 (2.5%)
            row=1,
            col=1,
        ):
            """TradingView 风格的绝对稳定水平支撑阻力位 (含智能二次间距抑制)"""
            if df.empty or len(df) < period:
                return

            recent = df.iloc[-period:].copy().reset_index(drop=True)
            current_price = recent["close"].iloc[-1]
            x_start = recent["datetime"].iloc[0]
            x_end = recent["datetime"].iloc[-1]

            # 1. 提取结构性高低点 (Pivots)
            recent["is_high"] = (
                recent["close"]
                == recent["close"].rolling(2 * pivot_window + 1, center=True).max()
            )
            recent["is_low"] = (
                recent["close"]
                == recent["close"].rolling(2 * pivot_window + 1, center=True).min()
            )

            high_prices = recent[recent["is_high"]]["close"].values
            low_prices = recent[recent["is_low"]]["close"].values

            if len(high_prices) == 0 or len(low_prices) == 0:
                return

            # 2. 一次聚类：归并微小噪声点
            def cluster_levels(prices, tolerance=0.015):
                clusters = []
                for p in sorted(prices):
                    matched = False
                    for c in clusters:
                        if abs(p - np.mean(c)) / np.mean(c) <= tolerance:
                            c.append(p)
                            matched = True
                            break
                    if not matched:
                        clusters.append([p])
                sorted_clusters = sorted(clusters, key=lambda x: len(x), reverse=True)
                return [np.mean(c) for c in sorted_clusters]

            resistance_levels = cluster_levels(high_prices)
            support_levels = cluster_levels(low_prices)

            # 3. 初始过滤：区分上下界
            raw_resistances = [p for p in resistance_levels if p > current_price]
            raw_supports = [p for p in support_levels if p < current_price]

            # 按距离当前价格由近到远排序
            raw_resistances = sorted(
                raw_resistances, key=lambda x: abs(x - current_price)
            )
            raw_supports = sorted(raw_supports, key=lambda x: abs(x - current_price))

            # 4. (最小物理间距抑制)
            def filter_close_levels(
                raw_resistances,
                raw_supports,
                current_price,
                min_dist_pct=0.05,  # 同类线条（支撑与支撑、阻力与阻力）的最小间距 (2.5%)
                min_channel_pct=0.05,  # 支撑与阻力之间的最小通道宽度 (3.0%)
                max_n=2,
            ):
                """
                智能去重与通道扩张算法：
                1. 消除同类靠得太近的虚线 (S1-S2 / R1-R2)
                2. 消除与当前价格过近、导致 S1 和 R1 挤在一起的无效狭窄通道
                """

                # --- 辅助校验：判断新候选位是否与已选列表里的所有线条都保持足够距离 ---
                def is_far_enough(level, selected_levels, min_dist):
                    for sel in selected_levels:
                        if abs(level - sel) / sel < min_dist:
                            return False
                    return True

                # 1. 挑选阻力线 (Resistances)
                valid_resistances = []
                for r in raw_resistances:
                    # 校验 A: 与当前价格不能贴太近（不能小于半个通道宽度）
                    if (r - current_price) / current_price < (min_channel_pct / 2):
                        continue
                    # 校验 B: 与已选阻力线保持同类间距
                    if is_far_enough(r, valid_resistances, min_dist_pct):
                        valid_resistances.append(r)
                    if len(valid_resistances) >= max_n:
                        break

                # 2. 挑选支撑线 (Supports)
                valid_supports = []
                for s in raw_supports:
                    # 校验 A: 与当前价格不能贴太近
                    if (current_price - s) / s < (min_channel_pct / 2):
                        continue
                    # 校验 B: 与已选支撑线保持同类间距
                    if is_far_enough(s, valid_supports, min_dist_pct):
                        valid_supports.append(s)
                    if len(valid_supports) >= max_n:
                        break

                # 3. 兜底保障：若筛选后 S1 和 R1 依然打破了最小通道阈值，进行“舍弱留强/顺延”
                if valid_resistances and valid_supports:
                    r1, s1 = valid_resistances[0], valid_supports[0]
                    if (r1 - s1) / s1 < min_channel_pct:
                        # 价格离阻力更近，剔除过度贴近的 R1，让阻力向上顺延至 R2
                        if abs(r1 - current_price) < abs(s1 - current_price):
                            valid_resistances.pop(0)
                        else:  # 离支撑更近，剔除 S1，让支撑向下顺延至 S2
                            valid_supports.pop(0)

                return valid_resistances, valid_supports

            # 初始区分上下界并按距离由近到远排序
            raw_resistances = sorted(
                [p for p in resistance_levels if p > current_price],
                key=lambda x: abs(x - current_price),
            )
            raw_supports = sorted(
                [p for p in support_levels if p < current_price],
                key=lambda x: abs(x - current_price),
            )

            valid_resistances, valid_supports = filter_close_levels(
                raw_resistances=raw_resistances,
                raw_supports=raw_supports,
                current_price=current_price,
                min_dist_pct=min_dist_pct,  # 同类间距 (2.5%)
                min_channel_pct=0.05,  # 支撑和阻力之间至少要拉开 3.5% 的通道空间
                max_n=max_lines,
            )

            # 5. 绘制阻力线
            for res_p in valid_resistances:
                fig.add_shape(
                    type="line",
                    x0=x_start,
                    y0=res_p,
                    x1=x_end,
                    y1=res_p,
                    line=dict(
                        color=self._get_alpha_color(cfg.get("short"), alpha=0.7),
                        width=1.2 * scale,
                        dash="solid",
                    ),
                    row=row,
                    col=col,
                    layer="below",
                )

            # 6. 绘制支撑线
            for sup_p in valid_supports:
                fig.add_shape(
                    type="line",
                    x0=x_start,
                    y0=sup_p,
                    x1=x_end,
                    y1=sup_p,
                    line=dict(
                        color=self._get_alpha_color(cfg.get("long"), alpha=0.7),
                        width=1.2 * scale,
                        dash="solid",
                    ),
                    row=row,
                    col=col,
                    layer="below",
                )

        add_horizontal_sr_levels(fig, df, period=len(df) if len(df) < 150 else 150)

        xmin = df["datetime"].min()
        xmax = df["datetime"].max()
        # 1. 蜡烛图 X轴
        fig.update_xaxes(
            mirror=False,
            tickangle=0,
            rangeslider_visible=False,
            showgrid=True,
            gridcolor=cfg["gridcolor"],
            gridwidth=0.5,
            showline=False,  # 你原本单独设置的
            linecolor=cfg["gridcolor"],
            linewidth=1,
            automargin=False,
            ticks="",  # 刻度线向内
            range=[xmin - timedelta(days=0.5), xmax + timedelta(days=2)],
            zeroline=False,
            nticks=6,
            tickmode="auto",
        )

        fig.update_yaxes(
            mirror=False,
            tickfont=dict(
                size=font_size,
                color=cfg["text-color"],
                family=self.font_family,
            ),
            title=dict(text=None),
            showline=False,
            linecolor=cfg["gridcolor"],
            linewidth=1,
            zeroline=False,
            gridcolor=cfg["gridcolor"],
            gridwidth=0.5,
            ticklabelposition="inside",
            tickangle=0,
            autorange=True,
        )
        # ----- 布局设置 -----
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=12 * scale),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            hoverlabel=dict(font_size=font_size, font_family=self.font_family),
            font=dict(family=self.font_family, size=font_size, color=cfg["text-color"]),
            hovermode="x",
            dragmode=False,
            showlegend=True,
            bargap=0.3 / scale,
            bargroupgap=0.3 / scale,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1,
                xanchor="left",
                x=0,
                font=dict(size=font_size),
            ),
        )

        return fig

    def industry_strength_chart(
        self,
        page: str,
        df: pd.DataFrame,
        theme: str = "light",
        client_width: int = 1440,
    ) -> go.Figure:
        # ------------------------------
        # 1. 基础配置与数据预处理
        # ------------------------------
        cfg = self.theme_config.get(theme, self.theme_config["light"])
        text_color = cfg["text-color"]
        scale, font_size = self._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )
        unified_scale, unified_font_size = self._get_font_sizes(
            client_width, base_font=16, min_scale=0.65, max_scale=1.05
        )

        df = df.copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        if "day_of_week" in df.columns:
            df = df[(df["day_of_week"] <= 4) & (df["week_order"] >= 1)].copy()
        df = df.sort_values("date").reset_index(drop=True)

        if df.empty:
            return go.Figure()

        # 解析行业前三列
        if "industry_top3" in df.columns:

            def parse(x):
                if pd.isna(x) or x == "":
                    return []
                return [item.strip() for item in str(x).split(",") if item.strip()]

            df["industry_top3_parsed"] = df["industry_top3"].apply(parse)
        else:
            df["industry_top3_parsed"] = [[] for _ in range(len(df))]

        # ------------------------------
        # 2. 计算全局热度排名
        # ------------------------------
        from collections import Counter

        industry_counter = Counter()
        for items in df["industry_top3_parsed"]:
            for item in items:
                if item:
                    industry_counter[item] += 1

        sorted_industries = [ind for ind, _ in industry_counter.most_common()]
        rank_map = {ind: i + 1 for i, ind in enumerate(sorted_industries)}

        # ------------------------------
        # 3. 准备标注文本
        # ------------------------------
        df["top1_industry"] = df["industry_top3_parsed"].apply(
            lambda lst: lst[0] if len(lst) > 0 else None
        )

        def format_label(ind):
            if ind is None:
                return ""
            rank = rank_map.get(ind, 999)
            if rank <= 3:
                return f"#{rank} {ind}"
            return ind

        df["annotation_text"] = df["top1_industry"].apply(format_label)
        df["annotation_text"] = df["annotation_text"].apply(
            lambda x: self._truncate_text_by_display_width(x, 14)
        )
        df_annot = df[df["annotation_text"] != ""].copy()

        if df_annot.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["date"], y=df["s_pnl"], mode="lines+markers"))
            return fig

        # ==============================================================
        # 根据 client_width 优化几何碰撞与线长参数 (无 font-size)
        # ==============================================================
        # 组1：画布物理尺寸 (宽度, 高度)
        CANVAS_W_PX, CANVAS_H_PX = (
            (380.0, 260.0) if client_width < 550 else (750.0, 420.0)
        )

        # 组2：文本框物理尺寸 (中文字宽, 英文字宽, Padding, 文本高度)
        CHAR_CN_W, CHAR_EN_W, PAD_W, TEXT_H = (
            (9.5, 5.5, 6.0, 15.0) if client_width < 550 else (12.0, 7.0, 8.0, 18.0)
        )

        # 组3：避让阶梯与间距 (Y轴阶梯, X轴安全缓冲, 连线空隙)
        Y_STEPS, SAFETY_GAP_X, STANDOFF_PX = (
            ([-16, 16, -28, 28, -40, 40, -52, 52, -64, 64], 4.0, 1)
            if client_width < 550
            else ([-18, 18, -32, 32, -46, 46, -60, 60, -74, 74], 6.0, 2)
        )

        # ==============================================================
        # 2. 纯垂直指示线 + 自适应 2D 防重叠避让算法
        # ==============================================================
        dates_ts = df_annot["date"].apply(lambda x: x.timestamp()).values
        min_ts, max_ts = dates_ts.min(), dates_ts.max()
        range_ts = max_ts - min_ts if max_ts > min_ts else 1.0

        y_vals = df_annot["s_pnl"].values
        y_min, y_max = y_vals.min(), y_vals.max()
        y_range = y_max - y_min if y_max > y_min else 1.0

        pts_px = []
        for i, ts in enumerate(dates_ts):
            x_p = ((ts - min_ts) / range_ts) * CANVAS_W_PX
            y_p = (1.0 - (y_vals[i] - y_min) / y_range) * CANVAS_H_PX
            pts_px.append((x_p, y_p))

        n_pts = len(pts_px)
        ax_offsets = [0] * n_pts  # 保持 100% 绝对垂直线
        ay_offsets = []
        xanchors = []
        placed_boxes = []

        for i, (x_px, y_px) in enumerate(pts_px):
            # 1. 动态估算物理字宽
            text_str = str(df_annot.iloc[i]["annotation_text"])
            text_w = (
                sum(CHAR_CN_W if ord(c) > 127 else CHAR_EN_W for c in text_str) + PAD_W
            )

            # 2. 左右边缘防溢出判定
            is_left_edge = i < int(n_pts * 0.15) or x_px < (CANVAS_W_PX * 0.12)
            is_right_edge = i > int(n_pts * 0.85) or x_px > (CANVAS_W_PX * 0.88)

            if is_left_edge:
                anchor_candidates = ["left"]
            elif is_right_edge:
                anchor_candidates = ["right"]
            else:
                anchor_candidates = ["center", "left", "right"]

            # 3. 波峰/波谷方向判定
            prev_y = y_vals[i - 1] if i > 0 else y_vals[i]
            next_y = y_vals[i + 1] if i < n_pts - 1 else y_vals[i]
            prefer_up = (
                (y_vals[i] >= prev_y and y_vals[i] >= next_y)
                if (
                    (y_vals[i] >= prev_y and y_vals[i] >= next_y)
                    or (y_vals[i] <= prev_y and y_vals[i] <= next_y)
                )
                else ((y_vals[i] - y_min) / y_range >= 0.5)
            )

            sorted_ay = sorted(
                Y_STEPS, key=lambda val: (0 if (val < 0) == prefer_up else 1)
            )

            selected_ay = None
            selected_anchor = None

            # 4. 2D 矩形碰撞检测
            for ay in sorted_ay:
                for anchor in anchor_candidates:
                    if anchor == "center":
                        b_min_x = x_px - (text_w / 2.0)
                        b_max_x = x_px + (text_w / 2.0)
                    elif anchor == "left":
                        b_min_x = x_px
                        b_max_x = x_px + text_w
                    else:
                        b_min_x = x_px - text_w
                        b_max_x = x_px

                    text_center_y = y_px + ay
                    b_min_y = text_center_y - (TEXT_H / 2.0)
                    b_max_y = text_center_y + (TEXT_H / 2.0)

                    collision = False
                    for box in placed_boxes:
                        if not (
                            (b_max_x + SAFETY_GAP_X) < box[0]
                            or (b_min_x - SAFETY_GAP_X) > box[1]
                            or (b_max_y + 2) < box[2]
                            or (b_min_y - 2) > box[3]
                        ):
                            collision = True
                            break

                    if not collision:
                        selected_ay = ay
                        selected_anchor = anchor
                        placed_boxes.append((b_min_x, b_max_x, b_min_y, b_max_y))
                        break

                if selected_ay is not None:
                    break

            # 5. 极小概率冲突时的兜底避让
            if selected_ay is None:
                selected_ay = (Y_STEPS[-1] + 12) if not prefer_up else (Y_STEPS[0] - 12)
                selected_anchor = anchor_candidates[0]
                placed_boxes.append(
                    (
                        x_px - (text_w / 2.0),
                        x_px + (text_w / 2.0),
                        y_px + selected_ay - (TEXT_H / 2.0),
                        y_px + selected_ay + (TEXT_H / 2.0),
                    )
                )

            ay_offsets.append(selected_ay)
            xanchors.append(selected_anchor)
        # ------------------------------
        # 6. 创建折线图
        # ------------------------------
        df["hover-text-color"] = df.apply(
            lambda row: (
                f"<b>{row['date'].strftime('%Y-%m-%d')}</b><br>"
                f"盈亏: {row['s_pnl']:,.2f}<br>"
                f"行业:<br>"
                f"{'<br>'.join(row['industry_top3_parsed']) if row['industry_top3_parsed'] else '无'}"
            ),
            axis=1,
        )
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["s_pnl"],
                name="每日盈亏",
                mode="lines+markers",
                line=dict(color=cfg.get("cumret-line-color"), width=1.5),
                marker=dict(size=6 * unified_scale, color=cfg.get("cumret-line-color")),
                text=df["hover-text-color"],
                hovertemplate="%{text}<extra></extra>",
                yaxis="y",
            )
        )
        ymax = df["s_pnl"].max()
        ymin = df["s_pnl"].min()

        # ------------------------------
        # 7. 添加标注
        # ------------------------------
        for i, row in df_annot.iterrows():
            final_ay = ay_offsets[i]

            fig.add_annotation(
                x=row["date"],
                y=row["s_pnl"],
                text=row["annotation_text"],
                showarrow=True,
                arrowhead=0,
                arrowsize=0.3,
                arrowwidth=0.4,
                arrowcolor=text_color,
                ax=0,
                ay=final_ay,
                xanchor=xanchors[i],
                yanchor="bottom" if final_ay < 0 else "top",
                standoff=STANDOFF_PX,
                startstandoff=0,
                bgcolor=cfg.get("legend-bg-color"),
                borderpad=1,
                borderwidth=0,
            )

        # ------------------------------
        # 8. 布局设置
        # ------------------------------
        x_range = [
            df["date"].min() - pd.Timedelta(days=0.05),
            df["date"].max() + pd.Timedelta(days=0.05),
        ]

        fig.update_layout(
            title="",
            dragmode=False,
            autosize=True,
            margin=dict(l=0, r=0, t=0, b=5),
            font=dict(family=self.font_family, size=font_size, color=text_color),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            hovermode="x",
            hoverlabel=dict(font_size=font_size, font_family=self.font_family),
            xaxis=dict(
                tickfont=dict(size=font_size, color=text_color),
                mirror=False,
                showline=False,
                linecolor=cfg["gridcolor"],
                linewidth=1,
                zeroline=True,
                gridcolor=cfg["gridcolor"],
                gridwidth=0.5,
                tickmode="auto",
                tickformat="%Y-%m-%d",
                hoverformat="%Y-%m-%d",
                range=x_range,
                showgrid=True,
                automargin=False,
                tickangle=0,
                nticks=4,
            ),
            yaxis=dict(
                showticklabels=False,
                mirror=True,
                showgrid=True,
                gridcolor=cfg["gridcolor"],
                gridwidth=0.5,
                side="left",
                zeroline=False,
                showline=False,
                dtick=(ymax - ymin) / 3,
                ticklabelposition="inside",
                ticks="",
                automargin=False,
                autorange=True,
            ),
            legend=dict(
                orientation="v",
                x=0,
                xanchor="left",
                y=1,
                yanchor="top",
                font=dict(size=font_size, color=text_color, family=self.font_family),
                bgcolor=cfg["legend-bg-color"],
                borderwidth=0,
                tracegroupgap=0,
            ),
            showlegend=True,
        )

        return fig
