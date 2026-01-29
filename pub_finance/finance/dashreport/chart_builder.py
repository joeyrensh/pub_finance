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


class ChartBuilder:
    @staticmethod
    def calendar_heatmap(
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
        # 简化主题颜色配置
        theme_configs = {
            "light": {
                "positive": "#d60a22",  # 红色 - 正数
                "negative": "#037b66",  # 绿色 - 负数
                "neutral": "#000000",  # 黑色 - 中性文本、行业文本、坐标轴
                "grid": "rgba(0, 0, 0, 0.3)",  # 网格线
                "background": "rgba(255, 255, 255, 0)",  # 透明背景
            },
            "dark": {
                "positive": "#ff6b6b",  # 亮红色 - 正数
                "negative": "#6bcfb5",  # 亮绿色 - 负数
                "neutral": "#ffffff",  # 白色 - 中性文本、行业文本、坐标轴
                "grid": "rgba(255, 255, 255, 0.3)",  # 网格线
                "background": "rgba(0, 0, 0, 0)",  # 透明背景
            },
        }

        font_family = (
            '-apple-system, BlinkMacSystemFont, "PingFang SC", '
            '"Helvetica Neue", Arial, sans-serif'
        )

        # 修改点1：调整宽高比为1.6
        fig_width = 1440
        fig_height = 900  # 保持原高度
        scale = client_width / fig_width
        scale = max(0.9, min(scale, 1))  # 防止过小 / 过大
        base_font_size = int(12 * scale)

        # 获取当前主题配置
        config = theme_configs.get(theme, theme_configs["light"])
        print("theme:", repr(theme))
        print("config:", config)

        # 休市颜色使用中性色 + 透明度
        holiday_color = config["neutral"].replace("#", "")
        if len(holiday_color) == 6:  # 如果是hex颜色
            holiday_color = f"rgba({int(holiday_color[0:2], 16)}, {int(holiday_color[2:4], 16)}, {int(holiday_color[4:6], 16)}, 0.5)"
        else:
            holiday_color = config["neutral"]

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
        abs_values = np.abs(df["s_pnl"])
        if len(abs_values) > 0:
            quantiles = np.quantile(abs_values, [0.2, 0.4, 0.6, 0.8])
        else:
            quantiles = [0, 0, 0, 0]

        max_size_increase = 10
        font_steps = np.linspace(base_font_size, base_font_size + max_size_increase, 5)

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

        # 为每个数据点添加文本annotation
        for i, row in df.iterrows():
            day_of_week = row["day_of_week"]
            week_order = row["week_order"]
            date_str = row["date"].strftime("%Y-%m-%d")
            year, month, day = date_str.split("-")
            industry_items = row["industry_top3_parsed"]
            col3_value = row["s_pnl"]

            # 确定字体大小（基于s_pnl的绝对值）
            abs_col3 = abs(col3_value)
            if abs_col3 <= quantiles[0]:
                dynamic_font_size = font_steps[0]
            elif abs_col3 <= quantiles[1]:
                dynamic_font_size = font_steps[1]
            elif abs_col3 <= quantiles[2]:
                dynamic_font_size = font_steps[2]
            elif abs_col3 <= quantiles[3]:
                dynamic_font_size = font_steps[3]
            else:
                dynamic_font_size = font_steps[4]

            dynamic_font_size = int(dynamic_font_size)

            # 确定颜色（根据s_pnl正负和主题）
            if col3_value > 0:
                text_color = config["positive"]
            elif col3_value < 0:
                text_color = config["negative"]
            else:
                text_color = config["neutral"]
                dynamic_font_size = base_font_size - 8  # 零值使用基础字体大小

            # 清理行业名称
            industry_items = [
                str(item).strip()
                for item in industry_items
                if item and str(item).strip()
            ]

            # 1. 添加上方的行业（第一个行业）
            if len(industry_items) >= 1:
                industry_text_top = industry_items[0]
                fig.add_annotation(
                    x=day_of_week,
                    y=week_order,
                    text=industry_text_top,
                    showarrow=False,
                    font=dict(
                        family=font_family,
                        size=base_font_size - 2,
                        color=config["neutral"],  # 使用中性色
                    ),
                    align="center",  # 保持居中
                    xanchor="center",  # 保持居中
                    yanchor="top",
                    # xshift=0,  # 不向左偏移
                    # yshift=20,
                    opacity=0.9,
                )
            fig.add_annotation(
                x=day_of_week - 0.48,
                y=week_order,
                text=month,
                showarrow=False,
                align="left",
                xanchor="left",
                yanchor="middle",
                xshift=0,
                yshift=int(7 * scale),
                font=dict(
                    family=font_family,
                    size=dynamic_font_size,
                    color=text_color,
                ),
                opacity=0.9,
            )
            fig.add_annotation(
                x=day_of_week - 0.48,
                y=week_order,
                text=day,
                showarrow=False,
                align="left",
                xanchor="left",
                yanchor="middle",
                xshift=0,
                yshift=-int(7 * scale),
                font=dict(
                    family=font_family,
                    size=dynamic_font_size,
                    color=text_color,
                ),
                opacity=0.9,
            )

            # 3. 添加下方的行业（第二个行业）
            if len(industry_items) >= 2:
                industry_text_bottom = industry_items[1]
                fig.add_annotation(
                    x=day_of_week,
                    y=week_order,
                    text=industry_text_bottom,
                    showarrow=False,
                    font=dict(
                        family=font_family,
                        size=base_font_size - 2,
                        color=config["neutral"],  # 使用中性色
                    ),
                    align="center",  # 保持居中
                    xanchor="center",  # 保持居中
                    yanchor="bottom",
                    # xshift=0,  # 不向左偏移
                    # yshift=-20,
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
                    family=font_family,
                    size=base_font_size,
                    weight="bold",
                    color=config["neutral"],  # 使用中性色
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
        )

        # 在每周之间添加横向分隔线
        unique_weeks_list = sorted(df["week_order"].unique()) if len(df) > 0 else []

        for week in unique_weeks_list[1:]:
            fig.add_hline(
                y=week - 0.5,
                line=dict(color=config["grid"], width=1),
                opacity=0.4,
                layer="below",
            )

        for day in range(1, 5):
            fig.add_vline(
                x=day - 0.5,
                line=dict(color=config["grid"], width=1),
                opacity=0.4,
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
                            family=font_family,
                            size=base_font_size - 3,
                            color=config["neutral"],  # 使用带透明度的中性色
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
        industry_text = (
            ", ".join([str(item) for item in industry_items[:3]])
            if industry_items
            else "无行业数据"
        )

        return f"日期: {date_str}<br>" f"星期: {day_name}<br>" f"行业: {industry_text}"

    @staticmethod
    def strategy_chart(
        df,
        theme="light",
        client_width=1440,
    ):
        # =========================
        # 0. 内部生成 group
        # =========================
        df_group = df.groupby("date")["pnl"].sum().reset_index()

        # =========================
        # 1. theme 配置（新增）
        # =========================
        theme_config = {
            "light": {
                "text": "#000000",
                "grid": "rgba(0, 0, 0, 0.2)",
                "legend_bg": "rgba(246, 248, 249, 0.8)",
            },
            "dark": {
                "text": "#ffffff",
                "grid": "rgba(255, 255, 255, 0.25)",
                "legend_bg": "rgba(20, 20, 20, 0.6)",
            },
        }

        cfg = theme_config.get(theme, theme_config["light"])
        text_color = cfg["text"]
        grid_color = cfg["grid"]
        legend_bg = cfg["legend_bg"]

        # =========================
        # 2. 字体 & scale（与 heatmap 对齐）
        # =========================
        fig_width = 1440
        fig_height = 900

        scale = client_width / fig_width
        scale = max(0.9, min(scale, 1))

        base_font_size = int(12 * scale)
        title_font_size = int(14 * scale)

        font_family = (
            '-apple-system, BlinkMacSystemFont, "PingFang SC", '
            '"Helvetica Neue", Arial, sans-serif'
        )

        # =========================
        # 3. 策略颜色（你提供的）
        # =========================
        strategy_colors_light = [
            "#0c6552",
            "#0d876d",
            "#00a380",
            "#00b89a",
            "#ffa700",
            "#d50b3e",
            "#a90a3f",
            "#7a0925",
        ]

        strategy_colors_dark = [
            "#0d7b67",
            "#0e987f",
            "#01b08f",
            "#00c4a6",
            "#ffa700",
            "#e90c4a",
            "#cf1745",
            "#b6183d",
        ]

        strategy_colors = (
            strategy_colors_dark if theme == "dark" else strategy_colors_light
        )

        # =========================
        # 4. 原有数值计算（不动）
        # =========================
        max_pnl = df_group["pnl"].max()
        min_pnl = df_group["pnl"].min()

        threshold = df["success_rate"].quantile(0.1)
        min_success_rate = df.loc[
            df["success_rate"] >= threshold,
            "success_rate",
        ].min()

        safe_rate = max(min_success_rate, 0.2)
        max_range = min(2 * max_pnl, max_pnl * 2 / safe_rate)

        # =========================
        # 5. tick offset（scale）
        # =========================
        def calc_tick_offset(min_pnl, max_pnl, num_ticks=6, char_width=10):
            def to_si(n):
                abs_n = abs(n)
                if abs_n >= 1e12:
                    return f"{n/1e12:.1f}T".rstrip("0").rstrip(".")
                elif abs_n >= 1e9:
                    return f"{n/1e9:.1f}B".rstrip("0").rstrip(".")
                elif abs_n >= 1e6:
                    return f"{n/1e6:.1f}M".rstrip("0").rstrip(".")
                elif abs_n >= 1e3:
                    return f"{n/1e3:.1f}K".rstrip("0").rstrip(".")
                else:
                    return str(int(n))

            ticks = np.linspace(min_pnl, max_pnl, num_ticks)
            tick_texts = [to_si(t) for t in ticks]
            max_len = max(len(t) for t in tick_texts)
            return -char_width * max_len

        offset = int(calc_tick_offset(min_pnl, max_range) * scale)

        # =========================
        # 6. 策略顺序 & 分组
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
            {"dash": "solid", "width": 3.5 * scale},
            {"dash": "solid", "width": 3.5 * scale},
            {"dash": "dashdot", "width": 3.2 * scale},
            {"dash": "dash", "width": 3.0 * scale},
            {"dash": "6,3", "width": 2.8 * scale},
            {"dash": "5,5", "width": 2.5 * scale},
            {"dash": "4,6", "width": 2.2 * scale},
            {"dash": "2,8", "width": 2.0 * scale},
        ]

        # =========================
        # 8. traces（仅颜色补齐）
        # =========================
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
                )
            )

            fig.add_trace(
                go.Bar(
                    x=data["date"],
                    y=data["pnl"],
                    name=strategy,
                    marker=dict(
                        color=color,
                        line=dict(color=color),
                    ),
                    yaxis="y2",
                    showlegend=False,
                )
            )

        # =========================
        # 9. Layout（补齐 text / grid / legend）
        # =========================
        fig.update_layout(
            xaxis=dict(
                mirror=True,
                ticks="outside",
                tickfont=dict(
                    family=font_family,
                    size=base_font_size,
                    color=text_color,
                ),
                showline=False,
                gridcolor=grid_color,
            ),
            yaxis=dict(
                side="left",
                mirror=True,
                ticks="inside",
                tickfont=dict(
                    family=font_family,
                    size=base_font_size,
                    color=text_color,
                ),
                showline=False,
                gridcolor=grid_color,
                dtick=0.1,
                fixedrange=True,
                range=[0, 1],
            ),
            yaxis2=dict(
                side="right",
                overlaying="y",
                showgrid=False,
                ticks="inside",
                tickfont=dict(
                    family=font_family,
                    size=base_font_size,
                    color=text_color,
                ),
                range=[0, max_range],
                ticklabelstandoff=offset,
                tickformat="~s",
            ),
            legend=dict(
                orientation="v",
                x=0.02,
                y=1,
                xanchor="left",
                yanchor="top",
                font=dict(
                    family=font_family,
                    size=base_font_size,
                    color=text_color,
                ),
                bgcolor=legend_bg,
                borderwidth=0,
            ),
            barmode="stack",
            bargap=0.2,
            bargroupgap=0.2,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=True,
        )

        return fig

    @staticmethod
    def trade_info_chart(
        df,
        theme="light",
        client_width=1440,
    ):
        import plotly.graph_objects as go
        import pandas as pd
        from datetime import timedelta

        # =========================
        # 1. theme 配置
        # =========================
        theme_config = {
            "light": {
                "text": "#000000",
                "grid": "rgba(0, 0, 0, 0.2)",
                "legend_bg": "rgba(246, 248, 249, 0.8)",
                "long": "#e01c3a",
                "short": "#0d876d",
            },
            "dark": {
                "text": "#ffffff",
                "grid": "rgba(255, 255, 255, 0.25)",
                "legend_bg": "rgba(20, 20, 20, 0.6)",
                "long": "#ff4d6d",
                "short": "#2ec4a6",
            },
        }

        cfg = theme_config.get(theme, theme_config["light"])
        text_color = cfg["text"]
        grid_color = cfg["grid"]

        # =========================
        # 2. font & scale（统一标准）
        # =========================
        fig_width = 1440
        fig_height = 900

        scale = client_width / fig_width
        scale = max(0.9, min(scale, 1))

        font_size = int(12 * scale)
        title_font_size = int(14 * scale)

        font_family = (
            '-apple-system, BlinkMacSystemFont, "PingFang SC", '
            '"Helvetica Neue", Arial, sans-serif'
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
                line=dict(color=cfg["long"], width=3 * scale),
                yaxis="y",
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
            )
        )

        # =========================
        # 4. Layout（light / dark + scale）
        # =========================
        fig.update_layout(
            title=dict(
                text="Last 180 days trade info",
                y=0.9,
                x=0.5,
                font=dict(
                    size=title_font_size,
                    color=text_color,
                    family=font_family,
                ),
            ),
            xaxis=dict(
                mirror=True,
                ticks="outside",
                tickfont=dict(
                    size=font_size,
                    color=text_color,
                    family=font_family,
                ),
                showline=False,
                gridcolor=grid_color,
                domain=[0, 1],
                automargin=True,
            ),
            yaxis=dict(
                side="left",
                mirror=True,
                ticks="outside",
                tickfont=dict(
                    size=font_size,
                    color=text_color,
                    family=font_family,
                ),
                showline=False,
                gridcolor=grid_color,
                ticklabelposition="outside",
                tickangle=0,
            ),
            legend=dict(
                orientation="v",
                x=0.02,
                y=1,
                xanchor="left",
                yanchor="top",
                font=dict(
                    size=font_size,
                    color=text_color,
                    family=font_family,
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
        )

        # =========================
        # 5. x-axis range（原逻辑）
        # =========================
        xmin = pd.to_datetime(df["buy_date"].min())
        xmax = pd.to_datetime(df["buy_date"].max())

        fig.update_xaxes(
            range=[
                xmin - timedelta(days=0.5),
                xmax + timedelta(days=0.5),
            ]
        )

        return fig

    @staticmethod
    def industry_pnl_trend(
        df,
        theme="light",
        client_width=1440,
    ):

        # =========================
        # 1. theme 配置
        # =========================
        theme_config = {
            "light": {
                "text": "#000000",
                "grid": "rgba(0, 0, 0, 0.2)",
                "axis_line": "rgba(0, 0, 0, 0.4)",
                "legend_bg": "rgba(246, 248, 249, 0.8)",
                "colors": [
                    "#0c6552",
                    "#0d876d",
                    "#00a380",
                    "#ffa700",
                    "#d50b3e",
                ],
            },
            "dark": {
                "text": "#ffffff",
                "grid": "rgba(255, 255, 255, 0.25)",
                "axis_line": "rgba(255, 255, 255, 0.4)",
                "legend_bg": "rgba(20, 20, 20, 0.6)",
                "colors": [
                    "#0d7b67",
                    "#0e987f",
                    "#01b08f",
                    "#ffa700",
                    "#e90c4a",
                ],
            },
        }

        cfg = theme_config.get(theme, theme_config["light"])
        text_color = cfg["text"]

        # =========================
        # 2. font & scale
        # =========================
        fig_width = 1440
        fig_height = 900

        scale = client_width / fig_width
        scale = max(0.9, min(scale, 1))

        font_size = int(12 * scale)
        title_font_size = int(14 * scale)

        font_family = (
            '-apple-system, BlinkMacSystemFont, "PingFang SC", '
            '"Helvetica Neue", Arial, sans-serif'
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
            color_discrete_sequence=cfg["colors"],
        )

        fig.update_traces(line=dict(width=3 * scale))

        # =========================
        # 4. X Axis（完整颜色定义）
        # =========================
        fig.update_xaxes(
            mirror=True,
            ticks="inside",
            ticklabelposition="inside",
            tickfont=dict(
                size=font_size,
                color=text_color,
                family=font_family,
            ),
            title=dict(
                text=None,
                font=dict(
                    size=title_font_size,
                    color=text_color,
                    family=font_family,
                ),
            ),
            showline=False,
            linecolor=cfg["axis_line"],
            zeroline=False,
            gridcolor=cfg["grid"],
            domain=[0, 1],
            automargin=True,
        )

        # =========================
        # 5. Y Axis（完整颜色定义）
        # =========================
        fig.update_yaxes(
            mirror=True,
            ticks="outside",
            tickfont=dict(
                size=font_size,
                color=text_color,
                family=font_family,
            ),
            title=dict(
                text=None,
                font=dict(
                    size=title_font_size,
                    color=text_color,
                    family=font_family,
                ),
            ),
            showline=False,
            linecolor=cfg["axis_line"],
            zeroline=False,
            gridcolor=cfg["grid"],
            ticklabelposition="outside",
            tickangle=0,
            autorange=True,
        )

        # =========================
        # 6. layout
        # =========================
        fig.update_layout(
            title=dict(
                text="Last 180 days top5 pnl",
                x=0.5,
                y=0.9,
                font=dict(
                    size=title_font_size,
                    color=text_color,
                    family=font_family,
                ),
            ),
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
                    family=font_family,
                ),
                bgcolor=cfg["legend_bg"],
                borderwidth=0,
                tracegroupgap=0,
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=0, b=0, l=0, r=0),
            autosize=True,
        )

        return fig

    @staticmethod
    def industry_position_treemap(
        df,
        theme="light",
        client_width=1440,
    ):
        # =========================
        # 1. theme 配置
        # =========================
        theme_config = {
            "light": {
                "text": "#000000",
                "border": "rgba(200, 200, 200, 0.8)",
                "legend_bg": "rgba(246, 248, 249, 0.8)",
            },
            "dark": {
                "text": "#ffffff",
                "border": "rgba(255, 255, 255, 0.4)",
                "legend_bg": "rgba(20, 20, 20, 0.6)",
            },
        }

        cfg = theme_config.get(theme, theme_config["light"])
        text_color = cfg["text"]

        # =========================
        # 2. font & scale
        # =========================
        fig_width = 1440
        fig_height = 900

        scale = client_width / fig_width
        scale = max(0.9, min(scale, 1))

        font_size = int(12 * scale)

        font_family = (
            '-apple-system, BlinkMacSystemFont, "PingFang SC", '
            '"Helvetica Neue", Arial, sans-serif'
        )

        # =========================
        # 3. 数据准备（不改你的逻辑）
        # =========================
        labels_wrapped_industry = df["industry"]
        values = df["cnt"]
        hex_colors = ["rgba(0,0,0,0)" for _ in range(20)]

        # =========================
        # 4. Treemap
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
                    family=font_family,
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
        # 5. layout（theme 对齐）
        # =========================
        fig.update_layout(
            title=dict(text=None),
            showlegend=False,
            margin=dict(t=0, b=20, l=0, r=0),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            treemapcolorway=hex_colors,
        )

        return fig

    @staticmethod
    def industry_profit_treemap(
        df,
        theme="light",
        client_width=1440,
    ):

        # =========================
        # 1. theme 配置
        # =========================
        theme_config = {
            "light": {
                "text": "#000000",
                "outside_text": "#777777",
                "border": "rgba(200, 200, 200, 0.8)",
            },
            "dark": {
                "text": "#ffffff",
                "outside_text": "#aaaaaa",
                "border": "rgba(255, 255, 255, 0.4)",
            },
        }

        cfg = theme_config.get(theme, theme_config["light"])
        text_color = cfg["text"]

        # =========================
        # 2. font & scale
        # =========================
        fig_width = 1440
        fig_height = 900

        scale = client_width / fig_width
        scale = max(0.9, min(scale, 1))

        font_size = int(12 * scale)

        font_family = (
            '-apple-system, BlinkMacSystemFont, "PingFang SC", '
            '"Helvetica Neue", Arial, sans-serif'
        )

        # =========================
        # 3. 数据准备（字段最小替换）
        # =========================
        labels = df["industry"]
        values = df["pl"]
        hex_colors = ["rgba(0,0,0,0)" for _ in range(20)]

        # =========================
        # 4. Treemap
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
                    family=font_family,
                ),
                outsidetextfont=dict(
                    color=cfg["outside_text"],
                    family=font_family,
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
        # 5. layout
        # =========================
        fig.update_layout(
            title=dict(text=None),
            showlegend=False,
            margin=dict(t=0, b=20, l=0, r=0),
            autosize=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            treemapcolorway=hex_colors,
        )

        return fig

    @staticmethod
    def annual_return(pnl: pd.Series, theme="light", client_width=1440):

        # =========================
        # 0. 基本校验
        # =========================
        if not isinstance(pnl, pd.Series):
            raise TypeError("pnl must be a pd.Series")

        if not isinstance(pnl.index, pd.DatetimeIndex):
            raise TypeError("pnl index must be DatetimeIndex")

        pnl = pnl.sort_index().dropna()

        # =========================
        # 1. theme 配置 (增强hover配置)
        # =========================
        theme_config = {
            "light": {
                "text": "#000000",
                "grid": "rgba(200,200,200,0.4)",
                "border": "rgba(200,200,200,0.6)",
                "cumret": "#e01c3a",
                "drawdown": "#0d876d",
                "drawdown_fill": "rgba(13,135,109,0.3)",
                "table_header": "rgba(245,245,245,0)",
                "table_cell": "rgba(0,0,0,0)",
                "hover_bg": "#ffffff",  # 新增: hover背景色
                "hover_text": "#000000",  # 新增: hover文字色
                "hover_border": "#cccccc",  # 新增: hover边框颜色
                "positive_color": "#e01c3a",  # 新增: 正数红色
                "negative_color": "#0d876d",  # 新增: 负数绿色
            },
            "dark": {
                "text": "#ffffff",
                "grid": "rgba(255,255,255,0.4)",
                "border": "rgba(255,255,255,0.4)",
                "cumret": "#ff6b6b",
                "drawdown": "#6bcfb5",
                "drawdown_fill": "rgba(107,207,181,0.3)",
                "table_header": "rgba(64,64,64,0)",
                "table_cell": "rgba(0,0,0,0)",
                "hover_bg": "#1a1a1a",  # 新增: 深色背景
                "hover_text": "#ffffff",  # 新增: 白色文字
                "hover_border": "#666666",  # 新增: hover边框颜色
                "positive_color": "#ff6b6b",  # 新增: 正数红色
                "negative_color": "#6bcfb5",  # 新增: 负数绿色
            },
        }

        cfg = theme_config.get(theme, theme_config["light"])
        text_color = cfg["text"]

        # =========================
        # 2. 尺寸 & 字体自适应
        # =========================
        base_width = 1440
        scale = max(0.8, min(client_width / base_width, 1.25))

        base_font = int(12 * scale)
        table_font = int(12 * scale)

        font_family = (
            '-apple-system, BlinkMacSystemFont, "PingFang SC", '
            '"Helvetica Neue", Arial, sans-serif'
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
        stats = []
        for year, grp in pnl.groupby(pnl.index.year):
            if len(grp) < 5:
                continue
            stats.append(
                {
                    "YEAR": str(year),
                    "Ann.R": ep.annual_return(grp),
                    "Cum.R": (1 + grp).prod() - 1,
                    "Mx.DD": ep.max_drawdown(grp),
                    "D.Rsk": np.percentile(grp, 5),
                }
            )

        perf_raw = pd.DataFrame(stats)
        if perf_raw.empty:
            raise ValueError("Not enough data to compute annual statistics")

        perf_raw = perf_raw.tail(2).set_index("YEAR")

        perf_fmt = perf_raw.copy()
        for c in perf_fmt.columns:
            perf_fmt[c] = perf_fmt[c].apply(lambda x: f"{x*100:.2f}%")

        # =========================
        # 5. 纵向 Table DataFrame
        # =========================
        table_labels = ["Ann.R", "Cum.R", "Mx.DD", "D.Rsk"]

        table_data = {
            " ": table_labels,
            **{
                year: perf_fmt.loc[year, ["Ann.R", "Cum.R", "Mx.DD", "D.Rsk"]].tolist()
                for year in perf_fmt.index
            },
        }

        table_df = pd.DataFrame(table_data)

        # =========================
        # 6. 准备表格字体颜色
        # =========================
        last_column = table_df.columns[-1]

        font_colors = []
        for col in table_df.columns:
            column_colors = []
            for i, val in enumerate(table_df[col]):
                if col == last_column and i >= 0:
                    try:
                        if i < len(table_df) and val != "":
                            num_val = float(val.replace("%", ""))
                            if num_val >= 0:
                                column_colors.append(cfg["positive_color"])
                            else:
                                column_colors.append(cfg["negative_color"])
                        else:
                            column_colors.append(text_color)
                    except (ValueError, AttributeError):
                        column_colors.append(text_color)
                else:
                    column_colors.append(text_color)
            font_colors.append(column_colors)

        cell_colors = [cfg["table_cell"]] * len(table_df.columns)

        # =========================
        # 7. Subplots - 使用完整宽度
        # =========================
        # 优先保证表格完整显示，使用更激进的宽度分配
        if client_width < 768:
            table_width_ratio = 0.42
            horizontal_spacing = 0.005
        elif client_width < 1024:
            table_width_ratio = 0.34
            horizontal_spacing = 0.008
        else:
            table_width_ratio = 0.28
            horizontal_spacing = 0.01

        fig = make_subplots(
            rows=1,
            cols=2,
            column_widths=[table_width_ratio, 1 - table_width_ratio],
            specs=[[{"type": "table"}, {"type": "xy", "secondary_y": True}]],
            horizontal_spacing=horizontal_spacing,
        )

        # =========================
        # 8. 左侧 Table - 紧贴左侧
        # =========================
        # 表格从0开始，紧贴左侧
        table_domain_left = 0.0
        table_domain_right = table_width_ratio

        fig.add_trace(
            go.Table(
                domain=dict(x=[table_domain_left, table_domain_right], y=[0.0, 1.0]),
                header=dict(
                    values=list(table_df.columns),
                    fill_color=cfg["table_header"],
                    line=dict(color=cfg["border"], width=1),
                    font=dict(size=table_font, color=text_color, family=font_family),
                    align=["left"] + ["center"] * (len(table_df.columns) - 1),
                    height=30 * scale,
                ),
                cells=dict(
                    values=[table_df[c] for c in table_df.columns],
                    fill_color=cell_colors,
                    line=dict(color=cfg["border"], width=1),
                    font=dict(
                        size=max(9, table_font - 1),
                        color=font_colors,
                        family=font_family,
                    ),
                    align=["left"] + ["center"] * (len(table_df.columns) - 1),
                    height=28 * scale,
                ),
                columnorder=list(range(len(table_df.columns))),
                columnwidth=[1.2] + [0.9] * (len(table_df.columns) - 1),  # 第一列更宽
            ),
            row=1,
            col=1,
        )

        # =========================
        # 9. 右侧主图 - 紧贴右侧
        # =========================
        # 图表紧贴右侧，使用剩余全部空间
        chart_domain_left = table_width_ratio + horizontal_spacing
        chart_domain_right = 1.0  # 直接到最右侧

        fig.add_trace(
            go.Scatter(
                x=cumulative.index,
                y=cumulative,
                mode="lines+markers",
                name="Cumulative Return",
                line=dict(color=cfg["cumret"], width=2.0 * scale),
                marker=dict(size=3.0 * scale),
                hovertemplate=(
                    "<span style='font-family: monospace'>"
                    "<b>Date</b>: %{x|%Y-%m-%d}<br>"
                    "<b>Cumulative Return</b>: %{y:.4f}"
                    "</span><extra></extra>"
                ),
            ),
            row=1,
            col=2,
            secondary_y=False,
        )

        fig.add_trace(
            go.Scatter(
                x=drawdown.index,
                y=drawdown,
                mode="lines",
                fill="tozeroy",
                name="Drawdown",
                line=dict(color=cfg["drawdown"], width=1.2 * scale),
                fillcolor=cfg["drawdown_fill"],
                hovertemplate=(
                    "<span style='font-family: monospace'>"
                    "<b>Date</b>: %{x|%Y-%m-%d}<br>"
                    "<b>Drawdown</b>: %{y:.2%}"
                    "</span><extra></extra>"
                ),
            ),
            row=1,
            col=2,
            secondary_y=True,
        )

        # =========================
        # 10. 智能标注系统
        # =========================
        def calculate_text_position(x_coord, y_coord, text, client_width):
            position = "top right"

            if isinstance(x_coord, pd.Timestamp):
                x_data = cumulative.index
                if len(x_data) > 0:
                    x_min, x_max = x_data.min(), x_data.max()
                    x_range = (x_max - x_min).total_seconds()

                    if (x_coord - x_min).total_seconds() > 0.9 * x_range:
                        position = "top left"
                    elif (x_coord - x_min).total_seconds() < 0.05 * x_range:
                        position = "top right"

            return position

        def get_compact_label(value, label_type, client_width):
            if client_width >= 1024:
                if label_type == "cumret":
                    return f"Max: {value:.2f}"
                elif label_type == "max_dd":
                    return f"MDD: {value:.1%}"
                elif label_type == "30d_dd":
                    return f"30D: {value:.1%}"
                elif label_type == "120d_dd":
                    return f"120D: {value:.1%}"
            elif client_width >= 768:
                if label_type == "cumret":
                    return f"M: {value:.1f}"
                elif label_type == "max_dd":
                    return f"D: {value:.1%}"
                elif label_type == "30d_dd":
                    return f"30: {value:.1%}"
                elif label_type == "120d_dd":
                    return f"12: {value:.1%}"
            else:
                if label_type == "cumret":
                    return f"M: {value:.1f}"
                elif label_type == "max_dd":
                    return f"D: {value:.1%}"
                elif label_type == "30d_dd":
                    return f"30: {value:.0%}"  # 0小数位
                elif label_type == "120d_dd":
                    return f"12: {value:.0%}"  # 0小数位

        if len(cumulative) > 0:
            cum_max_idx = cumulative.idxmax()
            cum_max_val = cumulative.max()
            max_position = calculate_text_position(
                cum_max_idx, cum_max_val, "", client_width
            )
            max_label = get_compact_label(cum_max_val, "cumret", client_width)

            fig.add_trace(
                go.Scatter(
                    x=[cum_max_idx],
                    y=[cum_max_val],
                    mode="markers+text",
                    marker=dict(symbol="circle", size=7 * scale, color=cfg["cumret"]),
                    text=[max_label],
                    textposition=max_position,
                    textfont=dict(
                        size=max(9, base_font - 2), color=text_color, family=font_family
                    ),
                    showlegend=False,
                    hovertemplate=(
                        f"<span style='font-family: monospace'>"
                        f"<b>Max Cumulative Return</b>: {cum_max_val:.4f}"
                        f"</span><extra></extra>"
                    ),
                ),
                row=1,
                col=2,
                secondary_y=False,
            )

        if len(drawdown) > 0:
            max_dd_idx = drawdown.idxmin()
            max_dd_val = drawdown.min()
            max_dd_position = calculate_text_position(
                max_dd_idx, max_dd_val, "", client_width
            )
            max_dd_label = get_compact_label(max_dd_val, "max_dd", client_width)

            fig.add_trace(
                go.Scatter(
                    x=[max_dd_idx],
                    y=[max_dd_val],
                    mode="markers+text",
                    marker=dict(symbol="circle", size=7 * scale, color=cfg["drawdown"]),
                    text=[max_dd_label],
                    textposition=max_dd_position,
                    textfont=dict(size=max(9, base_font - 2), color=text_color),
                    showlegend=False,
                    hovertemplate=(
                        f"<span style='font-family: monospace'>"
                        f"<b>Max Drawdown</b>: {max_dd_val:.2%}"
                        f"</span><extra></extra>"
                    ),
                ),
                row=1,
                col=2,
                secondary_y=True,
            )

            if len(drawdown) >= 30:
                w_30 = drawdown.iloc[-30:]
                idx_30 = w_30.idxmin()
                val_30 = w_30.loc[idx_30]

                if idx_30 != max_dd_idx:
                    offset_30 = (
                        0.03 * abs(max_dd_val)
                        if abs((idx_30 - max_dd_idx).days) < 7
                        else 0
                    )
                    position_30 = calculate_text_position(
                        idx_30, val_30 - offset_30, "", client_width
                    )
                    label_30 = get_compact_label(val_30, "30d_dd", client_width)

                    if (
                        position_30 == max_dd_position
                        and abs((idx_30 - max_dd_idx).days) < 14
                    ):
                        position_30 = (
                            "top left" if position_30 == "top right" else "top right"
                        )

                    fig.add_trace(
                        go.Scatter(
                            x=[idx_30],
                            y=[val_30 - offset_30],
                            mode="markers+text",
                            marker=dict(
                                symbol="diamond", size=6 * scale, color=cfg["drawdown"]
                            ),
                            text=[label_30],
                            textposition=position_30,
                            textfont=dict(size=max(9, base_font - 2), color=text_color),
                            showlegend=False,
                            hovertemplate=(
                                f"<span style='font-family: monospace'>"
                                f"<b>30D Max Drawdown</b>: {val_30:.2%}"
                                f"</span><extra></extra>"
                            ),
                        ),
                        row=1,
                        col=2,
                        secondary_y=True,
                    )

            if len(drawdown) >= 120:
                w_120 = drawdown.iloc[-120:]
                idx_120 = w_120.idxmin()
                val_120 = w_120.loc[idx_120]

                if idx_120 != max_dd_idx and (len(drawdown) < 30 or idx_120 != idx_30):
                    offset_120 = 0
                    if len(drawdown) >= 30 and "idx_30" in locals():
                        if abs((idx_120 - idx_30).days) < 7:
                            offset_120 = 0.05 * abs(val_120)

                    position_120 = calculate_text_position(
                        idx_120, val_120 - offset_120, "", client_width
                    )
                    label_120 = get_compact_label(val_120, "120d_dd", client_width)

                    positions_used = [max_dd_position]
                    if "position_30" in locals():
                        positions_used.append(position_30)

                    if position_120 in positions_used:
                        available_positions = [
                            "top right",
                            "top left",
                            "bottom right",
                            "bottom left",
                        ]
                        for pos in available_positions:
                            if pos not in positions_used:
                                position_120 = pos
                                break

                    fig.add_trace(
                        go.Scatter(
                            x=[idx_120],
                            y=[val_120 - offset_120],
                            mode="markers+text",
                            marker=dict(
                                symbol="diamond", size=6 * scale, color=cfg["drawdown"]
                            ),
                            text=[label_120],
                            textposition=position_120,
                            textfont=dict(size=max(9, base_font - 2), color=text_color),
                            showlegend=False,
                            hovertemplate=(
                                f"<span style='font-family: monospace'>"
                                f"<b>120D Max Drawdown</b>: {val_120:.2%}"
                                f"</span><extra></extra>"
                            ),
                        ),
                        row=1,
                        col=2,
                        secondary_y=True,
                    )

        # =========================
        # 11. Layout - 关键修改：确保占满宽度
        # =========================
        is_small_screen = client_width < 768
        fig.update_layout(
            autosize=True,
            # 关键：最小化边距，但为Y轴标签留出空间
            margin=dict(
                l=4 if is_small_screen else 0,
                r=4 if is_small_screen else 0,
                t=0,
                b=0,
            ),
            font=dict(size=base_font, color=text_color, family=font_family),
            legend=dict(
                x=0.98 if is_small_screen else 0.30,
                y=0.98,
                xanchor="right",
                yanchor="top",
                font=dict(size=max(9, base_font - 2)),
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified",
            hoverlabel=dict(
                bgcolor=cfg["hover_bg"],
                font_size=max(9, base_font - 1),
                font_family="monospace",
                font_color=cfg["hover_text"],
                bordercolor=cfg["hover_border"],
            ),
            # 关键：使用完整的域设置
            # xaxis=dict(domain=[chart_domain_left, chart_domain_right], anchor="y"),
            # xaxis2=dict(domain=[chart_domain_left, chart_domain_right], anchor="y2"),
        )

        # 左Y轴
        fig.update_yaxes(
            title_text="",
            gridcolor=cfg["grid"],
            tickfont=dict(
                size=max(9, base_font - 1), color=text_color, family=font_family
            ),
            row=1,
            col=2,
            secondary_y=False,
            showgrid=True,
            automargin=not is_small_screen,  # 自动边距确保标签显示
            tickmode="auto",
            # 设置合适的范围
            range=[0, cumulative.max() * 1.05] if len(cumulative) > 0 else [0, 1],
        )

        # 右Y轴 - 关键：确保显示
        fig.update_yaxes(
            title_text="",
            tickformat=".0%",
            gridcolor=cfg["grid"],
            tickfont=dict(
                size=max(9, base_font - 1), color=text_color, family=font_family
            ),
            row=1,
            col=2,
            secondary_y=True,
            showgrid=False,
            automargin=not is_small_screen,  # 自动边距确保标签显示
            tickmode="auto",
            # 确保有足够的空间显示负百分比
            range=[
                min(drawdown.min() * 1.1, -0.05) if len(drawdown) > 0 else -0.05,
                0.01,
            ],
            # 关键：设置侧边
            side="right",
            overlaying="y",
        )

        fig.update_xaxes(
            gridcolor=cfg["grid"],
            tickangle=-20 if client_width >= 768 else -25,
            tickfont=dict(
                size=max(9, base_font - 1), color=text_color, family=font_family
            ),
            row=1,
            col=2,
            title_text="",
            automargin=True,
            tickmode="auto",
            # 确保X轴使用整个图表宽度
            domain=[chart_domain_left, chart_domain_right],
        )

        # 为所有trace添加统一的hover样式
        for trace in fig.data:
            if hasattr(trace, "hovertemplate"):
                if "<span style=" not in trace.hovertemplate:
                    trace.hovertemplate = (
                        f"<span style='font-family: monospace'>"
                        f"{trace.hovertemplate.replace('<extra></extra>', '')}"
                        f"</span><extra></extra>"
                    )

        return fig
