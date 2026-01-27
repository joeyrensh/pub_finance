import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import ast
from finance.dashreport.utils import Header, make_dash_format_table
import pandas as pd
import pathlib
import os


class ChartBuilder:
    @staticmethod
    def calendar_heatmap(
        df,
        theme="light",  # 主题参数
    ):
        """
        从CSV文件生成日历热图交互式图表（仅周一到周五）
        支持动态主题切换

        Parameters:
        -----------
        df : pandas.DataFrame
            包含日历热图数据的DataFrame
        fig_width : int, default=1440
            图表宽度
        fig_height : int, default=900
            图表高度
        theme : str, default="light"
            主题模式，"light" 或 "dark"
        base_font_size : int, default=12
            基础字体大小
        font_family : str, default="Arial"
            字体家族

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
                "grid": "rgba(0, 0, 0, 0.2)",  # 网格线
                "background": "rgba(255, 255, 255, 0)",  # 透明背景
            },
            "dark": {
                "positive": "#ff6b6b",  # 亮红色 - 正数
                "negative": "#6bcfb5",  # 亮绿色 - 负数
                "neutral": "#ffffff",  # 白色 - 中性文本、行业文本、坐标轴
                "grid": "rgba(255, 255, 255, 0.2)",  # 网格线
                "background": "rgba(0, 0, 0, 0)",  # 透明背景
            },
        }
        base_font_size = 12  # 基础字体大小
        font_family = '-apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", Arial, sans-serif'
        fig_width = 1440
        fig_height = 900

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

        max_size_increase = 25
        font_steps = np.linspace(
            base_font_size - 8, base_font_size - 8 + max_size_increase, 5
        )

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
                    align="center",
                    xanchor="center",
                    yanchor="top",
                    yshift=20,
                    opacity=0.9,
                )

            # 2. 添加中间的日期（月份和日期分开两行）
            # 月份在上
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=month,
                showarrow=False,
                font=dict(
                    family=font_family,
                    weight="bold",
                    size=dynamic_font_size,
                    color=text_color,
                ),
                align="center",
                xanchor="center",
                yanchor="middle",
                yshift=dynamic_font_size * 0.3,
                opacity=0.9,
            )

            # 日期在下
            fig.add_annotation(
                x=day_of_week,
                y=week_order,
                text=day,
                showarrow=False,
                font=dict(
                    family=font_family,
                    weight="bold",
                    size=dynamic_font_size,
                    color=text_color,
                ),
                align="center",
                xanchor="center",
                yanchor="middle",
                yshift=-dynamic_font_size * 0.3,
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
                    align="center",
                    xanchor="center",
                    yanchor="bottom",
                    yshift=-20,
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
            autosize=True,
            dragmode=False,
        )

        # 在每周之间添加横向分隔线
        unique_weeks_list = sorted(df["week_order"].unique()) if len(df) > 0 else []

        for week in unique_weeks_list[1:]:
            fig.add_hline(
                y=week - 0.5,
                line=dict(color=config["grid"], width=1),
                opacity=0.2,
                layer="below",
            )

        for day in range(1, 5):
            fig.add_vline(
                x=day - 0.5,
                line=dict(color=config["grid"], width=1),
                opacity=0.2,
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

        return (
            f"日期: {date_str}<br>"
            f"星期: {day_name}<br>"
            f"数值: {value:.2f}<br>"
            f"行业: {industry_text}"
        )
