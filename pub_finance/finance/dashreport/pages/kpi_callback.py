import pandas as pd
from dash import html, Input, Output, callback, MATCH
from dash.exceptions import PreventUpdate
from finance.dashreport.data_loader import ReportDataLoader
import numpy as np


class KpiCallback:
    def setup_callback(self, app):
        """
        注册 KPI 更新回调，直接加载数据并生成卡片。
        """

        @callback(
            Output({"type": "kpi-container", "page": MATCH}, "children"),
            Input("current-theme", "data"),
            Input({"type": "kpi-container", "page": MATCH}, "id"),
            prevent_initial_call=False,  # 允许首次渲染
        )
        def update_kpi_cards(theme, container_id):
            """
            当页面加载或主题变化时，从数据源加载 overall 数据并生成 KPI 卡片。
            """
            # 从容器 ID 中提取 page 参数
            page = container_id.get("page")
            if not page:
                raise PreventUpdate

            # 使用 ReportDataLoader 加载数据
            datasets = ["overall"]
            data = ReportDataLoader.load(prefix=page, datasets=datasets)
            df_overall = data.get("overall")
            # ========== 新增：无数据时返回占位文本 ==========
            if df_overall is None or df_overall.empty:
                return html.Div(
                    "Run Backtest",
                    style={"color": "#999", "padding": "20px", "textAlign": "center"},
                )

            # 2. 提取最新一日数据（第一行，索引 0）
            row_today = df_overall.iloc[0]
            val_today = row_today.get("final_value", 0.0)
            cash_today = row_today.get("cash", 0.0)
            stock_cnt_today = row_today.get("stock_cnt", 1)

            if pd.isna(stock_cnt_today) or stock_cnt_today in [None, 0, ""]:
                stock_cnt_today = 1

            # 计算最新一日的 SWDI
            swdi_today = int(
                round(
                    ((val_today - cash_today) / 10000)
                    * (
                        2.0 - (cash_today / val_today if val_today > 0 else 1.0)
                    )  # 资金利用率因子
                    * (1.0 + np.log(stock_cnt_today)),  # 股票数量平滑因子
                    0,
                )
            )

            # 3. 初始化环比计算的上一日默认变量
            val_prev = None
            cash_prev = None
            stock_cnt_prev = None
            swdi_prev = None

            # 判断是否有第二行数据（即上一日，索引 1）
            has_prev = len(df_overall) > 1

            if has_prev:
                row_prev = df_overall.iloc[1]
                val_prev = row_prev.get("final_value", 0.0)
                cash_prev = row_prev.get("cash", 0.0)
                stock_cnt_prev = row_prev.get("stock_cnt", 1)

                if pd.isna(stock_cnt_prev) or stock_cnt_prev in [None, 0, ""]:
                    stock_cnt_prev = 1

                # 计算上一日的 SWDI
                swdi_prev = int(
                    round(
                        ((val_prev - cash_prev) / 10000)
                        * (
                            2.0 - (cash_prev / val_prev if val_prev > 0 else 1.0)
                        )  # 资金利用率因子
                        * (1.0 + np.log(stock_cnt_prev)),  # 股票数量平滑因子
                        0,
                    )
                )

            # 4. 统一定义环比百分比计算闭包函数（防零、防空安全计算）
            def calc_pct(today_val, prev_val):
                if (
                    not has_prev
                    or prev_val is None
                    or pd.isna(prev_val)
                    or prev_val == 0
                ):
                    return "-"
                pct = ((today_val - prev_val) / abs(prev_val)) * 100
                # 格式化为带有正负号的2位小数百分比
                return f"{pct:+.2f}%" if pct != 0 else "0.00%"

            # 5. 计算各个指标的环比百分比
            swdi_pct = calc_pct(swdi_today, swdi_prev)
            assets_pct = calc_pct(val_today, val_prev)
            cash_pct = calc_pct(cash_today, cash_prev)
            shares_pct = calc_pct(stock_cnt_today, stock_cnt_prev)

            # 6. 构建升级版的 KPI 结构 (格式: (指标名, 最新值, 环比值/补充说明))
            kpis = [
                ("SWDI", swdi_today, swdi_pct),
                ("ASSETS", f"{val_today / 10000:,.2f} 万", assets_pct),
                ("CASH", f"{cash_today / 10000:,.2f} 万", cash_pct),
                ("SHARES", f"{stock_cnt_today}", shares_pct),
                (
                    "DATE",
                    row_today.get("end_date", "")[-5:],
                    "Today",
                ),  # DATE 一栏不显示百分比，固定为 "Today"
            ]

            cards = []
            # 解包 3 个值 (label, value, pct)
            for label, value, pct in kpis:
                extra_class = "kpi-date" if label == "DATE" else ""

                # 动态判断环比文字的颜色样式（A股红涨绿跌，Today保持中性）
                if pct.startswith("+"):
                    pct_class = "kpi-pct-up"  # A股红涨
                elif pct.startswith("-"):
                    pct_class = "kpi-pct-down"  # A股绿跌
                else:
                    pct_class = "kpi-pct-neutral"

                cards.append(
                    html.Div(
                        [
                            html.Div(label, className="kpi-label"),
                            html.Div(value, className=f"kpi-value {extra_class}"),
                            # 环比/说明文本展示层
                            html.Div(pct, className=f"kpi-pct {pct_class}"),
                        ],
                        className="kpi-card",
                    )
                )
            return cards

    @staticmethod
    def get_container_id(page):
        """返回 KPI 卡片容器的 ID 字典（用于布局）"""
        return {"type": "kpi-container", "page": page}
