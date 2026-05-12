import pandas as pd
from dash import html, Input, Output, callback, MATCH
from dash.exceptions import PreventUpdate
from finance.dashreport.data_loader import ReportDataLoader


class KpiCallback:
    def setup_callback(self, app):
        """
        注册 KPI 更新回调，直接加载数据并生成卡片。
        """

        @app.callback(
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
                    "请执行回测",
                    style={"color": "#999", "padding": "20px", "textAlign": "center"},
                )

            # 取第一行数据
            row = df_overall.iloc[0]
            final_value = row.get("final_value", 0)
            cash = row.get("cash", 0)
            stock_cnt = row.get("stock_cnt", 1)
            end_date = row.get("end_date", "")

            # 处理股票数量异常值
            if pd.isna(stock_cnt) or stock_cnt in [None, 0, ""]:
                stock_cnt = 1

            # 计算 SWDI 指数
            swdi = int(
                round(
                    ((final_value - cash) - (stock_cnt * 10000 - cash)) / stock_cnt, 0
                )
            )

            # 构建 KPI 卡片
            kpis = [
                ("SWDI IDX", swdi),
                ("ASSETS", f"{final_value/10000:,.2f} 万"),
                ("CASH", f"{cash/10000:,.2f} 万"),
                ("COUNT", f"{stock_cnt}"),
                ("DATE", end_date),
            ]

            cards = []
            for label, value in kpis:
                extra_class = "kpi-date" if label == "DATE" else ""
                cards.append(
                    html.Div(
                        [
                            html.Div(label, className="kpi-label"),
                            html.Div(value, className=f"kpi-value {extra_class}"),
                        ],
                        className="kpi-card",
                    )
                )
            return cards

    @staticmethod
    def get_container_id(page):
        """返回 KPI 卡片容器的 ID 字典（用于布局）"""
        return {"type": "kpi-container", "page": page}
