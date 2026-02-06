"""
修正版：单个通用回调 + DataLoader 驱动
"""

from dash import Output, Input, MATCH
import dash
from finance.dashreport.data_loader import ReportDataLoader


class ChartCallback:
    """
    图表回调管理器 - 单个通用回调
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._charts = {}
            self._callback_registered = False
            self._initialized = True

    # =========================
    # 注册图表（不传 DataFrame）
    # =========================
    def register_chart(
        self,
        chart_type: str,
        page_prefix: str,
        chart_builder,
        datasets,
        index: int = 0,
    ):
        """
        datasets: tuple[str, ...]
        """
        key = f"{chart_type}_{page_prefix}_{index}"
        self._charts[key] = {
            "builder": chart_builder,
            "datasets": tuple(datasets),
            "chart_type": chart_type,
            "page": page_prefix,
            "index": index,
        }
        return key

    def get_chart_id(self, chart_type, page_prefix, index=0):
        return {
            "type": "dynamic-chart",
            "page": page_prefix,
            "chart": chart_type,
            "index": index,
        }

    # =========================
    # 全局唯一回调
    # =========================
    def setup_callback(self, app):
        if self._callback_registered:
            return

        @app.callback(
            Output(
                {
                    "type": "dynamic-chart",
                    "page": MATCH,
                    "chart": MATCH,
                    "index": MATCH,
                },
                "figure",
            ),
            Input("current-theme", "data"),
            Input("client-width", "data"),
            Input(
                {
                    "type": "dynamic-chart",
                    "page": MATCH,
                    "chart": MATCH,
                    "index": MATCH,
                },
                "id",
            ),
            prevent_initial_call=False,  # ⚠️ 允许首次渲染
        )
        def universal_chart_callback(theme, client_width, component_id):
            page = component_id["page"]
            chart_type = component_id["chart"]
            index = component_id["index"]

            key = f"{chart_type}_{page}_{index}"
            if key not in self._charts:
                return dash.no_update

            info = self._charts[key]
            builder = info["builder"]
            datasets = info["datasets"]

            theme = theme or "light"
            client_width = client_width or 1440

            try:
                # 🚀 核心：真正命中 mtime-aware LRU
                data_bundle = ReportDataLoader.load(
                    prefix=page,
                    datasets=datasets,
                )

                # ===== 关键：总是添加微小延迟 =====
                import time

                time.sleep(0.1)  # 50ms延迟，确保loading有显示时间

                # ===== 图表分发 =====
                if chart_type == "annual_return":
                    pnl, cash, total_value = data_bundle["annual_return"]
                    fig = builder.annual_return(
                        page=page,
                        pnl=pnl,
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "heatmap":
                    fig = builder.calendar_heatmap(
                        page=page,
                        df=data_bundle["heatmap"],
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "strategy":
                    fig = builder.strategy_chart(
                        page=page,
                        df=data_bundle["strategy"],
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "trade":
                    fig = builder.trade_info_chart(
                        page=page,
                        df=data_bundle["trade"],
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "pnl_trend":
                    fig = builder.industry_pnl_trend(
                        page=page,
                        df=data_bundle["pnl_trend"],
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "industry_position":
                    fig = builder.industry_position_treemap(
                        page=page,
                        df=data_bundle["industry_position"],
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "industry_profit":
                    fig = builder.industry_profit_treemap(
                        page=page,
                        df=data_bundle["industry_profit"],
                        theme=theme,
                        client_width=client_width,
                    )

                else:
                    return dash.no_update

                return fig
            except Exception as e:
                print(f"⚠️ 图表生成失败 {key}: {e}")
                return dash.no_update

        self._callback_registered = True
