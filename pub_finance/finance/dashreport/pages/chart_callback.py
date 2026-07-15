"""
修正版：单个通用回调 + DataLoader 驱动
"""

from dash import Output, Input, MATCH, callback, no_update
import plotly.graph_objects as go
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

    @staticmethod
    def _get_scale(
        base_fig_width=1440,
        client_width=None,
        min_scale=0.6,
        max_scale=1.05,
    ):
        """计算缩放比例"""
        if client_width is None:
            return 1.0
        if client_width < 550:
            scale = client_width / base_fig_width
        else:
            scale = 1.0
        return max(min_scale, min(scale, max_scale))

    @staticmethod
    def _get_font_sizes(client_width, base_font=12, min_scale=0.9, max_scale=1.05):
        """获取字体大小"""
        # 通过类名调用静态方法
        scale = ChartCallback._get_scale(1440, client_width, min_scale, max_scale)
        font_size = int(base_font * scale)
        return scale, font_size

    @staticmethod
    def _placeholder_figure(
        text: str = "Run Backtest", theme: str = "light", client_width: int = 1440
    ):
        """生成一个显示文本的占位图表"""
        bg_color = "rgba(255, 255, 255, 0)"
        # 获取适应屏幕的字体大小
        _, font_size = ChartCallback._get_font_sizes(
            client_width, base_font=12, min_scale=0.9, max_scale=1.05
        )
        fig = go.Figure()
        fig.add_annotation(
            text=text,
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=font_size, color="#999"),
        )
        fig.update_layout(
            template="plotly_white" if theme == "light" else "plotly_dark",
            paper_bgcolor=bg_color,
            plot_bgcolor=bg_color,
            xaxis=dict(showgrid=False, showticklabels=False, visible=False),
            yaxis=dict(showgrid=False, showticklabels=False, visible=False),
        )
        return fig

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

        @callback(
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
                return no_update

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
                    annual_data = data_bundle.get("annual_return")
                    if annual_data is None:
                        return self._placeholder_figure(
                            "Run Backtest", theme, client_width
                        )
                    pnl, cash, total_value = data_bundle["annual_return"]
                    fig = builder.annual_return(
                        page=page,
                        pnl=pnl,
                        theme=theme,
                        client_width=client_width,
                    )
                elif chart_type == "heatmap":
                    df = data_bundle["heatmap"]
                    if df is None or df.empty:
                        return self._placeholder_figure(
                            "Run Backtest", theme, client_width
                        )
                    fig = builder.calendar_heatmap(
                        page=page,
                        df=df,
                        theme=theme,
                        client_width=client_width,
                    )
                elif chart_type == "industry_strength":
                    df = data_bundle["industry_strength"]
                    if df is None or df.empty:
                        return self._placeholder_figure(
                            "Run Backtest", theme, client_width
                        )
                    fig = builder.industry_strength_chart(
                        page=page,
                        df=df,
                        theme=theme,
                        client_width=client_width,
                    )
                elif chart_type == "strategy":
                    df = data_bundle["strategy"]
                    if df is None or df.empty:
                        return self._placeholder_figure(
                            "Run Backtest", theme, client_width
                        )
                    fig = builder.strategy_chart(
                        page=page,
                        df=df,
                        theme=theme,
                        client_width=client_width,
                        bar_metric="avg",
                    )

                elif chart_type == "trade":
                    df = data_bundle["trade"]
                    if df is None or df.empty:
                        return self._placeholder_figure(
                            "Run Backtest", theme, client_width
                        )
                    fig = builder.trade_info_chart(
                        page=page,
                        df=df,
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "pnl_trend":
                    df = data_bundle["pnl_trend"]
                    if df is None or df.empty:
                        return self._placeholder_figure(
                            "Run Backtest", theme, client_width
                        )
                    fig = builder.industry_pnl_trend(
                        page=page,
                        df=df,
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "industry_position":
                    df = data_bundle["industry_position"]
                    if df is None or df.empty:
                        return self._placeholder_figure(
                            "Run Backtest", theme, client_width
                        )
                    fig = builder.industry_position_treemap(
                        page=page,
                        df=df,
                        theme=theme,
                        client_width=client_width,
                    )

                elif chart_type == "industry_profit":
                    df = data_bundle["industry_profit"]
                    if df is None or df.empty:
                        return self._placeholder_figure(
                            "Run Backtest", theme, client_width
                        )
                    fig = builder.industry_profit_treemap(
                        page=page,
                        df=df,
                        theme=theme,
                        client_width=client_width,
                    )

                else:
                    return no_update

                return fig
            except Exception as e:
                print(f"⚠️ Chart generation failed. {key}: {e}")
                return no_update

        self._callback_registered = True
