# callbacks/chart_callback.py
"""
修正版：单个通用回调处理所有图表
"""

from dash import callback, Output, Input, State, MATCH
import dash


class ChartCallback:
    """
    图表回调管理器 - 使用单个回调处理所有图表类型
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            # 存储所有图表数据：key -> (builder, data, chart_type)
            self._charts = {}
            # 记录已注册的回调，避免重复注册
            self._callback_registered = False
            self._initialized = True
            print("✅ ChartCallback 初始化完成")

    def register_chart(self, chart_type, page_prefix, chart_builder, df_data, index=0):
        """
        注册图表数据

        注意：这个方法只存储数据，不创建回调
        """
        key = f"{chart_type}_{page_prefix}_{index}"
        self._charts[key] = {
            "builder": chart_builder,
            "data": df_data,
            "type": chart_type,
            "page": page_prefix,
            "index": index,
        }
        print(f"✅ 注册图表: {key}")
        return key

    def get_chart_id(self, chart_type, page_prefix, index=0):
        """获取图表ID（与register_chart对应）"""
        return {
            "type": "dynamic-chart",  # 固定type，不要用chart_type
            "page": page_prefix,
            "chart": chart_type,  # 将chart_type作为chart字段
            "index": index,
        }

    def setup_callback(self, app):
        """
        设置回调 - 整个应用只调用一次

        重要：这个回调使用了唯一的输出模式
        """
        if self._callback_registered:
            print("⚠️ 回调已注册，跳过重复注册")
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
            State(
                {
                    "type": "dynamic-chart",
                    "page": MATCH,
                    "chart": MATCH,
                    "index": MATCH,
                },
                "id",
            ),
            prevent_initial_call=False,
        )
        def universal_chart_callback(theme, component_id):
            """通用图表回调函数"""
            # 从组件ID获取信息
            page = component_id.get("page", "")
            chart_type = component_id.get("chart", "")  # 这里是实际的图表类型
            index = component_id.get("index", 0)

            # 构建查找键（与register_chart一致）
            key = f"{chart_type}_{page}_{index}"

            # 检查是否有该图表的数据
            if key not in self._charts:
                return dash.no_update

            # 获取图表数据
            chart_info = self._charts[key]
            builder = chart_info["builder"]
            data = chart_info["data"]

            # 确保theme有值
            theme = theme or "light"

            try:
                # 根据图表类型调用对应的方法
                if chart_type == "heatmap":
                    return builder.calendar_heatmap(df=data, theme=theme)
                else:
                    # 尝试通用方法
                    method_name = f"{chart_type}_from_df"
                    if hasattr(builder, method_name):
                        method = getattr(builder, method_name)
                        return method(df=data, theme=theme)
            except Exception as e:
                print(f"⚠️ 图表生成错误 {key}: {e}")
                return dash.no_update

            return dash.no_update

        self._callback_registered = True
        print("✅ 通用图表回调设置完成")
