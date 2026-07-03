# finance/dashreport/table_callback.py

from dash import callback, Output, Input, MATCH, html, State, no_update, ctx, ALL
import dash
import time
from finance.dashreport.data_loader import ReportDataLoader
from finance.dashreport.utils import Header, make_dash_format_table


class TableCallback:
    """
    通用 Table 回调管理器
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._callback_registered = False
            self._initialized = True

    def setup_callback(self, app):
        """
        注册 table 回调（整个 app 只调用一次）
        """
        if self._callback_registered:
            return

        @app.callback(
            Output(
                {
                    "type": "dynamic-table",
                    "page": MATCH,
                    "table": MATCH,
                },
                "children",
            ),
            Input("current-theme", "data"),
            Input(
                {
                    "type": "dynamic-table",
                    "page": MATCH,
                    "table": MATCH,
                },
                "id",
            ),
            prevent_initial_call=False,  # ⚠️ 允许首屏加载
        )
        def render_table(theme, table_id):
            prefix = table_id["page"]
            table_name = table_id["table"]

            try:
                # ===== 根据 table 类型选择 datasets =====
                if table_name == "category":
                    datasets = ("overall", "category")
                elif table_name == "detail":
                    datasets = ("overall", "detail")
                elif table_name == "detail_short":
                    datasets = ("overall", "detail_short")
                elif table_name == "cn_etf":
                    datasets = ("overall", "cn_etf")
                else:
                    return dash.no_update

                data = ReportDataLoader.load(
                    prefix=prefix,
                    datasets=datasets,
                )

                df_overall = data.get("overall")

                # ===== 构造具体 table =====
                if table_name == "category":
                    if data["category"]["df"].empty:
                        return html.Div(
                            "请执行回测",
                            style={
                                "color": "#999",
                                "padding": "20px",
                                "textAlign": "center",
                            },
                        )
                    return make_dash_format_table(
                        data["category"]["df"],
                        data["category"]["formats"],
                        prefix,
                        df_overall.at[0, "end_date"],
                        table_name,
                    )

                if table_name == "detail":
                    if data["detail"]["df"].empty:
                        return html.Div(
                            "请执行回测",
                            style={
                                "color": "#999",
                                "padding": "20px",
                                "textAlign": "center",
                            },
                        )
                    return make_dash_format_table(
                        data["detail"]["df"],
                        data["detail"]["formats"],
                        prefix,
                        df_overall.at[0, "end_date"],
                        table_name,
                    )

                if table_name == "detail_short":
                    if data["detail_short"]["df"].empty:
                        return html.Div(
                            "请执行回测",
                            style={
                                "color": "#999",
                                "padding": "20px",
                                "textAlign": "center",
                            },
                        )
                    return make_dash_format_table(
                        data["detail_short"]["df"],
                        data["detail_short"]["formats"],
                        prefix,
                        df_overall.at[0, "end_date"],
                        table_name,
                    )

                if table_name == "cn_etf":
                    if data["cn_etf"]["df"].empty:
                        return html.Div(
                            "请执行回测",
                            style={
                                "color": "#999",
                                "padding": "20px",
                                "textAlign": "center",
                            },
                        )
                    return make_dash_format_table(
                        data["cn_etf"]["df"],
                        data["cn_etf"]["formats"],
                        prefix,
                        df_overall.at[0, "end_date"],
                        table_name,
                    )

            except Exception as e:
                print(f"⚠️ Table render error [{table_name}]: {e}")
                return dash.no_update

            return dash.no_update

        @app.callback(
            Output("store_selected_cell_info", "data"),
            Input({"type": "auto-table", "page": ALL, "table": ALL}, "selected_cells"),
            State({"type": "auto-table", "page": ALL, "table": ALL}, "id"),
            State(
                {"type": "auto-table", "page": ALL, "table": ALL},
                "derived_viewport_data",
            ),
            State("ai_is_loading", "data"),
            prevent_initial_call=True,
        )
        def capture_all_cell(
            all_selected_list, all_table_id_list, all_viewport_data, is_loading
        ):
            # Loading 全程拦截所有单元格操作，什么都不改
            if is_loading:
                return dash.no_update

            triggered_id = ctx.triggered_id
            if not triggered_id:
                return dash.no_update

            target_tables = ["detail", "trade"]
            if triggered_id.get("table") not in target_tables:
                return dash.no_update

            target_index = None
            for i, tid in enumerate(all_table_id_list):
                if tid == triggered_id:
                    target_index = i
                    break
            if target_index is None:
                return dash.no_update

            selected_cells = all_selected_list[target_index]
            viewport_data = all_viewport_data[target_index]
            if not selected_cells or not viewport_data:
                # 点击空白处，清空选中，隐藏按钮
                return None

            cell = selected_cells[0]
            col_id = cell.get("column_id", "")

            # ========== 关键改动 ==========
            if col_id != "NAME":
                # 点击非NAME列，主动清空选中，触发按钮隐藏
                return None

            row_idx = cell["row"]
            if row_idx >= len(viewport_data):
                return None

            row = viewport_data[row_idx]
            symbol = row.get("SYMBOL_o", row.get("SYMBOL", ""))
            name = row.get("NAME_o", row.get("NAME", ""))

            payload = {
                "row": row_idx,
                "page": triggered_id["page"],
                "table": triggered_id["table"],
                "symbol": symbol,
                "name": name,
            }
            return payload

        @app.callback(
            Output("global_btn_ai_summary", "style"),
            Input("store_selected_cell_info", "data"),
            Input("ai_is_loading", "data"),
            prevent_initial_call=True,
        )
        def toggle_ai_button_show(cell_info, is_loading):
            hide_style = {"display": "none"}
            show_style = {
                "display": "inline-block",
                "cursor": "pointer",
            }

            # 条件1：AI加载中 → 强制隐藏按钮
            if is_loading:
                return hide_style

            # 条件2：没有选中有效NAME单元格 → 隐藏按钮
            if cell_info is None:
                return hide_style

            # 选中合法NAME行 → 显示按钮
            return show_style

        @app.callback(
            Output("ai_summary_box", "children", allow_duplicate=True),
            Output("ai_summary_loading_trigger", "children", allow_duplicate=True),
            Output("ai_is_loading", "data", allow_duplicate=True),
            Output("ai_trigger", "data"),
            Input("global_btn_ai_summary", "n_clicks"),
            State("store_selected_cell_info", "data"),
            prevent_initial_call=True,
        )
        def start_ai(n_clicks, cell_info):
            if not n_clicks or not cell_info:
                return "未选中有效NAME列单元格", None, False, 0

            trigger_value = n_clicks or 0

            return "智能分析中，请稍候...", None, True, trigger_value

        @app.callback(
            Output("ai_summary_box", "children", allow_duplicate=True),
            Output("ai_summary_loading_trigger", "children", allow_duplicate=True),
            Output("ai_is_loading", "data", allow_duplicate=True),
            Input("ai_trigger", "data"),
            State("store_selected_cell_info", "data"),
            prevent_initial_call=True,
        )
        def execute_ai(trigger, cell_info):
            if not trigger or not cell_info:
                return "请选择股票", False

            symbol = cell_info.get("symbol", "")
            name = cell_info.get("name", "")

            # ---------- 执行 AI 调用（直接使用 symbol 和 name） ----------
            import os
            from openai import OpenAI
            from dotenv import load_dotenv

            load_dotenv()
            api_key = os.getenv("API_KEY")
            ai_result = ""
            try:
                client = OpenAI(
                    api_key=api_key,
                    base_url="https://ws-uaeaan6mql1ieioa.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
                )
                prompt = (
                    f"你是专业股票分析师，联网检索股票{symbol}最新财报与近期资讯。\n"
                    f"请在 150 字以内直接输出针对该股的专业投资结论，严禁包含任何思考过程或客套引导语。\n\n"
                    f"【核心分析要点】\n"
                    f"1. 财务状况（营收增速、毛利、现金流关键特征）\n"
                    f"2. 主营业务与行业排名/竞争力\n"
                    f"3. 核心多空因素（利好/利空/管理层核心动向）\n\n"
                    f"硬性红线（严格执行）：全文严禁出现A股、美股、中国市场、美国市场、沪深、纳斯达克、纽交所等任何市场归属描述，"
                    f"不要补充该股票所属市场相关注释、说明、备注，全程只分析企业经营基本面。"
                )
                response = client.responses.create(
                    model="qwen3.7-max",
                    input=prompt,
                    tools=[
                        {"type": "web_search"},
                        # {"type": "code_interpreter"},
                        {"type": "web_extractor"},
                    ],
                    max_output_tokens=300,
                    extra_body={"enable_thinking": True},
                )
                ai_result = response.output_text.strip()
            except Exception as e:
                ai_result = f"AI分析调用异常：{str(e)}"

            summary_content = (
                f"股票代码：{symbol} 股票名称：{name.strip('88+')} {ai_result}"
            )
            return summary_content, None, False

        @app.callback(
            Output({"type": "auto-table-count", "page": ALL, "table": ALL}, "children"),
            Input(
                {"type": "auto-table", "page": ALL, "table": ALL},
                "derived_virtual_indices",
            ),
        )
        def update_row_count(indices_list):
            return [
                f"Total {len(idx) if idx is not None else 0} Rows"
                for idx in indices_list
            ]

        self._callback_registered = True
