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

        # ========== 回调1：捕获所有表格单元格点击 ==========
        @app.callback(
            Output("store_selected_cell_info", "data"),
            Input(
                {"type": "auto-table", "page": ALL, "table": "detail"}, "selected_cells"
            ),
            State({"type": "auto-table", "page": ALL, "table": "detail"}, "id"),
            prevent_initial_call=True,
        )
        def capture_all_cell(all_selected_list, all_table_id_list):

            # 获取触发组件结构化ID
            triggered_id = ctx.triggered_id
            if not triggered_id:
                return no_update

            # 遍历匹配是第几个表格
            target_index = None
            for i, tid in enumerate(all_table_id_list):
                if tid == triggered_id:
                    target_index = i
                    break
            if target_index is None:
                return None

            selected_cells = all_selected_list[target_index]
            tbl_id = all_table_id_list[target_index]
            page = tbl_id["page"]
            tbl_key = tbl_id["table"]

            if not selected_cells:
                return None

            cell = selected_cells[0]
            col_id = cell.get("column_id", "")
            if col_id != "NAME":
                return None

            payload = {"row": cell["row"], "page": page, "table": tbl_key}
            return payload

        # ========== 回调2：控制AI按钮显隐 + 清空摘要 ==========
        @app.callback(
            Output("global_btn_ai_summary", "style"),
            Output("ai_summary_box", "children"),
            Input("store_selected_cell_info", "data"),
            prevent_initial_call=True,
        )
        def toggle_ai_button_show(cell_info):
            default_text = (
                "点击任意表格 NAME 列单元格，再点击上方【AI分析】生成个股量化摘要"
            )
            hide_style = {"display": "none", "margin": "12px 0 6px 0"}
            show_style = {
                "display": "inline-block",
                "margin": "12px 0 6px 0",
                "padding": "7px 18px",
                "cursor": "pointer",
            }
            if cell_info is None:
                return hide_style, default_text
            return show_style, ""

        @app.callback(
            Output("ai_summary_box", "children", allow_duplicate=True),
            Input("global_btn_ai_summary", "n_clicks"),
            State("store_selected_cell_info", "data"),
            State({"type": "auto-table", "page": ALL, "table": ALL}, "data"),
            State({"type": "auto-table", "page": ALL, "table": ALL}, "id"),
            prevent_initial_call=True,
        )
        def gen_ai_summary(n_clicks, cell_info, all_table_data_list, all_table_id_list):
            import os
            from openai import OpenAI

            if not n_clicks or not cell_info:
                return "未选中有效NAME列单元格"

            row_idx = cell_info["row"]
            target_page = cell_info["page"]
            target_table = cell_info["table"]

            target_rows = None
            # 遍历匹配表格ID，拿到对应表格数据
            for tbl_id, rows in zip(all_table_id_list, all_table_data_list):
                if tbl_id["page"] == target_page and tbl_id["table"] == target_table:
                    target_rows = rows
                    break

            if not target_rows or row_idx >= len(target_rows):
                return "表格数据读取异常"

            row = target_rows[row_idx]
            symbol = row.get("SYMBOL_o", row.get("SYMBOL", ""))
            stock_name = row.get("NAME_o", row.get("NAME", ""))

            # 调用阿里云百炼通义模型
            from dotenv import load_dotenv

            load_dotenv()  # 会强制从 .env 文件加载
            api_key = os.getenv("API_KEY")
            try:
                client = OpenAI(
                    api_key=api_key,
                    base_url="https://ws-uaeaan6mql1ieioa.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
                )
                prompt = f"你是一名专业的股票分析师，请查询美股'{symbol}'最新财报信息，请分析公司财务状况(包括营收增长，现金流等)，利好消息，管理层动作，100字以内。"
                response = client.responses.create(
                    model="qwen3.7-plus",
                    input=prompt,
                    tools=[
                        {"type": "web_search"},
                        {"type": "code_interpreter"},
                        {"type": "web_extractor"},
                    ],
                )
                ai_result = response.output_text.strip()
            except Exception as e:
                ai_result = f"AI分析调用异常：{str(e)}"

            summary_content = f"""股票代码：{symbol} 股票名称：{stock_name.strip("88+")} {ai_result}"""
            return summary_content

        self._callback_registered = True
